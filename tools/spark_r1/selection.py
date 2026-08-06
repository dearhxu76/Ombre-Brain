"""只消费 `EligibleSnapshot` 的确定性 Spark R1 选择器。"""

from __future__ import annotations

import random
import re

from .models import (
    CHANNELS,
    MAX_EVENTS,
    STRUCTURE_LEVELS,
    CueEvidence,
    SelectedMaterial,
    SparkChannelError,
    SparkInputError,
    SparkRequest,
    StructureEvidence,
    normalize_structure_value,
)
from .policy import EligibleSnapshot, EligibleSource


_WORD_TOKEN_RE = re.compile(r"[^\W_]+", re.UNICODE)
_CJK_SEGMENT_RE = re.compile(r"[\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff]+")
_STRUCTURE_WEIGHTS = {
    "entities": 0.15,
    "roles_actions": 0.35,
    "causal_constraints": 0.30,
    "transformations_outcomes": 0.20,
}
_MAX_EXCERPT_CHARS = 1_200
_MAX_STRUCTURAL_LEXICAL_OVERLAP = 0.35
_DISTANCE_MATCH_CALIPER = 0.05
_UNVERIFIED_ASSOCIATION = "候选关联仍需当前模型独立核验"


def _normalized(value: str) -> str:
    return normalize_structure_value(value)


def _is_cjk(char: str) -> bool:
    codepoint = ord(char)
    return (
        0x3400 <= codepoint <= 0x4DBF
        or 0x4E00 <= codepoint <= 0x9FFF
        or 0xF900 <= codepoint <= 0xFAFF
    )


def lexical_tokens(value: str) -> frozenset[str]:
    """用于排序的本地词元；不改变原文、哈希或引用跨度。"""

    normalized = _normalized(value)
    tokens = {
        token
        for token in _WORD_TOKEN_RE.findall(normalized)
        if not any(_is_cjk(char) for char in token)
    }
    for segment in _CJK_SEGMENT_RE.findall(normalized):
        tokens.update(segment)
        tokens.update(
            segment[index : index + 2]
            for index in range(max(0, len(segment) - 1))
        )
    return frozenset(token for token in tokens if token)


def lexical_overlap(left: str, right: str) -> float:
    left_tokens = lexical_tokens(left)
    right_tokens = lexical_tokens(right)
    if not left_tokens or not right_tokens:
        return 0.0
    return len(left_tokens & right_tokens) / len(left_tokens | right_tokens)


def _lexical_token_spans(value: str) -> tuple[tuple[str, int, int], ...]:
    """把用于 direct 的本地词元保守映射回未规范化原文跨度。"""

    spans: set[tuple[str, int, int]] = set()
    for match in _WORD_TOKEN_RE.finditer(value):
        raw = match.group(0)
        if any(_is_cjk(char) for char in raw):
            continue
        for token in _WORD_TOKEN_RE.findall(_normalized(raw)):
            if token and not any(_is_cjk(char) for char in token):
                spans.add((token, match.start(), match.end()))

    for match in _CJK_SEGMENT_RE.finditer(value):
        segment = match.group(0)
        for index in range(len(segment)):
            for width in (1, 2):
                end_index = index + width
                if end_index > len(segment):
                    continue
                raw = segment[index:end_index]
                normalized = _normalized(raw)
                for token in lexical_tokens(normalized):
                    if token and all(_is_cjk(char) for char in token):
                        spans.add(
                            (
                                token,
                                match.start() + index,
                                match.start() + end_index,
                            )
                        )
    return tuple(sorted(spans, key=lambda item: (item[1], item[2], item[0])))


def _lexical_hit_span(request: SparkRequest, source: EligibleSource) -> tuple[int, int] | None:
    request_tokens = lexical_tokens(request.tension)
    hits = [
        item
        for item in _lexical_token_spans(source.text)
        if item[0] in request_tokens
        and item[2] - item[1] <= _MAX_EXCERPT_CHARS
    ]
    if not hits:
        return None
    _, start, end = min(
        hits,
        key=lambda item: (
            -(item[2] - item[1]),
            -len(item[0]),
            item[1],
            item[2],
            item[0],
        ),
    )
    return start, end


def _normalized_structure_values(values: tuple[str, ...]) -> frozenset[str]:
    return frozenset(_normalized(value) for value in values if _normalized(value))


def _level_overlap(request: SparkRequest, source: EligibleSource, level: str) -> float:
    tension_values = _normalized_structure_values(request.tension_structure.values(level))
    source_values = _normalized_structure_values(source.structure.values(level))
    if not tension_values or not source_values:
        return 0.0
    return len(tension_values & source_values) / len(tension_values | source_values)


def structural_fit(request: SparkRequest, source: EligibleSource) -> float:
    return sum(
        _STRUCTURE_WEIGHTS[level] * _level_overlap(request, source, level)
        for level in STRUCTURE_LEVELS
    )


def _matching_structure_cues(
    request: SparkRequest,
    source: EligibleSource,
) -> tuple[tuple[str, CueEvidence], ...]:
    matches: list[tuple[str, CueEvidence]] = []
    for level in STRUCTURE_LEVELS:
        tension_keys = _normalized_structure_values(
            request.tension_structure.values(level)
        )
        if not tension_keys:
            continue
        matches.extend(
            (level, cue)
            for cue in source.structure.__getattribute__(level)
            if _normalized(cue.value) in tension_keys
        )
    return tuple(
        sorted(
            matches,
            key=lambda item: (
                STRUCTURE_LEVELS.index(item[0]),
                item[1].start,
                item[1].end,
                _normalized(item[1].value),
            ),
        )
    )


def _bounded_window(text_length: int, evidence_start: int, evidence_end: int) -> tuple[int, int]:
    width = min(text_length, _MAX_EXCERPT_CHARS)
    if text_length <= width:
        return 0, text_length
    midpoint = evidence_start + ((evidence_end - evidence_start) // 2)
    start = max(0, midpoint - (width // 2))
    start = min(start, text_length - width)
    if evidence_end > start + width:
        start = min(max(0, evidence_end - width), text_length - width)
    return start, start + width


def _structure_window(
    source: EligibleSource,
    matches: tuple[tuple[str, CueEvidence], ...],
) -> tuple[int, int]:
    if not matches:
        return 0, min(len(source.text), _MAX_EXCERPT_CHARS)
    width = min(len(source.text), _MAX_EXCERPT_CHARS)
    maximum_start = len(source.text) - width
    candidate_starts: set[int] = set()
    for _, cue in matches:
        centered_start, _ = _bounded_window(
            len(source.text),
            cue.start,
            cue.end,
        )
        candidate_starts.update(
            {
                centered_start,
                min(max(0, cue.start), maximum_start),
                min(max(0, cue.end - width), maximum_start),
            }
        )
    candidates = {
        (start, start + width)
        for start in candidate_starts
    }

    def window_score(span: tuple[int, int]) -> tuple[int, float, int]:
        start, end = span
        contained = [
            (level, cue)
            for level, cue in matches
            if cue.start >= start and cue.end <= end
        ]
        unique = {
            (level, _normalized(cue.value))
            for level, cue in contained
        }
        weighted = sum(_STRUCTURE_WEIGHTS[level] for level, _ in unique)
        return len(unique), weighted, -start

    return max(candidates, key=window_score)


def _evidence_within(
    source: EligibleSource,
    matches: tuple[tuple[str, CueEvidence], ...],
    span: tuple[int, int],
) -> tuple[StructureEvidence, ...]:
    start, end = span
    evidence: list[StructureEvidence] = []
    seen: set[tuple[str, str, int, int]] = set()
    for level, cue in matches:
        if cue.start < start or cue.end > end:
            continue
        key = (level, cue.value, cue.start, cue.end)
        if key in seen:
            continue
        seen.add(key)
        evidence.append(
            StructureEvidence(
                level=level,
                value=cue.value,
                source_hash=source.content_hash,
                start=cue.start,
                end=cue.end,
                extractor=source.structure.extractor,
                prompt_version=source.structure.prompt_version,
            )
        )
    return tuple(evidence)


def _shared_structure_from_evidence(
    request: SparkRequest,
    evidence: tuple[StructureEvidence, ...],
) -> tuple[str, ...]:
    evidence_keys = {
        (item.level, _normalized(item.value))
        for item in evidence
    }
    shared: list[str] = []
    seen: set[str] = set()
    for level in STRUCTURE_LEVELS:
        for value in request.tension_structure.values(level):
            normalized = _normalized(value)
            if (level, normalized) not in evidence_keys or normalized in seen:
                continue
            shared.append(value)
            seen.add(normalized)
    return tuple(shared)


def _mismatch(channel: str, request: SparkRequest, source: EligibleSource) -> str:
    if channel == "counterexample":
        return "角色或约束可能相似，但结果方向与适用条件可能相反。"
    if channel == "structural_distant":
        if _level_overlap(request, source, "transformations_outcomes") == 0.0:
            return "共享关系不保证转化结果或因果方向相同。"
        return "共享关系不表示两个材料属于同一事件或具有相同事实前提。"
    if channel.endswith("_control"):
        return "这段材料与当前张力的关联尚未建立，不能据此推断事实或因果。"
    return "表面词汇关联不表示两个材料属于同一事件或应得出相同结论。"


def _to_material(
    request: SparkRequest,
    source: EligibleSource,
    channel: str,
) -> SelectedMaterial:
    lexical_score = lexical_overlap(request.tension, source.text)
    structure_score = structural_fit(request, source)

    if channel == "direct":
        hit = _lexical_hit_span(request, source)
        span = (
            _bounded_window(len(source.text), *hit)
            if hit is not None
            else (0, min(len(source.text), _MAX_EXCERPT_CHARS))
        )
        # direct 是词汇基线；结构投影不得污染其生成视图或响应证据。
        evidence = ()
    elif channel in {"structural_distant", "counterexample"}:
        matches = _matching_structure_cues(request, source)
        span = _structure_window(source, matches)
        evidence = _evidence_within(source, matches, span)
    else:
        # 对照通道先冻结抽样，再计算诊断轴；引用窗口不由诊断分数改变。
        span = (0, min(len(source.text), _MAX_EXCERPT_CHARS))
        evidence = ()

    shared = (
        _shared_structure_from_evidence(request, evidence)
        if channel in {"structural_distant", "counterexample"}
        else ()
    )
    if not shared:
        shared = (_UNVERIFIED_ASSOCIATION,)
    start, end = span
    return SelectedMaterial(
        source_id=source.source_id,
        source_hash=source.content_hash,
        text=source.text,
        span_start=start,
        span_end=end,
        shared_structure=shared,
        structure_evidence=evidence,
        mismatch=_mismatch(channel, request, source),
        lexical_overlap=lexical_score,
        structural_fit=structure_score,
    )


def _require_structural_tension(request: SparkRequest) -> None:
    if not any(request.tension_structure.values(level) for level in STRUCTURE_LEVELS):
        raise SparkChannelError("structural channel requires explicit synthetic tension structure")


def _select_direct(
    snapshot: EligibleSnapshot,
    request: SparkRequest,
    limit: int,
) -> tuple[SelectedMaterial, ...]:
    ranked = [
        (
            lexical_overlap(request.tension, source.text),
            _lexical_hit_span(request, source),
            source,
        )
        for source in snapshot.sources
    ]
    ranked = [
        item
        for item in ranked
        if item[0] > 0.0
        and item[1] is not None
    ]
    ranked.sort(key=lambda item: (-item[0], item[2].source_id))
    return tuple(
        _to_material(request, source, "direct")
        for _, _, source in ranked[:limit]
    )


def _select_structural_distant(
    snapshot: EligibleSnapshot,
    request: SparkRequest,
    limit: int,
) -> tuple[SelectedMaterial, ...]:
    _require_structural_tension(request)
    ranked = [
        (
            structural_fit(request, source),
            lexical_overlap(request.tension, source.text),
            source,
        )
        for source in snapshot.sources
    ]
    ranked = [
        item
        for item in ranked
        if item[0] > 0.0 and item[1] <= _MAX_STRUCTURAL_LEXICAL_OVERLAP
    ]
    ranked.sort(key=lambda item: (-item[0], item[1], item[2].source_id))
    return tuple(
        _to_material(request, source, "structural_distant")
        for _, _, source in ranked[:limit]
    )


def _select_counterexample(
    snapshot: EligibleSnapshot,
    request: SparkRequest,
    limit: int,
) -> tuple[SelectedMaterial, ...]:
    _require_structural_tension(request)
    if not request.tension_structure.transformations_outcomes:
        raise SparkChannelError("counterexample channel requires an explicit synthetic outcome")
    ranked: list[tuple[float, float, str, EligibleSource]] = []
    for source in snapshot.sources:
        mechanism_fit = sum(
            _STRUCTURE_WEIGHTS[level] * _level_overlap(request, source, level)
            for level in ("roles_actions", "causal_constraints")
        )
        entity_overlap = _level_overlap(request, source, "entities")
        outcome_overlap = _level_overlap(request, source, "transformations_outcomes")
        source_outcomes = source.structure.transformations_outcomes
        if mechanism_fit > 0.0 and source_outcomes and outcome_overlap == 0.0:
            ranked.append((mechanism_fit, entity_overlap, source.source_id, source))
    ranked.sort(key=lambda item: (-item[0], -item[1], item[2]))
    return tuple(
        _to_material(request, source, "counterexample")
        for _, _, _, source in ranked[:limit]
    )


def _select_uniform_random(
    snapshot: EligibleSnapshot,
    request: SparkRequest,
    limit: int,
) -> tuple[SelectedMaterial, ...]:
    pool = list(snapshot.sources)
    if not pool:
        return ()
    rng = random.Random(request.seed)
    # 抽样结果在任何 lexical/structural 诊断之前冻结，诊断值不能反馈进抽样。
    selected = tuple(rng.sample(pool, k=min(limit, len(pool))))
    return tuple(
        _to_material(request, source, "uniform_random_control")
        for source in selected
    )


def _select_distance_matched_random(
    snapshot: EligibleSnapshot,
    request: SparkRequest,
    limit: int,
) -> tuple[SelectedMaterial, ...]:
    _require_structural_tension(request)
    if not snapshot.sources:
        return ()
    structural_reference = _select_structural_distant(snapshot, request, 1)
    if not structural_reference:
        return ()
    target = structural_reference[0].lexical_overlap
    reference_id = structural_reference[0].source_id
    matched = sorted(
        (
            source
            for source in snapshot.sources
            if source.source_id != reference_id
            and abs(lexical_overlap(request.tension, source.text) - target)
            <= _DISTANCE_MATCH_CALIPER
        ),
        key=lambda source: source.source_id,
    )
    if not matched:
        return ()
    rng = random.Random(request.seed)
    selected = tuple(rng.sample(matched, k=min(limit, len(matched))))
    return tuple(
        _to_material(request, source, "distance_matched_random_control")
        for source in selected
    )


def select_channel(
    snapshot: EligibleSnapshot,
    request: SparkRequest,
    channel: str,
    *,
    limit: int,
) -> tuple[SelectedMaterial, ...]:
    """执行一个固定通道；不存在普通 search 或未过滤池回退。"""

    if not isinstance(snapshot, EligibleSnapshot):
        raise SparkInputError("selector requires an EligibleSnapshot")
    if channel not in CHANNELS:
        raise SparkInputError("selector channel is unsupported")
    if type(limit) is not int or limit < 1 or limit > MAX_EVENTS:
        raise SparkInputError(f"selector limit must be an integer in [1, {MAX_EVENTS}]")
    selectors = {
        "direct": _select_direct,
        "structural_distant": _select_structural_distant,
        "counterexample": _select_counterexample,
        "uniform_random_control": _select_uniform_random,
        "distance_matched_random_control": _select_distance_matched_random,
    }
    return selectors[channel](snapshot, request, limit)
