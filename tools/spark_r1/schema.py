"""Spark R1 合成场景的严格 JSON 解析。

解析器拒绝重复键、NaN/Inf、过深结构、未知字段和宽松类型转换。它只把调用方
数据复制为不可变契约对象，不保留原始 dict/list 引用。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
from typing import Any

from .models import (
    CueEvidence,
    SCHEMA_VERSION,
    SparkInputError,
    SparkRequest,
    StructuralProjection,
    SyntheticEvent,
    SyntheticSnapshot,
    TensionStructure,
)


MAX_INPUT_BYTES = 1_500_000
MAX_JSON_DEPTH = 12
MAX_JSON_NODES = 10_000

_ROOT_FIELDS = frozenset(
    {
        "schema_version",
        "synthetic",
        "boundary_id",
        "data_cutoff",
        "tension",
        "tension_structure",
        "channels",
        "seed",
        "inspiration_requested",
        "max_candidates",
        "risk_domain",
        "events",
    }
)
_EVENT_FIELDS = frozenset(
    {
        "source_id",
        "text",
        "event_type",
        "status",
        "visibility",
        "resolved",
        "anchor",
        "dont_surface",
        "digested",
        "pinned",
        "protected",
        "permanent",
        "structure",
    }
)
_STRUCTURE_FIELDS = frozenset(
    {
        "source_hash",
        "extractor",
        "prompt_version",
        "entities",
        "roles_actions",
        "causal_constraints",
        "transformations_outcomes",
    }
)
_TENSION_STRUCTURE_FIELDS = frozenset(
    {
        "entities",
        "roles_actions",
        "causal_constraints",
        "transformations_outcomes",
    }
)
_CUE_FIELDS = frozenset({"value", "start", "end"})


@dataclass(frozen=True, slots=True)
class ParsedScenario:
    """一次已验证、与原始 JSON 深度分离的合成场景。"""

    snapshot: SyntheticSnapshot
    request: SparkRequest


def _reject_constant(_value: str) -> None:
    raise SparkInputError("JSON constants NaN and Infinity are not allowed")


def _object_without_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise SparkInputError("JSON contains a duplicate object key")
        result[key] = value
    return result


def _validate_json_tree(root: Any) -> None:
    node_count = 0
    stack: list[tuple[Any, int]] = [(root, 1)]
    while stack:
        value, depth = stack.pop()
        node_count += 1
        if node_count > MAX_JSON_NODES:
            raise SparkInputError("JSON exceeds the node budget")
        if depth > MAX_JSON_DEPTH:
            raise SparkInputError("JSON exceeds the nesting depth budget")
        if isinstance(value, dict):
            stack.extend((item, depth + 1) for item in value.values())
        elif isinstance(value, list):
            stack.extend((item, depth + 1) for item in value)
        elif value is not None and not isinstance(value, (str, int, float, bool)):
            raise SparkInputError("JSON contains an unsupported value type")


def strict_json_loads(payload: str | bytes) -> dict[str, Any]:
    """有界解析 JSON，并返回普通 dict；不做任何字段默认或类型修复。"""

    if isinstance(payload, str):
        try:
            encoded = payload.encode("utf-8", errors="strict")
        except UnicodeEncodeError as exc:
            raise SparkInputError("JSON contains invalid Unicode") from exc
    elif isinstance(payload, bytes):
        encoded = payload
    else:
        raise SparkInputError("scenario input must be UTF-8 text or bytes")
    if not encoded or len(encoded) > MAX_INPUT_BYTES:
        raise SparkInputError("scenario input size is outside the allowed range")
    try:
        text = encoded.decode("utf-8-sig", errors="strict")
        parsed = json.loads(
            text,
            parse_constant=_reject_constant,
            object_pairs_hook=_object_without_duplicates,
        )
    except SparkInputError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as exc:
        raise SparkInputError("scenario input is not valid bounded UTF-8 JSON") from exc
    _validate_json_tree(parsed)
    if type(parsed) is not dict:
        raise SparkInputError("scenario root must be an object")
    return parsed


def _validate_dict_serialization_budget(payload: dict[str, Any]) -> None:
    """按严格 JSON UTF-8 表示校验 dict，且不在异常中回显调用方数据。"""

    try:
        encoded = json.dumps(
            payload,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8", errors="strict")
    except (TypeError, ValueError, UnicodeEncodeError, RecursionError):
        raise SparkInputError(
            "scenario object is not strict serializable UTF-8 JSON"
        ) from None
    if not encoded or len(encoded) > MAX_INPUT_BYTES:
        raise SparkInputError("scenario input size is outside the allowed range")


def _require_exact_object(value: Any, expected: frozenset[str], label: str) -> dict[str, Any]:
    if type(value) is not dict:
        raise SparkInputError(f"{label} must be an object")
    if set(value) != expected:
        raise SparkInputError(f"{label} must contain exactly the documented fields")
    return value


def _require_list(value: Any, label: str) -> list[Any]:
    if type(value) is not list:
        raise SparkInputError(f"{label} must be an array")
    return value


def _require_aware_iso8601(value: Any) -> str:
    if not isinstance(value, str) or not value or len(value) > 64:
        raise SparkInputError("data_cutoff must be a bounded ISO-8601 string")
    candidate = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError as exc:
        raise SparkInputError("data_cutoff must be a valid ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise SparkInputError("data_cutoff must include a timezone")
    return value


def _parse_cues(raw: Any, label: str) -> tuple[CueEvidence, ...]:
    values = _require_list(raw, label)
    cues: list[CueEvidence] = []
    for index, item in enumerate(values):
        cue = _require_exact_object(item, _CUE_FIELDS, f"{label}[{index}]")
        cues.append(
            CueEvidence(
                value=cue["value"],
                start=cue["start"],
                end=cue["end"],
            )
        )
    return tuple(cues)


def _parse_structure(raw: Any, label: str) -> StructuralProjection:
    structure = _require_exact_object(raw, _STRUCTURE_FIELDS, label)
    return StructuralProjection(
        source_hash=structure["source_hash"],
        extractor=structure["extractor"],
        prompt_version=structure["prompt_version"],
        entities=_parse_cues(structure["entities"], f"{label}.entities"),
        roles_actions=_parse_cues(structure["roles_actions"], f"{label}.roles_actions"),
        causal_constraints=_parse_cues(
            structure["causal_constraints"],
            f"{label}.causal_constraints",
        ),
        transformations_outcomes=_parse_cues(
            structure["transformations_outcomes"],
            f"{label}.transformations_outcomes",
        ),
    )


def _parse_tension_structure(raw: Any) -> TensionStructure:
    structure = _require_exact_object(
        raw,
        _TENSION_STRUCTURE_FIELDS,
        "tension_structure",
    )
    return TensionStructure(
        **{
            level: tuple(_require_list(structure[level], f"tension_structure.{level}"))
            for level in _TENSION_STRUCTURE_FIELDS
        }
    )


def _parse_event(raw: Any, index: int) -> SyntheticEvent:
    event = _require_exact_object(raw, _EVENT_FIELDS, f"events[{index}]")
    return SyntheticEvent(
        source_id=event["source_id"],
        text=event["text"],
        event_type=event["event_type"],
        status=event["status"],
        visibility=event["visibility"],
        resolved=event["resolved"],
        anchor=event["anchor"],
        dont_surface=event["dont_surface"],
        digested=event["digested"],
        pinned=event["pinned"],
        protected=event["protected"],
        permanent=event["permanent"],
        structure=_parse_structure(event["structure"], f"events[{index}].structure"),
    )


def parse_scenario(payload: dict[str, Any] | str | bytes) -> ParsedScenario:
    """解析合成场景；未知 owner/path/vault 字段会被根 schema 直接拒绝。"""

    if isinstance(payload, (str, bytes)):
        raw = strict_json_loads(payload)
    elif type(payload) is dict:
        _validate_json_tree(payload)
        _validate_dict_serialization_budget(payload)
        raw = payload
    else:
        raise SparkInputError("scenario must be an object or bounded UTF-8 JSON")
    root = _require_exact_object(raw, _ROOT_FIELDS, "scenario")
    if type(root["schema_version"]) is not int or root["schema_version"] != SCHEMA_VERSION:
        raise SparkInputError("scenario schema_version is unsupported")
    if root["synthetic"] is not True:
        raise SparkInputError("scenario must be explicitly marked synthetic")
    events_raw = _require_list(root["events"], "events")
    snapshot = SyntheticSnapshot(
        schema_version=root["schema_version"],
        synthetic=root["synthetic"],
        boundary_id=root["boundary_id"],
        data_cutoff=_require_aware_iso8601(root["data_cutoff"]),
        events=tuple(_parse_event(event, index) for index, event in enumerate(events_raw)),
    )
    channels = tuple(_require_list(root["channels"], "channels"))
    request = SparkRequest(
        tension=root["tension"],
        tension_structure=_parse_tension_structure(root["tension_structure"]),
        channels=channels,
        seed=root["seed"],
        inspiration_requested=root["inspiration_requested"],
        max_candidates=root["max_candidates"],
        risk_domain=root["risk_domain"],
    )
    return ParsedScenario(snapshot=snapshot, request=request)
