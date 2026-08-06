"""Spark R1 的不可变合成数据契约。

本模块只描述离线研究对象，不导入 Ombre Brain 运行时，也不接受真实 vault、
owner、BucketManager 或任何可写资源。所有正文都被视为不可信存储数据。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
import math
import re
from typing import Any
import unicodedata


SCHEMA_VERSION = 1
MAX_EVENTS = 256
MAX_EVENT_TEXT_CHARS = 8_000
MAX_TOTAL_TEXT_CHARS = 500_000
MAX_TENSION_CHARS = 4_000
MAX_CUES_PER_LEVEL = 64
MAX_CUE_CHARS = 200
MAX_CANDIDATES = 3
MAX_SEED = (1 << 63) - 1

CHANNELS = frozenset(
    {
        "direct",
        "structural_distant",
        "counterexample",
        "uniform_random_control",
        "distance_matched_random_control",
    }
)
RISK_DOMAINS = frozenset({"general", "medical", "legal", "financial"})
KNOWN_EVENT_TYPES = frozenset(
    {"dynamic", "permanent", "feel", "plan", "letter", "i", "I", "self", "anchor"}
)
KNOWN_STATUSES = frozenset(
    {"active", "resolved", "abandoned", "archived", "deleted", "tombstone"}
)
KNOWN_VISIBILITIES = frozenset({"normal", "hidden", "private"})
STRUCTURE_LEVELS = (
    "entities",
    "roles_actions",
    "causal_constraints",
    "transformations_outcomes",
)

_SAFE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_INSTRUMENT_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/+-]{0,127}$")
_SAFE_PARAMETER_KEYS = frozenset(
    {
        "max_tokens",
        "output_mode",
        "seed",
        "temperature",
        "timeout_seconds",
        "top_p",
    }
)


class SparkError(ValueError):
    """R1 对外可安全归类的基础错误。"""

    code = "spark_error"

    def to_dict(self) -> dict[str, str]:
        return {"error": self.code, "message": str(self)}


class SparkInputError(SparkError):
    """合成输入不满足严格 schema 或资源边界。"""

    code = "spark_input_invalid"


class SparkPolicyError(SparkError):
    """策略字段未知或非法，必须整次失败关闭。"""

    code = "spark_policy_invalid"


class SparkBoundaryError(SparkError):
    """合成场景边界不一致。"""

    code = "spark_boundary_mismatch"


class SparkIntegrityError(SparkError):
    """来源 ID、哈希、跨度或候选来源失效。"""

    code = "spark_source_integrity_failed"


class SparkChannelError(SparkError):
    """某个互相独立的生成或评审通道失败。"""

    code = "spark_channel_failed"


class SparkInstrumentError(SparkError):
    """非超时的实验仪器或协议失败，必须整次失败。"""

    code = "spark_instrument_failed"


def source_hash(text: str) -> str:
    """按 ADR 约定对精确 UTF-8 bytes 计算 SHA-256。"""

    if not isinstance(text, str):
        raise SparkInputError("source text must be a string")
    try:
        encoded = text.encode("utf-8", errors="strict")
    except UnicodeEncodeError as exc:
        raise SparkInputError("source text contains invalid Unicode") from exc
    return sha256(encoded).hexdigest()


def normalize_structure_value(value: str) -> str:
    """结构比较和结构族去重共用的 NFKC/casefold 规范化。"""

    return " ".join(unicodedata.normalize("NFKC", value).casefold().split())


def _require_plain_int(value: Any, field_name: str, *, minimum: int, maximum: int) -> int:
    if type(value) is not int or value < minimum or value > maximum:
        raise SparkInputError(f"{field_name} must be an integer in [{minimum}, {maximum}]")
    return value


def _require_plain_bool(value: Any, field_name: str) -> bool:
    if type(value) is not bool:
        raise SparkPolicyError(f"{field_name} must be a boolean")
    return value


def _require_text(
    value: Any,
    field_name: str,
    *,
    maximum: int,
    allow_newlines: bool = True,
) -> str:
    if not isinstance(value, str):
        raise SparkInputError(f"{field_name} must be a string")
    if not value or len(value) > maximum:
        raise SparkInputError(f"{field_name} length is outside the allowed range")
    if not value.strip():
        raise SparkInputError(f"{field_name} must not be blank")
    try:
        value.encode("utf-8", errors="strict")
    except UnicodeEncodeError as exc:
        raise SparkInputError(f"{field_name} contains invalid Unicode") from exc
    if "\x00" in value:
        raise SparkInputError(f"{field_name} contains a NUL character")
    if not allow_newlines and any(char in value for char in ("\r", "\n")):
        raise SparkInputError(f"{field_name} must be a single line")
    return value


def _require_safe_id(value: Any, field_name: str) -> str:
    text = _require_text(value, field_name, maximum=128, allow_newlines=False)
    if not _SAFE_ID_RE.fullmatch(text):
        raise SparkInputError(f"{field_name} contains unsupported characters")
    return text


def _require_sha256(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not _SHA256_RE.fullmatch(value):
        raise SparkInputError(f"{field_name} must be a lowercase SHA-256 hex digest")
    return value


def _require_instrument_id(value: Any, field_name: str) -> str:
    text = _require_text(value, field_name, maximum=128, allow_newlines=False)
    if not _INSTRUMENT_ID_RE.fullmatch(text):
        raise SparkInputError(f"{field_name} must be a bounded instrument identifier")
    lowered = text.casefold()
    if lowered.startswith(("sk-", "bearer", "token-", "secret-", "password-")):
        raise SparkInputError(f"{field_name} resembles a credential")
    return text


def _validate_instrument_parameter(key: str, value: str) -> str:
    if value != value.strip():
        raise SparkInputError("instrument parameter value must not contain outer whitespace")
    if key == "output_mode":
        if value != "material_question":
            raise SparkInputError("instrument output_mode is unsupported in R1")
        return value
    if key in {"max_tokens", "seed"}:
        if not value.isascii() or not value.isdecimal():
            raise SparkInputError(f"instrument parameter {key} must be a decimal integer")
        number = int(value)
        maximum = 1_000_000 if key == "max_tokens" else MAX_SEED
        if number < 0 or number > maximum or (key == "max_tokens" and number == 0):
            raise SparkInputError(f"instrument parameter {key} is outside the allowed range")
        return value
    try:
        number = float(value)
    except ValueError as exc:
        raise SparkInputError(f"instrument parameter {key} must be numeric") from exc
    bounds = {
        "temperature": (0.0, 2.0),
        "timeout_seconds": (0.05, 120.0),
        "top_p": (0.0, 1.0),
    }
    minimum, maximum = bounds[key]
    if not math.isfinite(number) or number < minimum or number > maximum:
        raise SparkInputError(f"instrument parameter {key} is outside the allowed range")
    return value


def _require_aware_iso8601(value: Any, field_name: str) -> str:
    text = _require_text(value, field_name, maximum=64, allow_newlines=False)
    candidate = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError as exc:
        raise SparkInputError(f"{field_name} must be a valid ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise SparkInputError(f"{field_name} must include a timezone")
    return text


def _tuple_of_strings(values: Any, field_name: str) -> tuple[str, ...]:
    if not isinstance(values, tuple):
        raise SparkInputError(f"{field_name} must be an immutable tuple")
    if len(values) > MAX_CUES_PER_LEVEL:
        raise SparkInputError(f"{field_name} contains too many cues")
    normalized: list[str] = []
    for index, value in enumerate(values):
        text = _require_text(
            value,
            f"{field_name}[{index}]",
            maximum=MAX_CUE_CHARS,
            allow_newlines=False,
        ).strip()
        if not text:
            raise SparkInputError(f"{field_name}[{index}] is empty")
        normalized.append(text)
    if len(set(normalized)) != len(normalized):
        raise SparkInputError(f"{field_name} contains duplicate cues")
    return tuple(normalized)


@dataclass(frozen=True, slots=True)
class CueEvidence:
    """结构提示及其在精确源文本中的 0-based 半开跨度。"""

    value: str
    start: int
    end: int

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "value",
            _require_text(self.value, "cue.value", maximum=MAX_CUE_CHARS, allow_newlines=False).strip(),
        )
        _require_plain_int(self.start, "cue.start", minimum=0, maximum=MAX_EVENT_TEXT_CHARS)
        _require_plain_int(self.end, "cue.end", minimum=1, maximum=MAX_EVENT_TEXT_CHARS)
        if self.end <= self.start:
            raise SparkInputError("cue span must be a non-empty half-open interval")

    def to_dict(self) -> dict[str, Any]:
        return {"value": self.value, "start": self.start, "end": self.end}


def _tuple_of_cues(values: Any, field_name: str) -> tuple[CueEvidence, ...]:
    if not isinstance(values, tuple):
        raise SparkInputError(f"{field_name} must be an immutable tuple")
    if len(values) > MAX_CUES_PER_LEVEL:
        raise SparkInputError(f"{field_name} contains too many cues")
    for item in values:
        if not isinstance(item, CueEvidence):
            raise SparkInputError(f"{field_name} contains an invalid cue")
    return values


@dataclass(frozen=True, slots=True)
class StructuralProjection:
    """可重建、可出错且带来源的四层结构投影。"""

    source_hash: str
    extractor: str
    prompt_version: str
    entities: tuple[CueEvidence, ...] = ()
    roles_actions: tuple[CueEvidence, ...] = ()
    causal_constraints: tuple[CueEvidence, ...] = ()
    transformations_outcomes: tuple[CueEvidence, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "source_hash", _require_sha256(self.source_hash, "structure.source_hash"))
        object.__setattr__(
            self,
            "extractor",
            _require_instrument_id(self.extractor, "structure.extractor"),
        )
        object.__setattr__(
            self,
            "prompt_version",
            _require_instrument_id(
                self.prompt_version,
                "structure.prompt_version",
            ),
        )
        for level in STRUCTURE_LEVELS:
            object.__setattr__(self, level, _tuple_of_cues(getattr(self, level), f"structure.{level}"))

    def values(self, level: str) -> tuple[str, ...]:
        if level not in STRUCTURE_LEVELS:
            raise SparkInputError("unknown structure level")
        return tuple(cue.value for cue in getattr(self, level))

    def family_key(self) -> tuple[tuple[str, ...], ...]:
        return tuple(
            tuple(
                sorted(
                    {
                        normalize_structure_value(cue.value)
                        for cue in getattr(self, level)
                    }
                )
            )
            for level in STRUCTURE_LEVELS
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_hash": self.source_hash,
            "extractor": self.extractor,
            "prompt_version": self.prompt_version,
            **{
                level: [cue.to_dict() for cue in getattr(self, level)]
                for level in STRUCTURE_LEVELS
            },
        }


@dataclass(frozen=True, slots=True)
class TensionStructure:
    """测试场景显式给出的当前张力结构，不是 OB 记忆。"""

    entities: tuple[str, ...] = ()
    roles_actions: tuple[str, ...] = ()
    causal_constraints: tuple[str, ...] = ()
    transformations_outcomes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for level in STRUCTURE_LEVELS:
            object.__setattr__(
                self,
                level,
                _tuple_of_strings(getattr(self, level), f"tension_structure.{level}"),
            )

    def values(self, level: str) -> tuple[str, ...]:
        if level not in STRUCTURE_LEVELS:
            raise SparkInputError("unknown tension structure level")
        return getattr(self, level)

    def to_dict(self) -> dict[str, list[str]]:
        return {level: list(getattr(self, level)) for level in STRUCTURE_LEVELS}


@dataclass(frozen=True, slots=True)
class SyntheticEvent:
    """单条合成事件；所有策略字段均显式存在，不使用宽松默认值。"""

    source_id: str
    text: str
    event_type: str
    status: str
    visibility: str
    resolved: bool
    anchor: bool
    dont_surface: bool
    digested: bool
    pinned: bool
    protected: bool
    permanent: bool
    structure: StructuralProjection

    def __post_init__(self) -> None:
        object.__setattr__(self, "source_id", _require_safe_id(self.source_id, "source_id"))
        object.__setattr__(
            self,
            "text",
            _require_text(self.text, "event.text", maximum=MAX_EVENT_TEXT_CHARS),
        )
        object.__setattr__(
            self,
            "event_type",
            _require_text(
                self.event_type,
                "event.event_type",
                maximum=32,
                allow_newlines=False,
            ),
        )
        object.__setattr__(
            self,
            "status",
            _require_text(self.status, "event.status", maximum=32, allow_newlines=False),
        )
        object.__setattr__(
            self,
            "visibility",
            _require_text(
                self.visibility,
                "event.visibility",
                maximum=32,
                allow_newlines=False,
            ),
        )
        for field_name in (
            "resolved",
            "anchor",
            "dont_surface",
            "digested",
            "pinned",
            "protected",
            "permanent",
        ):
            object.__setattr__(
                self,
                field_name,
                _require_plain_bool(getattr(self, field_name), f"event.{field_name}"),
            )
        if not isinstance(self.structure, StructuralProjection):
            raise SparkInputError("event.structure must be a StructuralProjection")
        expected_hash = source_hash(self.text)
        if self.structure.source_hash != expected_hash:
            raise SparkIntegrityError("structure source hash does not match the event text")
        for level in STRUCTURE_LEVELS:
            for cue in getattr(self.structure, level):
                if cue.end > len(self.text):
                    raise SparkIntegrityError("structure cue span exceeds the event text")
                if not self.text[cue.start : cue.end].strip():
                    raise SparkIntegrityError("structure cue span must contain non-whitespace source text")

    @property
    def content_hash(self) -> str:
        return source_hash(self.text)


@dataclass(frozen=True, slots=True)
class SyntheticSnapshot:
    """单个合成边界内的冻结事件快照。"""

    boundary_id: str
    data_cutoff: str
    events: tuple[SyntheticEvent, ...]
    schema_version: int = SCHEMA_VERSION
    synthetic: bool = True

    def __post_init__(self) -> None:
        _require_plain_int(
            self.schema_version,
            "schema_version",
            minimum=SCHEMA_VERSION,
            maximum=SCHEMA_VERSION,
        )
        if self.synthetic is not True:
            raise SparkBoundaryError("snapshot must be explicitly marked synthetic")
        boundary_id = _require_safe_id(self.boundary_id, "boundary_id")
        if not boundary_id.startswith("synthetic:"):
            raise SparkBoundaryError("boundary_id must use the synthetic: namespace")
        object.__setattr__(self, "boundary_id", boundary_id)
        object.__setattr__(
            self,
            "data_cutoff",
            _require_aware_iso8601(self.data_cutoff, "data_cutoff"),
        )
        if not isinstance(self.events, tuple) or not self.events or len(self.events) > MAX_EVENTS:
            raise SparkInputError("events must be a non-empty immutable tuple within the event limit")
        if any(not isinstance(event, SyntheticEvent) for event in self.events):
            raise SparkInputError("events contains an invalid SyntheticEvent")
        source_ids = [event.source_id for event in self.events]
        if len(set(source_ids)) != len(source_ids):
            raise SparkIntegrityError("snapshot contains duplicate source IDs")
        if sum(len(event.text) for event in self.events) > MAX_TOTAL_TEXT_CHARS:
            raise SparkInputError("snapshot text exceeds the total character budget")


@dataclass(frozen=True, slots=True)
class SparkRequest:
    """一次显式请求的 R1 研究调用，不携带 owner 或路径参数。"""

    tension: str
    tension_structure: TensionStructure
    channels: tuple[str, ...]
    seed: int
    inspiration_requested: bool
    max_candidates: int = MAX_CANDIDATES
    risk_domain: str = "general"

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "tension",
            _require_text(self.tension, "tension", maximum=MAX_TENSION_CHARS),
        )
        if not isinstance(self.tension_structure, TensionStructure):
            raise SparkInputError("tension_structure must be a TensionStructure")
        if not isinstance(self.channels, tuple) or not self.channels:
            raise SparkInputError("channels must be a non-empty immutable tuple")
        if any(not isinstance(channel, str) for channel in self.channels):
            raise SparkInputError("channels must contain strings")
        if len(set(self.channels)) != len(self.channels):
            raise SparkInputError("channels contains duplicates")
        unknown_channels = sorted(set(self.channels) - CHANNELS)
        if unknown_channels:
            raise SparkInputError("channels contains an unsupported value")
        _require_plain_int(self.seed, "seed", minimum=0, maximum=MAX_SEED)
        if type(self.inspiration_requested) is not bool or not self.inspiration_requested:
            raise SparkInputError("Spark requires an explicit inspiration request")
        _require_plain_int(
            self.max_candidates,
            "max_candidates",
            minimum=1,
            maximum=MAX_CANDIDATES,
        )
        if not isinstance(self.risk_domain, str) or self.risk_domain not in RISK_DOMAINS:
            raise SparkInputError("risk_domain contains an unsupported value")


@dataclass(frozen=True, slots=True)
class InstrumentMetadata:
    """成功调用时由受信 adapter 声明的最小、无正文实验元数据。"""

    provider: str
    requested_model: str
    actual_model: str
    prompt_hash: str
    parameters: tuple[tuple[str, str], ...] = ()
    version_verified: bool = True

    def __post_init__(self) -> None:
        for field_name in ("provider", "requested_model", "actual_model"):
            object.__setattr__(
                self,
                field_name,
                _require_instrument_id(
                    getattr(self, field_name),
                    f"instrument.{field_name}",
                ),
            )
        object.__setattr__(self, "prompt_hash", _require_sha256(self.prompt_hash, "instrument.prompt_hash"))
        if not isinstance(self.parameters, tuple):
            raise SparkInputError("instrument.parameters must be an immutable tuple")
        normalized: list[tuple[str, str]] = []
        for item in self.parameters:
            if not isinstance(item, tuple) or len(item) != 2:
                raise SparkInputError("instrument.parameters contains an invalid pair")
            key = _require_safe_id(item[0], "instrument parameter key")
            if key not in _SAFE_PARAMETER_KEYS:
                raise SparkInputError("instrument parameter key is not on the safe allowlist")
            value = _require_text(
                item[1],
                "instrument parameter value",
                maximum=64,
                allow_newlines=False,
            )
            normalized.append((key, _validate_instrument_parameter(key, value)))
        if len({key for key, _ in normalized}) != len(normalized):
            raise SparkInputError("instrument.parameters contains duplicate keys")
        object.__setattr__(self, "parameters", tuple(normalized))
        if type(self.version_verified) is not bool:
            raise SparkInputError("instrument.version_verified must be a boolean")

    def to_dict(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "requested_model": self.requested_model,
            "actual_model": self.actual_model,
            "prompt_hash": self.prompt_hash,
            "parameters": {key: value for key, value in self.parameters},
            "version_verified": self.version_verified,
        }


@dataclass(frozen=True, slots=True)
class StructureEvidence:
    """最终候选可重建的结构提示证据。"""

    level: str
    value: str
    source_hash: str
    start: int
    end: int
    extractor: str
    prompt_version: str

    def __post_init__(self) -> None:
        if self.level not in STRUCTURE_LEVELS:
            raise SparkInputError("structure evidence level is unsupported")
        object.__setattr__(
            self,
            "value",
            _require_text(
                self.value,
                "structure_evidence.value",
                maximum=MAX_CUE_CHARS,
                allow_newlines=False,
            ).strip(),
        )
        object.__setattr__(
            self,
            "source_hash",
            _require_sha256(self.source_hash, "structure_evidence.source_hash"),
        )
        _require_plain_int(
            self.start,
            "structure_evidence.start",
            minimum=0,
            maximum=MAX_EVENT_TEXT_CHARS,
        )
        _require_plain_int(
            self.end,
            "structure_evidence.end",
            minimum=1,
            maximum=MAX_EVENT_TEXT_CHARS,
        )
        if self.end <= self.start:
            raise SparkIntegrityError("structure evidence span must be non-empty")
        object.__setattr__(
            self,
            "extractor",
            _require_instrument_id(self.extractor, "structure_evidence.extractor"),
        )
        object.__setattr__(
            self,
            "prompt_version",
            _require_instrument_id(
                self.prompt_version,
                "structure_evidence.prompt_version",
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "level": self.level,
            "value": self.value,
            "source_hash": self.source_hash,
            "span": {"start": self.start, "end": self.end},
            "extractor": self.extractor,
            "prompt_version": self.prompt_version,
        }


@dataclass(frozen=True, slots=True)
class SelectedMaterial:
    """Policy-safe 选择结果，供 generator 使用。"""

    source_id: str
    source_hash: str
    text: str
    span_start: int
    span_end: int
    shared_structure: tuple[str, ...]
    structure_evidence: tuple[StructureEvidence, ...]
    mismatch: str
    lexical_overlap: float
    structural_fit: float

    def __post_init__(self) -> None:
        object.__setattr__(self, "source_id", _require_safe_id(self.source_id, "material.source_id"))
        object.__setattr__(self, "source_hash", _require_sha256(self.source_hash, "material.source_hash"))
        object.__setattr__(
            self,
            "text",
            _require_text(self.text, "material.text", maximum=MAX_EVENT_TEXT_CHARS),
        )
        _require_plain_int(
            self.span_start,
            "material.span_start",
            minimum=0,
            maximum=MAX_EVENT_TEXT_CHARS,
        )
        _require_plain_int(
            self.span_end,
            "material.span_end",
            minimum=1,
            maximum=MAX_EVENT_TEXT_CHARS,
        )
        if self.span_end <= self.span_start or self.span_end > len(self.text):
            raise SparkIntegrityError("material span is outside the source text")
        object.__setattr__(
            self,
            "shared_structure",
            _tuple_of_strings(self.shared_structure, "material.shared_structure"),
        )
        if not isinstance(self.structure_evidence, tuple):
            raise SparkInputError("material.structure_evidence must be an immutable tuple")
        for evidence in self.structure_evidence:
            if not isinstance(evidence, StructureEvidence):
                raise SparkInputError("material.structure_evidence contains an invalid item")
            if evidence.source_hash != self.source_hash:
                raise SparkIntegrityError("structure evidence hash differs from the selected source")
            if evidence.start < self.span_start or evidence.end > self.span_end:
                raise SparkIntegrityError("structure evidence lies outside the selected excerpt")
        evidence_values = {
            normalize_structure_value(evidence.value)
            for evidence in self.structure_evidence
        }
        shared_values = {
            normalize_structure_value(value)
            for value in self.shared_structure
        }
        if not evidence_values <= shared_values:
            raise SparkIntegrityError("structure evidence is not represented in shared_structure")
        object.__setattr__(
            self,
            "mismatch",
            _require_text(self.mismatch, "material.mismatch", maximum=500).strip(),
        )
        for field_name in ("lexical_overlap", "structural_fit"):
            value = getattr(self, field_name)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise SparkInputError(f"material.{field_name} must be numeric")
            number = float(value)
            if not math.isfinite(number) or number < 0.0 or number > 1.0:
                raise SparkInputError(f"material.{field_name} must be finite and within [0, 1]")
            object.__setattr__(self, field_name, number)


@dataclass(frozen=True, slots=True)
class GenerationInput:
    """generator 的最小只读视图；不暴露完整来源、选择分或结构证据。"""

    tension: str
    risk_domain: str
    channel: str
    source_id: str
    source_hash: str
    span_start: int
    span_end: int
    material: str
    shared_structure: tuple[str, ...]
    mismatch: str
    instructions: bool = False
    may_call_tools: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "tension",
            _require_text(self.tension, "generation.tension", maximum=MAX_TENSION_CHARS),
        )
        if self.risk_domain not in RISK_DOMAINS:
            raise SparkInputError("generation risk_domain is unsupported")
        if self.channel not in CHANNELS:
            raise SparkInputError("generation channel is unsupported")
        object.__setattr__(
            self,
            "source_id",
            _require_safe_id(self.source_id, "generation.source_id"),
        )
        object.__setattr__(
            self,
            "source_hash",
            _require_sha256(self.source_hash, "generation.source_hash"),
        )
        _require_plain_int(
            self.span_start,
            "generation.span_start",
            minimum=0,
            maximum=MAX_EVENT_TEXT_CHARS,
        )
        _require_plain_int(
            self.span_end,
            "generation.span_end",
            minimum=1,
            maximum=MAX_EVENT_TEXT_CHARS,
        )
        if self.span_end <= self.span_start:
            raise SparkIntegrityError("generation span must be non-empty")
        object.__setattr__(
            self,
            "material",
            _require_text(self.material, "generation.material", maximum=1_500),
        )
        if self.span_end - self.span_start != len(self.material):
            raise SparkIntegrityError(
                "generation material length must match its exact source span"
            )
        object.__setattr__(
            self,
            "shared_structure",
            _tuple_of_strings(
                self.shared_structure,
                "generation.shared_structure",
            ),
        )
        object.__setattr__(
            self,
            "mismatch",
            _require_text(
                self.mismatch,
                "generation.mismatch",
                maximum=500,
            ).strip(),
        )
        if not self.shared_structure:
            raise SparkIntegrityError("generation input requires shared_structure")
        if self.instructions is not False or self.may_call_tools is not False:
            raise SparkBoundaryError("generation input cannot grant instruction or tool authority")


@dataclass(frozen=True, slots=True)
class GeneratedDraft:
    """generator 只能提交材料、问题和可核验来源字段。"""

    source_id: str
    source_hash: str
    span_start: int
    span_end: int
    material: str
    question: str
    shared_structure: tuple[str, ...]
    mismatch: str
    assumptions: tuple[str, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "source_id", _require_safe_id(self.source_id, "draft.source_id"))
        object.__setattr__(self, "source_hash", _require_sha256(self.source_hash, "draft.source_hash"))
        _require_plain_int(
            self.span_start,
            "draft.span_start",
            minimum=0,
            maximum=MAX_EVENT_TEXT_CHARS,
        )
        _require_plain_int(
            self.span_end,
            "draft.span_end",
            minimum=1,
            maximum=MAX_EVENT_TEXT_CHARS,
        )
        if self.span_end <= self.span_start:
            raise SparkIntegrityError("draft span must be non-empty")
        object.__setattr__(
            self,
            "material",
            _require_text(self.material, "draft.material", maximum=1_500),
        )
        object.__setattr__(
            self,
            "question",
            _require_text(self.question, "draft.question", maximum=1_000).strip(),
        )
        object.__setattr__(
            self,
            "shared_structure",
            _tuple_of_strings(self.shared_structure, "draft.shared_structure"),
        )
        object.__setattr__(
            self,
            "mismatch",
            _require_text(self.mismatch, "draft.mismatch", maximum=500).strip(),
        )
        object.__setattr__(
            self,
            "assumptions",
            _tuple_of_strings(self.assumptions, "draft.assumptions"),
        )
        if not self.shared_structure:
            raise SparkIntegrityError("draft must include shared_structure")
        if not self.assumptions:
            raise SparkIntegrityError("draft must include at least one assumption")


@dataclass(frozen=True, slots=True)
class GenerationResult:
    """一次 generator 调用的草案及 provider 实际回传元数据。"""

    draft: GeneratedDraft
    metadata: InstrumentMetadata

    def __post_init__(self) -> None:
        if not isinstance(self.draft, GeneratedDraft):
            raise SparkInputError("generation result draft is invalid")
        if not isinstance(self.metadata, InstrumentMetadata):
            raise SparkInputError("generation result metadata is invalid")


@dataclass(frozen=True, slots=True)
class EvaluationInput:
    """不给 evaluator 暴露来源通道标签，降低自证偏差。"""

    tension: str
    risk_domain: str
    material: str
    question: str
    shared_structure: tuple[str, ...]
    mismatch: str
    assumptions: tuple[str, ...]
    instructions: bool = False
    may_call_tools: bool = False

    def __post_init__(self) -> None:
        if self.instructions is not False or self.may_call_tools is not False:
            raise SparkBoundaryError("evaluation input cannot grant instruction or tool authority")


@dataclass(frozen=True, slots=True)
class EvaluationAxes:
    """分轴评审；不合成为单一灵感分或真值分。"""

    source_fidelity: float | None = None
    structural_fit: float | None = None
    appropriateness: float | None = None
    novelty_evidence: float | None = None
    usefulness: float | None = None
    risk: float | None = None

    def __post_init__(self) -> None:
        for field_name in (
            "source_fidelity",
            "structural_fit",
            "appropriateness",
            "novelty_evidence",
            "usefulness",
            "risk",
        ):
            value = getattr(self, field_name)
            if value is None:
                continue
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise SparkInputError(f"evaluation.{field_name} must be numeric or null")
            number = float(value)
            if not math.isfinite(number) or number < 0.0 or number > 1.0:
                raise SparkInputError(f"evaluation.{field_name} must be within [0, 1]")
            object.__setattr__(self, field_name, number)

    def to_dict(self) -> dict[str, float | None]:
        return {
            "source_fidelity": self.source_fidelity,
            "structural_fit": self.structural_fit,
            "appropriateness": self.appropriateness,
            "novelty_evidence": self.novelty_evidence,
            "usefulness": self.usefulness,
            "risk": self.risk,
        }


@dataclass(frozen=True, slots=True)
class EvaluationResult:
    """一次 evaluator 调用的分轴结果及 provider 实际回传元数据。"""

    axes: EvaluationAxes
    metadata: InstrumentMetadata

    def __post_init__(self) -> None:
        if not isinstance(self.axes, EvaluationAxes):
            raise SparkInputError("evaluation result axes are invalid")
        if not isinstance(self.metadata, InstrumentMetadata):
            raise SparkInputError("evaluation result metadata is invalid")


@dataclass(frozen=True, slots=True)
class InstrumentCall:
    """不含正文和来源 ID 的调用记录；超时不伪造未返回的仪器元数据。"""

    call_id: str
    phase: str
    channel: str
    status: str
    metadata: InstrumentMetadata | None

    def __post_init__(self) -> None:
        if not isinstance(self.call_id, str) or not re.fullmatch(
            r"call_[1-6]",
            self.call_id,
        ):
            raise SparkInputError("instrument call ID is invalid")
        if self.phase not in {"generator", "evaluator"}:
            raise SparkInputError("instrument call phase is invalid")
        if self.channel not in CHANNELS:
            raise SparkInputError("instrument call channel is invalid")
        if self.status not in {"ok", "timeout"}:
            raise SparkInputError("instrument call status is invalid")
        if self.status == "ok" and not isinstance(self.metadata, InstrumentMetadata):
            raise SparkInputError("successful instrument call requires metadata")
        if self.status == "timeout" and self.metadata is not None:
            raise SparkInputError("timed-out instrument call cannot claim returned metadata")

    def to_dict(self) -> dict[str, Any]:
        return {
            "call_id": self.call_id,
            "phase": self.phase,
            "channel": self.channel,
            "status": self.status,
            "metadata": self.metadata.to_dict() if self.metadata is not None else None,
        }


@dataclass(frozen=True, slots=True)
class SparkCandidate:
    """一次响应内的候选；candidate_id 不可跨调用检索。"""

    candidate_id: str
    channel: str
    source_id: str
    source_hash: str
    span_start: int
    span_end: int
    material: str
    question: str
    shared_structure: tuple[str, ...]
    structure_evidence: tuple[StructureEvidence, ...]
    mismatch: str
    assumptions: tuple[str, ...]
    selection_axes: tuple[tuple[str, float], ...]
    evaluation: EvaluationAxes | None
    generation_call_id: str
    evaluation_call_id: str | None
    generation_instrument: InstrumentMetadata
    evaluation_instrument: InstrumentMetadata | None

    def __post_init__(self) -> None:
        if not isinstance(self.candidate_id, str) or not re.fullmatch(
            r"candidate_[1-3]",
            self.candidate_id,
        ):
            raise SparkIntegrityError("candidate ID must be response-local")
        if not isinstance(self.channel, str) or self.channel not in CHANNELS:
            raise SparkInputError("candidate channel is unsupported")
        object.__setattr__(self, "source_id", _require_safe_id(self.source_id, "candidate.source_id"))
        object.__setattr__(self, "source_hash", _require_sha256(self.source_hash, "candidate.source_hash"))
        _require_plain_int(
            self.span_start,
            "candidate.span_start",
            minimum=0,
            maximum=MAX_EVENT_TEXT_CHARS,
        )
        _require_plain_int(
            self.span_end,
            "candidate.span_end",
            minimum=1,
            maximum=MAX_EVENT_TEXT_CHARS,
        )
        if self.span_end <= self.span_start:
            raise SparkIntegrityError("candidate span must be non-empty")
        object.__setattr__(
            self,
            "material",
            _require_text(self.material, "candidate.material", maximum=1_500),
        )
        object.__setattr__(
            self,
            "question",
            _require_text(self.question, "candidate.question", maximum=1_000).strip(),
        )
        object.__setattr__(
            self,
            "shared_structure",
            _tuple_of_strings(self.shared_structure, "candidate.shared_structure"),
        )
        if not isinstance(self.structure_evidence, tuple):
            raise SparkInputError("candidate.structure_evidence must be an immutable tuple")
        for evidence in self.structure_evidence:
            if not isinstance(evidence, StructureEvidence):
                raise SparkInputError("candidate.structure_evidence contains an invalid item")
            if evidence.source_hash != self.source_hash:
                raise SparkIntegrityError("candidate structure evidence hash mismatch")
            if evidence.start < self.span_start or evidence.end > self.span_end:
                raise SparkIntegrityError("candidate structure evidence lies outside its citation")
        object.__setattr__(
            self,
            "mismatch",
            _require_text(self.mismatch, "candidate.mismatch", maximum=500).strip(),
        )
        object.__setattr__(
            self,
            "assumptions",
            _tuple_of_strings(self.assumptions, "candidate.assumptions"),
        )
        if not self.shared_structure or not self.assumptions:
            raise SparkIntegrityError("candidate must preserve structure, mismatch, and assumptions")
        if not isinstance(self.selection_axes, tuple):
            raise SparkInputError("candidate.selection_axes must be an immutable tuple")
        axes: list[tuple[str, float]] = []
        for item in self.selection_axes:
            if not isinstance(item, tuple) or len(item) != 2:
                raise SparkInputError("candidate.selection_axes contains an invalid pair")
            key, value = item
            if key not in {"lexical_overlap", "structural_fit"}:
                raise SparkInputError("candidate.selection_axes contains an unsupported axis")
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise SparkInputError("candidate selection axis must be numeric")
            number = float(value)
            if not math.isfinite(number) or number < 0.0 or number > 1.0:
                raise SparkInputError("candidate selection axis must be within [0, 1]")
            axes.append((key, number))
        if len({key for key, _ in axes}) != len(axes):
            raise SparkInputError("candidate.selection_axes contains duplicate axes")
        object.__setattr__(self, "selection_axes", tuple(axes))
        if self.evaluation is not None and not isinstance(self.evaluation, EvaluationAxes):
            raise SparkInputError("candidate.evaluation must be EvaluationAxes or null")
        if not isinstance(self.generation_call_id, str) or not re.fullmatch(
            r"call_[1-6]",
            self.generation_call_id,
        ):
            raise SparkIntegrityError("candidate generation call ID is invalid")
        if self.evaluation_call_id is not None and (
            not isinstance(self.evaluation_call_id, str)
            or not re.fullmatch(r"call_[1-6]", self.evaluation_call_id)
        ):
            raise SparkIntegrityError("candidate evaluation call ID is invalid")
        if not isinstance(self.generation_instrument, InstrumentMetadata):
            raise SparkInputError("candidate generation instrument metadata is invalid")
        if len(
            {
                call_id
                for call_id in (
                    self.generation_call_id,
                    self.evaluation_call_id,
                )
                if call_id is not None
            }
        ) != (2 if self.evaluation_call_id is not None else 1):
            raise SparkIntegrityError("candidate cannot reuse one instrument call")
        if not (
            (self.evaluation is None)
            == (self.evaluation_instrument is None)
            == (self.evaluation_call_id is None)
        ):
            raise SparkIntegrityError(
                "candidate evaluation, call ID, and evaluator metadata must be present together"
            )
        if self.evaluation_instrument is not None and not isinstance(
            self.evaluation_instrument,
            InstrumentMetadata,
        ):
            raise SparkInputError("candidate evaluation instrument metadata is invalid")

    def to_dict(self) -> dict[str, Any]:
        return {
            "candidate_id": self.candidate_id,
            "channel": self.channel,
            "source": {
                "id": self.source_id,
                "hash": self.source_hash,
                "span": {"start": self.span_start, "end": self.span_end},
            },
            "material": self.material,
            "question": self.question,
            "shared_structure": list(self.shared_structure),
            "structure_evidence": [
                evidence.to_dict()
                for evidence in self.structure_evidence
            ],
            "mismatch": self.mismatch,
            "assumptions": list(self.assumptions),
            "selection_axes": {key: value for key, value in self.selection_axes},
            "evaluation": self.evaluation.to_dict() if self.evaluation is not None else None,
            "instruments": {
                "generator": {
                    "call_id": self.generation_call_id,
                    **self.generation_instrument.to_dict(),
                },
                "evaluator": (
                    {
                        "call_id": self.evaluation_call_id,
                        **self.evaluation_instrument.to_dict(),
                    }
                    if self.evaluation_instrument is not None
                    else None
                ),
            },
        }


@dataclass(frozen=True, slots=True)
class SparkResponse:
    """纯响应态结果；服务端不提供按 ID 再读取的接口。"""

    status: str
    candidates: tuple[SparkCandidate, ...]
    requested_channels: tuple[str, ...]
    omitted_channels: tuple[str, ...]
    instrument_calls: tuple[InstrumentCall, ...]
    snapshot_hash: str
    data_cutoff: str
    seed: int
    policy_version: str
    denial_counts: tuple[tuple[str, int], ...]
    persistent: bool = False
    lifetime: str = "response_only"
    retrievable: bool = False
    instructions: bool = False
    may_call_tools: bool = False
    schema_version: int = SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_plain_int(
            self.schema_version,
            "response.schema_version",
            minimum=SCHEMA_VERSION,
            maximum=SCHEMA_VERSION,
        )
        if not isinstance(self.status, str) or self.status not in {"ok", "partial", "empty"}:
            raise SparkInputError("response status is invalid")
        if not isinstance(self.candidates, tuple) or len(self.candidates) > MAX_CANDIDATES:
            raise SparkInputError("response candidates exceed the limit")
        if any(not isinstance(candidate, SparkCandidate) for candidate in self.candidates):
            raise SparkInputError("response contains an invalid candidate")
        if not isinstance(self.requested_channels, tuple) or not self.requested_channels:
            raise SparkInputError("response requested_channels must be a non-empty tuple")
        if any(not isinstance(channel, str) for channel in self.requested_channels):
            raise SparkInputError("response requested_channels must contain strings")
        if set(self.requested_channels) - CHANNELS:
            raise SparkInputError("response contains an unsupported requested channel")
        if len(set(self.requested_channels)) != len(self.requested_channels):
            raise SparkInputError("response requested_channels contains duplicates")
        if not isinstance(self.omitted_channels, tuple):
            raise SparkInputError("response omitted_channels must be an immutable tuple")
        if any(not isinstance(channel, str) for channel in self.omitted_channels):
            raise SparkInputError("response omitted_channels must contain strings")
        if set(self.omitted_channels) - set(self.requested_channels):
            raise SparkInputError("response omitted_channels must be requested channels")
        if len(set(self.omitted_channels)) != len(self.omitted_channels):
            raise SparkInputError("response omitted_channels contains duplicates")
        if (
            not isinstance(self.instrument_calls, tuple)
            or len(self.instrument_calls) > MAX_CANDIDATES * 2
        ):
            raise SparkInputError("response instrument_calls is invalid")
        if any(not isinstance(call, InstrumentCall) for call in self.instrument_calls):
            raise SparkInputError("response contains an invalid instrument call")
        expected_call_ids = [
            f"call_{index}"
            for index in range(1, len(self.instrument_calls) + 1)
        ]
        if [call.call_id for call in self.instrument_calls] != expected_call_ids:
            raise SparkIntegrityError("instrument call IDs must be response-local and sequential")
        if any(call.channel not in self.requested_channels for call in self.instrument_calls):
            raise SparkIntegrityError("instrument call channel was not requested")
        timeout_channels = {
            call.channel
            for call in self.instrument_calls
            if call.status == "timeout"
        }
        if timeout_channels != set(self.omitted_channels):
            raise SparkIntegrityError(
                "omitted channels must exactly match timed-out instrument calls"
            )
        if any(candidate.channel not in self.requested_channels for candidate in self.candidates):
            raise SparkIntegrityError("candidate channel was not requested")
        if any(candidate.channel in self.omitted_channels for candidate in self.candidates):
            raise SparkIntegrityError("omitted channel cannot retain candidates")
        expected_ids = [f"candidate_{index}" for index in range(1, len(self.candidates) + 1)]
        if [candidate.candidate_id for candidate in self.candidates] != expected_ids:
            raise SparkIntegrityError("candidate IDs must be response-local and sequential")
        for candidate in self.candidates:
            calls_by_id = {
                call.call_id: call
                for call in self.instrument_calls
            }
            generator_call = calls_by_id.get(candidate.generation_call_id)
            if not (
                generator_call is not None
                and generator_call.phase == "generator"
                and generator_call.status == "ok"
                and generator_call.channel == candidate.channel
                and generator_call.metadata == candidate.generation_instrument
            ):
                raise SparkIntegrityError(
                    "candidate generator metadata has no matching successful call"
                )
            if candidate.evaluation_instrument is not None:
                evaluator_call = calls_by_id.get(candidate.evaluation_call_id)
                if not (
                    evaluator_call is not None
                    and evaluator_call.phase == "evaluator"
                    and evaluator_call.status == "ok"
                    and evaluator_call.channel == candidate.channel
                    and evaluator_call.metadata == candidate.evaluation_instrument
                ):
                    raise SparkIntegrityError(
                        "candidate evaluator metadata has no matching successful call"
                    )
        generation_call_ids = [
            candidate.generation_call_id
            for candidate in self.candidates
        ]
        evaluation_call_ids = [
            candidate.evaluation_call_id
            for candidate in self.candidates
            if candidate.evaluation_call_id is not None
        ]
        if len(set(generation_call_ids)) != len(generation_call_ids):
            raise SparkIntegrityError("candidates cannot reuse a generator call")
        if len(set(evaluation_call_ids)) != len(evaluation_call_ids):
            raise SparkIntegrityError("candidates cannot reuse an evaluator call")
        if any(
            (
                self.persistent is not False,
                self.lifetime != "response_only",
                self.retrievable is not False,
                self.instructions is not False,
                self.may_call_tools is not False,
            )
        ):
            raise SparkBoundaryError("response cannot gain persistence, retrieval, instruction, or tool authority")
        _require_sha256(self.snapshot_hash, "response.snapshot_hash")
        object.__setattr__(
            self,
            "data_cutoff",
            _require_aware_iso8601(self.data_cutoff, "response.data_cutoff"),
        )
        _require_plain_int(self.seed, "response.seed", minimum=0, maximum=MAX_SEED)
        if self.policy_version != "spark-r1-policy/1":
            raise SparkBoundaryError("response policy version is unsupported")
        if not isinstance(self.denial_counts, tuple):
            raise SparkInputError("response denial_counts must be an immutable tuple")
        normalized_counts: list[tuple[str, int]] = []
        for item in self.denial_counts:
            if not isinstance(item, tuple) or len(item) != 2:
                raise SparkInputError("response denial_counts contains an invalid pair")
            key = _require_safe_id(item[0], "response denial reason")
            count = _require_plain_int(
                item[1],
                "response denial count",
                minimum=0,
                maximum=MAX_EVENTS * 16,
            )
            normalized_counts.append((key, count))
        if len({key for key, _ in normalized_counts}) != len(normalized_counts):
            raise SparkInputError("response denial_counts contains duplicate reasons")
        object.__setattr__(self, "denial_counts", tuple(normalized_counts))
        if self.status == "ok" and (not self.candidates or self.omitted_channels):
            raise SparkIntegrityError("ok response must contain candidates without omissions")
        if self.status == "empty" and (self.candidates or self.omitted_channels):
            raise SparkIntegrityError("empty response cannot contain candidates or omissions")
        if self.status == "partial" and (
            not self.candidates or not self.omitted_channels
        ):
            raise SparkIntegrityError(
                "partial response must contain a candidate and identify an omitted channel"
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "status": self.status,
            "persistent": self.persistent,
            "lifetime": self.lifetime,
            "retrievable": self.retrievable,
            "instructions": self.instructions,
            "may_call_tools": self.may_call_tools,
            "snapshot_hash": self.snapshot_hash,
            "data_cutoff": self.data_cutoff,
            "seed": self.seed,
            "policy_version": self.policy_version,
            "candidate_count": len(self.candidates),
            "candidates": [candidate.to_dict() for candidate in self.candidates],
            "requested_channels": list(self.requested_channels),
            "omitted_channels": list(self.omitted_channels),
            "instrument_calls": [call.to_dict() for call in self.instrument_calls],
            "denial_counts": {key: value for key, value in self.denial_counts},
        }
