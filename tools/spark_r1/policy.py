"""Spark R1 独立的 policy-first 合成快照过滤。

这里不复用产品 `SurfacePolicyVM`，因为 Spark 必须对未知模式、缺失字段和特殊
记忆类型失败关闭。选择器只接收本模块产生的 `EligibleSnapshot`。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from hashlib import sha256
import json
from typing import Any

from .models import (
    KNOWN_EVENT_TYPES,
    KNOWN_STATUSES,
    KNOWN_VISIBILITIES,
    MAX_EVENTS,
    MAX_EVENT_TEXT_CHARS,
    STRUCTURE_LEVELS,
    SparkInputError,
    SparkIntegrityError,
    SparkPolicyError,
    StructuralProjection,
    SyntheticEvent,
    SyntheticSnapshot,
    _require_aware_iso8601,
    _require_safe_id,
    _require_sha256,
    _require_text,
    source_hash,
)


POLICY_VERSION = "spark-r1-policy/1"
_ELIGIBLE_SEAL = object()


@dataclass(frozen=True, slots=True)
class EligibleSource:
    """已通过全部策略门的最小来源视图。"""

    source_id: str
    text: str
    content_hash: str
    structure: StructuralProjection

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "source_id",
            _require_safe_id(self.source_id, "eligible_source.source_id"),
        )
        object.__setattr__(
            self,
            "text",
            _require_text(
                self.text,
                "eligible_source.text",
                maximum=MAX_EVENT_TEXT_CHARS,
            ),
        )
        object.__setattr__(
            self,
            "content_hash",
            _require_sha256(
                self.content_hash,
                "eligible_source.content_hash",
            ),
        )
        if source_hash(self.text) != self.content_hash:
            raise SparkIntegrityError("eligible source hash does not match its text")
        if not isinstance(self.structure, StructuralProjection):
            raise SparkInputError(
                "eligible source structure must be a StructuralProjection"
            )
        if self.structure.source_hash != self.content_hash:
            raise SparkIntegrityError(
                "eligible source structure hash does not match its text"
            )
        for level in STRUCTURE_LEVELS:
            for cue in getattr(self.structure, level):
                if cue.end > len(self.text):
                    raise SparkIntegrityError(
                        "eligible source structure span exceeds its text"
                    )
                if not self.text[cue.start : cue.end].strip():
                    raise SparkIntegrityError(
                        "eligible source structure span must cite non-blank text"
                    )


@dataclass(frozen=True, slots=True)
class EligibleSnapshot:
    """选择器可见的唯一快照类型。"""

    boundary_id: str
    data_cutoff: str
    sources: tuple[EligibleSource, ...]
    snapshot_hash: str
    denial_counts: tuple[tuple[str, int], ...]
    policy_version: str = POLICY_VERSION
    _seal: object = field(default=None, repr=False, compare=False)

    def __post_init__(self) -> None:
        if (
            not isinstance(self._seal, tuple)
            or len(self._seal) != 2
            or self._seal[0] is not _ELIGIBLE_SEAL
        ):
            raise SparkPolicyError("eligible snapshot must be created by the Spark policy")
        boundary_id = _require_safe_id(
            self.boundary_id,
            "eligible_snapshot.boundary_id",
        )
        if not boundary_id.startswith("synthetic:"):
            raise SparkPolicyError("eligible snapshot boundary must remain synthetic")
        object.__setattr__(self, "boundary_id", boundary_id)
        object.__setattr__(
            self,
            "data_cutoff",
            _require_aware_iso8601(
                self.data_cutoff,
                "eligible_snapshot.data_cutoff",
            ),
        )
        if not isinstance(self.sources, tuple):
            raise SparkInputError("eligible sources must be an immutable tuple")
        if any(not isinstance(source, EligibleSource) for source in self.sources):
            raise SparkInputError("eligible sources contains an invalid item")
        if tuple(sorted(source.source_id for source in self.sources)) != tuple(
            source.source_id for source in self.sources
        ):
            raise SparkPolicyError("eligible sources must be sorted by source ID")
        if len({source.source_id for source in self.sources}) != len(self.sources):
            raise SparkPolicyError("eligible snapshot contains duplicate source IDs")
        object.__setattr__(
            self,
            "snapshot_hash",
            _require_sha256(
                self.snapshot_hash,
                "eligible_snapshot.snapshot_hash",
            ),
        )
        if not isinstance(self.denial_counts, tuple):
            raise SparkInputError(
                "eligible snapshot denial_counts must be an immutable tuple"
            )
        normalized_counts: list[tuple[str, int]] = []
        for item in self.denial_counts:
            if not isinstance(item, tuple) or len(item) != 2:
                raise SparkInputError(
                    "eligible snapshot denial_counts contains an invalid pair"
                )
            reason = _require_safe_id(
                item[0],
                "eligible_snapshot.denial_reason",
            )
            if (
                type(item[1]) is not int
                or item[1] < 0
                or item[1] > MAX_EVENTS * 16
            ):
                raise SparkInputError(
                    "eligible snapshot denial count is outside the allowed range"
                )
            normalized_counts.append((reason, item[1]))
        object.__setattr__(self, "denial_counts", tuple(normalized_counts))
        if tuple(sorted(normalized_counts)) != tuple(normalized_counts):
            raise SparkPolicyError(
                "eligible snapshot denial_counts must be sorted"
            )
        if len({reason for reason, _ in normalized_counts}) != len(
            normalized_counts
        ):
            raise SparkPolicyError(
                "eligible snapshot denial_counts contains duplicate reasons"
            )
        if self.policy_version != POLICY_VERSION:
            raise SparkPolicyError("eligible snapshot policy version is unsupported")
        expected_signature = _eligible_signature(
            boundary_id=self.boundary_id,
            data_cutoff=self.data_cutoff,
            sources=self.sources,
            snapshot_hash=self.snapshot_hash,
            denial_counts=self.denial_counts,
            policy_version=self.policy_version,
        )
        if self._seal[1] != expected_signature:
            raise SparkPolicyError(
                "eligible snapshot content differs from its policy seal"
            )

    @property
    def allowlist(self) -> frozenset[str]:
        return frozenset(source.source_id for source in self.sources)

    def get(self, source_id: str) -> EligibleSource | None:
        return next((source for source in self.sources if source.source_id == source_id), None)


def _event_policy_record(event: SyntheticEvent) -> dict[str, Any]:
    return {
        "source_id": event.source_id,
        "content_hash": event.content_hash,
        "event_type": event.event_type,
        "status": event.status,
        "visibility": event.visibility,
        "resolved": event.resolved,
        "anchor": event.anchor,
        "dont_surface": event.dont_surface,
        "digested": event.digested,
        "pinned": event.pinned,
        "protected": event.protected,
        "permanent": event.permanent,
        "structure": {
            "source_hash": event.structure.source_hash,
            "extractor": event.structure.extractor,
            "prompt_version": event.structure.prompt_version,
            **{
                level: [
                    {"value": cue.value, "start": cue.start, "end": cue.end}
                    for cue in getattr(event.structure, level)
                ]
                for level in STRUCTURE_LEVELS
            },
        },
    }


def _snapshot_digest(snapshot: SyntheticSnapshot) -> str:
    payload = {
        "schema_version": snapshot.schema_version,
        "policy_version": POLICY_VERSION,
        "boundary_id": snapshot.boundary_id,
        "data_cutoff": snapshot.data_cutoff,
        "events": [
            _event_policy_record(event)
            for event in sorted(snapshot.events, key=lambda item: item.source_id)
        ],
    }
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return sha256(encoded).hexdigest()


def _eligible_signature(
    *,
    boundary_id: str,
    data_cutoff: str,
    sources: tuple[EligibleSource, ...],
    snapshot_hash: str,
    denial_counts: tuple[tuple[str, int], ...],
    policy_version: str,
) -> str:
    """把私有构造权与全部可选内容绑定，阻止 ``dataclasses.replace`` 绕过。"""

    payload = {
        "boundary_id": boundary_id,
        "data_cutoff": data_cutoff,
        "snapshot_hash": snapshot_hash,
        "policy_version": policy_version,
        "denial_counts": list(denial_counts),
        "sources": [
            {
                "source_id": source.source_id,
                "content_hash": source.content_hash,
                "structure": source.structure.to_dict(),
            }
            for source in sources
        ],
    }
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return sha256(encoded).hexdigest()


def _validate_policy_vocabulary(event: SyntheticEvent) -> None:
    if event.event_type not in KNOWN_EVENT_TYPES:
        raise SparkPolicyError("snapshot contains an unknown event type")
    if event.status not in KNOWN_STATUSES:
        raise SparkPolicyError("snapshot contains an unknown event status")
    if event.visibility not in KNOWN_VISIBILITIES:
        raise SparkPolicyError("snapshot contains an unknown visibility state")


def _denial_reasons(event: SyntheticEvent) -> tuple[str, ...]:
    reasons: list[str] = []
    if event.event_type != "dynamic":
        reasons.append("event_type")
    if event.status != "active":
        reasons.append("status")
    if event.visibility != "normal":
        reasons.append("visibility")
    if event.resolved:
        reasons.append("resolved")
    for field_name in (
        "anchor",
        "dont_surface",
        "digested",
        "pinned",
        "protected",
        "permanent",
    ):
        if getattr(event, field_name):
            reasons.append(field_name)
    return tuple(reasons)


def build_eligible_snapshot(snapshot: SyntheticSnapshot) -> EligibleSnapshot:
    """先验证全快照策略词表，再产生不含禁用来源的冻结 allowlist。"""

    if not isinstance(snapshot, SyntheticSnapshot):
        raise SparkInputError("policy requires a SyntheticSnapshot")
    for event in snapshot.events:
        _validate_policy_vocabulary(event)

    denial_counts: dict[str, int] = {}
    eligible: list[EligibleSource] = []
    for event in sorted(snapshot.events, key=lambda item: item.source_id):
        reasons = _denial_reasons(event)
        if reasons:
            for reason in reasons:
                denial_counts[reason] = denial_counts.get(reason, 0) + 1
            continue
        eligible.append(
            EligibleSource(
                source_id=event.source_id,
                text=event.text,
                content_hash=event.content_hash,
                structure=event.structure,
            )
        )

    frozen_sources = tuple(eligible)
    frozen_denial_counts = tuple(sorted(denial_counts.items()))
    snapshot_hash = _snapshot_digest(snapshot)
    signature = _eligible_signature(
        boundary_id=snapshot.boundary_id,
        data_cutoff=snapshot.data_cutoff,
        sources=frozen_sources,
        snapshot_hash=snapshot_hash,
        denial_counts=frozen_denial_counts,
        policy_version=POLICY_VERSION,
    )
    return EligibleSnapshot(
        boundary_id=snapshot.boundary_id,
        data_cutoff=snapshot.data_cutoff,
        sources=frozen_sources,
        snapshot_hash=snapshot_hash,
        denial_counts=frozen_denial_counts,
        _seal=(_ELIGIBLE_SEAL, signature),
    )
