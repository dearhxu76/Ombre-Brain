"""Spark R1 的合成来源刷新与确定性候选复核。"""

from __future__ import annotations

from threading import Lock

from .models import (
    STRUCTURE_LEVELS,
    GeneratedDraft,
    SelectedMaterial,
    SparkBoundaryError,
    SparkIntegrityError,
    SyntheticSnapshot,
    normalize_structure_value,
)
from .policy import EligibleSnapshot


_NO_EVIDENCE_SHARED_STRUCTURE = ("候选关联仍需当前模型独立核验",)


class SyntheticSnapshotProvider:
    """只保存内存合成快照，用于模拟模型调用期间的 stale 变化。

    该 provider 不接受路径、owner、manager 或回调，也没有文件与网络能力。
    """

    __slots__ = ("_boundary_id", "_lock", "_revision", "_snapshot")

    def __init__(self, snapshot: SyntheticSnapshot) -> None:
        if not isinstance(snapshot, SyntheticSnapshot):
            raise SparkBoundaryError("provider requires a SyntheticSnapshot")
        self._boundary_id = snapshot.boundary_id
        self._snapshot = snapshot
        self._revision = 0
        self._lock = Lock()

    @property
    def boundary_id(self) -> str:
        return self._boundary_id

    def current(self) -> SyntheticSnapshot:
        with self._lock:
            return self._snapshot

    def current_state(self) -> tuple[SyntheticSnapshot, int]:
        """原子返回当前合成快照与单调 revision。"""

        with self._lock:
            return self._snapshot, self._revision

    def replace(self, snapshot: SyntheticSnapshot) -> None:
        if not isinstance(snapshot, SyntheticSnapshot):
            raise SparkBoundaryError("provider replacement requires a SyntheticSnapshot")
        if snapshot.boundary_id != self._boundary_id:
            raise SparkBoundaryError("provider cannot cross a synthetic boundary")
        with self._lock:
            self._snapshot = snapshot
            self._revision += 1


def verify_draft(
    *,
    draft: GeneratedDraft,
    material: SelectedMaterial,
    final_snapshot: EligibleSnapshot,
) -> str:
    """复核 allowlist、哈希、精确 code-point span，并返回核心派生的引用正文。"""

    if draft.source_id != material.source_id:
        raise SparkIntegrityError("generator returned a source outside its selected material")
    if draft.source_hash != material.source_hash:
        raise SparkIntegrityError("generator returned a mismatched source hash")
    if draft.span_start != material.span_start or draft.span_end != material.span_end:
        raise SparkIntegrityError("generator changed the selected source span")
    if draft.shared_structure != material.shared_structure:
        raise SparkIntegrityError("generator changed the selected shared structure")
    if draft.mismatch != material.mismatch:
        raise SparkIntegrityError("generator changed the required mismatch")

    current = final_snapshot.get(material.source_id)
    if current is None:
        raise SparkIntegrityError("selected source is no longer policy eligible")
    if current.content_hash != material.source_hash:
        raise SparkIntegrityError("selected source changed during generation")
    if material.span_end > len(current.text):
        raise SparkIntegrityError("selected source span is stale")

    projection = current.structure
    available_evidence = {
        (
            level,
            cue.value,
            cue.start,
            cue.end,
            current.content_hash,
            projection.extractor,
            projection.prompt_version,
        )
        for level in STRUCTURE_LEVELS
        for cue in getattr(projection, level)
    }
    for evidence in material.structure_evidence:
        key = (
            evidence.level,
            evidence.value,
            evidence.start,
            evidence.end,
            evidence.source_hash,
            evidence.extractor,
            evidence.prompt_version,
        )
        if key not in available_evidence:
            raise SparkIntegrityError(
                "selected structure evidence is not present in the current source projection"
            )

    evidence_values = {
        normalize_structure_value(evidence.value)
        for evidence in material.structure_evidence
    }
    shared_values = {
        normalize_structure_value(value)
        for value in material.shared_structure
    }
    if evidence_values:
        if shared_values != evidence_values:
            raise SparkIntegrityError(
                "shared structure contains a value without cited structure evidence"
            )
    elif material.shared_structure != _NO_EVIDENCE_SHARED_STRUCTURE:
        raise SparkIntegrityError(
            "shared structure requires cited structure evidence"
        )

    excerpt = current.text[material.span_start : material.span_end]
    if not excerpt or draft.material != excerpt:
        raise SparkIntegrityError("candidate material is not the exact selected source excerpt")
    return excerpt
