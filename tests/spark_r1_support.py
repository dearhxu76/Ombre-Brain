"""Spark R1 测试场景构造器；只生成纯 JSON 合成数据。"""

from __future__ import annotations

from copy import deepcopy
from pathlib import Path
import sys
from typing import Any


ROOT = Path(__file__).resolve().parent.parent
TOOLS_ROOT = ROOT / "tools"
if str(TOOLS_ROOT) not in sys.path:
    sys.path.insert(0, str(TOOLS_ROOT))

from spark_r1 import source_hash  # noqa: E402


STRUCTURE_LEVELS = (
    "entities",
    "roles_actions",
    "causal_constraints",
    "transformations_outcomes",
)


def cue(text: str, value: str, token: str | None = None) -> dict[str, Any]:
    support = token or value
    start = text.index(support)
    return {"value": value, "start": start, "end": start + len(support)}


def event_payload(
    source_id: str,
    text: str,
    *,
    event_type: str = "dynamic",
    status: str = "active",
    visibility: str = "normal",
    resolved: bool = False,
    anchor: bool = False,
    dont_surface: bool = False,
    digested: bool = False,
    pinned: bool = False,
    protected: bool = False,
    permanent: bool = False,
    structure: dict[str, list[dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    levels = structure or {level: [] for level in STRUCTURE_LEVELS}
    return {
        "source_id": source_id,
        "text": text,
        "event_type": event_type,
        "status": status,
        "visibility": visibility,
        "resolved": resolved,
        "anchor": anchor,
        "dont_surface": dont_surface,
        "digested": digested,
        "pinned": pinned,
        "protected": protected,
        "permanent": permanent,
        "structure": {
            "source_hash": source_hash(text),
            "extractor": "synthetic-fixture-v1",
            "prompt_version": "fixture-no-model-v1",
            **{level: deepcopy(levels.get(level, [])) for level in STRUCTURE_LEVELS},
        },
    }


def scenario_payload(
    events: list[dict[str, Any]] | None = None,
    *,
    channels: list[str] | None = None,
    tension: str = "资源受限时如何保持服务可靠",
    tension_structure: dict[str, list[str]] | None = None,
    boundary_id: str = "synthetic:test-a",
    seed: int = 7,
    inspiration_requested: bool = True,
    max_candidates: int = 3,
    risk_domain: str = "general",
) -> dict[str, Any]:
    if events is None:
        text = "旧项目在资源受限时通过缩小批次并保留回滚路径恢复了稳定。"
        events = [
            event_payload(
                "source-a",
                text,
                structure={
                    "entities": [cue(text, "项目", "项目")],
                    "roles_actions": [cue(text, "缩小批次", "缩小批次")],
                    "causal_constraints": [cue(text, "资源受限", "资源受限")],
                    "transformations_outcomes": [cue(text, "恢复稳定", "恢复了稳定")],
                },
            )
        ]
    return {
        "schema_version": 1,
        "synthetic": True,
        "boundary_id": boundary_id,
        "data_cutoff": "2026-07-30T00:00:00+08:00",
        "tension": tension,
        "tension_structure": tension_structure
        or {
            "entities": ["项目"],
            "roles_actions": ["缩小批次"],
            "causal_constraints": ["资源受限"],
            "transformations_outcomes": ["恢复稳定"],
        },
        "channels": channels or ["direct", "structural_distant"],
        "seed": seed,
        "inspiration_requested": inspiration_requested,
        "max_candidates": max_candidates,
        "risk_domain": risk_domain,
        "events": deepcopy(events),
    }
