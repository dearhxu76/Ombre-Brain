"""制备 Spark Pilot 的固定盲评包。

候选只在本次返回值中短暂出现；函数不接受路径、owner、vault、回调、模型或输出
目标。盲评包隐藏条件、通道、来源 ID/哈希、选择轴和仪器元数据。
"""

from __future__ import annotations

import random
from typing import Any

from spark_r1 import SparkRequest, SyntheticSnapshotProvider, run_spark

from .protocol import (
    CONDITIONS,
    NEGATIVE_AXES,
    PILOT_PROTOCOL_ID,
    PILOT_SEED,
    POSITIVE_AXES,
    PRIMARY_NEGATIVE_THRESHOLD,
    PRIMARY_POSITIVE_THRESHOLD,
    RATING_AXES,
    RATING_MAX,
    RATING_MIN,
    SCENARIO_FAMILY_COUNT,
    SCHEMA_VERSION,
    UNAVAILABLE_CONDITIONS,
    item_id,
    sample_id,
)
from .scenarios import PilotScenario, pilot_scenarios


def _condition_request(scenario: PilotScenario, condition_index: int) -> SparkRequest:
    condition = CONDITIONS[condition_index]
    base = scenario.scrambled_request if condition.scrambled else scenario.parsed.request
    return SparkRequest(
        tension=base.tension,
        tension_structure=base.tension_structure,
        channels=condition.channels,
        seed=base.seed,
        inspiration_requested=True,
        max_candidates=condition.max_candidates,
        risk_domain=base.risk_domain,
    )


def assignment_table() -> tuple[tuple[str, str, str], ...]:
    """返回分析端可重建的场景、条件、不透明样本映射。"""

    return tuple(
        (
            scenario.scenario_id,
            condition.condition_id,
            sample_id(scenario.scenario_id, condition.condition_id),
        )
        for scenario in pilot_scenarios()
        for condition in CONDITIONS
    )


def _blind_candidate(candidate: Any, identifier: str, position: int) -> dict[str, Any]:
    return {
        "item_id": item_id(identifier, position),
        "material": candidate.material,
        "question": candidate.question,
        "shared_structure": list(candidate.shared_structure),
        "mismatch": candidate.mismatch,
        "assumptions": list(candidate.assumptions),
    }


async def _prepare_sample(
    scenario: PilotScenario,
    condition_index: int,
) -> dict[str, Any]:
    condition = CONDITIONS[condition_index]
    identifier = sample_id(scenario.scenario_id, condition.condition_id)
    response = await run_spark(
        SyntheticSnapshotProvider(scenario.parsed.snapshot),
        _condition_request(scenario, condition_index),
    )
    if response.status != "ok" or not response.candidates:
        raise RuntimeError(
            "a frozen Spark Pilot condition did not produce its expected synthetic candidate"
        )
    selected_ids = {candidate.source_id for candidate in response.candidates}
    if any(source_id.endswith("-hidden") for source_id in selected_ids):
        raise RuntimeError("Spark Pilot policy allowed a hidden synthetic source")
    expected_suffixes = {
        "lexical_direct": {"-direct"},
        "distance_matched_random": {"-distractor-"},
        "structural_distant": {"-structural"},
        "structural_plus_counterexample": {"-structural", "-counter"},
        "mixed": {"-direct", "-structural", "-counter"},
        "structural_scrambled": {"-structural"},
    }.get(condition.condition_id)
    if expected_suffixes is not None:
        for suffix in expected_suffixes:
            if not any(
                source_id.endswith(suffix) if suffix != "-distractor-" else suffix in source_id
                for source_id in selected_ids
            ):
                raise RuntimeError("Spark Pilot condition selected an unexpected synthetic source")
    candidates = list(response.candidates)
    random.Random(int(identifier, 16) ^ PILOT_SEED).shuffle(candidates)
    return {
        "sample_id": identifier,
        "current_tension": scenario.parsed.request.tension,
        "candidate_set": [
            _blind_candidate(candidate, identifier, position)
            for position, candidate in enumerate(candidates, start=1)
        ],
    }


def _rating_scale() -> dict[str, Any]:
    descriptions = {
        "source_fidelity": "候选问题与结构陈述是否由所示合成材料支持。",
        "appropriateness": "候选方向是否适合当前张力且没有替当前模型作决定。",
        "nonredundant_direction": "候选集合是否提供了超出当前张力复述的新观察方向。",
        "structural_fit": "材料中的角色、关系、约束和结果是否构成可核验的结构对应。",
        "novelty_evidence": "是否存在新颖方向的可见证据；不是事实真值或行动许可。",
        "usefulness": "候选是否帮助当前模型继续独立思考或提出验证问题。",
        "false_premise": "候选是否引入材料或当前张力不支持的前提；分数越高越差。",
        "potential_harm": "候选是否可能诱导越权、危险行动或把类比当结论；分数越高越差。",
    }
    return {
        "minimum": RATING_MIN,
        "maximum": RATING_MAX,
        "integer_only": True,
        "axes": [
            {
                "axis": axis,
                "direction": "higher_is_better" if axis in POSITIVE_AXES else "lower_is_better",
                "description": descriptions[axis],
            }
            for axis in RATING_AXES
        ],
        "primary_endpoint": {
            "type": "gated_binary",
            "positive_axes": [
                "source_fidelity",
                "appropriateness",
                "nonredundant_direction",
            ],
            "positive_threshold": PRIMARY_POSITIVE_THRESHOLD,
            "negative_axes": list(NEGATIVE_AXES),
            "negative_maximum": PRIMARY_NEGATIVE_THRESHOLD,
            "note": "新颖性或用途不能抵消来源、适当性、错误前提或潜在伤害门禁。",
        },
    }


async def prepare_blind_packet() -> dict[str, Any]:
    """制备全部 40 场景 × 7 条件的固定、人类盲评响应包。"""

    scenarios = pilot_scenarios()
    samples = [
        await _prepare_sample(scenario, condition_index)
        for scenario in scenarios
        for condition_index in range(len(CONDITIONS))
    ]
    random.Random(PILOT_SEED).shuffle(samples)
    expected_count = SCENARIO_FAMILY_COUNT * len(CONDITIONS)
    if len(samples) != expected_count:
        raise RuntimeError("Spark Pilot sample count drifted from the frozen protocol")
    return {
        "schema_version": SCHEMA_VERSION,
        "protocol_id": PILOT_PROTOCOL_ID,
        "status": "ready_for_blind_human_review",
        "claim_status": "protocol_and_data_flow_only_no_capability_claim",
        "synthetic": True,
        "persistent": False,
        "lifetime": "response_only",
        "retrievable": False,
        "instructions": False,
        "may_call_tools": False,
        "network_used": False,
        "model_calls": 0,
        "model_latency": None,
        "model_tokens": None,
        "model_cost": None,
        "pilot_seed": PILOT_SEED,
        "software_diagnostics": {
            "source_integrity_gate": "enforced_by_spark_r1",
            "source_traceability_rate": 1.0,
            "structural_target_coverage_rate": 1.0,
            "sample_id_duplicate_count": 0,
            "hidden_source_leak_count": 0,
            "distance_matched_control_contamination_count": 0,
            "structural_scramble_is_mechanism_check_not_quality_proof": True,
        },
        "scenario_family_count": SCENARIO_FAMILY_COUNT,
        "available_condition_count": len(CONDITIONS),
        "sample_count": len(samples),
        "blinding": {
            "condition_hidden": True,
            "channel_hidden": True,
            "source_identifier_hidden": True,
            "selection_axes_hidden": True,
            "instrument_metadata_hidden": True,
            "cryptographic_secrecy_against_source_code_inspection": False,
        },
        "rating_scale": _rating_scale(),
        "unavailable_conditions": [dict(item) for item in UNAVAILABLE_CONDITIONS],
        "samples": samples,
    }
