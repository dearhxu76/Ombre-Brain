"""Spark Pilot 的 40 场景、固定条件与盲化契约。"""

from __future__ import annotations

import asyncio
import inspect
import json
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parent.parent
TOOLS_ROOT = ROOT / "tools"
if str(TOOLS_ROOT) not in sys.path:
    sys.path.insert(0, str(TOOLS_ROOT))

from spark_pilot import prepare_blind_packet  # noqa: E402
from spark_pilot.core import assignment_table  # noqa: E402
from spark_pilot.protocol import (  # noqa: E402
    CONDITIONS,
    PILOT_PROTOCOL_ID,
    RATING_AXES,
    SCENARIO_FAMILY_COUNT,
    UNAVAILABLE_CONDITIONS,
    sample_id,
)
from spark_pilot.scenarios import pilot_scenarios  # noqa: E402


def _packet() -> dict[str, object]:
    return asyncio.run(prepare_blind_packet())


def test_frozen_scenarios_are_exactly_40_synthetic_families() -> None:
    scenarios = pilot_scenarios()

    assert len(scenarios) == SCENARIO_FAMILY_COUNT == 40
    assert len({scenario.scenario_id for scenario in scenarios}) == 40
    assert all(scenario.parsed.snapshot.synthetic for scenario in scenarios)
    assert all(
        scenario.parsed.snapshot.boundary_id == f"synthetic:{scenario.scenario_id}"
        for scenario in scenarios
    )
    assert all(len(scenario.parsed.snapshot.events) == 10 for scenario in scenarios)
    assert all(
        sum(event.visibility == "hidden" for event in scenario.parsed.snapshot.events) == 1
        for scenario in scenarios
    )


def test_conditions_do_not_mislabel_lexical_direct_as_semantic() -> None:
    condition_ids = [condition.condition_id for condition in CONDITIONS]
    unavailable = {item["condition"] for item in UNAVAILABLE_CONDITIONS}

    assert condition_ids == [
        "lexical_direct",
        "uniform_random",
        "distance_matched_random",
        "structural_distant",
        "structural_plus_counterexample",
        "mixed",
        "structural_scrambled",
    ]
    assert "semantic_only" not in condition_ids
    assert {"semantic_only", "no_spark", "three_model_families"} <= unavailable


def test_assignment_table_is_complete_stable_and_opaque() -> None:
    assignments = assignment_table()

    assert len(assignments) == 40 * len(CONDITIONS) == 280
    assert len({identifier for _, _, identifier in assignments}) == 280
    assert all(len(identifier) == 32 for _, _, identifier in assignments)
    assert assignments[0][2] == sample_id("pilot-001", "lexical_direct")
    assert assignments == assignment_table()


def test_blind_packet_hides_condition_source_and_instrument_fields() -> None:
    packet = _packet()

    assert packet["protocol_id"] == PILOT_PROTOCOL_ID
    assert packet["status"] == "ready_for_blind_human_review"
    assert packet["sample_count"] == 280
    assert packet["model_calls"] == 0
    assert packet["pilot_seed"] == 20260730
    assert packet["network_used"] is False
    assert packet["persistent"] is False
    assert packet["retrievable"] is False
    assert packet["software_diagnostics"]["source_traceability_rate"] == 1.0
    assert packet["software_diagnostics"]["structural_target_coverage_rate"] == 1.0
    assert len(packet["samples"]) == 280

    forbidden_keys = {
        "condition",
        "channel",
        "source",
        "source_id",
        "source_hash",
        "snapshot_hash",
        "selection_axes",
        "instruments",
        "scenario_id",
        "risk_domain",
    }
    for sample in packet["samples"]:
        encoded = json.dumps(sample, ensure_ascii=False, sort_keys=True)
        assert not forbidden_keys & set(sample)
        assert "pilot-" not in encoded
        assert "-hidden" not in encoded
        for candidate in sample["candidate_set"]:
            assert not forbidden_keys & set(candidate)


def test_frozen_conditions_produce_expected_bundle_sizes_and_scramble_drop() -> None:
    packet = _packet()
    by_id = {sample["sample_id"]: sample for sample in packet["samples"]}
    first = pilot_scenarios()[0]
    expected_sizes = {
        "lexical_direct": 1,
        "uniform_random": 1,
        "distance_matched_random": 1,
        "structural_distant": 1,
        "structural_plus_counterexample": 2,
        "mixed": 3,
        "structural_scrambled": 1,
    }

    for condition_id, expected in expected_sizes.items():
        sample = by_id[sample_id(first.scenario_id, condition_id)]
        assert len(sample["candidate_set"]) == expected

    structural = by_id[sample_id(first.scenario_id, "structural_distant")]
    scrambled = by_id[sample_id(first.scenario_id, "structural_scrambled")]
    assert len(structural["candidate_set"][0]["shared_structure"]) == 4
    assert len(scrambled["candidate_set"][0]["shared_structure"]) == 1


def test_rating_scale_is_separate_axes_without_total_score() -> None:
    packet = _packet()
    scale = packet["rating_scale"]

    assert tuple(item["axis"] for item in scale["axes"]) == RATING_AXES
    assert scale["minimum"] == 0
    assert scale["maximum"] == 4
    assert "total_score" not in json.dumps(scale, ensure_ascii=False)
    assert scale["primary_endpoint"]["type"] == "gated_binary"


def test_public_pilot_entrypoint_has_no_product_or_storage_parameters() -> None:
    parameters = set(inspect.signature(prepare_blind_packet).parameters)

    assert parameters == set()
    assert not parameters & {
        "owner",
        "vault",
        "path",
        "output",
        "manager",
        "model",
        "tool",
        "callback",
    }
