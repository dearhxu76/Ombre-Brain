"""Spark Pilot 盲评输入、配对统计与无正文聚合回归。"""

from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
import sys

import pytest


ROOT = Path(__file__).resolve().parent.parent
TOOLS_ROOT = ROOT / "tools"
if str(TOOLS_ROOT) not in sys.path:
    sys.path.insert(0, str(TOOLS_ROOT))

from spark_pilot import PilotInputError, analyze_ratings  # noqa: E402
from spark_pilot.core import assignment_table  # noqa: E402
from spark_pilot.protocol import RATING_AXES  # noqa: E402


def _scores(condition: str) -> dict[str, int]:
    strong = condition in {
        "structural_distant",
        "structural_plus_counterexample",
        "mixed",
    }
    if strong:
        values = {axis: 4 for axis in RATING_AXES}
        values["false_premise"] = 0
        values["potential_harm"] = 0
        return values
    values = {axis: 1 for axis in RATING_AXES}
    values["false_premise"] = 2
    values["potential_harm"] = 2
    return values


def _complete_payload() -> dict[str, object]:
    ratings = []
    for rater_id in ("rater-a", "rater-b"):
        for _, condition, identifier in assignment_table():
            ratings.append(
                {
                    "sample_id": identifier,
                    "rater_id": rater_id,
                    **_scores(condition),
                }
            )
    return {"schema_version": 1, "phase": "analyze", "ratings": ratings}


def test_complete_analysis_reports_all_pairs_and_retains_zero_results() -> None:
    result = analyze_ratings(_complete_payload())

    assert result["status"] == "complete_descriptive_pilot"
    assert result["rater_count"] == 2
    assert result["complete_rater_count"] == 2
    assert result["rating_count"] == 560
    assert result["missing_rating_count"] == 0
    assert result["content_retained"] is False
    assert result["pilot_seed"] == 20260730
    assert result["sample_identifiers_retained"] is False
    assert result["rater_identifiers_retained"] is False
    assert len(result["conditions"]) == 7
    assert len(result["comparisons"]) == 5
    assert all(item["paired_scenario_count"] == 40 for item in result["comparisons"])
    assert any(
        item["mean_primary_difference"] == 0.0
        for item in result["comparisons"]
    )
    assert result["negative_or_inconclusive_comparison_count"] >= 1
    assert result["confirmation_planning"]["confirmation_sample_size"] is None
    assert result["model_instrumentation"]["within_model_repetition"] is None


def test_analysis_output_contains_no_body_or_input_identifiers() -> None:
    result = analyze_ratings(_complete_payload())
    encoded = json.dumps(result, ensure_ascii=False, sort_keys=True)

    assert "rater-a" not in encoded
    assert "rater-b" not in encoded
    assert assignment_table()[0][2] not in encoded
    assert '"material":' not in encoded
    assert '"current_tension":' not in encoded
    assert "single_total_score" in encoded
    assert '"single_total_score": false' in encoded


def test_partial_rating_set_is_descriptive_and_reports_missingness() -> None:
    payload = _complete_payload()
    payload["ratings"] = payload["ratings"][:1]

    result = analyze_ratings(payload)

    assert result["status"] == "incomplete_descriptive_pilot"
    assert result["rater_count"] == 1
    assert result["complete_rater_count"] == 0
    assert result["expected_rating_count_for_observed_raters"] == 280
    assert result["missing_rating_count"] == 279
    assert result["minimum_human_review_satisfied"] is False


@pytest.mark.parametrize(
    "mutation",
    (
        "unknown_sample",
        "duplicate",
        "boolean_score",
        "extra_field",
        "credential_rater",
    ),
)
def test_analysis_rejects_invalid_or_ambiguous_ratings(mutation: str) -> None:
    payload = _complete_payload()
    payload["ratings"] = payload["ratings"][:2]
    if mutation == "unknown_sample":
        payload["ratings"][0]["sample_id"] = "0" * 32
    elif mutation == "duplicate":
        payload["ratings"][1] = deepcopy(payload["ratings"][0])
    elif mutation == "boolean_score":
        payload["ratings"][0]["source_fidelity"] = True
    elif mutation == "extra_field":
        payload["ratings"][0]["comment"] = "not allowed"
    else:
        payload["ratings"][0]["rater_id"] = "sk-secret"

    with pytest.raises(PilotInputError):
        analyze_ratings(payload)


def test_analysis_rejects_unknown_root_fields_and_empty_ratings() -> None:
    with pytest.raises(PilotInputError):
        analyze_ratings(
            {
                "schema_version": 1,
                "phase": "analyze",
                "ratings": [],
                "output_path": "x",
            }
        )
