"""Spark Pilot 数值盲评的 aggregate-no-body 描述性分析。"""

from __future__ import annotations

from collections import defaultdict
from itertools import combinations
import math
import random
import re
from statistics import mean, stdev
from typing import Any

from .core import assignment_table
from .protocol import (
    CONDITIONS,
    NEGATIVE_AXES,
    PILOT_PROTOCOL_ID,
    PILOT_SEED,
    PRIMARY_NEGATIVE_THRESHOLD,
    PRIMARY_POSITIVE_THRESHOLD,
    RATING_AXES,
    RATING_MAX,
    RATING_MIN,
    SCENARIO_FAMILY_COUNT,
    SCHEMA_VERSION,
    UNAVAILABLE_CONDITIONS,
    PilotInputError,
    primary_success,
)


MAX_RATERS = 20
_RATER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,63}$")
_ROOT_FIELDS = frozenset({"schema_version", "phase", "ratings"})
_RATING_FIELDS = frozenset({"sample_id", "rater_id", *RATING_AXES})
_COMPARISONS = (
    (
        "structural_vs_distance_matched",
        "structural_distant",
        "distance_matched_random",
    ),
    (
        "structural_vs_lexical_direct",
        "structural_distant",
        "lexical_direct",
    ),
    (
        "counterexample_bundle_vs_structural",
        "structural_plus_counterexample",
        "structural_distant",
    ),
    ("mixed_vs_uniform_random", "mixed", "uniform_random"),
    (
        "structural_vs_scrambled",
        "structural_distant",
        "structural_scrambled",
    ),
)


def _finite_or_none(value: float | None) -> float | None:
    if value is None or not math.isfinite(value):
        return None
    return round(value, 6)


def _mean(values: list[float]) -> float | None:
    return mean(values) if values else None


def _bootstrap_ci(values: list[float]) -> list[float] | None:
    if len(values) < 2:
        return None
    rng = random.Random(PILOT_SEED ^ len(values))
    estimates = sorted(
        mean(rng.choices(values, k=len(values)))
        for _ in range(5_000)
    )
    last = len(estimates) - 1
    return [
        round(estimates[round(0.025 * last)], 6),
        round(estimates[round(0.975 * last)], 6),
    ]


def _paired_dz(values: list[float]) -> float | None:
    if len(values) < 2:
        return None
    spread = stdev(values)
    if spread == 0.0:
        return None
    return mean(values) / spread


def _sign_test(values: list[float]) -> float | None:
    positives = sum(value > 0.0 for value in values)
    negatives = sum(value < 0.0 for value in values)
    count = positives + negatives
    if count == 0:
        return None
    tail = min(positives, negatives)
    probability = sum(math.comb(count, index) for index in range(tail + 1)) / (2**count)
    return min(1.0, probability * 2.0)


def _holm_adjust(comparisons: list[dict[str, Any]]) -> None:
    valid = [
        (index, comparison["sign_test_p_value"])
        for index, comparison in enumerate(comparisons)
        if comparison["sign_test_p_value"] is not None
    ]
    valid.sort(key=lambda item: item[1])
    running = 0.0
    total = len(valid)
    for rank, (index, probability) in enumerate(valid):
        adjusted = min(1.0, probability * (total - rank))
        running = max(running, adjusted)
        comparisons[index]["holm_adjusted_p_value"] = round(running, 6)


def _validate_payload(payload: dict[str, Any]) -> list[dict[str, Any]]:
    if type(payload) is not dict or set(payload) != _ROOT_FIELDS:
        raise PilotInputError("analysis input must contain exactly the documented fields")
    if type(payload["schema_version"]) is not int or payload["schema_version"] != SCHEMA_VERSION:
        raise PilotInputError("analysis schema version is unsupported")
    if payload["phase"] != "analyze":
        raise PilotInputError("analysis phase must be analyze")
    ratings = payload["ratings"]
    if type(ratings) is not list or not ratings:
        raise PilotInputError("ratings must be a non-empty array")
    expected_maximum = SCENARIO_FAMILY_COUNT * len(CONDITIONS) * MAX_RATERS
    if len(ratings) > expected_maximum:
        raise PilotInputError("ratings exceed the frozen rater budget")
    normalized: list[dict[str, Any]] = []
    for rating in ratings:
        if type(rating) is not dict or set(rating) != _RATING_FIELDS:
            raise PilotInputError("each rating must contain exactly the documented fields")
        sample_identifier = rating["sample_id"]
        rater_identifier = rating["rater_id"]
        if not isinstance(sample_identifier, str) or not re.fullmatch(
            r"[0-9a-f]{32}", sample_identifier
        ):
            raise PilotInputError("sample_id is invalid")
        if not isinstance(rater_identifier, str) or not _RATER_RE.fullmatch(rater_identifier):
            raise PilotInputError("rater_id is invalid")
        if rater_identifier.casefold().startswith(
            ("sk-", "bearer", "token-", "secret-", "password-")
        ):
            raise PilotInputError("rater_id resembles a credential")
        scores: dict[str, int] = {}
        for axis in RATING_AXES:
            value = rating[axis]
            if type(value) is not int or value < RATING_MIN or value > RATING_MAX:
                raise PilotInputError("rating axes must be bounded integers")
            scores[axis] = value
        normalized.append(
            {
                "sample_id": sample_identifier,
                "rater_id": rater_identifier,
                "scores": scores,
                "primary": primary_success(scores),
            }
        )
    return normalized


def _agreement(
    records_by_sample: dict[str, list[dict[str, Any]]],
) -> dict[str, Any]:
    primary_pairs: list[int] = []
    axis_differences: dict[str, list[float]] = defaultdict(list)
    multi_rated_samples = 0
    for records in records_by_sample.values():
        if len(records) < 2:
            continue
        multi_rated_samples += 1
        for left, right in combinations(records, 2):
            primary_pairs.append(int(left["primary"] == right["primary"]))
            for axis in RATING_AXES:
                axis_differences[axis].append(
                    abs(left["scores"][axis] - right["scores"][axis])
                )
    return {
        "method": "pairwise descriptive agreement_no_chance_correction",
        "multi_rated_sample_count": multi_rated_samples,
        "primary_pair_count": len(primary_pairs),
        "primary_exact_agreement": _finite_or_none(_mean(primary_pairs)),
        "axis_mean_absolute_difference": {
            axis: _finite_or_none(_mean(axis_differences[axis]))
            for axis in RATING_AXES
        },
        "note": "这是 Pilot 描述量，不替代确认研究预注册的评审者一致性方法。",
    }


def analyze_ratings(payload: dict[str, Any]) -> dict[str, Any]:
    """验证盲评并返回无正文、无 sample/rater ID 的描述性聚合。"""

    ratings = _validate_payload(payload)
    assignments = assignment_table()
    assignment_by_sample = {
        identifier: (scenario_id, condition_id)
        for scenario_id, condition_id, identifier in assignments
    }
    expected_samples = frozenset(assignment_by_sample)
    seen: set[tuple[str, str]] = set()
    rater_samples: dict[str, set[str]] = defaultdict(set)
    records_by_sample: dict[str, list[dict[str, Any]]] = defaultdict(list)
    condition_records: dict[str, list[dict[str, Any]]] = defaultdict(list)
    scenario_condition: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for record in ratings:
        identifier = record["sample_id"]
        if identifier not in assignment_by_sample:
            raise PilotInputError("ratings contain an unknown sample_id")
        duplicate_key = (record["rater_id"], identifier)
        if duplicate_key in seen:
            raise PilotInputError("ratings contain a duplicate rater/sample pair")
        seen.add(duplicate_key)
        scenario_id, condition_id = assignment_by_sample[identifier]
        rater_samples[record["rater_id"]].add(identifier)
        records_by_sample[identifier].append(record)
        condition_records[condition_id].append(record)
        scenario_condition[(scenario_id, condition_id)].append(record)

    rater_count = len(rater_samples)
    if rater_count > MAX_RATERS:
        raise PilotInputError("ratings exceed the frozen rater budget")
    complete_raters = sum(samples == expected_samples for samples in rater_samples.values())
    all_raters_complete = complete_raters == rater_count
    minimum_human_review = rater_count >= 2 and all_raters_complete

    conditions: list[dict[str, Any]] = []
    for condition in CONDITIONS:
        records = condition_records[condition.condition_id]
        conditions.append(
            {
                "condition": condition.condition_id,
                "rating_count": len(records),
                "rated_scenario_count": len(
                    {
                        scenario_id
                        for (scenario_id, condition_id), values in scenario_condition.items()
                        if condition_id == condition.condition_id and values
                    }
                ),
                "primary_success_rate": _finite_or_none(
                    _mean([record["primary"] for record in records])
                ),
                "axis_means": {
                    axis: _finite_or_none(
                        _mean([record["scores"][axis] for record in records])
                    )
                    for axis in RATING_AXES
                },
            }
        )

    comparisons: list[dict[str, Any]] = []
    for comparison_id, treatment, reference in _COMPARISONS:
        primary_differences: list[float] = []
        axis_differences: dict[str, list[float]] = defaultdict(list)
        for scenario_id in {scenario_id for scenario_id, _, _ in assignments}:
            treatment_records = scenario_condition[(scenario_id, treatment)]
            reference_records = scenario_condition[(scenario_id, reference)]
            treatment_by_rater = {
                record["rater_id"]: record for record in treatment_records
            }
            reference_by_rater = {
                record["rater_id"]: record for record in reference_records
            }
            common_raters = sorted(set(treatment_by_rater) & set(reference_by_rater))
            if not common_raters:
                continue
            primary_differences.append(
                mean(
                    treatment_by_rater[rater]["primary"]
                    - reference_by_rater[rater]["primary"]
                    for rater in common_raters
                )
            )
            for axis in RATING_AXES:
                treatment_mean = mean(
                    treatment_by_rater[rater]["scores"][axis]
                    for rater in common_raters
                )
                reference_mean = mean(
                    reference_by_rater[rater]["scores"][axis]
                    for rater in common_raters
                )
                # 风险轴反向后，正值始终表示 treatment 更好。
                difference = treatment_mean - reference_mean
                if axis in NEGATIVE_AXES:
                    difference = -difference
                axis_differences[axis].append(difference)
        effect = _mean(primary_differences)
        interval = _bootstrap_ci(primary_differences)
        if effect is None or effect == 0.0:
            direction = "zero_or_unavailable"
        elif effect > 0.0:
            direction = "treatment_higher"
        else:
            direction = "treatment_lower"
        probability = _sign_test(primary_differences)
        comparisons.append(
            {
                "comparison": comparison_id,
                "treatment": treatment,
                "reference": reference,
                "paired_scenario_count": len(primary_differences),
                "mean_primary_difference": _finite_or_none(effect),
                "bootstrap_95_ci": interval,
                "paired_effect_dz": _finite_or_none(_paired_dz(primary_differences)),
                "sign_test_p_value": _finite_or_none(probability),
                "holm_adjusted_p_value": None,
                "direction": direction,
                "axis_mean_differences_positive_means_treatment_better": {
                    axis: _finite_or_none(_mean(axis_differences[axis]))
                    for axis in RATING_AXES
                },
            }
        )
    _holm_adjust(comparisons)

    expected_rating_count = len(expected_samples) * rater_count
    observed_sample_count = len(records_by_sample)
    inconclusive_or_nonpositive = sum(
        comparison["mean_primary_difference"] is None
        or comparison["mean_primary_difference"] <= 0.0
        or comparison["bootstrap_95_ci"] is None
        or comparison["bootstrap_95_ci"][0] <= 0.0 <= comparison["bootstrap_95_ci"][1]
        for comparison in comparisons
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "protocol_id": PILOT_PROTOCOL_ID,
        "status": (
            "complete_descriptive_pilot"
            if minimum_human_review
            else "incomplete_descriptive_pilot"
        ),
        "claim_status": "no_capability_claim_confirmation_not_run",
        "synthetic": True,
        "persistent": False,
        "lifetime": "aggregate_no_body",
        "retrievable": False,
        "content_retained": False,
        "sample_identifiers_retained": False,
        "rater_identifiers_retained": False,
        "scenario_family_count": SCENARIO_FAMILY_COUNT,
        "pilot_seed": PILOT_SEED,
        "expected_samples_per_rater": len(expected_samples),
        "observed_sample_count": observed_sample_count,
        "rating_count": len(ratings),
        "expected_rating_count_for_observed_raters": expected_rating_count,
        "missing_rating_count": expected_rating_count - len(ratings),
        "rater_count": rater_count,
        "complete_rater_count": complete_raters,
        "minimum_human_review_satisfied": minimum_human_review,
        "primary_endpoint": {
            "type": "gated_binary",
            "positive_threshold": PRIMARY_POSITIVE_THRESHOLD,
            "negative_maximum": PRIMARY_NEGATIVE_THRESHOLD,
            "single_total_score": False,
        },
        "conditions": conditions,
        "comparisons": comparisons,
        "multiple_comparison_method": "Holm correction over frozen primary comparisons",
        "rater_agreement": _agreement(records_by_sample),
        "negative_or_inconclusive_comparison_count": inconclusive_or_nonpositive,
        "missingness_and_outliers": {
            "missing_ratings_reported": True,
            "outlier_exclusions": 0,
            "provider_failures": 0,
            "note": "Pilot 不删除离群评分，也没有 provider 调用可归因。",
        },
        "confirmation_planning": {
            "human_rating_matrix_complete": minimum_human_review,
            "ready_for_preregistration": False,
            "minimum_meaningful_effect": None,
            "confirmation_sample_size": None,
            "reason": (
                "最小有意义效应、非劣界值与确认集功效必须由 Pilot 结果后人工预注册；"
                "真正的 semantic-only、三模型家族和独立模型评审仍未执行。"
            ),
        },
        "model_instrumentation": {
            "model_calls": 0,
            "latency": None,
            "tokens": None,
            "cost": None,
            "within_model_repetition": None,
            "between_model_repetition": None,
        },
        "unavailable_conditions": [dict(item) for item in UNAVAILABLE_CONDITIONS],
    }
