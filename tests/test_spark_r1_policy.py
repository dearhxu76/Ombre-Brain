"""Spark R1 policy-first、随机对照与选择器边界。"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import replace

import pytest

from tests.spark_r1_support import event_payload, scenario_payload

from spark_r1 import (
    SparkInputError,
    SparkPolicyError,
    parse_scenario,
)
from spark_r1.policy import build_eligible_snapshot
import spark_r1.selection as selection
from spark_r1.selection import select_channel


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("event_type", "permanent", "event_type"),
        ("event_type", "feel", "event_type"),
        ("event_type", "plan", "event_type"),
        ("event_type", "letter", "event_type"),
        ("event_type", "i", "event_type"),
        ("event_type", "I", "event_type"),
        ("event_type", "self", "event_type"),
        ("event_type", "anchor", "event_type"),
        ("status", "resolved", "status"),
        ("status", "abandoned", "status"),
        ("status", "archived", "status"),
        ("status", "deleted", "status"),
        ("status", "tombstone", "status"),
        ("visibility", "hidden", "visibility"),
        ("visibility", "private", "visibility"),
        ("resolved", True, "resolved"),
        ("anchor", True, "anchor"),
        ("dont_surface", True, "dont_surface"),
        ("digested", True, "digested"),
        ("pinned", True, "pinned"),
        ("protected", True, "protected"),
        ("permanent", True, "permanent"),
    ],
)
def test_every_known_denial_gate_filters_before_selection(field, value, reason):
    denied = event_payload(
        "denied-high-score",
        "资源受限 资源受限 资源受限 保持服务可靠",
    )
    denied[field] = value
    eligible = event_payload("eligible-low-score", "另一个普通事件")
    parsed = parse_scenario(
        scenario_payload(
            [denied, eligible],
            channels=["direct"],
        )
    )

    snapshot = build_eligible_snapshot(parsed.snapshot)
    selected = select_channel(snapshot, parsed.request, "direct", limit=3)

    assert snapshot.allowlist == frozenset({"eligible-low-score"})
    assert "denied-high-score" not in {item.source_id for item in selected}
    assert dict(snapshot.denial_counts)[reason] >= 1


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("event_type", "unknown"),
        ("status", "unknown"),
        ("visibility", "unknown"),
    ],
)
def test_unknown_policy_vocabulary_fails_the_whole_snapshot(field, value):
    raw = scenario_payload()
    raw["events"][0][field] = value
    parsed = parse_scenario(raw)

    with pytest.raises(SparkPolicyError):
        build_eligible_snapshot(parsed.snapshot)


def test_snapshot_digest_and_uniform_random_are_input_order_independent():
    events = [
        event_payload(f"source-{index}", f"普通合成事件 {index}")
        for index in range(8)
    ]
    forward = parse_scenario(
        scenario_payload(
            events,
            channels=["uniform_random_control"],
            seed=19,
        )
    )
    backward = parse_scenario(
        scenario_payload(
            list(reversed(events)),
            channels=["uniform_random_control"],
            seed=19,
        )
    )
    left = build_eligible_snapshot(forward.snapshot)
    right = build_eligible_snapshot(backward.snapshot)

    left_ids = [
        item.source_id
        for item in select_channel(left, forward.request, "uniform_random_control", limit=3)
    ]
    right_ids = [
        item.source_id
        for item in select_channel(right, backward.request, "uniform_random_control", limit=3)
    ]

    assert left.snapshot_hash == right.snapshot_hash
    assert left_ids == right_ids


def test_uniform_random_freezes_ids_before_calculating_diagnostic_scores(monkeypatch):
    events = [
        event_payload(f"source-{index}", f"合成事件 {index}")
        for index in range(5)
    ]
    parsed = parse_scenario(
        scenario_payload(events, channels=["uniform_random_control"])
    )
    snapshot = build_eligible_snapshot(parsed.snapshot)
    baseline = select_channel(
        snapshot,
        parsed.request,
        "uniform_random_control",
        limit=3,
    )
    baseline_ids = [item.source_id for item in baseline]
    lexical_calls: list[str] = []
    structural_calls: list[str] = []

    def diagnostic_lexical(_tension, text):
        lexical_calls.append(text)
        return 0.75

    def diagnostic_structural(_request, source):
        structural_calls.append(source.source_id)
        return 0.25

    monkeypatch.setattr(selection, "lexical_overlap", diagnostic_lexical)
    monkeypatch.setattr(selection, "structural_fit", diagnostic_structural)

    selected = select_channel(
        snapshot,
        parsed.request,
        "uniform_random_control",
        limit=3,
    )

    assert [item.source_id for item in selected] == baseline_ids
    assert lexical_calls == [item.text for item in selected]
    assert structural_calls == baseline_ids
    assert all(item.lexical_overlap == 0.75 for item in selected)
    assert all(item.structural_fit == 0.25 for item in selected)


def test_uniform_random_is_reproducible_but_varies_across_seeds():
    events = [
        event_payload(f"source-{index}", f"合成事件 {index}")
        for index in range(10)
    ]
    seen: set[tuple[str, ...]] = set()
    for seed in range(12):
        parsed = parse_scenario(
            scenario_payload(
                deepcopy(events),
                channels=["uniform_random_control"],
                seed=seed,
            )
        )
        snapshot = build_eligible_snapshot(parsed.snapshot)
        ids = tuple(
            item.source_id
            for item in select_channel(
                snapshot,
                parsed.request,
                "uniform_random_control",
                limit=3,
            )
        )
        again = tuple(
            item.source_id
            for item in select_channel(
                snapshot,
                parsed.request,
                "uniform_random_control",
                limit=3,
            )
        )
        assert ids == again
        seen.add(ids)

    assert len(seen) > 1


def test_selector_rejects_raw_snapshot_and_has_no_unfiltered_fallback():
    parsed = parse_scenario(scenario_payload(channels=["direct"]))

    with pytest.raises(SparkInputError):
        select_channel(parsed.snapshot, parsed.request, "direct", limit=3)


def test_policy_seal_is_bound_to_the_exact_eligible_snapshot_content():
    parsed = parse_scenario(scenario_payload(channels=["direct"]))
    eligible = build_eligible_snapshot(parsed.snapshot)
    injected = replace(
        eligible.sources[0],
        source_id="policy-bypass-attempt",
    )

    with pytest.raises(SparkPolicyError):
        replace(eligible, sources=(injected, *eligible.sources[1:]))
