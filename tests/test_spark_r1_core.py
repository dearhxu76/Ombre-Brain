"""Spark R1 通道编排、盲评、stale 复核与响应生命周期。"""

from __future__ import annotations

import asyncio
from dataclasses import fields, replace

import pytest

from tests.spark_r1_support import cue, event_payload, scenario_payload

from spark_r1 import (
    EvaluationAxes,
    EvaluationInput,
    EvaluationResult,
    GeneratedDraft,
    GenerationInput,
    GenerationResult,
    InstrumentMetadata,
    MaterialQuestionGenerator,
    SparkBoundaryError,
    SparkInputError,
    SparkInstrumentError,
    SparkIntegrityError,
    SyntheticSnapshotProvider,
    aggregate_manifest,
    parse_scenario,
    run_spark,
    source_hash,
)
from spark_r1.policy import build_eligible_snapshot
from spark_r1.selection import select_channel


def _metadata(name: str) -> InstrumentMetadata:
    return InstrumentMetadata(
        provider="test",
        requested_model=name,
        actual_model=name,
        prompt_hash=source_hash(name),
        parameters=(("temperature", "0"),),
        version_verified=True,
    )


def _draft_from_input(
    request: GenerationInput,
    *,
    question: str = "待核验问题",
    assumptions: tuple[str, ...] = ("待核验前提",),
) -> GeneratedDraft:
    return GeneratedDraft(
        source_id=request.source_id,
        source_hash=request.source_hash,
        span_start=request.span_start,
        span_end=request.span_end,
        material=request.material,
        question=question,
        shared_structure=request.shared_structure,
        mismatch=request.mismatch,
        assumptions=assumptions,
    )


def _two_channel_payload():
    direct_text = "资源受限时保持服务可靠，需要先识别最小恢复路径。"
    structural_text = "遥远案例通过缩小批次，在容量约束下恢复了稳定。"
    return scenario_payload(
        [
            event_payload("direct-source", direct_text),
            event_payload(
                "structural-source",
                structural_text,
                structure={
                    "entities": [],
                    "roles_actions": [cue(structural_text, "缩小批次")],
                    "causal_constraints": [cue(structural_text, "资源受限", "容量约束")],
                    "transformations_outcomes": [cue(structural_text, "恢复稳定", "恢复了稳定")],
                },
            ),
        ],
        channels=["direct", "structural_distant"],
        max_candidates=2,
    )


@pytest.mark.asyncio
async def test_default_harness_returns_at_most_three_response_only_candidates():
    events = [
        event_payload(f"source-{index}", f"资源受限时保持服务可靠的合成材料 {index}")
        for index in range(5)
    ]
    parsed = parse_scenario(
        scenario_payload(
            events,
            channels=["direct"],
            max_candidates=3,
        )
    )

    response = await run_spark(
        SyntheticSnapshotProvider(parsed.snapshot),
        parsed.request,
    )
    data = response.to_dict()

    assert response.status == "ok"
    assert [item.candidate_id for item in response.candidates] == [
        "candidate_1",
        "candidate_2",
        "candidate_3",
    ]
    assert data["persistent"] is False
    assert data["lifetime"] == "response_only"
    assert data["retrievable"] is False
    assert data["instructions"] is False
    assert data["may_call_tools"] is False
    assert all(item["material"] for item in data["candidates"])
    assert all(item["mismatch"] for item in data["candidates"])
    assert all(item["assumptions"] for item in data["candidates"])
    assert len(data["instrument_calls"]) == 3
    assert all(
        item["instruments"]["generator"]["actual_model"]
        == "material-question-template-v1"
        for item in data["candidates"]
    )
    assert all("answer" not in item and "action" not in item for item in data["candidates"])


@pytest.mark.asyncio
async def test_evaluator_is_blind_to_channel_seed_and_generator_identity():
    parsed = parse_scenario(scenario_payload(channels=["direct"]))

    class SpyEvaluator:
        metadata = _metadata("blind-evaluator")

        def __init__(self):
            self.requests: list[EvaluationInput] = []

        async def evaluate(self, request):
            self.requests.append(request)
            return EvaluationResult(
                axes=EvaluationAxes(
                    source_fidelity=1.0,
                    structural_fit=0.5,
                    appropriateness=0.5,
                    novelty_evidence=None,
                    usefulness=0.5,
                    risk=0.0,
                ),
                metadata=self.metadata,
            )

    evaluator = SpyEvaluator()
    response = await run_spark(
        SyntheticSnapshotProvider(parsed.snapshot),
        parsed.request,
        evaluator=evaluator,
    )

    assert response.candidates[0].evaluation is not None
    assert evaluator.requests
    visible_fields = {field.name for field in fields(EvaluationInput)}
    assert "channel" not in visible_fields
    assert "seed" not in visible_fields
    assert "generator" not in visible_fields
    assert all(request.instructions is False for request in evaluator.requests)
    assert all(request.may_call_tools is False for request in evaluator.requests)


@pytest.mark.asyncio
async def test_generator_receives_only_exact_minimal_excerpt_view():
    parsed = parse_scenario(scenario_payload(channels=["direct"]))

    class SpyGenerator:
        def __init__(self):
            self.requests = []

        async def generate(self, request):
            self.requests.append(request)
            return GenerationResult(
                draft=_draft_from_input(request),
                metadata=_metadata("minimal-generator"),
            )

    generator = SpyGenerator()
    response = await run_spark(
        SyntheticSnapshotProvider(parsed.snapshot),
        parsed.request,
        generator=generator,
    )

    request = generator.requests[0]
    visible_fields = {field.name for field in fields(GenerationInput)}
    assert visible_fields == {
        "tension",
        "risk_domain",
        "channel",
        "source_id",
        "source_hash",
        "span_start",
        "span_end",
        "material",
        "shared_structure",
        "mismatch",
        "instructions",
        "may_call_tools",
    }
    assert len(request.material) == request.span_end - request.span_start
    assert request.material == response.candidates[0].material


@pytest.mark.asyncio
async def test_one_independent_generator_timeout_only_omits_its_channel():
    parsed = parse_scenario(_two_channel_payload())
    baseline = MaterialQuestionGenerator()

    class ConditionalGenerator:
        metadata = _metadata("conditional-generator")

        async def generate(self, request):
            if request.channel == "direct":
                await asyncio.sleep(0.2)
            return await baseline.generate(request)

    response = await run_spark(
        SyntheticSnapshotProvider(parsed.snapshot),
        parsed.request,
        generator=ConditionalGenerator(),
        channel_timeout_seconds=0.05,
    )

    assert response.status == "partial"
    assert response.omitted_channels == ("direct",)
    assert {candidate.channel for candidate in response.candidates} == {
        "structural_distant"
    }
    timeout_call = next(
        call
        for call in response.instrument_calls
        if call.phase == "generator"
        and call.channel == "direct"
        and call.status == "timeout"
    )
    assert timeout_call.metadata is None


@pytest.mark.asyncio
async def test_non_timeout_generator_failure_fails_whole_call_without_provider_detail():
    parsed = parse_scenario(scenario_payload(channels=["direct"]))
    marker = "provider-secret-must-not-surface"

    class ExplodingGenerator:
        async def generate(self, _request):
            raise RuntimeError(marker)

    with pytest.raises(SparkInstrumentError) as captured:
        await run_spark(
            SyntheticSnapshotProvider(parsed.snapshot),
            parsed.request,
            generator=ExplodingGenerator(),
        )

    assert marker not in str(captured.value)
    assert captured.value.__cause__ is None


@pytest.mark.asyncio
async def test_all_channel_generator_timeouts_fail_whole_call_without_fallback():
    parsed = parse_scenario(_two_channel_payload())

    class SlowGenerator:
        metadata = _metadata("slow-generator")

        async def generate(self, _request):
            await asyncio.sleep(0.2)
            raise AssertionError("cancelled coroutine must not return")

    with pytest.raises(SparkInstrumentError, match="all candidate channels timed out"):
        await run_spark(
            SyntheticSnapshotProvider(parsed.snapshot),
            parsed.request,
            generator=SlowGenerator(),
            channel_timeout_seconds=0.05,
        )


@pytest.mark.asyncio
async def test_single_channel_generator_timeout_fails_whole_call():
    parsed = parse_scenario(scenario_payload(channels=["direct"]))

    class SlowGenerator:
        async def generate(self, _request):
            await asyncio.sleep(0.2)

    with pytest.raises(SparkInstrumentError, match="all candidate channels timed out"):
        await run_spark(
            SyntheticSnapshotProvider(parsed.snapshot),
            parsed.request,
            generator=SlowGenerator(),
            channel_timeout_seconds=0.05,
        )


@pytest.mark.asyncio
async def test_one_independent_evaluator_timeout_only_omits_its_channel():
    parsed = parse_scenario(_two_channel_payload())

    class ConditionalEvaluator:
        async def evaluate(self, request):
            if "最小恢复路径" in request.material:
                await asyncio.sleep(0.2)
            return EvaluationResult(
                axes=EvaluationAxes(source_fidelity=1.0),
                metadata=_metadata("conditional-evaluator"),
            )

    response = await run_spark(
        SyntheticSnapshotProvider(parsed.snapshot),
        parsed.request,
        evaluator=ConditionalEvaluator(),
        channel_timeout_seconds=0.05,
    )

    assert response.status == "partial"
    assert response.omitted_channels == ("direct",)
    assert {candidate.channel for candidate in response.candidates} == {
        "structural_distant"
    }
    timeout_call = next(
        call
        for call in response.instrument_calls
        if call.phase == "evaluator"
        and call.channel == "direct"
        and call.status == "timeout"
    )
    assert timeout_call.metadata is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        scenario_payload(channels=["direct"]),
        _two_channel_payload(),
    ],
    ids=["single-channel", "all-channels"],
)
async def test_no_successful_evaluation_after_timeouts_fails_whole_call(payload):
    parsed = parse_scenario(payload)

    class SlowEvaluator:
        async def evaluate(self, _request):
            await asyncio.sleep(0.2)

    with pytest.raises(SparkInstrumentError, match="all candidate channels timed out"):
        await run_spark(
            SyntheticSnapshotProvider(parsed.snapshot),
            parsed.request,
            evaluator=SlowEvaluator(),
            channel_timeout_seconds=0.05,
        )


@pytest.mark.asyncio
async def test_cooperative_timeout_finishes_adapter_cleanup():
    parsed = parse_scenario(scenario_payload(channels=["direct"]))
    cleanup_finished = asyncio.Event()

    class CooperativeGenerator:
        async def generate(self, _request):
            try:
                await asyncio.sleep(10)
            finally:
                cleanup_finished.set()

    with pytest.raises(SparkInstrumentError):
        await run_spark(
            SyntheticSnapshotProvider(parsed.snapshot),
            parsed.request,
            generator=CooperativeGenerator(),
            channel_timeout_seconds=0.05,
        )
    assert cleanup_finished.is_set()


@pytest.mark.asyncio
async def test_late_result_after_suppressed_cancellation_is_still_timeout():
    parsed = parse_scenario(scenario_payload(channels=["direct"]))
    baseline = MaterialQuestionGenerator()
    finished = asyncio.Event()

    class SuppressOnceGenerator:
        async def generate(self, request):
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                await asyncio.sleep(0.03)
                return await baseline.generate(request)
            finally:
                finished.set()

    loop = asyncio.get_running_loop()
    started = loop.time()
    with pytest.raises(SparkInstrumentError, match="all candidate channels timed out"):
        await run_spark(
            SyntheticSnapshotProvider(parsed.snapshot),
            parsed.request,
            generator=SuppressOnceGenerator(),
            channel_timeout_seconds=0.05,
        )

    assert loop.time() - started >= 0.07
    assert finished.is_set()


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["content", "hidden", "deleted"])
async def test_source_change_or_policy_change_during_generation_fails_whole_call(mutation):
    raw = scenario_payload(channels=["direct"])
    parsed = parse_scenario(raw)
    provider = SyntheticSnapshotProvider(parsed.snapshot)
    baseline = MaterialQuestionGenerator()

    changed = scenario_payload(channels=["direct"])
    if mutation == "content":
        changed["events"][0]["text"] += " 已改变"
        changed["events"][0]["structure"]["source_hash"] = source_hash(
            changed["events"][0]["text"]
        )
    elif mutation == "hidden":
        changed["events"][0]["visibility"] = "hidden"
    else:
        changed["events"][0]["status"] = "deleted"
    replacement = parse_scenario(changed).snapshot

    class MutatingGenerator:
        metadata = _metadata("mutating-generator")

        async def generate(self, request):
            draft = await baseline.generate(request)
            provider.replace(replacement)
            return draft

    with pytest.raises(SparkIntegrityError):
        await run_spark(
            provider,
            parsed.request,
            generator=MutatingGenerator(),
        )


@pytest.mark.asyncio
async def test_unselected_source_change_also_invalidates_the_frozen_snapshot():
    selected = event_payload("selected", "资源受限时保持服务可靠")
    unselected = event_payload("unselected", "与张力无关的合成材料")
    raw = scenario_payload(
        [selected, unselected],
        channels=["direct"],
        max_candidates=1,
    )
    parsed = parse_scenario(raw)
    provider = SyntheticSnapshotProvider(parsed.snapshot)
    baseline = MaterialQuestionGenerator()

    changed = scenario_payload(
        [selected, event_payload("unselected", "未选中来源也发生了变化")],
        channels=["direct"],
        max_candidates=1,
    )
    replacement = parse_scenario(changed).snapshot

    class MutatingGenerator:
        metadata = _metadata("mutating-unselected-generator")

        async def generate(self, request):
            draft = await baseline.generate(request)
            provider.replace(replacement)
            return draft

    with pytest.raises(SparkIntegrityError):
        await run_spark(
            provider,
            parsed.request,
            generator=MutatingGenerator(),
        )


@pytest.mark.asyncio
async def test_stale_source_is_rejected_before_evaluator_receives_it():
    raw = scenario_payload(channels=["direct"])
    parsed = parse_scenario(raw)
    provider = SyntheticSnapshotProvider(parsed.snapshot)
    baseline = MaterialQuestionGenerator()

    hidden = scenario_payload(channels=["direct"])
    hidden["events"][0]["visibility"] = "hidden"
    replacement = parse_scenario(hidden).snapshot

    class MutatingGenerator:
        metadata = _metadata("hide-before-evaluation")

        async def generate(self, request):
            draft = await baseline.generate(request)
            provider.replace(replacement)
            return draft

    class SpyEvaluator:
        metadata = _metadata("must-not-run")

        def __init__(self):
            self.calls = 0

        async def evaluate(self, _request):
            self.calls += 1
            return EvaluationResult(
                axes=EvaluationAxes(source_fidelity=1.0),
                metadata=self.metadata,
            )

    evaluator = SpyEvaluator()
    with pytest.raises(SparkIntegrityError):
        await run_spark(
            provider,
            parsed.request,
            generator=MutatingGenerator(),
            evaluator=evaluator,
        )
    assert evaluator.calls == 0


@pytest.mark.asyncio
async def test_change_then_restore_is_detected_by_monotonic_provider_revision():
    parsed = parse_scenario(scenario_payload(channels=["direct"]))
    provider = SyntheticSnapshotProvider(parsed.snapshot)
    baseline = MaterialQuestionGenerator()

    changed = scenario_payload(channels=["direct"])
    changed["events"][0]["visibility"] = "hidden"
    replacement = parse_scenario(changed).snapshot

    class RestoreGenerator:
        metadata = _metadata("change-then-restore")

        async def generate(self, request):
            draft = await baseline.generate(request)
            provider.replace(replacement)
            provider.replace(parsed.snapshot)
            return draft

    with pytest.raises(SparkIntegrityError):
        await run_spark(
            provider,
            parsed.request,
            generator=RestoreGenerator(),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "forgery",
    ["source_id", "source_hash", "span", "material", "shared_structure", "mismatch"],
)
async def test_forged_source_fields_fail_the_whole_call(forgery):
    parsed = parse_scenario(scenario_payload(channels=["direct"]))
    baseline = MaterialQuestionGenerator()

    class ForgingGenerator:
        metadata = _metadata("forging-generator")

        async def generate(self, request):
            baseline_result = await baseline.generate(request)
            valid = baseline_result.draft
            values = {
                "source_id": valid.source_id,
                "source_hash": valid.source_hash,
                "span_start": valid.span_start,
                "span_end": valid.span_end,
                "material": valid.material,
                "question": valid.question,
                "shared_structure": valid.shared_structure,
                "mismatch": valid.mismatch,
                "assumptions": valid.assumptions,
            }
            if forgery == "source_id":
                values["source_id"] = "forged-source"
            elif forgery == "source_hash":
                values["source_hash"] = "0" * 64
            elif forgery == "span":
                values["span_end"] = valid.span_end - 1
                values["material"] = valid.material[:-1]
            elif forgery == "material":
                values["material"] = "伪造引用"
            elif forgery == "shared_structure":
                values["shared_structure"] = ("伪造共享结构",)
            else:
                values["mismatch"] = "伪造不对应处"
            return GenerationResult(
                draft=GeneratedDraft(**values),
                metadata=self.metadata,
            )

    with pytest.raises(SparkIntegrityError):
        await run_spark(
            SyntheticSnapshotProvider(parsed.snapshot),
            parsed.request,
            generator=ForgingGenerator(),
        )


def test_provider_cannot_replace_with_another_synthetic_boundary():
    left = parse_scenario(scenario_payload(boundary_id="synthetic:left")).snapshot
    right = parse_scenario(scenario_payload(boundary_id="synthetic:right")).snapshot
    provider = SyntheticSnapshotProvider(left)

    with pytest.raises(SparkBoundaryError):
        provider.replace(right)
    assert provider.current().boundary_id == "synthetic:left"


def test_structural_channel_depends_on_structure_and_scramble_removes_match():
    text = "遥远团队通过缩小批次，在容量约束下恢复了稳定。"
    event = event_payload(
        "structural-source",
        text,
        structure={
            "entities": [cue(text, "团队")],
            "roles_actions": [cue(text, "缩小批次")],
            "causal_constraints": [cue(text, "资源受限", "容量约束")],
            "transformations_outcomes": [cue(text, "恢复稳定", "恢复了稳定")],
        },
    )
    matched = parse_scenario(
        scenario_payload([event], channels=["structural_distant"])
    )
    scrambled = parse_scenario(
        scenario_payload(
            [event],
            channels=["structural_distant"],
            tension_structure={
                "entities": ["顾客"],
                "roles_actions": ["扩大范围"],
                "causal_constraints": ["无限资源"],
                "transformations_outcomes": ["立即完成"],
            },
        )
    )

    matched_results = select_channel(
        build_eligible_snapshot(matched.snapshot),
        matched.request,
        "structural_distant",
        limit=3,
    )
    scrambled_results = select_channel(
        build_eligible_snapshot(scrambled.snapshot),
        scrambled.request,
        "structural_distant",
        limit=3,
    )

    assert [item.source_id for item in matched_results] == ["structural-source"]
    assert scrambled_results == ()


def test_structural_distant_rejects_surface_neighbour_before_ranking():
    levels = (
        "entities",
        "roles_actions",
        "causal_constraints",
        "transformations_outcomes",
    )
    values = {
        "entities": "E",
        "roles_actions": "R",
        "causal_constraints": "C",
        "transformations_outcomes": "O",
    }

    def structured_event(source_id, text):
        return event_payload(
            source_id,
            text,
            structure={
                level: [cue(text, values[level])]
                for level in levels
            },
        )

    parsed = parse_scenario(
        scenario_payload(
            [
                structured_event(
                    "surface-neighbour",
                    "E R C O alpha beta gamma exact",
                ),
                structured_event(
                    "lexically-distant",
                    "E R C O unrelated vocabulary",
                ),
            ],
            channels=["structural_distant"],
            tension="alpha beta gamma exact",
            tension_structure={
                level: [values[level]]
                for level in levels
            },
        )
    )

    selected = select_channel(
        build_eligible_snapshot(parsed.snapshot),
        parsed.request,
        "structural_distant",
        limit=3,
    )

    assert [item.source_id for item in selected] == ["lexically-distant"]
    assert selected[0].structural_fit == 1.0
    assert selected[0].lexical_overlap == 0.0


def test_counterexample_prioritizes_shared_constraints_with_different_outcome():
    opposite_text = "团队缩小批次应对容量约束，但最终仍然失败。"
    same_text = "团队缩小批次应对容量约束，并最终恢复稳定。"
    entity_only_text = "团队讨论了另一个无关领域，并出现不同结果。"
    missing_outcome_text = "团队缩小批次应对容量约束，但材料没有结果字段。"
    events = [
        event_payload(
            "opposite-outcome",
            opposite_text,
            structure={
                "entities": [cue(opposite_text, "团队")],
                "roles_actions": [cue(opposite_text, "缩小批次")],
                "causal_constraints": [cue(opposite_text, "资源受限", "容量约束")],
                "transformations_outcomes": [cue(opposite_text, "失败")],
            },
        ),
        event_payload(
            "same-outcome",
            same_text,
            structure={
                "entities": [cue(same_text, "团队")],
                "roles_actions": [cue(same_text, "缩小批次")],
                "causal_constraints": [cue(same_text, "资源受限", "容量约束")],
                "transformations_outcomes": [cue(same_text, "恢复稳定")],
            },
        ),
        event_payload(
            "entity-only",
            entity_only_text,
            structure={
                "entities": [cue(entity_only_text, "团队")],
                "roles_actions": [],
                "causal_constraints": [],
                "transformations_outcomes": [cue(entity_only_text, "不同结果")],
            },
        ),
        event_payload(
            "missing-outcome",
            missing_outcome_text,
            structure={
                "entities": [cue(missing_outcome_text, "团队")],
                "roles_actions": [cue(missing_outcome_text, "缩小批次")],
                "causal_constraints": [
                    cue(missing_outcome_text, "资源受限", "容量约束")
                ],
                "transformations_outcomes": [],
            },
        ),
    ]
    parsed = parse_scenario(
        scenario_payload(events, channels=["counterexample"])
    )

    selected = select_channel(
        build_eligible_snapshot(parsed.snapshot),
        parsed.request,
        "counterexample",
        limit=2,
    )

    assert [item.source_id for item in selected] == ["opposite-outcome"]
    assert "结果方向" in selected[0].mismatch


def test_distance_matched_random_is_reproducible_and_policy_safe():
    levels = (
        "entities",
        "roles_actions",
        "causal_constraints",
        "transformations_outcomes",
    )
    structure_values = {
        "entities": "E",
        "roles_actions": "R",
        "causal_constraints": "C",
        "transformations_outcomes": "O",
    }
    structure_text = "E R C O unrelated"
    events = [
        event_payload(
            "structural-reference",
            structure_text,
            structure={
                level: [cue(structure_text, structure_values[level])]
                for level in levels
            },
        ),
        *[
            event_payload(f"matched-{index}", f"unrelated token{index}")
            for index in range(5)
        ],
        *[
            event_payload(f"far-{index}", f"alpha beta gamma token{index}")
            for index in range(8)
        ],
    ]
    parsed = parse_scenario(
        scenario_payload(
            events,
            channels=["distance_matched_random_control"],
            seed=23,
            tension="alpha beta gamma",
            tension_structure={
                level: [structure_values[level]]
                for level in levels
            },
        )
    )
    eligible = build_eligible_snapshot(parsed.snapshot)

    first = select_channel(
        eligible,
        parsed.request,
        "distance_matched_random_control",
        limit=3,
    )
    second = select_channel(
        eligible,
        parsed.request,
        "distance_matched_random_control",
        limit=3,
    )

    assert [item.source_id for item in first] == [item.source_id for item in second]
    assert {item.source_id for item in first} <= eligible.allowlist
    assert len(first) == 3
    assert all(item.source_id.startswith("matched-") for item in first)
    assert all(item.lexical_overlap == 0.0 for item in first)

    from dataclasses import replace

    seen = {
        tuple(
            item.source_id
            for item in select_channel(
                eligible,
                replace(parsed.request, seed=seed),
                "distance_matched_random_control",
                limit=3,
            )
        )
        for seed in range(8)
    }
    assert len(seen) > 1


def test_distance_matched_random_returns_empty_without_a_candidate_inside_caliper():
    structure_text = "E R C O unrelated"
    events = [
        event_payload(
            "structural-reference",
            structure_text,
            structure={
                "entities": [cue(structure_text, "E")],
                "roles_actions": [cue(structure_text, "R")],
                "causal_constraints": [cue(structure_text, "C")],
                "transformations_outcomes": [cue(structure_text, "O")],
            },
        ),
        event_payload("far-control", "alpha beta gamma"),
    ]
    parsed = parse_scenario(
        scenario_payload(
            events,
            channels=["distance_matched_random_control"],
            tension="alpha beta gamma",
            tension_structure={
                "entities": ["E"],
                "roles_actions": ["R"],
                "causal_constraints": ["C"],
                "transformations_outcomes": ["O"],
            },
        )
    )

    selected = select_channel(
        build_eligible_snapshot(parsed.snapshot),
        parsed.request,
        "distance_matched_random_control",
        limit=3,
    )

    assert selected == ()


def test_direct_window_contains_late_hit_and_preserves_exact_crlf_whitespace():
    late_text = " \r\n\t" + ("x" * 1_700) + " needle " + ("y" * 100)
    late = parse_scenario(
        scenario_payload(
            [event_payload("late-source", late_text)],
            channels=["direct"],
            tension="needle",
        )
    )

    late_material = select_channel(
        build_eligible_snapshot(late.snapshot),
        late.request,
        "direct",
        limit=1,
    )[0]
    late_excerpt = late_material.text[
        late_material.span_start : late_material.span_end
    ]

    assert late_material.span_start > 0
    assert late_material.span_end - late_material.span_start == 1_200
    assert "needle" in late_excerpt
    assert late_excerpt == late_text[
        late_material.span_start : late_material.span_end
    ]

    leading_text = "\r\n  needle🙂e\u0301 remains exact"
    leading = parse_scenario(
        scenario_payload(
            [event_payload("leading-source", leading_text)],
            channels=["direct"],
            tension="needle",
        )
    )
    leading_material = select_channel(
        build_eligible_snapshot(leading.snapshot),
        leading.request,
        "direct",
        limit=1,
    )[0]

    assert leading_material.span_start == 0
    assert leading_material.span_end == len(leading_text)
    assert leading_material.text[
        leading_material.span_start : leading_material.span_end
    ] == leading_text


def test_structure_claims_only_include_evidence_inside_the_bounded_citation():
    text = "EARLY" + ("x" * 1_800) + "LATE"
    event = event_payload(
        "spread-evidence",
        text,
        structure={
            "entities": [],
            "roles_actions": [
                cue(text, "first mechanism", "EARLY"),
                cue(text, "second mechanism", "LATE"),
            ],
            "causal_constraints": [],
            "transformations_outcomes": [],
        },
    )
    parsed = parse_scenario(
        scenario_payload(
            [event],
            channels=["structural_distant"],
            tension="unrelated question",
            tension_structure={
                "entities": [],
                "roles_actions": ["first mechanism", "second mechanism"],
                "causal_constraints": [],
                "transformations_outcomes": [],
            },
        )
    )

    material = select_channel(
        build_eligible_snapshot(parsed.snapshot),
        parsed.request,
        "structural_distant",
        limit=1,
    )[0]

    assert material.span_end - material.span_start == 1_200
    assert material.shared_structure == ("first mechanism",)
    assert [item.value for item in material.structure_evidence] == [
        "first mechanism"
    ]
    assert all(
        material.span_start <= item.start < item.end <= material.span_end
        for item in material.structure_evidence
    )
    assert material.structure_evidence[0].source_hash == source_hash(text)
    assert material.structure_evidence[0].extractor == "synthetic-fixture-v1"
    assert material.structure_evidence[0].prompt_version == "fixture-no-model-v1"


def test_structure_window_finds_dense_pair_that_cue_centred_windows_miss():
    text = ("x" * 500) + "FIRST" + ("x" * 1_095) + "SECOND" + ("x" * 600)
    parsed = parse_scenario(
        scenario_payload(
            [
                event_payload(
                    "dense-pair",
                    text,
                    structure={
                        "entities": [],
                        "roles_actions": [
                            cue(text, "first mechanism", "FIRST"),
                            cue(text, "second mechanism", "SECOND"),
                        ],
                        "causal_constraints": [],
                        "transformations_outcomes": [],
                    },
                )
            ],
            channels=["structural_distant"],
            tension="unrelated question",
            tension_structure={
                "entities": [],
                "roles_actions": ["first mechanism", "second mechanism"],
                "causal_constraints": [],
                "transformations_outcomes": [],
            },
        )
    )

    material = select_channel(
        build_eligible_snapshot(parsed.snapshot),
        parsed.request,
        "structural_distant",
        limit=1,
    )[0]

    assert material.span_end - material.span_start == 1_200
    assert material.shared_structure == (
        "first mechanism",
        "second mechanism",
    )
    assert [item.value for item in material.structure_evidence] == [
        "first mechanism",
        "second mechanism",
    ]
    assert all(
        material.span_start <= item.start < item.end <= material.span_end
        for item in material.structure_evidence
    )


def test_integrity_rejects_shifted_or_missing_structure_evidence():
    from dataclasses import replace

    from spark_r1.integrity import verify_draft

    text = "EARLY mechanism remains auditable"
    parsed = parse_scenario(
        scenario_payload(
            [
                event_payload(
                    "evidence-source",
                    text,
                    structure={
                        "entities": [],
                        "roles_actions": [
                            cue(text, "shared mechanism", "EARLY")
                        ],
                        "causal_constraints": [],
                        "transformations_outcomes": [],
                    },
                )
            ],
            channels=["structural_distant"],
            tension="unrelated question",
            tension_structure={
                "entities": [],
                "roles_actions": ["shared mechanism"],
                "causal_constraints": [],
                "transformations_outcomes": [],
            },
        )
    )
    eligible = build_eligible_snapshot(parsed.snapshot)
    material = select_channel(
        eligible,
        parsed.request,
        "structural_distant",
        limit=1,
    )[0]
    evidence = material.structure_evidence[0]
    shifted = replace(evidence, start=evidence.start + 1)
    shifted_material = replace(material, structure_evidence=(shifted,))
    missing_material = replace(material, structure_evidence=())

    def draft_for(selected):
        return GeneratedDraft(
            source_id=selected.source_id,
            source_hash=selected.source_hash,
            span_start=selected.span_start,
            span_end=selected.span_end,
            material=selected.text[selected.span_start : selected.span_end],
            question="这项结构映射是否成立？",
            shared_structure=selected.shared_structure,
            mismatch=selected.mismatch,
            assumptions=("仍需独立核验。",),
        )

    with pytest.raises(SparkIntegrityError, match="structure evidence"):
        verify_draft(
            draft=draft_for(shifted_material),
            material=shifted_material,
            final_snapshot=eligible,
        )
    with pytest.raises(SparkIntegrityError, match="structure requires"):
        verify_draft(
            draft=draft_for(missing_material),
            material=missing_material,
            final_snapshot=eligible,
        )


@pytest.mark.asyncio
async def test_family_deduplication_normalizes_and_backfills_a_later_family():
    levels = (
        "entities",
        "roles_actions",
        "causal_constraints",
        "transformations_outcomes",
    )
    tokens = {
        "entities": "E",
        "roles_actions": "R",
        "causal_constraints": "C",
        "transformations_outcomes": "O",
    }
    values = {
        "entities": "system",
        "roles_actions": "reduce",
        "causal_constraints": "limited",
        "transformations_outcomes": "stable",
    }

    def same_family_event(source_id, *, fullwidth=False, duplicate=False):
        text = f"E R C O {source_id}"
        structure = {}
        for level in levels:
            value = values[level]
            if fullwidth:
                value = "".join(
                    chr(ord(char) + 0xFEE0) if "!" <= char <= "~" else char
                    for char in value
                )
            entries = [cue(text, value, tokens[level])]
            if duplicate:
                entries.append(cue(text, value, tokens[level]))
            structure[level] = entries
        return event_payload(source_id, text, structure=structure)

    distinct_text = "X Y C Z distinct"
    events = [
        same_family_event("same-0"),
        same_family_event("same-1", fullwidth=True),
        same_family_event("same-2", duplicate=True),
        event_payload(
            "distinct-4",
            distinct_text,
            structure={
                "entities": [],
                "roles_actions": [],
                "causal_constraints": [
                    cue(distinct_text, values["causal_constraints"], "C")
                ],
                "transformations_outcomes": [],
            },
        ),
    ]
    parsed = parse_scenario(
        scenario_payload(
            events,
            channels=["structural_distant"],
            tension="current dilemma",
            tension_structure={
                level: [values[level]]
                for level in levels
            },
            max_candidates=3,
        )
    )

    response = await run_spark(
        SyntheticSnapshotProvider(parsed.snapshot),
        parsed.request,
    )

    assert [item.source_id for item in response.candidates] == [
        "same-0",
        "distinct-4",
    ]
    assert len(response.candidates) <= 3


@pytest.mark.asyncio
async def test_uniform_control_keeps_three_source_uniform_draws_from_one_family():
    events = []
    for index in range(8):
        text = f"shared pattern {index}"
        events.append(
            event_payload(
                f"source-{index}",
                text,
                structure={
                    "entities": [cue(text, "same family", "shared")],
                    "roles_actions": [],
                    "causal_constraints": [],
                    "transformations_outcomes": [],
                },
            )
        )
    parsed = parse_scenario(
        scenario_payload(
            events,
            channels=["uniform_random_control"],
            seed=3,
            max_candidates=3,
        )
    )

    response = await run_spark(
        SyntheticSnapshotProvider(parsed.snapshot),
        parsed.request,
    )

    assert len(response.candidates) == 3
    assert len({item.source_id for item in response.candidates}) == 3
    assert all(
        item.channel == "uniform_random_control"
        for item in response.candidates
    )


@pytest.mark.asyncio
async def test_direct_keeps_three_sources_from_one_structure_family_without_evidence():
    events = []
    for index in range(3):
        text = f"shared direct pattern {index}"
        events.append(
            event_payload(
                f"direct-{index}",
                text,
                structure={
                    "entities": [cue(text, "same family", "shared")],
                    "roles_actions": [],
                    "causal_constraints": [],
                    "transformations_outcomes": [],
                },
            )
        )
    parsed = parse_scenario(
        scenario_payload(
            events,
            channels=["direct"],
            tension="shared direct pattern",
            max_candidates=3,
        )
    )

    response = await run_spark(
        SyntheticSnapshotProvider(parsed.snapshot),
        parsed.request,
    )

    assert len(response.candidates) == 3
    assert all(candidate.structure_evidence == () for candidate in response.candidates)
    assert {
        candidate.shared_structure
        for candidate in response.candidates
    } == {("候选关联仍需当前模型独立核验",)}


@pytest.mark.asyncio
async def test_two_synthetic_boundaries_run_concurrently_without_cross_source_cache():
    left_text = "左侧合成边界的唯一材料"
    right_text = "右侧合成边界的唯一材料"
    left = parse_scenario(
        scenario_payload(
            [event_payload("shared-id", left_text)],
            boundary_id="synthetic:left",
            channels=["uniform_random_control"],
        )
    )
    right = parse_scenario(
        scenario_payload(
            [event_payload("shared-id", right_text)],
            boundary_id="synthetic:right",
            channels=["uniform_random_control"],
        )
    )

    left_response, right_response = await asyncio.gather(
        run_spark(SyntheticSnapshotProvider(left.snapshot), left.request),
        run_spark(SyntheticSnapshotProvider(right.snapshot), right.request),
    )

    assert left_response.candidates[0].material == left_text
    assert right_response.candidates[0].material == right_text
    assert left_response.snapshot_hash != right_response.snapshot_hash


@pytest.mark.asyncio
async def test_aggregate_manifest_contains_no_body_source_id_or_tension():
    parsed = parse_scenario(scenario_payload(channels=["direct"]))
    response = await run_spark(
        SyntheticSnapshotProvider(parsed.snapshot),
        parsed.request,
    )

    manifest = aggregate_manifest(response)
    rendered = str(manifest)

    assert manifest["content_retained"] is False
    assert "candidates" not in manifest
    assert response.candidates[0].source_id not in rendered
    assert response.candidates[0].material not in rendered
    assert parsed.request.tension not in rendered


@pytest.mark.asyncio
async def test_aggregate_manifest_hashes_untrusted_instrument_identifiers():
    marker = "source-id-shaped-model-marker"
    parsed = parse_scenario(
        scenario_payload(
            [event_payload(marker, "资源受限时保持服务可靠")],
            channels=["direct"],
        )
    )

    class IdentifierInjectingGenerator:
        async def generate(self, request):
            return GenerationResult(
                draft=_draft_from_input(request),
                metadata=InstrumentMetadata(
                    provider=marker,
                    requested_model=marker,
                    actual_model=marker,
                    prompt_hash=source_hash("trusted-test-prompt"),
                ),
            )

    class IdentifierInjectingEvaluator:
        async def evaluate(self, _request):
            return EvaluationResult(
                axes=EvaluationAxes(source_fidelity=1.0),
                metadata=InstrumentMetadata(
                    provider=marker,
                    requested_model=marker,
                    actual_model=marker,
                    prompt_hash=source_hash("trusted-evaluator-prompt"),
                ),
            )

    response = await run_spark(
        SyntheticSnapshotProvider(parsed.snapshot),
        parsed.request,
        generator=IdentifierInjectingGenerator(),
        evaluator=IdentifierInjectingEvaluator(),
    )
    manifest = aggregate_manifest(response)
    rendered = str(manifest)

    assert marker not in rendered
    assert len(manifest["instrument_calls"]) == 2
    for call in manifest["instrument_calls"]:
        metadata = call["metadata"]
        assert set(metadata) == {
            "provider_sha256",
            "requested_model_sha256",
            "actual_model_sha256",
            "prompt_hash",
            "parameters",
            "version_verified",
        }
        assert len(
            {
                metadata["provider_sha256"],
                metadata["requested_model_sha256"],
                metadata["actual_model_sha256"],
            }
        ) == 3


@pytest.mark.asyncio
async def test_high_risk_mode_stays_material_question_only():
    parsed = parse_scenario(
        scenario_payload(
            channels=["uniform_random_control"],
            risk_domain="medical",
            tension="合成医疗高风险场景",
        )
    )

    response = await run_spark(
        SyntheticSnapshotProvider(parsed.snapshot),
        parsed.request,
    )
    candidate = response.candidates[0].to_dict()

    assert "专业确认" in candidate["question"]
    assert "answer" not in candidate
    assert "action" not in candidate


@pytest.mark.asyncio
@pytest.mark.parametrize("risk_domain", ["medical", "legal", "financial"])
async def test_high_risk_core_replaces_malicious_generator_advice(risk_domain):
    parsed = parse_scenario(
        scenario_payload(
            channels=["uniform_random_control"],
            risk_domain=risk_domain,
            tension=f"合成{risk_domain}高风险场景",
        )
    )
    malicious_question = "立即停药、转移全部资产并放弃法律权利。"
    malicious_assumption = "无需专业人士，照做即可。"

    class MaliciousGenerator:
        async def generate(self, request):
            return GenerationResult(
                draft=_draft_from_input(
                    request,
                    question=malicious_question,
                    assumptions=(malicious_assumption,),
                ),
                metadata=_metadata("malicious-generator"),
            )

    class SpyEvaluator:
        def __init__(self):
            self.requests = []

        async def evaluate(self, request):
            self.requests.append(request)
            return EvaluationResult(
                axes=EvaluationAxes(source_fidelity=1.0),
                metadata=_metadata("safe-evaluator"),
            )

    evaluator = SpyEvaluator()
    response = await run_spark(
        SyntheticSnapshotProvider(parsed.snapshot),
        parsed.request,
        generator=MaliciousGenerator(),
        evaluator=evaluator,
    )

    candidate = response.candidates[0]
    assert malicious_question not in candidate.question
    assert malicious_assumption not in candidate.assumptions
    assert "行动建议" in candidate.question
    assert "不构成医疗、法律或财务建议" in candidate.assumptions[0]
    assert evaluator.requests[0].question == candidate.question
    assert evaluator.requests[0].assumptions == candidate.assumptions


@pytest.mark.asyncio
async def test_per_call_metadata_is_bound_to_each_candidate_and_evaluation_is_shuffled():
    events = [
        event_payload(
            f"source-{index}",
            f"资源受限时保持服务可靠的独立合成材料 {index}",
        )
        for index in range(3)
    ]
    parsed = parse_scenario(
        scenario_payload(
            events,
            channels=["direct"],
            seed=7,
            max_candidates=3,
        )
    )
    baseline = MaterialQuestionGenerator()

    class PerCallGenerator:
        async def generate(self, request):
            result = await baseline.generate(request)
            return GenerationResult(
                draft=result.draft,
                metadata=_metadata(f"gen-{request.source_id}"),
            )

    class OrderedSpyEvaluator:
        def __init__(self):
            self.material_order: list[str] = []

        async def evaluate(self, request):
            self.material_order.append(request.material)
            call_number = len(self.material_order)
            return EvaluationResult(
                axes=EvaluationAxes(
                    source_fidelity=1.0,
                    structural_fit=0.5,
                    appropriateness=0.5,
                    novelty_evidence=0.5,
                    usefulness=0.5,
                    risk=0.0,
                ),
                metadata=_metadata(f"eval-call-{call_number}"),
            )

    evaluator = OrderedSpyEvaluator()
    response = await run_spark(
        SyntheticSnapshotProvider(parsed.snapshot),
        parsed.request,
        generator=PerCallGenerator(),
        evaluator=evaluator,
    )

    response_order = [candidate.material for candidate in response.candidates]
    assert sorted(evaluator.material_order) == sorted(response_order)
    assert evaluator.material_order != response_order
    for candidate in response.candidates:
        assert (
            candidate.generation_instrument.actual_model
            == f"gen-{candidate.source_id}"
        )
    candidates_by_material = {
        candidate.material: candidate for candidate in response.candidates
    }
    for call_number, material in enumerate(evaluator.material_order, start=1):
        evaluation_instrument = candidates_by_material[material].evaluation_instrument
        assert evaluation_instrument is not None
        assert evaluation_instrument.actual_model == f"eval-call-{call_number}"
    assert [call.phase for call in response.instrument_calls] == [
        "generator",
        "generator",
        "generator",
        "evaluator",
        "evaluator",
        "evaluator",
    ]
    assert all(call.status == "ok" for call in response.instrument_calls)


@pytest.mark.asyncio
async def test_generator_and_evaluator_must_be_isolated_objects():
    parsed = parse_scenario(scenario_payload(channels=["direct"]))
    baseline = MaterialQuestionGenerator()

    class DualInstrument:
        async def generate(self, request):
            return await baseline.generate(request)

        async def evaluate(self, _request):
            return EvaluationResult(
                axes=EvaluationAxes(),
                metadata=_metadata("dual-evaluator"),
            )

    dual = DualInstrument()
    with pytest.raises(SparkInputError):
        await run_spark(
            SyntheticSnapshotProvider(parsed.snapshot),
            parsed.request,
            generator=dual,
            evaluator=dual,
        )


@pytest.mark.asyncio
async def test_non_timeout_evaluator_failure_fails_whole_call_without_provider_detail():
    parsed = parse_scenario(scenario_payload(channels=["direct"]))
    marker = "evaluator-secret-must-not-surface"

    class ExplodingEvaluator:
        async def evaluate(self, _request):
            raise RuntimeError(marker)

    with pytest.raises(SparkInstrumentError) as captured:
        await run_spark(
            SyntheticSnapshotProvider(parsed.snapshot),
            parsed.request,
            evaluator=ExplodingEvaluator(),
        )

    assert marker not in str(captured.value)
    assert captured.value.__cause__ is None


@pytest.mark.asyncio
async def test_raw_protocol_objects_fail_the_whole_call():
    parsed = parse_scenario(scenario_payload(channels=["direct"]))
    baseline = MaterialQuestionGenerator()

    class LegacyGenerator:
        async def generate(self, request):
            return (await baseline.generate(request)).draft

    with pytest.raises(SparkInstrumentError):
        await run_spark(
            SyntheticSnapshotProvider(parsed.snapshot),
            parsed.request,
            generator=LegacyGenerator(),
        )


@pytest.mark.asyncio
async def test_structural_channel_without_required_tension_is_call_input_error():
    parsed = parse_scenario(
        scenario_payload(
            channels=["structural_distant"],
            tension_structure={
                "entities": [],
                "roles_actions": [],
                "causal_constraints": [],
                "transformations_outcomes": [],
            },
        )
    )

    with pytest.raises(SparkInputError):
        await run_spark(
            SyntheticSnapshotProvider(parsed.snapshot),
            parsed.request,
        )


@pytest.mark.asyncio
async def test_total_timeout_fails_whole_call_instead_of_returning_partial():
    parsed = parse_scenario(scenario_payload(channels=["direct"]))

    class SlowGenerator:
        async def generate(self, _request):
            await asyncio.sleep(0.2)
            raise AssertionError("cancelled generator must not return")

    with pytest.raises(SparkInstrumentError, match="total timeout"):
        await run_spark(
            SyntheticSnapshotProvider(parsed.snapshot),
            parsed.request,
            generator=SlowGenerator(),
            channel_timeout_seconds=1.0,
            total_timeout_seconds=0.05,
        )


@pytest.mark.asyncio
async def test_response_rejects_forged_instrument_call_consistency():
    parsed = parse_scenario(scenario_payload(channels=["direct"]))
    response = await run_spark(
        SyntheticSnapshotProvider(parsed.snapshot),
        parsed.request,
    )
    successful_call = response.instrument_calls[0]
    timeout_call = replace(successful_call, status="timeout", metadata=None)

    with pytest.raises(SparkIntegrityError, match="omitted channels"):
        replace(
            response,
            instrument_calls=(timeout_call,),
        )
    with pytest.raises(SparkIntegrityError, match="omitted channels"):
        replace(
            response,
            status="partial",
            candidates=(),
            omitted_channels=("direct",),
        )

    forged_candidate = replace(
        response.candidates[0],
        generation_instrument=_metadata("forged-generator"),
    )
    with pytest.raises(SparkIntegrityError, match="generator metadata"):
        replace(
            response,
            candidates=(forged_candidate,),
        )


@pytest.mark.asyncio
async def test_response_rejects_candidate_call_id_reuse():
    parsed = parse_scenario(
        scenario_payload(
            [
                event_payload("source-0", "资源受限时保持服务可靠 0"),
                event_payload("source-1", "资源受限时保持服务可靠 1"),
            ],
            channels=["direct"],
            max_candidates=2,
        )
    )
    response = await run_spark(
        SyntheticSnapshotProvider(parsed.snapshot),
        parsed.request,
    )
    reused = replace(
        response.candidates[1],
        generation_call_id=response.candidates[0].generation_call_id,
        generation_instrument=response.candidates[0].generation_instrument,
    )

    with pytest.raises(SparkIntegrityError, match="reuse a generator call"):
        replace(
            response,
            candidates=(response.candidates[0], reused),
        )
