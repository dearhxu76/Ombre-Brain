"""Spark R1 合成离线 harness。

核心顺序执行 policy、选择、generator、盲化 evaluator 与最终来源复核。它没有
文件、网络、日志、工具或 OB 运行时对象；外部实验仪器只能通过异步 Protocol 注入。
"""

from __future__ import annotations

import asyncio
from collections import Counter
from dataclasses import dataclass, replace
import math
import random
from typing import Any, Protocol

from .integrity import SyntheticSnapshotProvider, verify_draft
from .models import (
    EvaluationAxes,
    EvaluationInput,
    EvaluationResult,
    GeneratedDraft,
    GenerationInput,
    GenerationResult,
    InstrumentCall,
    InstrumentMetadata,
    SelectedMaterial,
    SparkCandidate,
    SparkChannelError,
    SparkInputError,
    SparkInstrumentError,
    SparkIntegrityError,
    SparkRequest,
    SparkResponse,
    source_hash,
)
from .policy import POLICY_VERSION, EligibleSnapshot, build_eligible_snapshot
from .selection import select_channel


class CandidateGenerator(Protocol):
    """隔离 generator 契约；adapter 自己负责客户端硬超时和版本回传。

    这是数据最小化边界，不是进程沙箱；注入对象仍须由 harness 所有者信任，
    并传播 ``CancelledError``、配置底层硬超时且不得遗留后台任务。
    """

    async def generate(self, request: GenerationInput) -> GenerationResult:
        """生成一个不带行动权、且带本次实际仪器元数据的材料/问题草案。"""


class CandidateEvaluator(Protocol):
    """盲化 evaluator 契约；请求中没有通道、seed 或 generator 标识。

    这是数据最小化边界，不是进程沙箱；注入对象仍须由 harness 所有者信任，
    并传播 ``CancelledError``、配置底层硬超时且不得遗留后台任务。
    """

    async def evaluate(self, request: EvaluationInput) -> EvaluationResult:
        """返回带本次实际仪器元数据的分轴评审，不返回单一总分。"""


@dataclass(frozen=True, slots=True)
class _StagedCandidate:
    channel: str
    material: SelectedMaterial
    draft: GeneratedDraft
    generation_call_id: str
    generation_instrument: InstrumentMetadata
    evaluation: EvaluationAxes | None
    evaluation_call_id: str | None
    evaluation_instrument: InstrumentMetadata | None


class MaterialQuestionGenerator:
    """无网络的确定性基线，只把精确来源片段转成待核验问题。"""

    metadata = InstrumentMetadata(
        provider="local",
        requested_model="material-question-template-v1",
        actual_model="material-question-template-v1",
        prompt_hash=source_hash("spark-r1/local-material-question/v1"),
        parameters=(("output_mode", "material_question"),),
        version_verified=True,
    )

    async def generate(self, request: GenerationInput) -> GenerationResult:
        if request.risk_domain == "general":
            question = (
                "这段历史材料与当前张力可能共享哪些关系或约束？"
                "哪些事实差异会使这个类比不成立？"
            )
        else:
            question = (
                "在不形成行动建议的前提下，这段材料与当前张力有哪些待核验的结构联系、"
                "不对应处和需要专业确认的前提？"
            )
        return GenerationResult(
            draft=GeneratedDraft(
                source_id=request.source_id,
                source_hash=request.source_hash,
                span_start=request.span_start,
                span_end=request.span_end,
                material=request.material,
                question=question,
                shared_structure=request.shared_structure,
                mismatch=request.mismatch,
                assumptions=("共享结构是否真实存在，仍需当前模型结合本轮事实独立核验。",),
            ),
            metadata=self.metadata,
        )


MAX_TOTAL_TIMEOUT_SECONDS = 300.0


def _validate_timeout(
    value: float,
    field_name: str,
    *,
    maximum: float,
) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise SparkInputError(f"{field_name} must be numeric")
    timeout = float(value)
    if not math.isfinite(timeout) or timeout < 0.05 or timeout > maximum:
        raise SparkInputError(f"{field_name} must be within [0.05, {maximum:g}]")
    return timeout


def _require_generator(value: CandidateGenerator | None) -> CandidateGenerator:
    generator: CandidateGenerator = value if value is not None else MaterialQuestionGenerator()
    if not callable(getattr(generator, "generate", None)):
        raise SparkInputError("generator must implement async generate")
    return generator


def _require_evaluator(value: CandidateEvaluator | None) -> CandidateEvaluator | None:
    if value is None:
        return None
    if not callable(getattr(value, "evaluate", None)):
        raise SparkInputError("evaluator must implement async evaluate")
    return value


async def _generate(
    generator: CandidateGenerator,
    request: GenerationInput,
    timeout: float,
) -> GenerationResult:
    deadline = asyncio.get_running_loop().time() + timeout
    try:
        result = await asyncio.wait_for(
            generator.generate(request),
            timeout=timeout,
        )
    except asyncio.TimeoutError:
        raise SparkChannelError("generator timed out") from None
    except asyncio.CancelledError:
        raise
    except Exception:
        raise SparkInstrumentError(
            "generator failed without exposing provider details"
        ) from None
    if asyncio.get_running_loop().time() >= deadline:
        raise SparkChannelError("generator timed out")
    if not isinstance(result, GenerationResult):
        raise SparkInstrumentError("generator returned an invalid protocol object")
    return result


async def _evaluate(
    evaluator: CandidateEvaluator,
    request: EvaluationInput,
    timeout: float,
) -> EvaluationResult:
    deadline = asyncio.get_running_loop().time() + timeout
    try:
        result = await asyncio.wait_for(
            evaluator.evaluate(request),
            timeout=timeout,
        )
    except asyncio.TimeoutError:
        raise SparkChannelError("evaluator timed out") from None
    except asyncio.CancelledError:
        raise
    except Exception:
        raise SparkInstrumentError(
            "evaluator failed without exposing provider details"
        ) from None
    if asyncio.get_running_loop().time() >= deadline:
        raise SparkChannelError("evaluator timed out")
    if not isinstance(result, EvaluationResult):
        raise SparkInstrumentError("evaluator returned an invalid protocol object")
    return result


def _family_key(snapshot: EligibleSnapshot, material: SelectedMaterial) -> tuple[Any, ...]:
    source = snapshot.get(material.source_id)
    if source is None:
        raise SparkIntegrityError("selector returned a source outside the eligible allowlist")
    family = source.structure.family_key()
    if any(level for level in family):
        return ("structure", *family)
    return ("source", source.source_id)


def _allocate_quota_round_robin(
    snapshot: EligibleSnapshot,
    request: SparkRequest,
    selected: dict[str, tuple[SelectedMaterial, ...]],
) -> dict[str, tuple[SelectedMaterial, ...]]:
    """按通道轮询分配候选配额，不承诺后续 provider 调用或呈现顺序。"""
    allocated: dict[str, list[SelectedMaterial]] = {
        channel: [] for channel in request.channels
    }
    seen_sources: set[str] = set()
    seen_families: set[tuple[Any, ...]] = set()
    position = 0
    while sum(len(items) for items in allocated.values()) < request.max_candidates:
        progressed = False
        for channel in request.channels:
            items = selected.get(channel, ())
            if position >= len(items):
                continue
            material = items[position]
            progressed = True
            family = (
                _family_key(snapshot, material)
                if channel in {"structural_distant", "counterexample"}
                else ("source", material.source_id)
            )
            if material.source_id in seen_sources or family in seen_families:
                continue
            allocated[channel].append(material)
            seen_sources.add(material.source_id)
            seen_families.add(family)
            if sum(len(items) for items in allocated.values()) >= request.max_candidates:
                break
        if not progressed:
            break
        position += 1
    return {channel: tuple(items) for channel, items in allocated.items()}


def _refresh_unchanged(
    provider: SyntheticSnapshotProvider,
    initial: EligibleSnapshot,
    initial_revision: int,
) -> EligibleSnapshot:
    current_raw, revision = provider.current_state()
    if revision != initial_revision:
        raise SparkIntegrityError("synthetic snapshot changed during an async boundary")
    if current_raw.boundary_id != initial.boundary_id:
        raise SparkIntegrityError("synthetic boundary changed during an async boundary")
    if current_raw.data_cutoff != initial.data_cutoff:
        raise SparkIntegrityError("snapshot cutoff changed during an async boundary")
    current = build_eligible_snapshot(current_raw)
    if current.snapshot_hash != initial.snapshot_hash:
        raise SparkIntegrityError("synthetic snapshot integrity changed during an async boundary")
    return current


def _evaluation_shuffle_seed(
    snapshot: EligibleSnapshot,
    request: SparkRequest,
) -> int:
    material = (
        f"spark-r1/evaluator-order/v1:{request.seed}:{snapshot.snapshot_hash}"
    )
    return int(source_hash(material), 16)


async def _run_spark_once(
    provider: SyntheticSnapshotProvider,
    request: SparkRequest,
    *,
    generator: CandidateGenerator,
    evaluator: CandidateEvaluator | None,
    channel_timeout_seconds: float,
) -> SparkResponse:
    initial_raw, initial_revision = provider.current_state()
    if initial_raw.boundary_id != provider.boundary_id:
        raise SparkIntegrityError("provider boundary changed")
    initial = build_eligible_snapshot(initial_raw)

    selected_by_channel: dict[str, tuple[SelectedMaterial, ...]] = {}
    omitted: list[str] = []
    instrument_calls: list[InstrumentCall] = []

    def record_call(
        *,
        phase: str,
        channel: str,
        status: str,
        metadata: InstrumentMetadata | None,
    ) -> str:
        call_id = f"call_{len(instrument_calls) + 1}"
        instrument_calls.append(
            InstrumentCall(
                call_id=call_id,
                phase=phase,
                channel=channel,
                status=status,
                metadata=metadata,
            )
        )
        return call_id

    for channel in request.channels:
        try:
            selected_by_channel[channel] = select_channel(
                initial,
                request,
                channel,
                limit=max(1, len(initial.sources)),
            )
        except SparkChannelError:
            raise SparkInputError(
                f"{channel} channel prerequisites are not satisfied"
            ) from None

    allocated = _allocate_quota_round_robin(initial, request, selected_by_channel)
    staged: list[_StagedCandidate] = []
    for channel in request.channels:
        channel_candidates: list[_StagedCandidate] = []
        try:
            for material in allocated[channel]:
                exact_excerpt = material.text[
                    material.span_start : material.span_end
                ]
                generation = await _generate(
                    generator,
                    GenerationInput(
                        tension=request.tension,
                        risk_domain=request.risk_domain,
                        channel=channel,
                        source_id=material.source_id,
                        source_hash=material.source_hash,
                        span_start=material.span_start,
                        span_end=material.span_end,
                        material=exact_excerpt,
                        shared_structure=material.shared_structure,
                        mismatch=material.mismatch,
                    ),
                    channel_timeout_seconds,
                )
                generation_call_id = record_call(
                    phase="generator",
                    channel=channel,
                    status="ok",
                    metadata=generation.metadata,
                )
                refreshed = _refresh_unchanged(
                    provider,
                    initial,
                    initial_revision,
                )
                verify_draft(
                    draft=generation.draft,
                    material=material,
                    final_snapshot=refreshed,
                )
                draft = generation.draft
                if request.risk_domain != "general":
                    # 高风险问题与前提由核心固定，provider 草案不得成为行动建议出口。
                    draft = replace(
                        draft,
                        question=(
                            "在不形成行动建议的前提下，这段材料与当前张力有哪些待核验的"
                            "结构联系、不对应处和需要相应领域专业确认的前提？"
                        ),
                        assumptions=(
                            "该材料不构成医疗、法律或财务建议，任何现实行动均须由相应领域专业人士确认。",
                            "共享结构是否真实存在，仍需当前模型结合本轮事实独立核验。",
                        ),
                    )
                channel_candidates.append(
                    _StagedCandidate(
                        channel=channel,
                        material=material,
                        draft=draft,
                        generation_call_id=generation_call_id,
                        generation_instrument=generation.metadata,
                        evaluation=None,
                        evaluation_call_id=None,
                        evaluation_instrument=None,
                    )
                )
        except SparkChannelError:
            record_call(
                phase="generator",
                channel=channel,
                status="timeout",
                metadata=None,
            )
            omitted.append(channel)
            continue
        staged.extend(channel_candidates)

    if evaluator is not None and staged:
        evaluation_order = list(range(len(staged)))
        random.Random(
            _evaluation_shuffle_seed(initial, request)
        ).shuffle(evaluation_order)
        for index in evaluation_order:
            item = staged[index]
            if item.channel in omitted:
                continue
            refreshed = _refresh_unchanged(
                provider,
                initial,
                initial_revision,
            )
            exact_excerpt = verify_draft(
                draft=item.draft,
                material=item.material,
                final_snapshot=refreshed,
            )
            try:
                evaluation = await _evaluate(
                    evaluator,
                    EvaluationInput(
                        tension=request.tension,
                        risk_domain=request.risk_domain,
                        material=exact_excerpt,
                        question=item.draft.question,
                        shared_structure=item.draft.shared_structure,
                        mismatch=item.draft.mismatch,
                        assumptions=item.draft.assumptions,
                    ),
                    channel_timeout_seconds,
                )
            except SparkChannelError:
                record_call(
                    phase="evaluator",
                    channel=item.channel,
                    status="timeout",
                    metadata=None,
                )
                omitted.append(item.channel)
                continue
            evaluation_call_id = record_call(
                phase="evaluator",
                channel=item.channel,
                status="ok",
                metadata=evaluation.metadata,
            )
            refreshed = _refresh_unchanged(
                provider,
                initial,
                initial_revision,
            )
            verify_draft(
                draft=item.draft,
                material=item.material,
                final_snapshot=refreshed,
            )
            staged[index] = replace(
                item,
                evaluation=evaluation.axes,
                evaluation_call_id=evaluation_call_id,
                evaluation_instrument=evaluation.metadata,
            )

    omitted_set = set(omitted)
    staged = [
        item
        for item in staged
        if item.channel not in omitted_set
    ]
    if omitted_set and not staged:
        raise SparkInstrumentError(
            "all candidate channels timed out before producing a successful candidate"
        )
    final = _refresh_unchanged(provider, initial, initial_revision)

    candidates: list[SparkCandidate] = []
    for item in staged:
        exact_excerpt = verify_draft(
            draft=item.draft,
            material=item.material,
            final_snapshot=final,
        )
        candidates.append(
            SparkCandidate(
                candidate_id=f"candidate_{len(candidates) + 1}",
                channel=item.channel,
                source_id=item.material.source_id,
                source_hash=item.material.source_hash,
                span_start=item.material.span_start,
                span_end=item.material.span_end,
                material=exact_excerpt,
                question=item.draft.question,
                shared_structure=item.draft.shared_structure,
                structure_evidence=item.material.structure_evidence,
                mismatch=item.draft.mismatch,
                assumptions=item.draft.assumptions,
                selection_axes=(
                    ("lexical_overlap", item.material.lexical_overlap),
                    ("structural_fit", item.material.structural_fit),
                ),
                evaluation=item.evaluation,
                generation_call_id=item.generation_call_id,
                evaluation_call_id=item.evaluation_call_id,
                generation_instrument=item.generation_instrument,
                evaluation_instrument=item.evaluation_instrument,
            )
        )

    unique_omitted = tuple(
        channel for channel in request.channels if channel in omitted_set
    )
    if unique_omitted:
        status = "partial"
    elif candidates:
        status = "ok"
    else:
        status = "empty"
    return SparkResponse(
        status=status,
        candidates=tuple(candidates),
        requested_channels=request.channels,
        omitted_channels=unique_omitted,
        instrument_calls=tuple(instrument_calls),
        snapshot_hash=final.snapshot_hash,
        data_cutoff=final.data_cutoff,
        seed=request.seed,
        policy_version=POLICY_VERSION,
        denial_counts=final.denial_counts,
    )


async def run_spark(
    provider: SyntheticSnapshotProvider,
    request: SparkRequest,
    *,
    generator: CandidateGenerator | None = None,
    evaluator: CandidateEvaluator | None = None,
    channel_timeout_seconds: float = 30.0,
    total_timeout_seconds: float = 120.0,
) -> SparkResponse:
    """运行一次纯合成 Spark R1 调用。

    policy/snapshot/来源完整性、仪器协议和非超时 provider 错误均整次失败；
    只有单个异步仪器调用的 `asyncio.TimeoutError` 且仍有其他通道形成合格候选时，
    才可以省略故障通道并返回 `partial`；任何失败都不会触发普通 search 或
    未过滤回退。
    """

    if not isinstance(provider, SyntheticSnapshotProvider):
        raise SparkInputError("run_spark requires a SyntheticSnapshotProvider")
    if not isinstance(request, SparkRequest):
        raise SparkInputError("run_spark requires a SparkRequest")
    channel_timeout = _validate_timeout(
        channel_timeout_seconds,
        "channel_timeout_seconds",
        maximum=120.0,
    )
    total_timeout = _validate_timeout(
        total_timeout_seconds,
        "total_timeout_seconds",
        maximum=MAX_TOTAL_TIMEOUT_SECONDS,
    )
    active_generator = _require_generator(generator)
    active_evaluator = _require_evaluator(evaluator)
    if active_evaluator is active_generator:
        raise SparkInputError("generator and evaluator must be isolated objects")
    try:
        return await asyncio.wait_for(
            _run_spark_once(
                provider,
                request,
                generator=active_generator,
                evaluator=active_evaluator,
                channel_timeout_seconds=channel_timeout,
            ),
            timeout=total_timeout,
        )
    except asyncio.TimeoutError:
        raise SparkInstrumentError("Spark total timeout exceeded") from None


def aggregate_manifest(response: SparkResponse) -> dict[str, Any]:
    """生成可跨运行保留的无正文、无来源 ID 汇总清单。"""

    if not isinstance(response, SparkResponse):
        raise SparkInputError("aggregate_manifest requires a SparkResponse")

    def aggregate_metadata(
        metadata: InstrumentMetadata | None,
    ) -> dict[str, Any] | None:
        if metadata is None:
            return None

        def identifier_digest(field: str, value: str) -> str:
            return source_hash(
                f"spark-r1/aggregate-instrument-id/v1:{field}\0{value}"
            )

        # adapter 回传的标识属于不可信实验元数据。响应态保留原值以便当次审计，
        # 可持久聚合只保留域分离摘要，防止把正文或 source_id 偷塞进标识字段。
        return {
            "provider_sha256": identifier_digest(
                "provider",
                metadata.provider,
            ),
            "requested_model_sha256": identifier_digest(
                "requested_model",
                metadata.requested_model,
            ),
            "actual_model_sha256": identifier_digest(
                "actual_model",
                metadata.actual_model,
            ),
            "prompt_hash": metadata.prompt_hash,
            "parameters": {
                key: value
                for key, value in metadata.parameters
            },
            "version_verified": metadata.version_verified,
        }

    counts = Counter(candidate.channel for candidate in response.candidates)
    return {
        "schema_version": response.schema_version,
        "status": response.status,
        "complete": response.status != "partial",
        "persistent": False,
        "lifetime": "aggregate_no_body",
        "retrievable": False,
        "content_retained": False,
        "candidate_count": len(response.candidates),
        "channels": [
            {
                "channel": channel,
                "candidate_count": counts.get(channel, 0),
                "omitted": channel in response.omitted_channels,
            }
            for channel in response.requested_channels
        ],
        "snapshot_hash": response.snapshot_hash,
        "data_cutoff": response.data_cutoff,
        "seed": response.seed,
        "policy_version": response.policy_version,
        "denial_counts": {key: value for key, value in response.denial_counts},
        "instrument_calls": [
            {
                "call_id": call.call_id,
                "phase": call.phase,
                "channel": call.channel,
                "status": call.status,
                "metadata": aggregate_metadata(call.metadata),
            }
            for call in response.instrument_calls
        ],
    }
