"""40 个冻结的纯合成 Spark Pilot 场景族。

每个场景都只存在于内存，包含词项复述、结构远邻、反例、随机干扰项和一个应被
policy 排除的隐藏结构项。结构标注由确定性 fixture 直接给出，不调用模型。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from spark_r1 import (
    ParsedScenario,
    SparkRequest,
    TensionStructure,
    parse_scenario,
    source_hash,
)

from .protocol import PILOT_SEED, SCENARIO_FAMILY_COUNT


_LEVELS = (
    "entities",
    "roles_actions",
    "causal_constraints",
    "transformations_outcomes",
)


@dataclass(frozen=True, slots=True)
class _Archetype:
    entity: str
    role_action: str
    constraint: str
    outcome: str
    counter_outcome: str
    tension_problem: str


@dataclass(frozen=True, slots=True)
class _Surface:
    current_context: str
    distant_context: str
    counter_context: str
    risk_domain: str


@dataclass(frozen=True, slots=True)
class PilotScenario:
    """一个固定场景及其结构打乱请求。"""

    scenario_id: str
    parsed: ParsedScenario
    scrambled_request: SparkRequest


_ARCHETYPES = (
    _Archetype(
        "受约束协作体",
        "分阶段暴露",
        "反馈存在滞后",
        "提前纠偏",
        "放大故障",
        "一次铺开可能掩盖早期偏差",
    ),
    _Archetype(
        "多路径系统",
        "保留冗余路径",
        "存在单点失效",
        "维持连续",
        "冗余同步失败",
        "关键环节只有一条可用通路",
    ),
    _Archetype(
        "流动网络",
        "削减入口负载",
        "下游容量固定",
        "恢复流动",
        "上游积压恶化",
        "新增请求持续快于处理速度",
    ),
    _Archetype(
        "反馈回路",
        "缩短反馈周期",
        "信号存在延迟",
        "更早校正",
        "噪声被放大",
        "判断总在结果发生很久以后才回来",
    ),
    _Archetype(
        "选择过程",
        "先做可逆试验",
        "当前信息不足",
        "降低锁定成本",
        "临时方案固化",
        "一旦选定就很难撤回但证据仍不充分",
    ),
    _Archetype(
        "适应性组合",
        "保留异质备选",
        "环境高度不确定",
        "提高适应性",
        "协调成本失控",
        "未来条件摇摆而候选方案正在变得单一",
    ),
    _Archetype(
        "波动调节器",
        "设置分段阈值",
        "资源持续波动",
        "避免过载",
        "阈值发生振荡",
        "负荷忽高忽低且统一规则频繁失效",
    ),
    _Archetype(
        "耦合组件群",
        "隔离故障域",
        "风险沿耦合传播",
        "限制影响范围",
        "隔离阻断协作",
        "一个局部问题可能迅速牵连全部参与者",
    ),
)

_SURFACES = (
    _Surface("社区节庆筹备", "轨道望远镜调试", "社区仓储轮值", "general"),
    _Surface("独立杂志排期", "潮间带修复工程", "编辑部旧刊迁移", "general"),
    _Surface("流动诊所排班", "低温物流中转", "门诊档案交接", "medical"),
    _Surface("互助合作社预算", "离网电池阵列", "合作社票据归档", "financial"),
    _Surface("公益法律援助排班", "远洋船队补给", "案卷数字化小组", "legal"),
)

_DISTRACTORS = (
    "陶窑师傅记录釉面颜色随湿度缓慢变化。",
    "候鸟观察员按月更换望远镜的防尘镜片。",
    "剧团服装组重新编号了仓库中的旧纽扣。",
    "果园工人比较了三种梨树的开花日期。",
    "地图修复员为褪色海图补录了纸张纤维信息。",
    "钟表匠把不同年代的发条按长度排成样本册。",
)


def _cue(text: str, value: str) -> dict[str, Any]:
    start = text.index(value)
    return {"value": value, "start": start, "end": start + len(value)}


def _event(
    source_id: str,
    text: str,
    *,
    structure: dict[str, tuple[str, ...]] | None = None,
    visibility: str = "normal",
) -> dict[str, Any]:
    values = structure or {level: () for level in _LEVELS}
    return {
        "source_id": source_id,
        "text": text,
        "event_type": "dynamic",
        "status": "active",
        "visibility": visibility,
        "resolved": False,
        "anchor": False,
        "dont_surface": False,
        "digested": False,
        "pinned": False,
        "protected": False,
        "permanent": False,
        "structure": {
            "source_hash": source_hash(text),
            "extractor": "spark-pilot-fixture-v1",
            "prompt_version": "deterministic-no-model-v1",
            **{
                level: [_cue(text, value) for value in values.get(level, ())]
                for level in _LEVELS
            },
        },
    }


def _structure(archetype: _Archetype, outcome: str) -> dict[str, tuple[str, ...]]:
    return {
        "entities": (archetype.entity,),
        "roles_actions": (archetype.role_action,),
        "causal_constraints": (archetype.constraint,),
        "transformations_outcomes": (outcome,),
    }


def _scenario(index: int, archetype: _Archetype, surface: _Surface) -> PilotScenario:
    scenario_id = f"pilot-{index:03d}"
    tension = (
        f"{surface.current_context}遇到一个抉择：{archetype.tension_problem}。"
        "在作出决定前，需要一个可核验而非替代判断的新观察角度。"
    )
    direct_text = (
        f"旧讨论只是重复了这句话：{tension}"
        "它没有说明可迁移的关系，也没有新增证据。"
    )
    structural_text = (
        f"{surface.distant_context}中的{archetype.entity}曾{archetype.role_action}。"
        f"当时{archetype.constraint}，结果是{archetype.outcome}。"
    )
    counter_text = (
        f"{surface.current_context}旁支的{surface.counter_context}包含{archetype.entity}，"
        f"同样{archetype.role_action}并且{archetype.constraint}，"
        f"但结果变成{archetype.counter_outcome}。"
    )
    hidden_text = (
        f"隐藏草稿声称{archetype.entity}通过{archetype.role_action}应对"
        f"{archetype.constraint}并得到{archetype.outcome}。"
    )
    events = [
        _event(f"{scenario_id}-direct", direct_text),
        _event(
            f"{scenario_id}-structural",
            structural_text,
            structure=_structure(archetype, archetype.outcome),
        ),
        _event(
            f"{scenario_id}-counter",
            counter_text,
            structure=_structure(archetype, archetype.counter_outcome),
        ),
        *[
            _event(
                f"{scenario_id}-distractor-{position}",
                f"{text} 合成样本序号为{index}-{position}。",
            )
            for position, text in enumerate(_DISTRACTORS, start=1)
        ],
        _event(
            f"{scenario_id}-hidden",
            hidden_text,
            structure=_structure(archetype, archetype.outcome),
            visibility="hidden",
        ),
    ]
    tension_structure = {
        "entities": [archetype.entity],
        "roles_actions": [archetype.role_action],
        "causal_constraints": [archetype.constraint],
        "transformations_outcomes": [archetype.outcome],
    }
    payload = {
        "schema_version": 1,
        "synthetic": True,
        "boundary_id": f"synthetic:{scenario_id}",
        "data_cutoff": "2026-07-30T00:00:00+08:00",
        "tension": tension,
        "tension_structure": tension_structure,
        "channels": ["structural_distant"],
        "seed": PILOT_SEED + index,
        "inspiration_requested": True,
        "max_candidates": 1,
        "risk_domain": surface.risk_domain,
        "events": events,
    }
    parsed = parse_scenario(payload)
    scrambled_from = _ARCHETYPES[index % len(_ARCHETYPES)]
    scrambled_request = SparkRequest(
        tension=parsed.request.tension,
        tension_structure=TensionStructure(
            entities=(archetype.entity,),
            roles_actions=(scrambled_from.role_action,),
            causal_constraints=(scrambled_from.constraint,),
            transformations_outcomes=(scrambled_from.outcome,),
        ),
        channels=("structural_distant",),
        seed=parsed.request.seed,
        inspiration_requested=True,
        max_candidates=1,
        risk_domain=parsed.request.risk_domain,
    )
    return PilotScenario(
        scenario_id=scenario_id,
        parsed=parsed,
        scrambled_request=scrambled_request,
    )


def pilot_scenarios() -> tuple[PilotScenario, ...]:
    """按冻结顺序返回 8 个结构原型 × 5 个表面域的 40 个场景。"""

    scenarios: list[PilotScenario] = []
    index = 1
    for archetype in _ARCHETYPES:
        for surface in _SURFACES:
            scenarios.append(_scenario(index, archetype, surface))
            index += 1
    if len(scenarios) != SCENARIO_FAMILY_COUNT:
        raise RuntimeError("Spark Pilot scenario count drifted from the frozen protocol")
    return tuple(scenarios)
