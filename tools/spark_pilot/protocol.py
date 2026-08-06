"""Spark Pilot 的冻结协议常量与不透明标识。

本模块只描述离线合成研究条件，不注册产品工具，也不接受路径、owner、vault、
模型配置或持久化目标。
"""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256


SCHEMA_VERSION = 1
PILOT_PROTOCOL_ID = "spark-r1-pilot-40-v1"
PILOT_SEED = 20260730
SCENARIO_FAMILY_COUNT = 40
RATING_MIN = 0
RATING_MAX = 4

RATING_AXES = (
    "source_fidelity",
    "appropriateness",
    "nonredundant_direction",
    "structural_fit",
    "novelty_evidence",
    "usefulness",
    "false_premise",
    "potential_harm",
)
POSITIVE_AXES = (
    "source_fidelity",
    "appropriateness",
    "nonredundant_direction",
    "structural_fit",
    "novelty_evidence",
    "usefulness",
)
NEGATIVE_AXES = ("false_premise", "potential_harm")
PRIMARY_POSITIVE_THRESHOLD = 3
PRIMARY_NEGATIVE_THRESHOLD = 1


class PilotInputError(ValueError):
    """Pilot 输入或冻结协议不满足严格边界。"""

    code = "spark_pilot_input_invalid"


@dataclass(frozen=True, slots=True)
class PilotCondition:
    """一个固定、可复现的 R1 条件。"""

    condition_id: str
    channels: tuple[str, ...]
    max_candidates: int
    scrambled: bool = False


CONDITIONS = (
    PilotCondition("lexical_direct", ("direct",), 1),
    PilotCondition("uniform_random", ("uniform_random_control",), 1),
    PilotCondition(
        "distance_matched_random",
        ("distance_matched_random_control",),
        1,
    ),
    PilotCondition("structural_distant", ("structural_distant",), 1),
    PilotCondition(
        "structural_plus_counterexample",
        ("structural_distant", "counterexample"),
        2,
    ),
    PilotCondition(
        "mixed",
        ("direct", "structural_distant", "counterexample"),
        3,
    ),
    PilotCondition(
        "structural_scrambled",
        ("structural_distant",),
        1,
        scrambled=True,
    ),
)
CONDITION_IDS = tuple(condition.condition_id for condition in CONDITIONS)

UNAVAILABLE_CONDITIONS = (
    {
        "condition": "no_spark",
        "reason": "尚无冻结的宿主无-Spark配对输出，不能用空候选冒充基线。",
    },
    {
        "condition": "semantic_only",
        "reason": "R1 direct 是本地词项重叠，不是真正的语义检索基线。",
    },
    {
        "condition": "three_model_families",
        "reason": "本轮没有模型 API 仪器或真实模型调用凭据。",
    },
    {
        "condition": "isolated_model_evaluation",
        "reason": "本轮只制备人工盲评包，不用本地模板冒充独立模型评审。",
    },
    {
        "condition": "output_strength_ablation",
        "reason": "R1 当前只冻结 material_question 输出强度。",
    },
)


def _opaque_id(domain: str, *parts: str) -> str:
    material = "\0".join((f"spark-pilot/{domain}/v1", *parts))
    return sha256(material.encode("utf-8", errors="strict")).hexdigest()[:32]


def sample_id(scenario_id: str, condition_id: str) -> str:
    """生成不暴露条件名或场景序号的稳定样本 ID。"""

    return _opaque_id(PILOT_PROTOCOL_ID, scenario_id, condition_id)


def item_id(sample_identifier: str, position: int) -> str:
    """生成样本内不暴露来源 ID 的稳定候选项 ID。"""

    return _opaque_id("item", sample_identifier, str(position))


def primary_success(scores: dict[str, int]) -> int:
    """按冻结门禁计算二元主终点，不计算单一灵感总分。"""

    positives_pass = all(
        scores[axis] >= PRIMARY_POSITIVE_THRESHOLD
        for axis in (
            "source_fidelity",
            "appropriateness",
            "nonredundant_direction",
        )
    )
    negatives_pass = all(
        scores[axis] <= PRIMARY_NEGATIVE_THRESHOLD
        for axis in NEGATIVE_AXES
    )
    return int(positives_pass and negatives_pass)
