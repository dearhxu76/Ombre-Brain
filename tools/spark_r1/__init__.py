"""Spark R1 纯合成、只读研究 harness。

该包位于仓库根 `tools/`，不属于产品 `src/tools`，不会进入 Docker 镜像、
Dashboard 热更新或 MCP 工具清单。
"""

from .core import (
    CandidateEvaluator,
    CandidateGenerator,
    MaterialQuestionGenerator,
    aggregate_manifest,
    run_spark,
)
from .integrity import SyntheticSnapshotProvider
from .models import (
    CueEvidence,
    EvaluationAxes,
    EvaluationInput,
    EvaluationResult,
    GeneratedDraft,
    GenerationInput,
    GenerationResult,
    InstrumentCall,
    InstrumentMetadata,
    SparkBoundaryError,
    SparkCandidate,
    SparkChannelError,
    SparkError,
    SparkInputError,
    SparkInstrumentError,
    SparkIntegrityError,
    SparkPolicyError,
    SparkRequest,
    SparkResponse,
    StructureEvidence,
    StructuralProjection,
    SyntheticEvent,
    SyntheticSnapshot,
    TensionStructure,
    source_hash,
)
from .schema import ParsedScenario, parse_scenario, strict_json_loads

__all__ = [
    "CandidateEvaluator",
    "CandidateGenerator",
    "CueEvidence",
    "EvaluationAxes",
    "EvaluationInput",
    "EvaluationResult",
    "GeneratedDraft",
    "GenerationInput",
    "GenerationResult",
    "InstrumentCall",
    "InstrumentMetadata",
    "MaterialQuestionGenerator",
    "ParsedScenario",
    "SparkBoundaryError",
    "SparkCandidate",
    "SparkChannelError",
    "SparkError",
    "SparkInputError",
    "SparkInstrumentError",
    "SparkIntegrityError",
    "SparkPolicyError",
    "SparkRequest",
    "SparkResponse",
    "StructureEvidence",
    "StructuralProjection",
    "SyntheticEvent",
    "SyntheticSnapshot",
    "SyntheticSnapshotProvider",
    "TensionStructure",
    "aggregate_manifest",
    "parse_scenario",
    "run_spark",
    "source_hash",
    "strict_json_loads",
]
