"""Spark R2 的最小 aggregate-only shadow runner。

公开入口只接受已经通过 R1 严格 schema 的合成场景和两个有界超时。它不接受路径、
owner、输出位置、模型 adapter、回调或工具对象。
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from typing import Any

from spark_r1 import (
    ParsedScenario,
    SparkBoundaryError,
    SparkInputError,
    SyntheticSnapshotProvider,
    aggregate_manifest,
    run_spark,
)

from .vault import SHADOW_SCHEMA_VERSION, _EphemeralShadowVault


_R1_AGGREGATE_FIELDS = frozenset(
    {
        "candidate_count",
        "channels",
        "complete",
        "content_retained",
        "data_cutoff",
        "denial_counts",
        "instrument_calls",
        "lifetime",
        "persistent",
        "policy_version",
        "retrievable",
        "schema_version",
        "seed",
        "snapshot_hash",
        "status",
    }
)
_ALLOWED_STATUSES = frozenset({"ok", "partial", "empty"})
_ALLOWED_PHASES = frozenset({"generator", "evaluator"})
_ALLOWED_CALL_STATUSES = frozenset({"ok", "timeout"})


def _plain_int(value: Any, label: str, *, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise SparkBoundaryError(f"{label} is invalid at the shadow boundary")
    return value


def _plain_bool(value: Any, label: str) -> bool:
    if type(value) is not bool:
        raise SparkBoundaryError(f"{label} is invalid at the shadow boundary")
    return value


@dataclass(frozen=True, slots=True)
class ShadowManifest:
    """可离开 shadow 生命周期的最小、无正文研究汇总。"""

    status: str
    complete: bool
    candidate_count: int
    channels: tuple[tuple[str, int, bool], ...]
    denial_counts: tuple[tuple[str, int], ...]
    instrument_counts: tuple[tuple[str, str, int], ...]
    seed: int
    policy_version: str
    vault_cleaned: bool

    def __post_init__(self) -> None:
        if self.status not in _ALLOWED_STATUSES:
            raise SparkBoundaryError("shadow status is invalid")
        _plain_bool(self.complete, "shadow completeness")
        _plain_int(self.candidate_count, "shadow candidate count")
        if self.candidate_count > 3:
            raise SparkBoundaryError("shadow candidate count exceeds the approved limit")
        if not isinstance(self.channels, tuple) or not self.channels:
            raise SparkBoundaryError("shadow channels must be a non-empty tuple")
        for channel, count, omitted in self.channels:
            if not isinstance(channel, str) or not channel:
                raise SparkBoundaryError("shadow channel is invalid")
            _plain_int(count, "shadow channel count")
            _plain_bool(omitted, "shadow omitted state")
        if len({channel for channel, _, _ in self.channels}) != len(self.channels):
            raise SparkBoundaryError("shadow channels cannot contain duplicates")
        if sum(count for _, count, _ in self.channels) != self.candidate_count:
            raise SparkBoundaryError("shadow channel counts do not match candidate count")
        if not isinstance(self.denial_counts, tuple):
            raise SparkBoundaryError("shadow denial counts must be immutable")
        for reason, count in self.denial_counts:
            if not isinstance(reason, str) or not reason:
                raise SparkBoundaryError("shadow denial reason is invalid")
            _plain_int(count, "shadow denial count")
        if len({reason for reason, _ in self.denial_counts}) != len(self.denial_counts):
            raise SparkBoundaryError("shadow denial reasons cannot contain duplicates")
        if not isinstance(self.instrument_counts, tuple):
            raise SparkBoundaryError("shadow instrument counts must be immutable")
        for phase, status, count in self.instrument_counts:
            if phase not in _ALLOWED_PHASES or status not in _ALLOWED_CALL_STATUSES:
                raise SparkBoundaryError("shadow instrument aggregate is invalid")
            _plain_int(count, "shadow instrument count")
        if len({(phase, status) for phase, status, _ in self.instrument_counts}) != len(
            self.instrument_counts
        ):
            raise SparkBoundaryError("shadow instrument aggregates cannot contain duplicates")
        _plain_int(self.seed, "shadow seed")
        if self.policy_version != "spark-r1-policy/1":
            raise SparkBoundaryError("shadow policy version is unsupported")
        if self.vault_cleaned is not True:
            raise SparkBoundaryError("shadow manifest cannot escape before vault cleanup")

    def to_dict(self) -> dict[str, Any]:
        """返回字段固定、无正文、无来源标识的可序列化对象。"""

        return {
            "schema_version": SHADOW_SCHEMA_VERSION,
            "mode": "shadow",
            "status": self.status,
            "complete": self.complete,
            "candidate_count": self.candidate_count,
            "channels": [
                {
                    "channel": channel,
                    "candidate_count": count,
                    "omitted": omitted,
                }
                for channel, count, omitted in self.channels
            ],
            "denial_counts": dict(self.denial_counts),
            "instrument_counts": [
                {"phase": phase, "status": status, "count": count}
                for phase, status, count in self.instrument_counts
            ],
            "seed": self.seed,
            "policy_version": self.policy_version,
            "persistent": False,
            "lifetime": "shadow_aggregate_only",
            "retrievable": False,
            "published": False,
            "user_visible": False,
            "content_retained": False,
            "candidate_material_retained": False,
            "source_identifiers_retained": False,
            "instructions": False,
            "may_call_tools": False,
            "network_used": False,
            "product_runtime": False,
            "mcp_exposed": False,
            "dream_schema_changed": False,
            "vault_kind": "ephemeral_test_data",
            "vault_cleaned": self.vault_cleaned,
        }


def _project_aggregate(
    aggregate: dict[str, Any],
    *,
    vault_cleaned: bool,
) -> ShadowManifest:
    if type(aggregate) is not dict or set(aggregate) != _R1_AGGREGATE_FIELDS:
        raise SparkBoundaryError("R1 aggregate schema changed outside the R2 allowlist")
    if any(
        (
            aggregate["schema_version"] != 1,
            aggregate["persistent"] is not False,
            aggregate["lifetime"] != "aggregate_no_body",
            aggregate["retrievable"] is not False,
            aggregate["content_retained"] is not False,
        )
    ):
        raise SparkBoundaryError("R1 aggregate lifecycle escaped the shadow boundary")
    status = aggregate["status"]
    if status not in _ALLOWED_STATUSES:
        raise SparkBoundaryError("R1 aggregate status is invalid")
    complete = _plain_bool(aggregate["complete"], "R1 aggregate completeness")
    candidate_count = _plain_int(
        aggregate["candidate_count"],
        "R1 aggregate candidate count",
    )

    raw_channels = aggregate["channels"]
    if type(raw_channels) is not list or not raw_channels:
        raise SparkBoundaryError("R1 aggregate channels are invalid")
    channels: list[tuple[str, int, bool]] = []
    for item in raw_channels:
        if type(item) is not dict or set(item) != {
            "candidate_count",
            "channel",
            "omitted",
        }:
            raise SparkBoundaryError("R1 channel aggregate schema changed")
        channel = item["channel"]
        if not isinstance(channel, str) or not channel:
            raise SparkBoundaryError("R1 aggregate channel is invalid")
        channels.append(
            (
                channel,
                _plain_int(item["candidate_count"], "R1 channel count"),
                _plain_bool(item["omitted"], "R1 omitted state"),
            )
        )

    raw_denials = aggregate["denial_counts"]
    if type(raw_denials) is not dict:
        raise SparkBoundaryError("R1 denial aggregate is invalid")
    denial_counts: list[tuple[str, int]] = []
    for reason, count in sorted(raw_denials.items()):
        if not isinstance(reason, str) or not reason:
            raise SparkBoundaryError("R1 denial reason is invalid")
        denial_counts.append((reason, _plain_int(count, "R1 denial count")))

    raw_calls = aggregate["instrument_calls"]
    if type(raw_calls) is not list:
        raise SparkBoundaryError("R1 instrument aggregate is invalid")
    instrument_counter: Counter[tuple[str, str]] = Counter()
    for call in raw_calls:
        if type(call) is not dict or set(call) != {
            "call_id",
            "channel",
            "metadata",
            "phase",
            "status",
        }:
            raise SparkBoundaryError("R1 instrument call schema changed")
        phase = call["phase"]
        call_status = call["status"]
        if phase not in _ALLOWED_PHASES or call_status not in _ALLOWED_CALL_STATUSES:
            raise SparkBoundaryError("R1 instrument call escaped the shadow allowlist")
        instrument_counter[(phase, call_status)] += 1

    seed = _plain_int(aggregate["seed"], "R1 aggregate seed")
    policy_version = aggregate["policy_version"]
    if not isinstance(policy_version, str):
        raise SparkBoundaryError("R1 policy version is invalid")
    manifest = ShadowManifest(
        status=status,
        complete=complete,
        candidate_count=candidate_count,
        channels=tuple(channels),
        denial_counts=tuple(denial_counts),
        instrument_counts=tuple(
            (phase, call_status, count)
            for (phase, call_status), count in sorted(instrument_counter.items())
        ),
        seed=seed,
        policy_version=policy_version,
        vault_cleaned=vault_cleaned,
    )
    if manifest.complete is not (manifest.status != "partial"):
        raise SparkBoundaryError("shadow completeness does not match its status")
    return manifest


async def run_shadow(
    scenario: ParsedScenario,
    *,
    channel_timeout_seconds: float = 30.0,
    total_timeout_seconds: float = 120.0,
) -> ShadowManifest:
    """在一次性 test_data Markdown vault 上运行并只返回无正文聚合。"""

    if not isinstance(scenario, ParsedScenario):
        raise SparkInputError("run_shadow requires a ParsedScenario")
    vault = _EphemeralShadowVault(scenario)
    projection: dict[str, Any]
    with vault:
        round_tripped = vault.read_scenario()
        response = await run_spark(
            SyntheticSnapshotProvider(round_tripped.snapshot),
            round_tripped.request,
            channel_timeout_seconds=channel_timeout_seconds,
            total_timeout_seconds=total_timeout_seconds,
        )
        projection = aggregate_manifest(response)
        del response, round_tripped
    return _project_aggregate(projection, vault_cleaned=vault.cleaned)
