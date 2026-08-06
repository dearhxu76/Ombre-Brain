"""Spark R2 aggregate-only shadow 的输出与生命周期契约。"""

from __future__ import annotations

import asyncio
import inspect

import pytest

from tests.spark_r1_support import event_payload, scenario_payload

from spark_r1 import SparkInputError, parse_scenario
from spark_shadow import ShadowManifest, run_shadow
from spark_shadow.vault import _EphemeralShadowVault


@pytest.mark.asyncio
async def test_shadow_returns_only_smaller_no_body_aggregate():
    marker_id = "source-must-not-escape-shadow"
    marker_body = "正文不得离开 aggregate-only shadow 生命周期"
    scenario = parse_scenario(
        scenario_payload(
            [event_payload(marker_id, marker_body)],
            channels=["uniform_random_control"],
        )
    )

    manifest = await run_shadow(scenario)
    data = manifest.to_dict()
    rendered = repr(data)

    assert isinstance(manifest, ShadowManifest)
    assert data["mode"] == "shadow"
    assert data["status"] == "ok"
    assert data["candidate_count"] == 1
    assert marker_id not in rendered
    assert marker_body not in rendered
    assert "snapshot_hash" not in data
    assert "data_cutoff" not in data
    assert "instrument_calls" not in data
    assert "candidates" not in data
    assert data["content_retained"] is False
    assert data["candidate_material_retained"] is False
    assert data["source_identifiers_retained"] is False


@pytest.mark.asyncio
async def test_shadow_boundary_flags_are_fixed_and_vault_is_cleaned():
    manifest = await run_shadow(parse_scenario(scenario_payload(channels=["direct"])))
    data = manifest.to_dict()

    assert {
        "persistent": data["persistent"],
        "retrievable": data["retrievable"],
        "published": data["published"],
        "user_visible": data["user_visible"],
        "instructions": data["instructions"],
        "may_call_tools": data["may_call_tools"],
        "network_used": data["network_used"],
        "product_runtime": data["product_runtime"],
        "mcp_exposed": data["mcp_exposed"],
        "dream_schema_changed": data["dream_schema_changed"],
    } == {
        "persistent": False,
        "retrievable": False,
        "published": False,
        "user_visible": False,
        "instructions": False,
        "may_call_tools": False,
        "network_used": False,
        "product_runtime": False,
        "mcp_exposed": False,
        "dream_schema_changed": False,
    }
    assert data["lifetime"] == "shadow_aggregate_only"
    assert data["vault_kind"] == "ephemeral_test_data"
    assert data["vault_cleaned"] is True


def test_shadow_public_api_accepts_no_path_owner_adapter_callback_or_tool():
    forbidden = {
        "owner",
        "owner_id",
        "vault",
        "vault_path",
        "buckets_dir",
        "path",
        "output",
        "generator",
        "evaluator",
        "callback",
        "tool",
        "manager",
    }

    assert set(inspect.signature(run_shadow).parameters) == {
        "scenario",
        "channel_timeout_seconds",
        "total_timeout_seconds",
    }
    assert forbidden.isdisjoint(inspect.signature(run_shadow).parameters)


@pytest.mark.parametrize("value", [None, {}, scenario_payload()])
@pytest.mark.asyncio
async def test_shadow_requires_the_frozen_parsed_scenario_contract(value):
    with pytest.raises(SparkInputError):
        await run_shadow(value)


@pytest.mark.parametrize("mode", ["missing", "false"])
def test_shadow_scenario_still_fails_closed_without_explicit_inspiration(mode):
    payload = scenario_payload()
    if mode == "missing":
        del payload["inspiration_requested"]
    else:
        payload["inspiration_requested"] = False

    with pytest.raises(SparkInputError):
        parse_scenario(payload)


@pytest.mark.asyncio
async def test_shadow_uses_distinct_owned_vaults_for_concurrent_runs(monkeypatch):
    roots = []
    original_enter = _EphemeralShadowVault.__enter__

    def tracking_enter(vault):
        entered = original_enter(vault)
        roots.append(vault.root)
        return entered

    monkeypatch.setattr(_EphemeralShadowVault, "__enter__", tracking_enter)
    scenario = parse_scenario(scenario_payload(channels=["direct"]))

    first, second = await asyncio.gather(run_shadow(scenario), run_shadow(scenario))

    assert first.vault_cleaned is True
    assert second.vault_cleaned is True
    assert len(roots) == 2
    assert roots[0] != roots[1]
    assert all(not root.exists() for root in roots)
