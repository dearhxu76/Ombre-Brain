"""Spark R2 临时 vault、CLI 与产品隔离安全回归。"""

from __future__ import annotations

import ast
import importlib.util
import inspect
import json
import os
import re
import subprocess
import sys

import pytest

from tests.spark_r1_support import ROOT, scenario_payload

from spark_r1 import SparkBoundaryError, SparkError, SparkInputError, parse_scenario
from spark_shadow import run_shadow
from spark_shadow.vault import (
    _EphemeralShadowVault,
    _parse_markdown,
)


SHADOW_PACKAGE = ROOT / "tools" / "spark_shadow"
SHADOW_CLI = ROOT / "tools" / "evaluate_spark_shadow.py"


def _run_cli(payload: dict, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SHADOW_CLI), *args],
        input=json.dumps(payload, ensure_ascii=False),
        text=True,
        encoding="utf-8",
        capture_output=True,
        cwd=str(ROOT),
        timeout=15,
        check=False,
    )


def _load_cli_module():
    spec = importlib.util.spec_from_file_location("spark_shadow_cli_under_test", SHADOW_CLI)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_vault_markdown_round_trip_requires_test_data_provenance():
    scenario = parse_scenario(scenario_payload(channels=["direct"]))

    with _EphemeralShadowVault(scenario) as vault:
        bucket = vault.root / "test_data" / "bucket-0001.md"
        rendered = bucket.read_text(encoding="utf-8")
        assert "created_by: spark-shadow-r2" in rendered
        assert "erasable: true" in rendered
        assert "kind: test" in rendered
        assert vault.read_scenario() == scenario
        root = vault.root

    assert vault.cleaned is True
    assert not root.exists()


@pytest.mark.parametrize(
    ("original", "replacement"),
    [
        ("kind: test", "kind: real"),
        ("type: dynamic", "type: anchor"),
        ("spark-shadow-0001", "spark-shadow-9999"),
    ],
)
def test_vault_rejects_tampered_bucket_metadata(original, replacement):
    scenario = parse_scenario(scenario_payload(channels=["direct"]))

    with _EphemeralShadowVault(scenario) as vault:
        bucket = vault.root / "test_data" / "bucket-0001.md"
        text = bucket.read_text(encoding="utf-8")
        assert original in text
        bucket.write_text(text.replace(original, replacement, 1), encoding="utf-8")
        with pytest.raises(SparkError):
            vault.read_scenario()


def test_vault_rejects_tampered_body_marker_and_unexpected_entries():
    scenario = parse_scenario(scenario_payload(channels=["direct"]))

    with _EphemeralShadowVault(scenario) as vault:
        bucket = vault.root / "test_data" / "bucket-0001.md"
        bucket.write_text(bucket.read_text(encoding="utf-8") + "篡改", encoding="utf-8")
        with pytest.raises(SparkError):
            vault.read_scenario()

    with _EphemeralShadowVault(scenario) as vault:
        marker = vault.root / ".spark-shadow-owner.json"
        marker.write_text("{}", encoding="utf-8")
        with pytest.raises(SparkError):
            vault.read_scenario()

    with _EphemeralShadowVault(scenario) as vault:
        (vault.root / "unexpected.txt").write_text("unexpected", encoding="utf-8")
        with pytest.raises(SparkBoundaryError):
            vault.read_scenario()


def test_vault_rejects_yaml_aliases_and_duplicate_frontmatter_keys():
    duplicate = b"---\nid: one\nid: two\ntype: dynamic\nprovenance: {}\nspark_shadow_payload: '{}'\n---\nbody"
    alias = b"---\nid: &id one\ntype: dynamic\nprovenance: {}\nspark_shadow_payload: *id\n---\nbody"

    with pytest.raises(SparkInputError):
        _parse_markdown(duplicate)
    with pytest.raises(SparkInputError):
        _parse_markdown(alias)


def test_vault_rejects_symlinked_bucket_when_platform_allows_symlinks(tmp_path):
    scenario = parse_scenario(scenario_payload(channels=["direct"]))

    with _EphemeralShadowVault(scenario) as vault:
        bucket = vault.root / "test_data" / "bucket-0001.md"
        target = tmp_path / "target.md"
        target.write_bytes(bucket.read_bytes())
        bucket.unlink()
        try:
            os.symlink(target, bucket)
        except (NotImplementedError, OSError):
            pytest.skip("当前平台或权限不允许创建测试符号链接")
        with pytest.raises(SparkBoundaryError):
            vault.read_scenario()


@pytest.mark.asyncio
async def test_shadow_cleans_owned_vault_after_success_and_failure(monkeypatch):
    import spark_shadow.core as core

    roots = []
    original_enter = _EphemeralShadowVault.__enter__

    def tracking_enter(vault):
        entered = original_enter(vault)
        roots.append(vault.root)
        return entered

    async def fail_run(*_args, **_kwargs):
        raise SparkInputError("forced shadow failure")

    monkeypatch.setattr(_EphemeralShadowVault, "__enter__", tracking_enter)
    scenario = parse_scenario(scenario_payload(channels=["direct"]))
    manifest = await run_shadow(scenario)
    assert manifest.vault_cleaned is True
    monkeypatch.setattr(core, "run_spark", fail_run)
    with pytest.raises(SparkInputError):
        await run_shadow(scenario)

    assert len(roots) == 2
    assert all(not root.exists() for root in roots)


@pytest.mark.asyncio
async def test_shadow_cleans_owned_vault_after_cancellation(monkeypatch):
    import asyncio
    import spark_shadow.core as core

    roots = []
    started = asyncio.Event()
    original_enter = _EphemeralShadowVault.__enter__

    def tracking_enter(vault):
        entered = original_enter(vault)
        roots.append(vault.root)
        return entered

    async def wait_until_cancelled(*_args, **_kwargs):
        started.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(_EphemeralShadowVault, "__enter__", tracking_enter)
    monkeypatch.setattr(core, "run_spark", wait_until_cancelled)
    scenario = parse_scenario(scenario_payload(channels=["direct"]))

    task = asyncio.create_task(run_shadow(scenario))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert len(roots) == 1
    assert not roots[0].exists()


def test_shadow_cli_never_offers_candidate_path_vault_or_owner_options():
    cli = _load_cli_module()
    option_strings = {
        option
        for action in cli.build_parser()._actions
        for option in action.option_strings
    }
    forbidden_options = {
        "--emit-candidates",
        "--owner",
        "--vault",
        "--vault-path",
        "--output",
        "--config",
    }

    assert forbidden_options.isdisjoint(option_strings)
    assert set(inspect.signature(run_shadow).parameters) == {
        "scenario",
        "channel_timeout_seconds",
        "total_timeout_seconds",
    }


@pytest.mark.parametrize(
    "args",
    [
        ("--emit-candidates",),
        ("--vault", "DO-NOT-ECHO-PATH"),
        ("--owner", "DO-NOT-ECHO-OWNER"),
        ("--output", "DO-NOT-ECHO-OUTPUT"),
    ],
)
def test_shadow_cli_rejects_forbidden_options_without_echo(args):
    result = _run_cli(scenario_payload(), *args)

    assert result.returncode == 2
    assert json.loads(result.stdout)["errors"] == [
        {"scope": "call", "code": "spark_shadow_argument_invalid"}
    ]
    assert "DO-NOT-ECHO" not in result.stdout
    assert "DO-NOT-ECHO" not in result.stderr
    assert result.stderr == ""


def test_shadow_cli_only_emits_aggregate_and_requires_inspiration_true():
    payload = scenario_payload(channels=["direct"])
    marker_id = payload["events"][0]["source_id"]
    marker_body = payload["events"][0]["text"]

    result = _run_cli(payload)

    assert result.returncode == 0, result.stderr
    data = json.loads(result.stdout)
    assert data["vault_cleaned"] is True
    assert "candidates" not in data
    assert "snapshot_hash" not in data
    assert marker_id not in result.stdout
    assert marker_body not in result.stdout

    payload["inspiration_requested"] = False
    denied = _run_cli(payload)
    assert denied.returncode == 2
    assert json.loads(denied.stdout)["errors"] == [
        {"scope": "call", "code": "spark_input_invalid"}
    ]


def test_shadow_package_has_no_product_network_process_or_model_client_imports():
    forbidden_prefixes = (
        "bucket_manager",
        "decay_engine",
        "source_store",
        "server",
        "web",
        "requests",
        "httpx",
        "urllib",
        "socket",
        "subprocess",
        "openai",
        "anthropic",
    )
    imports = set()
    for path in SHADOW_PACKAGE.glob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imports.add(node.module)

    assert not any(
        name == prefix or name.startswith(prefix + ".")
        for name in imports
        for prefix in forbidden_prefixes
    )


def test_shadow_remains_outside_product_mcp_and_docker_surfaces_after_product_opt_in():
    dockerignore = (ROOT / ".dockerignore").read_text(encoding="utf-8")
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    server = (ROOT / "src" / "server.py").read_text(encoding="utf-8")
    public_tools = (
        ROOT / "src" / "ombrebrain" / "protocol" / "public_tools.py"
    ).read_text(encoding="utf-8")

    assert re.search(r"(?m)^tools/$", dockerignore)
    assert "spark_shadow" not in dockerfile
    assert "spark_shadow" not in server
    assert "spark_shadow" not in public_tools
    assert len(re.findall(r"(?m)^@mcp\.tool\(\)", server)) == 16
    server_tree = ast.parse(server, filename=str(ROOT / "src" / "server.py"))
    dream_functions = [
        node
        for node in server_tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == "dream"
    ]
    assert len(dream_functions) == 1
    assert "inspiration" in {
        argument.arg for argument in dream_functions[0].args.args
    }
    assert "spark_shadow" not in (
        ROOT / "src" / "tools" / "dream" / "inspiration.py"
    ).read_text(encoding="utf-8")
