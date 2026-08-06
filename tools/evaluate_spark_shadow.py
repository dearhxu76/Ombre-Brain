#!/usr/bin/env python3
"""从 stdin 运行 Spark R2 aggregate-only shadow。

脚本不接受候选输出、owner、vault、配置或输出路径；stdout 只返回无正文汇总。
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
from pathlib import Path
import sys
from typing import Any


_TOOLS_ROOT = Path(__file__).resolve().parent
if str(_TOOLS_ROOT) not in sys.path:
    sys.path.insert(0, str(_TOOLS_ROOT))

from spark_r1 import SparkError, parse_scenario  # noqa: E402
from spark_r1.core import MAX_TOTAL_TIMEOUT_SECONDS  # noqa: E402
from spark_r1.schema import MAX_INPUT_BYTES  # noqa: E402
from spark_shadow import run_shadow  # noqa: E402


_MIN_TIMEOUT_SECONDS = 0.05
_MAX_CHANNEL_TIMEOUT_SECONDS = 120.0
_EXIT_FAILED = 2
_EXIT_PARTIAL = 3


class _ArgumentError(Exception):
    """丢弃 argparse 原始报错，避免回显不受信参数。"""


class _SafeArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        del message
        raise _ArgumentError from None


def build_parser() -> argparse.ArgumentParser:
    parser = _SafeArgumentParser(
        description=(
            "运行仅接受 stdin 合成 JSON、仅返回无正文聚合且要求 "
            "inspiration_requested=true 的 Spark R2 shadow"
        ),
        allow_abbrev=False,
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=30.0,
        help="每个本地确定性通道的预算，范围 0.05–120 秒",
    )
    parser.add_argument(
        "--total-timeout-seconds",
        type=float,
        default=120.0,
        help=f"整次 shadow 的预算，范围 0.05–{MAX_TOTAL_TIMEOUT_SECONDS:g} 秒",
    )
    return parser


def _failed_payload(code: str) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "mode": "shadow",
        "status": "failed",
        "complete": False,
        "candidate_count": 0,
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
        "errors": [{"scope": "call", "code": code}],
    }


def _print_json(payload: dict[str, Any]) -> None:
    print(
        json.dumps(
            payload,
            ensure_ascii=True,
            sort_keys=True,
            indent=2,
            allow_nan=False,
        )
    )


def _validate_timeout(value: float, *, maximum: float) -> None:
    if not math.isfinite(value) or not _MIN_TIMEOUT_SECONDS <= value <= maximum:
        raise _ArgumentError from None


async def _run(args: argparse.Namespace, raw: bytes) -> dict[str, Any]:
    scenario = parse_scenario(raw)
    manifest = await run_shadow(
        scenario,
        channel_timeout_seconds=args.timeout_seconds,
        total_timeout_seconds=args.total_timeout_seconds,
    )
    return manifest.to_dict()


def main(argv: list[str] | None = None) -> int:
    try:
        args = build_parser().parse_args(argv)
        _validate_timeout(
            args.timeout_seconds,
            maximum=_MAX_CHANNEL_TIMEOUT_SECONDS,
        )
        _validate_timeout(
            args.total_timeout_seconds,
            maximum=MAX_TOTAL_TIMEOUT_SECONDS,
        )
    except _ArgumentError:
        _print_json(_failed_payload("spark_shadow_argument_invalid"))
        return _EXIT_FAILED

    raw = sys.stdin.buffer.read(MAX_INPUT_BYTES + 1)
    if len(raw) > MAX_INPUT_BYTES:
        _print_json(_failed_payload("spark_shadow_input_invalid"))
        return _EXIT_FAILED
    try:
        payload = asyncio.run(_run(args, raw))
    except SparkError as exc:
        _print_json(_failed_payload(exc.code))
        return _EXIT_FAILED
    except Exception:
        _print_json(_failed_payload("spark_shadow_internal_error"))
        return _EXIT_FAILED
    _print_json(payload)
    return _EXIT_PARTIAL if payload.get("status") == "partial" else 0


if __name__ == "__main__":
    raise SystemExit(main())
