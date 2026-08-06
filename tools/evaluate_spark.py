#!/usr/bin/env python3
"""从标准输入运行 Spark R1 纯合成研究场景。

输入场景必须显式声明 `inspiration_requested=true`。默认只输出不含正文和来源 ID
的聚合清单；只有显式传入 `--emit-candidates` 才把本次响应态候选写到 stdout。
脚本不接受 owner、vault、配置或输出文件路径。
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

from spark_r1 import (  # noqa: E402
    SparkError,
    SyntheticSnapshotProvider,
    aggregate_manifest,
    parse_scenario,
    run_spark,
)
from spark_r1.core import MAX_TOTAL_TIMEOUT_SECONDS  # noqa: E402
from spark_r1.schema import MAX_INPUT_BYTES  # noqa: E402


_MIN_TIMEOUT_SECONDS = 0.05
_MAX_CHANNEL_TIMEOUT_SECONDS = 120.0
_EXIT_FAILED = 2
_EXIT_PARTIAL = 3


class _ArgumentError(Exception):
    """不保存 argparse 原始错误文本，避免把用户输入回显到终端。"""


class _SafeArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        del message
        raise _ArgumentError from None


def build_parser() -> argparse.ArgumentParser:
    parser = _SafeArgumentParser(
        description=(
            "运行只接受 stdin 合成 JSON 且要求显式 inspiration_requested=true 的 "
            "Spark R1 离线研究 harness"
        ),
        allow_abbrev=False,
    )
    parser.add_argument(
        "--emit-candidates",
        action="store_true",
        help="显式把本次响应态候选写到 stdout；默认只输出无正文聚合清单",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=30.0,
        help=(
            "每个受信异步 generator/evaluator 调用的协作式取消预算，"
            "范围 0.05–120 秒"
        ),
    )
    parser.add_argument(
        "--total-timeout-seconds",
        type=float,
        default=120.0,
        help=(
            "整次离线评估的协作式取消预算，范围 "
            f"0.05–{MAX_TOTAL_TIMEOUT_SECONDS:g} 秒"
        ),
    )
    return parser


def _failed_payload(code: str) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "status": "failed",
        "complete": False,
        "persistent": False,
        "lifetime": "response_only",
        "retrievable": False,
        "candidate_count": 0,
        "candidates": [],
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


async def _run(args: argparse.Namespace, raw: bytes) -> dict[str, Any]:
    scenario = parse_scenario(raw)
    response = await run_spark(
        SyntheticSnapshotProvider(scenario.snapshot),
        scenario.request,
        channel_timeout_seconds=args.timeout_seconds,
        total_timeout_seconds=args.total_timeout_seconds,
    )
    return response.to_dict() if args.emit_candidates else aggregate_manifest(response)


def _validate_timeout(value: float, *, maximum: float) -> None:
    if not math.isfinite(value) or not _MIN_TIMEOUT_SECONDS <= value <= maximum:
        raise _ArgumentError from None


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
        _print_json(_failed_payload("spark_argument_invalid"))
        return _EXIT_FAILED

    raw = sys.stdin.buffer.read(MAX_INPUT_BYTES + 1)
    if len(raw) > MAX_INPUT_BYTES:
        _print_json(_failed_payload("spark_input_invalid"))
        return _EXIT_FAILED
    try:
        payload = asyncio.run(_run(args, raw))
    except SparkError as exc:
        _print_json(_failed_payload(exc.code))
        return _EXIT_FAILED
    except Exception:
        _print_json(_failed_payload("spark_internal_error"))
        return _EXIT_FAILED
    _print_json(payload)
    return _EXIT_PARTIAL if payload.get("status") == "partial" else 0


if __name__ == "__main__":
    raise SystemExit(main())
