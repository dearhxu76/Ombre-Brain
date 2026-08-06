#!/usr/bin/env python3
"""制备 Spark Pilot 盲评包或分析 stdin 数值评分。

脚本只向 stdout 返回响应态盲评材料或 aggregate-no-body 分析；不接受 owner、vault、
路径、输出文件、模型、网络或产品配置。
"""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
import sys
from typing import Any


_TOOLS_ROOT = Path(__file__).resolve().parent
if str(_TOOLS_ROOT) not in sys.path:
    sys.path.insert(0, str(_TOOLS_ROOT))

from spark_pilot import (  # noqa: E402
    PilotInputError,
    analyze_ratings,
    prepare_blind_packet,
)


MAX_ANALYSIS_INPUT_BYTES = 5_000_000
MAX_JSON_DEPTH = 8
MAX_JSON_NODES = 100_000
_EXIT_FAILED = 2
_EXIT_INCOMPLETE = 3


class _ArgumentError(Exception):
    """不回显 argparse 的不受信原始错误。"""


class _SafeArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        del message
        raise _ArgumentError from None


def build_parser() -> argparse.ArgumentParser:
    parser = _SafeArgumentParser(
        description="制备固定 Spark Pilot 人工盲评包，或聚合分析 stdin 数值评分",
        allow_abbrev=False,
    )
    parser.add_argument("phase", choices=("prepare", "analyze"))
    return parser


def _reject_constant(_value: str) -> None:
    raise PilotInputError("JSON constants NaN and Infinity are not allowed")


def _object_without_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise PilotInputError("JSON contains a duplicate object key")
        result[key] = value
    return result


def _strict_json_loads(raw: bytes) -> dict[str, Any]:
    if not raw or len(raw) > MAX_ANALYSIS_INPUT_BYTES:
        raise PilotInputError("analysis input size is outside the allowed range")
    try:
        text = raw.decode("utf-8-sig", errors="strict")
        payload = json.loads(
            text,
            parse_constant=_reject_constant,
            object_pairs_hook=_object_without_duplicates,
        )
    except PilotInputError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError):
        raise PilotInputError("analysis input is not valid bounded UTF-8 JSON") from None
    nodes = 0
    stack: list[tuple[Any, int]] = [(payload, 1)]
    while stack:
        value, depth = stack.pop()
        nodes += 1
        if nodes > MAX_JSON_NODES or depth > MAX_JSON_DEPTH:
            raise PilotInputError("analysis JSON exceeds the structural budget")
        if isinstance(value, dict):
            stack.extend((item, depth + 1) for item in value.values())
        elif isinstance(value, list):
            stack.extend((item, depth + 1) for item in value)
        elif value is not None and not isinstance(value, (str, int, float, bool)):
            raise PilotInputError("analysis JSON contains an unsupported value type")
    if type(payload) is not dict:
        raise PilotInputError("analysis root must be an object")
    return payload


def _failed_payload(code: str) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "status": "failed",
        "complete": False,
        "persistent": False,
        "lifetime": "aggregate_no_body",
        "retrievable": False,
        "content_retained": False,
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


def main(argv: list[str] | None = None) -> int:
    try:
        args = build_parser().parse_args(argv)
    except _ArgumentError:
        _print_json(_failed_payload("spark_pilot_argument_invalid"))
        return _EXIT_FAILED

    raw = sys.stdin.buffer.read(MAX_ANALYSIS_INPUT_BYTES + 1)
    try:
        if args.phase == "prepare":
            if raw.strip():
                raise PilotInputError("prepare does not accept stdin content")
            payload = asyncio.run(prepare_blind_packet())
        else:
            payload = analyze_ratings(_strict_json_loads(raw))
    except PilotInputError as exc:
        _print_json(_failed_payload(exc.code))
        return _EXIT_FAILED
    except Exception:
        _print_json(_failed_payload("spark_pilot_internal_error"))
        return _EXIT_FAILED
    _print_json(payload)
    if payload.get("status") == "incomplete_descriptive_pilot":
        return _EXIT_INCOMPLETE
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
