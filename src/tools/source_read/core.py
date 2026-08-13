"""按桶 ID 与精确标题读取单条记忆背后的不可变原文证据。"""

from __future__ import annotations

import asyncio
import unicodedata
from typing import Any

from ombrebrain.storage.source_store import normalize_source_refs
from utils import count_tokens_approx, normalize_memory_title

from .. import _runtime as rt
from ..plan.core import is_letter_bucket, letter_lock_state


_DEFAULT_MAX_TOKENS = 6000
_MAX_MAX_TOKENS = 20000
# count_tokens_approx 至少为每个字符计 0.05 token。捕获每 token 20
# 个字符已足以选出能放入预算的最长前缀，同时把常驻分页缓冲限制在
# 公开最大预算下约 400 KiB。
_MAX_CHARS_PER_TOKEN = 20


def _normalized_title(value: object) -> str:
    try:
        return normalize_memory_title(value)
    except ValueError:
        # 兼容可能早于标题上限而存在的历史桶。
        return " ".join(unicodedata.normalize("NFC", str(value or "")).split())


def _single_line_header_value(value: object) -> str:
    """防止历史数据中的控制字符伪造响应头。"""

    output: list[str] = []
    for char in _normalized_title(value):
        if char == "\\":
            output.append("\\\\")
        elif unicodedata.category(char).startswith("C"):
            output.append(f"\\u{ord(char):04x}")
        else:
            output.append(char)
    return "".join(output)


def _collect_evidence_window(
    source_store: Any,
    source_refs: list[dict[str, Any]],
    *,
    scope: str,
    cursor: int,
    capture_limit: int,
) -> tuple[str, int]:
    """每次只读一个源，并且只保留当前分页需要的窗口。"""

    page_start = cursor
    page_end = cursor + capture_limit
    page_parts: list[str] = []
    total_chars = 0
    emitted_chunks = 0
    seen_full_sources: set[str] = set()

    def append_segment(segment: str) -> None:
        nonlocal total_chars
        segment_start = total_chars
        total_chars += len(segment)
        overlap_start = max(page_start, segment_start)
        overlap_end = min(page_end, total_chars)
        if overlap_start < overlap_end:
            page_parts.append(
                segment[overlap_start - segment_start : overlap_end - segment_start]
            )

    for source_ref in source_refs:
        ref = source_ref["ref"]
        if scope == "full_source":
            if ref in seen_full_sources:
                continue
            seen_full_sources.add(ref)
        elif not source_ref["ranges"]:
            # 空范围表示“未声明事件摘录”，绝不表示“全文”。
            continue

        content = source_store.read(ref)
        if scope == "event":
            content = source_store.select_ranges(content, source_ref["ranges"])
        if emitted_chunks:
            append_segment("\n\n")
        append_segment(content)
        emitted_chunks += 1

    return "".join(page_parts), total_chars


def _render_page(
    *,
    bucket_id: str,
    title: str,
    scope: str,
    cursor: int,
    body: str,
    total_chars: int,
) -> str:
    end = cursor + len(body)
    next_cursor = end if end < total_chars else 0
    header = (
        f"bucket_id={_single_line_header_value(bucket_id)}\n"
        f"title={_single_line_header_value(title)}\n"
        f"scope={scope}\n"
        f"cursor={cursor}\n"
        f"next_cursor={next_cursor}\n"
        f"total_chars={total_chars}\n"
    )
    return header + "\n" + body


async def dispatch(
    bucket_id: str,
    expected_title: str,
    scope: str = "event",
    cursor: int = 0,
    max_tokens: int = _DEFAULT_MAX_TOKENS,
) -> str:
    bucket_id = str(bucket_id or "").strip()
    expected_title = _normalized_title(expected_title)
    scope = str(scope or "event").strip().lower()
    if not bucket_id or not expected_title:
        return "source_read 需要 bucket_id 和 expected_title。"
    if scope not in {"event", "full_source"}:
        return "scope 仅支持 event 或 full_source。"
    try:
        cursor = max(0, int(cursor))
        max_tokens = max(200, min(_MAX_MAX_TOKENS, int(max_tokens)))
    except (TypeError, ValueError, OverflowError):
        return "cursor 和 max_tokens 必须是整数。"

    getter = getattr(rt.bucket_mgr, "get_including_archive", rt.bucket_mgr.get)
    bucket = await getter(bucket_id)
    if not bucket:
        return f"未找到桶 {bucket_id}。"
    if is_letter_bucket(bucket) and letter_lock_state(bucket, "ai")["locked"]:
        return "这封信尚未向你开放，拒绝读取其原文证据。"
    metadata = bucket.get("metadata") or {}
    actual_title = _normalized_title(metadata.get("title"))
    if not actual_title:
        return "该桶没有可供精确校验的显式标题，拒绝读取原文。"
    if expected_title != actual_title:
        return "标题不匹配，拒绝读取原文。请使用该桶的精确 title。"

    try:
        source_refs = normalize_source_refs(metadata.get("source_refs") or [])
    except ValueError:
        return "该桶的原文证据引用格式无效，拒绝读取。"
    if not source_refs:
        return "该桶没有原文证据引用。"
    if scope == "event" and not any(item["ranges"] for item in source_refs):
        return (
            "该桶未声明事件原文范围，拒绝将整份原文作为事件返回。"
            "如确需整份原文，请显式使用 scope=full_source。"
        )

    try:
        evidence_window, total_chars = await asyncio.to_thread(
            _collect_evidence_window,
            rt.source_store,
            source_refs,
            scope=scope,
            cursor=cursor,
            capture_limit=max_tokens * _MAX_CHARS_PER_TOKEN,
        )
    except (OSError, UnicodeError, ValueError):
        return "原文证据读取或完整性校验失败。"
    if cursor >= total_chars:
        return f"原文已读完（cursor={cursor}，总字符 {total_chars}）。"

    low, high = 0, len(evidence_window)
    chosen = 0
    while low <= high:
        mid = (low + high) // 2
        candidate = _render_page(
            bucket_id=bucket_id,
            title=actual_title,
            scope=scope,
            cursor=cursor,
            body=evidence_window[:mid],
            total_chars=total_chars,
        )
        if count_tokens_approx(candidate) <= max_tokens:
            chosen = mid
            low = mid + 1
        else:
            high = mid - 1

    if chosen == 0:
        return "max_tokens 不足以容纳原文分页头，请提高后重试。"
    return _render_page(
        bucket_id=bucket_id,
        title=actual_title,
        scope=scope,
        cursor=cursor,
        body=evidence_window[:chosen],
        total_chars=total_chars,
    )
