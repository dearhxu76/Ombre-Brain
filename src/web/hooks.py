"""
========================================
web/hooks.py — breath 浮现挂载点（HTTP hook）
========================================

- /breath-hook：对话开头由外部 hook 拉取，返回应浮现的记忆（pinned + 未解决采样）。
  主记忆段复用无参数 breath 的实时默认桶数/token 配置，不另设压缩或采样规则；
  Letter/I 是 SessionStart 专属补充，使用独立的小额度，不挤占主 breath 预算。
  protected 主池与 Letter/I 附加池都不通过 hook 主动注入。

不提供 /dream-hook：dream 按哲学不是义务、不该每次开场自动触发（详见下方端点处注释）。

给外部 SessionStart hook / 自动化用；默认需要 Dashboard 登录态或 hook token。
通过 sh.fire_webhook 推送事件。

对外暴露：register(mcp)。
========================================
"""

import asyncio
import os
import threading
import time
from collections import OrderedDict, deque
from contextlib import asynccontextmanager

from tools import breath as _t_breath
from tools.plan.core import (
    is_letter_bucket,
    letter_lock_state,
    normalize_expired_lock,
)

from . import _shared as sh

logger = sh.logger

_HOOK_CONCURRENCY = 2
_HOOK_RATE_WINDOW_SECONDS = 60.0
_HOOK_RATE_SOURCE_LIMIT = 10
_HOOK_RATE_GLOBAL_LIMIT = 60
_HOOK_RATE_SOURCE_CAP = 2048
_hook_slots = threading.BoundedSemaphore(_HOOK_CONCURRENCY)
_hook_rate_lock = threading.Lock()
_hook_source_events: OrderedDict[str, deque[float]] = OrderedDict()
_hook_global_events: deque[float] = deque()

try:
    from utils import count_tokens_approx, strip_wikilinks, get_ai_name  # type: ignore
except ImportError:  # pragma: no cover
    from ..utils import count_tokens_approx, strip_wikilinks, get_ai_name  # type: ignore


def _truthy(value) -> bool:
    return str(value or "").strip().lower() in ("1", "true", "yes", "on")


def _hook_setting(name: str, default=None):
    hooks_cfg = (getattr(sh, "config", {}) or {}).get("hooks") or {}
    return hooks_cfg.get(name, default)


def _header_value(request, name: str) -> str:
    headers = getattr(request, "headers", {}) or {}
    try:
        return str(headers.get(name, "") or "")
    except Exception:
        wanted = name.lower()
        for k, v in dict(headers).items():
            if str(k).lower() == wanted:
                return str(v or "")
    return ""


def _is_hook_request_authorized(request) -> bool:
    """Protect hook endpoints that can expose memory text.

    Public hooks can still be enabled deliberately with OMBRE_HOOK_ALLOW_PUBLIC=1
    or config hooks.allow_public=true. Otherwise a dashboard session or a hook
    token is required.
    """
    allow_public = _truthy(os.environ.get("OMBRE_HOOK_ALLOW_PUBLIC")) or _truthy(
        _hook_setting("allow_public")
    )
    if allow_public:
        return True

    token = (os.environ.get("OMBRE_HOOK_TOKEN") or str(_hook_setting("token", "") or "")).strip()
    if token:
        auth = _header_value(request, "authorization")
        supplied = [
            _header_value(request, "x-ombre-hook-token"),
            auth[7:] if auth.startswith("Bearer ") else "",
        ]
        if any(v and sh._constant_time_text_equal(v, token) for v in supplied):
            return True

    try:
        return bool(sh._is_authenticated(request))
    except Exception:
        return False


def _valid_hook_token(request) -> bool:
    token = (os.environ.get("OMBRE_HOOK_TOKEN") or str(_hook_setting("token", "") or "")).strip()
    if not token:
        return False
    auth = _header_value(request, "authorization")
    supplied = (
        _header_value(request, "x-ombre-hook-token"),
        auth[7:] if auth.startswith("Bearer ") else "",
    )
    return any(
        value and sh._constant_time_text_equal(value, token)
        for value in supplied
    )


def _hook_source_key(request) -> str:
    resolver = getattr(sh, "_client_key", None)
    if callable(resolver):
        try:
            return str(resolver(request))[:200]
        except Exception:
            pass
    client = getattr(request, "client", None)
    return str(getattr(client, "host", "unknown") or "unknown")[:200]


def _admit_hook_request(request) -> bool:
    """Bound provider-cost amplification with finite per-source/global state."""

    now = time.monotonic()
    cutoff = now - _HOOK_RATE_WINDOW_SECONDS
    key = _hook_source_key(request)
    with _hook_rate_lock:
        while _hook_global_events and _hook_global_events[0] <= cutoff:
            _hook_global_events.popleft()
        if len(_hook_global_events) >= _HOOK_RATE_GLOBAL_LIMIT:
            return False

        events = _hook_source_events.get(key)
        if events is None:
            events = deque()
            _hook_source_events[key] = events
        while events and events[0] <= cutoff:
            events.popleft()
        if len(events) >= _HOOK_RATE_SOURCE_LIMIT:
            _hook_source_events.move_to_end(key)
            return False

        events.append(now)
        _hook_global_events.append(now)
        _hook_source_events.move_to_end(key)
        while len(_hook_source_events) > _HOOK_RATE_SOURCE_CAP:
            _hook_source_events.popitem(last=False)
        return True


def _bounded_text(value, limit: int = 200) -> str:
    return str(value or "")[:limit]


@asynccontextmanager
async def _timeout_after(seconds: float):
    """Python 3.10-compatible total timeout that preserves external cancel."""

    task = asyncio.current_task()
    if task is None:
        yield
        return
    expired = False

    def cancel_for_timeout() -> None:
        nonlocal expired
        expired = True
        task.cancel()

    handle = asyncio.get_running_loop().call_later(max(0.0, seconds), cancel_for_timeout)
    try:
        yield
    except asyncio.CancelledError as exc:
        if expired:
            raise TimeoutError from exc
        raise
    finally:
        handle.cancel()


def register(mcp) -> None:

    @mcp.custom_route("/breath-hook", methods=["GET"])
    async def breath_hook(request):
        from starlette.responses import PlainTextResponse
        if not _is_hook_request_authorized(request):
            return PlainTextResponse("", status_code=401)

        # Token-authenticated SessionStart is the AI consumer.  A valid
        # Dashboard session is the human consumer.  Deliberately public hooks
        # remain unauthenticated and can never receive locked Letter content.
        if _valid_hook_token(request):
            caller_side = "ai"
        else:
            try:
                caller_side = "human" if sh._is_authenticated(request) else None
            except Exception:
                caller_side = None

        # This endpoint can expose memory text and is intended for a non-browser
        # SessionStart hook.  Do not let an ambient dashboard cookie turn a
        # cross-origin GET into a memory read; explicit hook tokens are unaffected.
        public = _truthy(os.environ.get("OMBRE_HOOK_ALLOW_PUBLIC")) or _truthy(
            _hook_setting("allow_public")
        )
        cross_site = _header_value(request, "sec-fetch-site").strip().lower() == "cross-site"
        if (
            (_header_value(request, "origin") or cross_site)
            and not public
            and not _valid_hook_token(request)
        ):
            return PlainTextResponse("", status_code=403)
        if not _admit_hook_request(request):
            return PlainTextResponse("", status_code=429, headers={"Retry-After": "60"})
        if not _hook_slots.acquire(blocking=False):
            return PlainTextResponse("", status_code=429, headers={"Retry-After": "5"})

        def setting_int(name: str, default: int, minimum: int, maximum: int) -> int:
            try:
                value = int(_hook_setting(name, default))
            except (TypeError, ValueError, OverflowError):
                value = default
            return max(minimum, min(maximum, value))

        timeout_seconds = setting_int("timeout_seconds", 45, 5, 120)
        extras_budget = setting_int("extras_max_tokens", 2000, 200, 10000)
        no_store_headers = {
            "Cache-Control": "no-store",
            "X-Content-Type-Options": "nosniff",
        }

        try:
            async with _timeout_after(timeout_seconds):
                # SessionStart must surface exactly the same ordinary memory as
                # the public zero-argument breath() tool.  In particular, do not
                # keep a second hook-only result/token budget or run provider
                # dehydration here: the helper reads the live surfacing defaults
                # (breath_max_results / breath_max_tokens) itself.
                main_surface = await _t_breath.surface_default_memories()
                if caller_side == "ai":
                    deletion_store = getattr(sh, "deletion_requests", None)
                    if deletion_store is not None:
                        deletion_batch = await deletion_store.render_pending_batch()
                        if deletion_batch:
                            main_surface = (
                                f"{deletion_batch}\n\n{main_surface}"
                                if main_surface else deletion_batch
                            )
                all_buckets = await sh.bucket_mgr.list_all(include_archive=False)
                parts: list[str] = []
                extras: list[str] = []
                extras_remaining = extras_budget

                def append_main(block: str) -> bool:
                    block = str(block or "").strip()
                    if not block:
                        return False
                    parts.append(block)
                    return True

                def append_extra(block: str) -> bool:
                    nonlocal extras_remaining
                    block = str(block or "").strip()
                    if not block:
                        return False
                    cost = count_tokens_approx(block) + 2
                    if cost > extras_remaining:
                        return False
                    extras.append(block)
                    extras_remaining -= cost
                    return True

                append_main(main_surface)

                letters = [
                    bucket for bucket in all_buckets
                    if is_letter_bucket(bucket)
                    and not _truthy(bucket["metadata"].get("protected"))
                ]
                normalized_letters = []
                letter_states = {}
                for letter in letters:
                    state = letter_lock_state(letter, caller_side)
                    letter, state = await normalize_expired_lock(
                        letter,
                        state,
                        caller_side,
                        bucket_mgr=sh.bucket_mgr,
                    )
                    if not letter:
                        continue
                    normalized_letters.append(letter)
                    letter_states[letter["id"]] = state
                letters = normalized_letters
                if letters:
                    def latest(*authors: str) -> dict | None:
                        wanted = set(authors)
                        pool = [
                            letter for letter in letters
                            if letter["metadata"].get("author") in wanted
                            and not letter_states[letter["id"]]["locked"]
                        ]
                        if not pool:
                            return None
                        pool.sort(
                            key=lambda bucket: (
                                bucket["metadata"].get("letter_date")
                                or bucket["metadata"].get("created", "")
                            ),
                            reverse=True,
                        )
                        return pool[0]

                    for tag, letter in (
                        ("user→你", latest("user")),
                        ("你→user", latest(get_ai_name(), "claude")),
                    ):
                        if letter is None:
                            continue
                        meta = letter["metadata"]
                        state = letter_states[letter["id"]]
                        if state["stored_lock_type"] != "none":
                            # Locked Letters created by V1 always snapshot the
                            # actual writer name.  Even the owner's full-text
                            # excerpt must not introduce generic side labels.
                            tag = str(meta.get("writer_name") or "").strip() or tag
                        date = meta.get("letter_date") or str(meta.get("created", ""))[:10]
                        title = _bounded_text(meta.get("title") or meta.get("name"), 200)
                        excerpt = strip_wikilinks(str(letter.get("content") or ""))[:400]
                        append_extra(
                            f"💌 [{tag}] {date}{(' · ' + title) if title else ''}\n{excerpt}"
                        )

                    # Locked incoming Letters are an independent existence
                    # signal.  Do not let a newer ordinary Letter hide an older
                    # still-locked one, and do not change the normal "latest
                    # visible letter per direction" injection above.
                    if caller_side is not None:
                        incoming_by_writer: dict[str, list[tuple[dict, dict]]] = {}
                        for letter in letters:
                            state = letter_states[letter["id"]]
                            if not state["locked"]:
                                continue
                            meta = letter.get("metadata") or {}
                            writer_name = str(meta.get("writer_name") or "").strip()
                            if not writer_name:
                                continue
                            incoming_by_writer.setdefault(writer_name, []).append(
                                (letter, state)
                            )

                        for writer_name, incoming in incoming_by_writer.items():
                            _representative, state = incoming[0]
                            if len(incoming) > 1:
                                notice = f"{writer_name}给你留了 {len(incoming)} 封仍未解锁的信。"
                            elif state["lock_type"] == "timed":
                                when = str(state["unlock_date"] or "").replace("T", " ")[:16]
                                notice = f"{writer_name}给你留了一封带锁的信，将于 {when} 解锁。"
                            else:
                                notice = f"{writer_name}给你留了一封永久锁信，当前不可查看。"
                            append_extra(notice)

                self_buckets = [
                    bucket for bucket in all_buckets
                    if not is_letter_bucket(bucket)
                    and not _truthy(bucket["metadata"].get("protected"))
                    and (
                        bucket["metadata"].get("type") == "i"
                        or "__i__" in (bucket["metadata"].get("tags") or [])
                    )
                ]
                self_buckets.sort(
                    key=lambda bucket: bucket["metadata"].get("created", ""),
                    reverse=True,
                )
                for bucket in self_buckets[:3]:
                    meta = bucket["metadata"]
                    tags = meta.get("tags") or []
                    aspect = next(
                        (
                            _bounded_text(tag, 100).removeprefix("aspect:")
                            for tag in tags
                            if isinstance(tag, str) and tag.startswith("aspect:")
                        ),
                        "",
                    )
                    raw = strip_wikilinks(str(bucket.get("content") or ""))
                    excerpt = raw[:300]
                    append_extra(
                        f"🪞{str(meta.get('created') or '')[:10]}"
                        f"{f' [{aspect}]' if aspect else ''}\n{excerpt}"
                    )

                if not parts:
                    try:
                        await asyncio.wait_for(
                            sh.fire_webhook("breath_hook", {"surfaced": 0}),
                            timeout=3,
                        )
                    except Exception as exc:
                        logger.warning("breath_hook telemetry failed: %s", exc)
                    return PlainTextResponse("", headers=no_store_headers)

                if extras:
                    parts.append("=== SessionStart 补充 ===\n" + "\n---\n".join(extras))
                body_text = "[Ombre Brain - SessionStart]\n" + "\n---\n".join(parts)
                try:
                    await asyncio.wait_for(
                        sh.fire_webhook(
                            "breath_hook",
                            {"surfaced": len(parts), "chars": len(body_text)},
                        ),
                        timeout=3,
                    )
                except Exception as exc:
                    logger.warning("breath_hook telemetry failed: %s", exc)
                return PlainTextResponse(body_text, headers=no_store_headers)
        except TimeoutError:
            logger.warning("Breath hook exceeded %ss total timeout", timeout_seconds)
            return PlainTextResponse(
                "",
                status_code=504,
                headers={**no_store_headers, "Retry-After": "10"},
            )
        except Exception as e:
            logger.warning(f"Breath hook failed: {e}")
            return PlainTextResponse("", headers=no_store_headers)
        finally:
            _hook_slots.release()

    # 注意：这里**故意不再提供 /dream-hook**。
    # 按 OB 的设计哲学，dream（做梦消化）不是义务、不该在每次会话开始被自动触发——
    # 它只应在「需要消化时」由模型主动调用 MCP 的 dream 工具。把它做成 SessionStart hook
    # 会把「主动消化」异化成「每次开场的强制动作」，与哲学冲突，故移除该端点。
