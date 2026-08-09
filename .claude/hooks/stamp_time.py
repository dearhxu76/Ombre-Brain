#!/usr/bin/env python3
# ============================================================
# UserPromptSubmit Hook: stamp the current time
# 每轮对话钩子：盖一个时间戳
#
# On every user turn, read the clock once and print the reading so the
# model receives it as context before it starts answering.
#
# 容器和服务器的时区通常是 UTC，模型自身也读不到钟点——系统上下文只给
# 日期。让钩子机械地读一次表，比让模型记得自己调用时间工具更可靠：
# 不会漏，也不占模型一个动作。
#
# 输出：【2026-08-09 周日 23:38 · Asia/Shanghai】
#
# 用 Python 而不是 shell，是为了和 session_breath.py 一样跨平台：
# 同一份仓库配置在 Windows / macOS / Linux 上行为一致。
#
# Config:
#   OMBRE_STAMP_TZ   — IANA 时区名，默认 Asia/Shanghai
#   OMBRE_STAMP_SKIP — 设为 "1" 临时关掉
# ============================================================

import os
import sys
from datetime import datetime, timedelta, timezone

_WEEKDAYS = ("周一", "周二", "周三", "周四", "周五", "周六", "周日")

# 没有 tzdata 时可以安全退回固定偏移的时区：这些地区当前不实行夏令时，
# 固定偏移对「此刻几点」是精确的，不是近似。
_FIXED_OFFSET_FALLBACK = {
    "Asia/Shanghai": 8,
    "Asia/Hong_Kong": 8,
    "Asia/Taipei": 8,
    "Asia/Singapore": 8,
    "Asia/Tokyo": 9,
    "UTC": 0,
}


def _resolve_tz(name):
    """Resolve an IANA name, falling back to a fixed offset without tzdata.

    Windows ships no zoneinfo database, so ZoneInfo("Asia/Shanghai") raises
    unless the `tzdata` package happens to be installed. Returning None means
    "I could not read this clock" — the caller says so rather than silently
    substituting local time.
    """
    try:
        from zoneinfo import ZoneInfo

        return ZoneInfo(name)
    except Exception:
        hours = _FIXED_OFFSET_FALLBACK.get(name)
        if hours is None:
            return None
        return timezone(timedelta(hours=hours))


def main():
    if os.environ.get("OMBRE_STAMP_SKIP") == "1":
        sys.exit(0)

    name = (os.environ.get("OMBRE_STAMP_TZ") or "Asia/Shanghai").strip()
    tz = _resolve_tz(name)

    if tz is None:
        # 读不到这只表就说读不到，不拿别的时间冒充它。
        print(f"【表不在：读不到时区 {name}】")
        sys.exit(0)

    now = datetime.now(tz)
    print(f"【{now:%Y-%m-%d} {_WEEKDAYS[now.weekday()]} {now:%H:%M} · {name}】")


if __name__ == "__main__":
    main()
