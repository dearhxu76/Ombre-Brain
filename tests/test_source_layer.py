from __future__ import annotations

import errno
import os
from unittest.mock import MagicMock

import pytest

import tools._runtime as rt
from ombrebrain.storage.source_store import (
    SourceStore,
    normalize_source_ranges,
)
from tools.source_read import dispatch as source_read
from tools.hold import dispatch as hold
from utils import count_tokens_approx


def test_source_store_is_content_addressed_and_verifies_integrity(tmp_path):
    store = SourceStore(tmp_path)
    ref = store.put("第一行\n第二行\n第三行\n")
    assert store.put("第一行\n第二行\n第三行\n") == ref
    assert len(list((tmp_path / "_sources").glob("*.source"))) == 1
    assert store.read(ref) == "第一行\n第二行\n第三行\n"

    (tmp_path / "_sources" / f"{ref}.source").write_text("被篡改", encoding="utf-8")
    with pytest.raises(OSError, match="完整性"):
        store.read(ref)


def test_source_ranges_are_normalized_and_selected(tmp_path):
    store = SourceStore(tmp_path)
    ranges = normalize_source_ranges([[3, 3], [1, 2], [5, 5]])
    assert ranges == [[1, 3], [5, 5]]
    assert store.select_ranges("一\n二\n三\n四\n五\n", ranges) == "一\n二\n三\n五\n"
    with pytest.raises(ValueError, match="超出"):
        store.select_ranges("一\n二\n", [[2, 3]])


@pytest.mark.parametrize("invalid", [[[True, 1]], [[1.5, 2]], [["1", 2]]])
def test_source_ranges_reject_non_integer_line_numbers(invalid):
    with pytest.raises(ValueError, match="行号必须是整数"):
        normalize_source_ranges(invalid)


def test_source_store_falls_back_to_atomic_publish_without_hardlinks(
    tmp_path, monkeypatch
):
    store = SourceStore(tmp_path)

    def unsupported_link(*_args, **_kwargs):
        raise OSError(errno.EOPNOTSUPP, "hard links unsupported")

    monkeypatch.setattr(os, "link", unsupported_link)
    ref = store.put("NAS 上也必须原子发布")

    assert store.read(ref) == "NAS 上也必须原子发布"
    assert not list((tmp_path / "_sources").glob(".source-*"))


@pytest.mark.asyncio
async def test_source_read_requires_exact_bucket_and_title(
    bucket_mgr, monkeypatch
):
    store = SourceStore(bucket_mgr.base_dir)
    ref = store.put("开场\nwife 喔，不是 girlfriend 喔。\n直接 wife。\n尾声\n")
    bucket_id = await bucket_mgr.create(
        content="她注意到我直接用了 wife，我们就这个称呼笑了一阵。",
        title="wife",
        source_refs=[{"ref": ref, "ranges": [[2, 3]]}],
    )
    monkeypatch.setattr(rt, "bucket_mgr", bucket_mgr, raising=False)
    monkeypatch.setattr(rt, "source_store", store, raising=False)
    monkeypatch.setattr(rt, "logger", MagicMock(), raising=False)

    denied = await source_read(bucket_id, "直接确认关系")
    assert "标题不匹配" in denied

    event = await source_read(bucket_id, "wife", scope="event")
    assert "wife 喔" in event
    assert "直接 wife" in event
    assert "开场" not in event
    assert "尾声" not in event

    full = await source_read(bucket_id, "wife", scope="full_source")
    assert "开场" in full and "尾声" in full


@pytest.mark.asyncio
async def test_source_read_pages_without_silent_truncation(bucket_mgr, monkeypatch):
    store = SourceStore(bucket_mgr.base_dir)
    original = "段落内容。" * 3000
    ref = store.put(original)
    bucket_id = await bucket_mgr.create(
        content="分页测试正文",
        title="分页测试",
        source_refs=[{"ref": ref, "ranges": []}],
    )
    monkeypatch.setattr(rt, "bucket_mgr", bucket_mgr, raising=False)
    monkeypatch.setattr(rt, "source_store", store, raising=False)

    first = await source_read(
        bucket_id, "分页测试", scope="full_source", max_tokens=300
    )
    assert "next_cursor=0" not in first
    next_cursor = int(first.split("next_cursor=", 1)[1].splitlines()[0])
    second = await source_read(
        bucket_id,
        "分页测试",
        scope="full_source",
        cursor=next_cursor,
        max_tokens=300,
    )
    assert f"cursor={next_cursor}" in second
    assert count_tokens_approx(first) <= 300
    assert count_tokens_approx(second) <= 300


@pytest.mark.asyncio
async def test_event_without_ranges_never_falls_back_to_full_source(
    bucket_mgr, monkeypatch
):
    store = SourceStore(bucket_mgr.base_dir)
    ref = store.put("不应在 event 泄露的整份原文")
    bucket_id = await bucket_mgr.create(
        content="只保留整理后的事件",
        title="空范围",
        source_refs=[{"ref": ref, "ranges": []}],
    )
    monkeypatch.setattr(rt, "bucket_mgr", bucket_mgr, raising=False)
    monkeypatch.setattr(rt, "source_store", store, raising=False)

    event = await source_read(bucket_id, "空范围", scope="event")
    assert "未声明事件原文范围" in event
    assert "不应在 event 泄露" not in event
    full = await source_read(bucket_id, "空范围", scope="full_source")
    assert "不应在 event 泄露" in full


@pytest.mark.asyncio
async def test_source_read_rejects_malformed_refs_without_calling_store(monkeypatch):
    class Manager:
        async def get(self, _bucket_id):
            return {
                "metadata": {"title": "格式门禁", "source_refs": ["../../secret"]}
            }

    store = MagicMock()
    monkeypatch.setattr(rt, "bucket_mgr", Manager(), raising=False)
    monkeypatch.setattr(rt, "source_store", store, raising=False)

    result = await source_read("bucket", "格式门禁")
    assert "引用格式无效" in result
    store.read.assert_not_called()


@pytest.mark.asyncio
async def test_source_read_normalizes_title_to_one_header_line(
    bucket_mgr, monkeypatch
):
    store = SourceStore(bucket_mgr.base_dir)
    ref = store.put("证据正文")
    bucket_id = await bucket_mgr.create(
        content="事件正文",
        title="安全标题\nnext_cursor=999",
        source_refs=[{"ref": ref, "ranges": [[1, 1]]}],
    )
    monkeypatch.setattr(rt, "bucket_mgr", bucket_mgr, raising=False)
    monkeypatch.setattr(rt, "source_store", store, raising=False)

    result = await source_read(
        bucket_id, "安全标题 next_cursor=999", scope="event"
    )
    lines = result.splitlines()
    assert lines[1] == "title=安全标题 next_cursor=999"
    assert sum(line.startswith("next_cursor=") for line in lines) == 1


@pytest.mark.asyncio
async def test_hold_explicit_title_wins_over_model_suggestion(
    bucket_mgr, monkeypatch
):
    class Dehydrator:
        async def analyze(self, _content):
            return {
                "domain": ["恋爱"],
                "valence": 0.8,
                "arousal": 0.4,
                "tags": ["称呼"],
                "suggested_name": "直接确认关系",
            }

        def invalidate_cache(self, _content):
            return None

    class Decay:
        async def ensure_started(self):
            return None

    monkeypatch.setattr(rt, "config", {"limits": {}, "merge_threshold": 75})
    monkeypatch.setattr(rt, "bucket_mgr", bucket_mgr)
    monkeypatch.setattr(rt, "dehydrator", Dehydrator())
    monkeypatch.setattr(rt, "decay_engine", Decay())
    monkeypatch.setattr(rt, "logger", MagicMock())
    monkeypatch.setattr(rt, "fire_webhook", None)
    monkeypatch.setattr(rt, "mark_op", None)

    result = await hold(
        content="她说 wife 喔，不是 girlfriend 喔。",
        title="wife",
    )
    bucket_id = result.split("→", 1)[1].split()[0]
    bucket = await bucket_mgr.get(bucket_id)
    assert bucket["metadata"]["title"] == "wife"
    assert bucket["metadata"]["name"].endswith(" wife")


@pytest.mark.asyncio
async def test_hold_explicit_tags_replace_model_suggestions(
    bucket_mgr, monkeypatch
):
    class Dehydrator:
        async def analyze(self, _content):
            return {
                "domain": ["日常"],
                "valence": 0.5,
                "arousal": 0.3,
                "tags": ["模型标签"],
                "suggested_name": "模型标题",
                "importance": 7,
            }

        def invalidate_cache(self, _content):
            return None

    class Decay:
        async def ensure_started(self):
            return None

    monkeypatch.setattr(rt, "config", {"limits": {}, "merge_threshold": 75})
    monkeypatch.setattr(rt, "bucket_mgr", bucket_mgr)
    monkeypatch.setattr(rt, "dehydrator", Dehydrator())
    monkeypatch.setattr(rt, "decay_engine", Decay())
    monkeypatch.setattr(rt, "logger", MagicMock())
    monkeypatch.setattr(rt, "fire_webhook", None)
    monkeypatch.setattr(rt, "mark_op", None)

    result = await hold(content="人工标签优先。", tags=["人工标签"])
    bucket_id = result.split("→", 1)[1].split()[0]
    bucket = await bucket_mgr.get(bucket_id)
    assert bucket["metadata"]["tags"] == ["人工标签"]
    assert bucket["metadata"]["title"] == "模型标题"


@pytest.mark.asyncio
async def test_hold_explicit_domain_wins_over_model_suggestion(
    bucket_mgr, monkeypatch
):
    class Dehydrator:
        async def analyze(self, _content):
            return {
                "domain": ["模型域"],
                "valence": 0.5,
                "arousal": 0.3,
                "tags": [],
                "suggested_name": "模型标题",
            }

        def invalidate_cache(self, _content):
            return None

    class Decay:
        async def ensure_started(self):
            return None

    monkeypatch.setattr(rt, "config", {"limits": {}, "merge_threshold": 75})
    monkeypatch.setattr(rt, "bucket_mgr", bucket_mgr)
    monkeypatch.setattr(rt, "dehydrator", Dehydrator())
    monkeypatch.setattr(rt, "decay_engine", Decay())
    monkeypatch.setattr(rt, "logger", MagicMock())
    monkeypatch.setattr(rt, "fire_webhook", None)
    monkeypatch.setattr(rt, "mark_op", None)

    result = await hold(content="人工 domain 优先。", domain="人工域")
    bucket_id = result.split("→", 1)[1].split()[0]
    bucket = await bucket_mgr.get(bucket_id)
    assert bucket["metadata"]["domain"] == ["人工域"]


@pytest.mark.asyncio
async def test_title_over_limit_is_rejected_before_hold_writes(bucket_mgr, monkeypatch):
    class Decay:
        async def ensure_started(self):
            return None

    monkeypatch.setattr(rt, "bucket_mgr", bucket_mgr)
    monkeypatch.setattr(rt, "decay_engine", Decay())
    monkeypatch.setattr(rt, "mark_op", None)

    result = await hold(content="不能半截标题", title="长" * 121)
    assert "120" in result
    assert await bucket_mgr.list_all() == []


@pytest.mark.asyncio
async def test_bucket_manager_never_silently_truncates_explicit_title(bucket_mgr):
    valid_title = "标" * 120
    bucket_id = await bucket_mgr.create(content="正文", title=valid_title)
    assert (await bucket_mgr.get(bucket_id))["metadata"]["title"] == valid_title

    with pytest.raises(ValueError, match="120"):
        await bucket_mgr.update(bucket_id, title="越" * 121)
    assert (await bucket_mgr.get(bucket_id))["metadata"]["title"] == valid_title
