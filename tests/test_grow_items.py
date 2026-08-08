"""grow 预拆分（items）模式测试。

issue 诉求：上层 AI 已拆好的正文应逐字入库，消除「廉价 LLM 二次拆分改写」这次失真。
硬承诺：
1. 传 items 时**绝不调用 digest / merge**（谁调谁炸），只调 analyze 打元数据；
2. 存进桶的正文与传入**一字不差**；
3. 同批共享 grow_batch_id；不传 items 时行为不变（走原 digest 路径）。
"""
from unittest.mock import MagicMock
from pathlib import Path

import pytest

import tools._runtime as rt
from tools.grow import dispatch
from tools.grow.core import grow_core, grow_items
from tools.source_read import dispatch as source_read
from errors import PublicToolError
from ombrebrain.storage.source_store import SourceStore


class StubDehydrator:
    """items 模式只允许 analyze；digest/merge 一旦被调用即判定失真回归。"""

    def __init__(self):
        self.analyze_calls = 0

    async def analyze(self, content):
        self.analyze_calls += 1
        return {"domain": ["工作"], "valence": 0.6, "arousal": 0.4,
                "tags": ["标签"], "suggested_name": "事件"}

    async def digest(self, content):
        raise AssertionError("items 模式不允许调用 digest（会造成二次改写失真）")

    async def merge(self, old_content, new_content):
        raise AssertionError("items 模式不允许调用 merge（会压缩老+新）")


class NoopDecay:
    is_running = True

    async def ensure_started(self):
        return None

    def calculate_score(self, meta):
        return 1.0


@pytest.fixture
def grow_rt(bucket_mgr, monkeypatch):
    stub = StubDehydrator()
    monkeypatch.setattr(rt, "config", {"limits": {}, "merge_threshold": 75}, raising=False)
    monkeypatch.setattr(rt, "bucket_mgr", bucket_mgr, raising=False)
    monkeypatch.setattr(rt, "dehydrator", stub, raising=False)
    monkeypatch.setattr(rt, "decay_engine", NoopDecay(), raising=False)
    monkeypatch.setattr(rt, "logger", MagicMock(), raising=False)
    monkeypatch.setattr(rt, "fire_webhook", None, raising=False)
    monkeypatch.setattr(rt, "source_store", SourceStore(bucket_mgr.base_dir), raising=False)
    return bucket_mgr, stub


@pytest.mark.asyncio
async def test_items_stored_verbatim_no_digest(grow_rt):
    bucket_mgr, stub = grow_rt
    original = [
        "昨天和产品经理开会，分歧在多租户 SaaS 的隐私边界。",
        "晚上把 grow 的 items 模式草稿写完了，明天验证。",
    ]
    out = await grow_items(list(original))

    assert "2条(预拆分·逐字)" in out
    assert stub.analyze_calls == 2  # 每条只打标一次

    # 正文一字不差地存进了桶
    buckets = await bucket_mgr.list_all(include_archive=False)
    stored = sorted(b["content"] for b in buckets)
    assert stored == sorted(original)


@pytest.mark.asyncio
async def test_items_share_batch_and_metadata(grow_rt):
    bucket_mgr, stub = grow_rt
    await grow_items(["第一条内容啊啊啊啊啊", "第二条内容哦哦哦哦哦"])
    buckets = await bucket_mgr.list_all(include_archive=False)
    batch_ids = {b["metadata"].get("grow_batch_id") for b in buckets}
    assert len(batch_ids) == 1 and next(iter(batch_ids))  # 同批共享一个非空 batch_id
    for b in buckets:
        assert b["metadata"].get("source_tool") == "grow"
        assert "工作" in (b["metadata"].get("domain") or [])


@pytest.mark.asyncio
async def test_empty_items_creates_nothing(grow_rt):
    bucket_mgr, stub = grow_rt
    out = await grow_items(["", "   ", None])
    assert "未创建任何桶" in out
    assert stub.analyze_calls == 0
    assert await bucket_mgr.list_all(include_archive=False) == []


@pytest.mark.asyncio
async def test_dict_items_take_content_field(grow_rt):
    bucket_mgr, stub = grow_rt
    await grow_items([{"content": "对象形式的一条正文内容"}, "字符串形式的一条正文内容"])
    buckets = await bucket_mgr.list_all(include_archive=False)
    stored = sorted(b["content"] for b in buckets)
    assert stored == sorted(["对象形式的一条正文内容", "字符串形式的一条正文内容"])


@pytest.mark.asyncio
async def test_dispatch_routes_to_items_when_provided(grow_rt):
    bucket_mgr, stub = grow_rt
    # 传 items 时 content 是共享原文证据，不进入 digest；items 仍是最终正文。
    out = await dispatch(content="x" * 100, items=["逐字入库的唯一正文条目内容"])
    assert "预拆分·逐字" in out
    buckets = await bucket_mgr.list_all(include_archive=False)
    assert [b["content"] for b in buckets] == ["逐字入库的唯一正文条目内容"]
    assert buckets[0]["metadata"]["source_refs"][0]["ref"].startswith("src_")


@pytest.mark.asyncio
async def test_dict_items_preserve_explicit_metadata_and_title(grow_rt):
    bucket_mgr, stub = grow_rt
    await grow_items([{
        "title": "wife",
        "content": "她说 wife 喔，不是 girlfriend 喔。",
        "tags": ["老婆", "称呼"],
        "importance": 8,
        "domain": ["恋爱"],
        "valence": 0.9,
        "arousal": 0.6,
    }])
    bucket = (await bucket_mgr.list_all(include_archive=False))[0]
    metadata = bucket["metadata"]
    assert metadata["title"] == "wife"
    assert metadata["tags"] == ["老婆", "称呼"]
    assert metadata["importance"] == 8
    assert metadata["domain"] == ["恋爱"]
    assert metadata["valence"] == 0.9
    assert metadata["arousal"] == 0.6
    assert stub.analyze_calls == 0


@pytest.mark.asyncio
async def test_missing_title_and_importance_are_filled_by_analysis(grow_rt):
    bucket_mgr, _stub = grow_rt

    class MetadataDehydrator(StubDehydrator):
        async def analyze(self, content):
            self.analyze_calls += 1
            return {
                "domain": ["工作"],
                "valence": 0.6,
                "arousal": 0.4,
                "tags": ["模型补全"],
                "suggested_name": "模型补齐标题",
                "importance": 7,
            }

    dehydrator = MetadataDehydrator()
    rt.dehydrator = dehydrator
    await grow_items([{"content": "只给正文，其他元数据由整理者补齐。"}])

    bucket = (await bucket_mgr.list_all(include_archive=False))[0]
    assert bucket["metadata"]["title"] == "模型补齐标题"
    assert bucket["metadata"]["importance"] == 7
    assert bucket["metadata"]["tags"] == ["模型补全"]
    assert dehydrator.analyze_calls == 1


@pytest.mark.asyncio
async def test_explicit_empty_tags_are_not_refilled_by_model(grow_rt):
    bucket_mgr, _stub = grow_rt
    await grow_items([{
        "content": "人工明确决定不设标签。",
        "title": "空标签决定",
        "tags": [],
    }])

    bucket = (await bucket_mgr.list_all(include_archive=False))[0]
    assert bucket["metadata"]["tags"] == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "item, expected",
    [
        ({"content": "字段拼错", "titel": "x"}, "未支持字段"),
        ({"content": 123}, "content 必须是字符串"),
        ({"content": "越界重要度", "importance": 11}, "importance"),
        ({"content": "过长标题", "title": "长" * 121}, "120"),
    ],
)
async def test_invalid_structured_item_is_rejected_before_any_write(
    grow_rt, item, expected
):
    bucket_mgr, stub = grow_rt
    out = await grow_items([item], source_content="这份原文也不应先落盘")

    assert expected in out
    assert stub.analyze_calls == 0
    assert await bucket_mgr.list_all(include_archive=False) == []
    assert not list((Path(bucket_mgr.base_dir) / "_sources").glob("*.source"))


@pytest.mark.asyncio
async def test_source_ranges_without_source_content_are_rejected(grow_rt):
    bucket_mgr, stub = grow_rt
    out = await grow_items([
        {"content": "事件", "title": "标题", "source_ranges": [[1, 1]]}
    ])

    assert "source_ranges 需要同时提供 content" in out
    assert stub.analyze_calls == 0
    assert await bucket_mgr.list_all(include_archive=False) == []


@pytest.mark.asyncio
async def test_invalid_source_range_rejects_batch_before_any_write(grow_rt):
    bucket_mgr, _stub = grow_rt
    out = await grow_items(
        [{"title": "越界", "content": "最终正文", "source_ranges": [[3, 4]]}],
        source_content="只有一行",
    )
    assert "超出原文总行数" in out
    assert await bucket_mgr.list_all(include_archive=False) == []
    assert not list((Path(bucket_mgr.base_dir) / "_sources").glob("*.source"))


@pytest.mark.asyncio
async def test_obviously_shifted_source_ranges_are_rejected_before_write(grow_rt):
    bucket_mgr, stub = grow_rt
    source = (
        "开场：这两行只是寒暄。\n"
        "今天先聊点别的。\n"
        "[蓝鲸计划]\n"
        "蓝鲸计划讨论海洋声呐与深海航线。\n"
        "大家决定继续整理蓝鲸观测记录。\n"
        "[樱桃烘焙]\n"
        "樱桃烘焙测试黄油面团与烤箱温度。\n"
        "最后记录樱桃派的配方调整。\n"
        "[火星旅行]\n"
        "火星旅行讨论着陆舱与红色沙丘路线。\n"
        "最后确认火星基地的补给窗口。\n"
    )
    items = [
        {
            "title": "蓝鲸计划",
            "content": "蓝鲸计划围绕海洋声呐、深海航线和观测记录展开。",
            "source_ranges": [[1, 2]],
        },
        {
            "title": "樱桃烘焙",
            "content": "樱桃烘焙记录黄油面团、烤箱温度和樱桃派配方。",
            "source_ranges": [[3, 5]],
        },
        {
            "title": "火星旅行",
            "content": "火星旅行记录着陆舱、红色沙丘路线与基地补给窗口。",
            "source_ranges": [[6, 8]],
        },
    ]

    out = await dispatch(content=source, items=items)

    assert "source_ranges 疑似与 items 错位" in out
    assert stub.analyze_calls == 0
    assert await bucket_mgr.list_all(include_archive=False) == []
    assert not list((Path(bucket_mgr.base_dir) / "_sources").glob("*.source"))


@pytest.mark.asyncio
async def test_single_title_anchor_mismatch_does_not_guess(grow_rt):
    bucket_mgr, _stub = grow_rt
    source = (
        "开场寒暄。\n"
        "这里是调用方明确指定的证据行。\n"
        "[蓝鲸计划]\n"
        "蓝鲸计划讨论海洋声呐与深海航线。\n"
    )
    out = await dispatch(
        content=source,
        items=[{
            "title": "蓝鲸计划",
            "content": "蓝鲸计划围绕海洋声呐与深海航线展开。",
            "source_ranges": [[1, 2]],
        }],
    )

    assert "1条(预拆分·逐字)" in out
    buckets = await bucket_mgr.list_all(include_archive=False)
    assert len(buckets) == 1
    assert buckets[0]["metadata"]["source_refs"][0]["ranges"] == [[1, 2]]


@pytest.mark.asyncio
async def test_aligned_multi_event_source_ranges_round_trip(grow_rt):
    bucket_mgr, _stub = grow_rt
    source = (
        "开场：这两行只是寒暄。\n"
        "今天先聊点别的。\n"
        "[蓝鲸计划]\n"
        "蓝鲸计划讨论海洋声呐与深海航线。\n"
        "大家决定继续整理蓝鲸观测记录。\n"
        "[樱桃烘焙]\n"
        "樱桃烘焙测试黄油面团与烤箱温度。\n"
        "最后记录樱桃派的配方调整。\n"
        "[火星旅行]\n"
        "火星旅行讨论着陆舱与红色沙丘路线。\n"
        "最后确认火星基地的补给窗口。\n"
    )
    items = [
        {
            "title": "蓝鲸计划",
            "content": "蓝鲸计划围绕海洋声呐、深海航线和观测记录展开。",
            "source_ranges": [[3, 5]],
        },
        {
            "title": "樱桃烘焙",
            "content": "樱桃烘焙记录黄油面团、烤箱温度和樱桃派配方。",
            "source_ranges": [[6, 8]],
        },
        {
            "title": "火星旅行",
            "content": "火星旅行记录着陆舱、红色沙丘路线与基地补给窗口。",
            "source_ranges": [[9, 11]],
        },
    ]

    out = await dispatch(content=source, items=items)
    assert "3条(预拆分·逐字)" in out

    buckets = await bucket_mgr.list_all(include_archive=False)
    by_title = {bucket["metadata"]["title"]: bucket for bucket in buckets}
    first = await source_read(by_title["蓝鲸计划"]["id"], "蓝鲸计划")
    middle = await source_read(by_title["樱桃烘焙"]["id"], "樱桃烘焙")
    last = await source_read(by_title["火星旅行"]["id"], "火星旅行")
    full = await source_read(
        by_title["蓝鲸计划"]["id"], "蓝鲸计划", scope="full_source"
    )
    denied = await source_read(by_title["蓝鲸计划"]["id"], "错误标题")

    assert "深海航线" in first and "樱桃派" not in first
    assert "樱桃派" in middle and "火星基地" not in middle
    assert "火星基地" in last and "蓝鲸观测" not in last
    assert "开场：这两行只是寒暄。" in full and "火星基地" in full
    assert "标题不匹配" in denied


@pytest.mark.asyncio
async def test_dispatch_without_items_unchanged(grow_rt):
    # 不传 items：长文仍走 digest 路径（stub.digest 会炸，grow_core 包成 RuntimeError）
    # → 证明默认路径没变、digest 仍在原路径被调用
    with pytest.raises(RuntimeError):
        await dispatch(content="这是一段超过三十个字的长文本内容需要走 digest 拆分路径来验证向后兼容性没有被破坏")


@pytest.mark.asyncio
async def test_grow_digest_failure_hides_provider_detail(grow_rt):
    _bucket_mgr, _stub = grow_rt
    provider_secret = "sk-provider-secret https://provider.invalid/private"

    class FailingDehydrator:
        async def digest(self, _content):
            raise RuntimeError(provider_secret)

    rt.dehydrator = FailingDehydrator()

    with pytest.raises(PublicToolError) as caught:
        await grow_core("这是一段需要调用摘要服务的长内容，长度足够进入 grow 的日记拆分主路径。")

    assert "OMBRE_COMPRESS_API_KEY" in caught.value.public_message
    assert provider_secret not in caught.value.public_message
    assert provider_secret not in str(caught.value)
    rendered_logs = "\n".join(
        str(call) for call in rt.logger.error.call_args_list
    )
    assert "RuntimeError" in rendered_logs
    assert provider_secret not in rendered_logs
