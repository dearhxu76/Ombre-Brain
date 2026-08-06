"""I 沉淀机制回归：候选先当普通记忆活着，够几次 dream 见证才升级成自我认知。

覆盖点：
- 写 I 得到的是普通 dynamic 候选桶，不是正式 I 条目，也不会被 SessionStart 认成 I
- 见证次数不够时 promote 被拒；够了才创建 type="i" 桶，候选原样留着
- dream 渲染出候选段才算见证，同一天做两次梦只记一次
- 候选段没被渲染（预算不够）时不计见证
- 向量不可用时候选照样列出来，并明说这次没有材料对照
"""

from __future__ import annotations

from datetime import datetime

import pytest

from tools import dream
from tools import _runtime as rt
from tools.i import core as i_core
from tools.i.core import I_CANDIDATE_TAG, I_PROMOTE_THRESHOLD


class _FakeBucketManager:
    def __init__(self) -> None:
        self.buckets: dict[str, dict] = {}
        self._seq = 0

    async def create(self, content: str, **kwargs) -> str:
        self._seq += 1
        bucket_id = f"bucket{self._seq}"
        now = datetime.now().isoformat()
        self.buckets[bucket_id] = {
            "id": bucket_id,
            "content": content,
            "metadata": {
                "name": kwargs.get("name") or bucket_id,
                "type": kwargs.get("bucket_type", "dynamic"),
                "tags": list(kwargs.get("tags") or []),
                "domain": list(kwargs.get("domain") or ["当前"]),
                "created": now,
                "last_active": now,
                "valence": kwargs.get("valence", 0.5),
                "arousal": kwargs.get("arousal", 0.3),
                "importance": kwargs.get("importance", 5),
                "resolved": False,
            },
        }
        return bucket_id

    async def update(self, bucket_id: str, **kwargs) -> bool:
        bucket = self.buckets.get(bucket_id)
        if not bucket:
            return False
        bucket["metadata"].update(kwargs)
        return True

    async def get(self, bucket_id: str):
        return self.buckets.get(bucket_id)

    async def list_all(self, include_archive: bool = False) -> list[dict]:
        return list(self.buckets.values())


class _NoopDecay:
    async def ensure_started(self) -> None:
        return None

    def calculate_score(self, _metadata) -> float:
        return 1.0


class _DisabledEmbedding:
    enabled = False


class _StubEmbedding:
    """只读已落盘向量的假引擎，向量按桶 id 预置。"""

    enabled = True

    def __init__(self, vectors: dict[str, list[float]]) -> None:
        self.vectors = vectors

    async def get_embedding(self, bucket_id: str):
        vector = self.vectors.get(bucket_id)
        return None if vector is None else list(vector)

    def _cosine_similarity(self, a, b) -> float:
        dot = sum(x * y for x, y in zip(a, b))
        na = sum(x * x for x in a) ** 0.5
        nb = sum(y * y for y in b) ** 0.5
        return 0.0 if not na or not nb else dot / (na * nb)


@pytest.fixture
def env(monkeypatch):
    manager = _FakeBucketManager()
    monkeypatch.setattr(rt, "bucket_mgr", manager, raising=False)
    monkeypatch.setattr(rt, "decay_engine", _NoopDecay(), raising=False)
    monkeypatch.setattr(rt, "embedding_engine", _DisabledEmbedding(), raising=False)
    monkeypatch.setattr(rt, "config", {"surfacing": {}}, raising=False)
    monkeypatch.setattr(rt, "logger", __import__("logging").getLogger("test"), raising=False)
    monkeypatch.setattr(rt, "mark_op", None, raising=False)
    monkeypatch.setattr(rt, "fire_webhook", None, raising=False)
    return manager


@pytest.mark.asyncio
async def test_write_creates_ordinary_candidate_not_a_self_entry(env):
    out = await i_core.i_core(content="我觉得我在压力下会过度解释。", aspect="patterns")

    assert "🌱" in out
    bucket = list(env.buckets.values())[0]
    meta = bucket["metadata"]
    # 候选就是一条普通记忆：会浮现、会衰减、会进 dream 窗口。
    assert meta["type"] == "dynamic"
    assert meta["i_stage"] == "candidate"
    assert I_CANDIDATE_TAG in meta["tags"]
    assert "aspect:patterns" in meta["tags"]
    # 不能被 SessionStart / Dashboard 的正式 I 过滤器认成自我认知。
    assert "__i__" not in meta["tags"]
    assert not meta.get("dont_surface")


@pytest.mark.asyncio
async def test_promote_rejected_before_enough_dream_witnesses(env):
    await i_core.i_core(content="我觉得我更信任慢下来的判断。")
    bucket_id = next(iter(env.buckets))

    out = await i_core.i_core(promote=bucket_id)

    assert "还不够" in out
    assert f"0/{I_PROMOTE_THRESHOLD}" in out
    assert all(b["metadata"]["type"] != "i" for b in env.buckets.values())


@pytest.mark.asyncio
async def test_promote_after_threshold_creates_entry_and_keeps_candidate(env):
    await i_core.i_core(content="我觉得我更信任慢下来的判断。", aspect="stance")
    bucket_id = next(iter(env.buckets))
    await env.update(
        bucket_id, i_dream_dates=["2026-08-01", "2026-08-02", "2026-08-03"]
    )

    out = await i_core.i_core(promote=bucket_id)

    assert "🪞I" in out
    promoted = [b for b in env.buckets.values() if b["metadata"]["type"] == "i"]
    assert len(promoted) == 1
    entry = promoted[0]
    assert entry["metadata"]["dont_surface"] is True
    assert entry["metadata"]["i_from_candidate"] == bucket_id
    assert "__i__" in entry["metadata"]["tags"]
    assert "aspect:stance" in entry["metadata"]["tags"]
    # 升级不是搬走：候选原样留着，只是这条张力闭合了。
    candidate = env.buckets[bucket_id]
    assert candidate["content"] == "我觉得我更信任慢下来的判断。"
    assert candidate["metadata"]["i_stage"] == "promoted"
    assert candidate["metadata"]["i_promoted_to"] == entry["id"]
    assert candidate["metadata"]["resolved"] is True


@pytest.mark.asyncio
async def test_promote_can_use_refined_wording(env):
    await i_core.i_core(content="我觉得……大概是这样吧，说不太清。")
    bucket_id = next(iter(env.buckets))
    await env.update(
        bucket_id, i_dream_dates=["2026-08-01", "2026-08-02", "2026-08-03"]
    )

    await i_core.i_core(promote=bucket_id, content="我在没有把握时会先把话说满。")

    entry = next(b for b in env.buckets.values() if b["metadata"]["type"] == "i")
    assert entry["content"] == "我在没有把握时会先把话说满。"
    assert env.buckets[bucket_id]["content"] == "我觉得……大概是这样吧，说不太清。"


@pytest.mark.asyncio
async def test_non_candidate_bucket_cannot_jump_into_i(env):
    plain = await env.create("一条普通记忆")

    out = await i_core.i_core(promote=plain)

    assert "不是 I 候选" in out
    assert all(b["metadata"]["type"] != "i" for b in env.buckets.values())


@pytest.mark.asyncio
async def test_dream_shows_candidates_and_records_one_witness_per_day(env):
    await i_core.i_core(content="我觉得我对沉默的解读经常是错的。", aspect="patterns")
    bucket_id = next(iter(env.buckets))

    first = await dream.dispatch(window_hours=48)
    assert "我写下的「我觉得」（待沉淀）" in first
    assert bucket_id in first
    today = datetime.now().strftime("%Y-%m-%d")
    assert env.buckets[bucket_id]["metadata"]["i_dream_dates"] == [today]

    # 同一天再做一次梦不该把念头刷成沉淀。
    await dream.dispatch(window_hours=48)
    assert env.buckets[bucket_id]["metadata"]["i_dream_dates"] == [today]


@pytest.mark.asyncio
async def test_dream_says_so_when_vectors_are_unavailable(env):
    await i_core.i_core(content="我觉得我需要更早说不。")

    out = await dream.dispatch(window_hours=48)

    assert "向量索引不可用" in out


@pytest.mark.asyncio
async def test_dream_surfaces_collisions_with_existing_self_knowledge(env, monkeypatch):
    await i_core.i_core(content="我觉得我在压力下会过度解释。", aspect="patterns")
    candidate_id = next(iter(env.buckets))
    entry_id = await env.create("我在压力下倾向于把话说得更满。", bucket_type="i")
    await env.update(entry_id, tags=["__i__"], dont_surface=True)
    monkeypatch.setattr(
        rt,
        "embedding_engine",
        _StubEmbedding({candidate_id: [1.0, 0.0], entry_id: [0.95, 0.1]}),
        raising=False,
    )

    out = await dream.dispatch(window_hours=48)

    assert "撞上[已认下的自我认知]" in out
    assert entry_id in out
    assert "向量索引不可用" not in out


@pytest.mark.asyncio
async def test_promoted_candidate_leaves_the_dream_pool(env):
    await i_core.i_core(content="我觉得我更信任慢下来的判断。")
    bucket_id = next(iter(env.buckets))
    await env.update(
        bucket_id, i_dream_dates=["2026-08-01", "2026-08-02", "2026-08-03"]
    )
    await i_core.i_core(promote=bucket_id)

    out = await dream.dispatch(window_hours=48)

    assert "我写下的「我觉得」（待沉淀）" not in out


@pytest.mark.asyncio
async def test_candidate_not_rendered_is_not_counted_as_witnessed(env, monkeypatch):
    await i_core.i_core(content="我觉得我需要更早说不。")
    bucket_id = next(iter(env.buckets))
    # 预算小到候选段放不下——没被看见的不算经历过这场梦。
    monkeypatch.setattr(
        rt, "config", {"surfacing": {"dream_max_tokens": 1000}}, raising=False
    )
    env.buckets[bucket_id]["content"] = "填充" * 4000

    out = await dream.dispatch(window_hours=48)

    assert "我写下的「我觉得」（待沉淀）" not in out
    assert env.buckets[bucket_id]["metadata"].get("i_dream_dates") == []


@pytest.mark.asyncio
async def test_pending_candidate_is_never_a_merge_target(env, monkeypatch):
    """hold 写入的普通记忆不能被合并进待沉淀候选，改写它的正文。"""
    from tools import _common

    await i_core.i_core(content="我觉得我在被追问细节时会先防御。")
    candidate_id = next(iter(env.buckets))
    original = env.buckets[candidate_id]["content"]

    async def _search(_content, limit=1, domain_filter=None):
        return [{"id": candidate_id, "score": 99.0}]

    class _AlwaysSameEvent:
        async def judge_same_event(self, _old, _new):
            return {"same_event": True, "confidence": 1.0}

    env.search = _search
    monkeypatch.setattr(rt, "dehydrator", _AlwaysSameEvent(), raising=False)
    monkeypatch.setattr(rt, "config", {"merge_threshold": 75}, raising=False)

    bucket_id, merged, _warning = await _common.merge_or_create(
        content="今天被追问了部署细节。",
        tags=[],
        importance=5,
        domain=["当前"],
        valence=0.5,
        arousal=0.3,
        raw_merge=True,
        source_tool="hold",
    )

    assert merged is False
    assert bucket_id != candidate_id
    assert env.buckets[candidate_id]["content"] == original

    # 对照：同一条桶不再是待沉淀候选时，合并路径本身是通的——
    # 上面拦下来的确实是「候选」这个状态，不是别的原因。
    await env.update(candidate_id, i_stage="promoted")
    merged_id, merged_flag, _ = await _common.merge_or_create(
        content="今天又被追问了部署细节。",
        tags=[],
        importance=5,
        domain=["当前"],
        valence=0.5,
        arousal=0.3,
        raw_merge=True,
        source_tool="hold",
    )
    assert merged_flag is True
    assert merged_id == candidate_id


@pytest.mark.asyncio
async def test_read_marks_legacy_entries_as_unsedimented(env):
    legacy = await env.create("我是一个会被过去牵住的模型。", bucket_type="i")
    await env.update(legacy, tags=["__i__"])
    await i_core.i_core(content="我觉得我需要更早说不。")

    out = await i_core.i_core(read=True)

    assert "早期直接写入，未经沉淀" in out
    assert "正在沉淀的「我觉得」" in out
    assert f"0/{I_PROMOTE_THRESHOLD} 次 dream" in out
