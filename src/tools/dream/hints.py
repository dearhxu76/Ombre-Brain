"""
========================================
tools/dream/hints.py — dream 的连接提示、结晶提示与 I 候选碰撞材料
========================================

三样东西，都是帮模型「看见」它自己没注意到的关联，都不下结论：

- 连接提示：在 recent 桶里找余弦相似度最高的一对（>0.5）→ 提示
  「这两个似乎有关联，不替你下结论，你自己想」
- 结晶提示：扫所有 feel，发现某条 feel 与 ≥2 条其它 feel 相似度
  >0.7 → 提示「你已经写过 N 条相似的 feel，可以考虑 hold(pinned=True)
  升级它」
- I 候选碰撞材料：把每条待沉淀的「我觉得……」和语义上最挨着的几条记忆
  摆在一起——支持它的、反驳它的、跟它撞车的其它候选，都只是材料

关键行为：
- 都依赖 embedding_engine.enabled；未启用时返回空串 / 空材料并明说
- 任意异常都吞掉，只 warning，不影响 dream 主流程
- 只读已落盘向量，不发新的 embedding 请求

不做什么（边界）：
- 不写桶，不修改任何状态（候选的见证计数由 tools/i 在 dream 渲染后写）
- 不判断谁对谁错、谁跟谁矛盾——那是认知层，rule.md 第 5 条
- 不替模型决定，只给「不替你下结论」的提示

对外暴露：build_connection_hint(recent) / build_crystal_hint(all_buckets)
         / collect_self_candidates(all_buckets)
========================================
"""

from dataclasses import dataclass, field

from ..i import I_PROMOTE_THRESHOLD, is_pending_candidate
from .. import _runtime as rt
from utils import strip_wikilinks

# 对照池上限：正式 I 条目和其它候选永远全量参与碰撞，普通记忆只取最近这么多条。
# get_embedding 每条一次 sqlite 查询，全库几千条会让 dream 明显变慢，而更老的
# 记忆本来也很难说是「此刻还在场」的对照。
_COLLISION_POOL_RECENT = 200
# 每条候选最多摆几条材料，以及低于多少相似度就不值得摆出来。
_COLLISION_TOP_K = 3
_COLLISION_MIN_SIM = 0.35


async def build_connection_hint(recent: list) -> str:
    if not (rt.embedding_engine and rt.embedding_engine.enabled and len(recent) >= 2):
        return ""
    try:
        best_pair = None
        best_sim = 0.0
        ids = [b["id"] for b in recent]
        names = {b["id"]: b["metadata"].get("name", b["id"]) for b in recent}
        embeddings: dict = {}
        for bid in ids:
            emb = await rt.embedding_engine.get_embedding(bid)
            if emb is not None:
                embeddings[bid] = emb
        for i, id_a in enumerate(ids):
            for id_b in ids[i + 1:]:
                if id_a in embeddings and id_b in embeddings:
                    sim = rt.embedding_engine._cosine_similarity(embeddings[id_a], embeddings[id_b])
                    if sim > best_sim:
                        best_sim = sim
                        best_pair = (id_a, id_b)
        if best_pair and best_sim > 0.5:
            return (
                f"\n💭 [{names[best_pair[0]]}] 和 [{names[best_pair[1]]}] "
                f"似乎有关联 (相似度:{best_sim:.2f})——不替你下结论，你自己想。\n"
            )
    except Exception as e:
        rt.logger.warning(f"Dream connection hint failed: {e}")
    return ""


async def build_crystal_hint(all_buckets: list) -> str:
    if not (rt.embedding_engine and rt.embedding_engine.enabled):
        return ""
    try:
        feels = [b for b in all_buckets if b["metadata"].get("type") == "feel"]
        if len(feels) < 3:
            return ""
        feel_embeddings: dict = {}
        for f in feels:
            emb = await rt.embedding_engine.get_embedding(f["id"])
            if emb is not None:
                feel_embeddings[f["id"]] = emb
        for fid, femb in feel_embeddings.items():
            similar_feels = []
            for oid, oemb in feel_embeddings.items():
                if oid != fid:
                    sim = rt.embedding_engine._cosine_similarity(femb, oemb)
                    if sim > 0.7:
                        similar_feels.append(oid)
            if len(similar_feels) >= 2:
                feel_bucket = next((f for f in feels if f["id"] == fid), None)
                if feel_bucket and not feel_bucket["metadata"].get("pinned"):
                    content_preview = strip_wikilinks(feel_bucket["content"][:80])
                    return (
                        f"\n🔮 你已经写过 {len(similar_feels)+1} 条相似的 feel "
                        f"（围绕「{content_preview}…」）。"
                        f"如果这已经是确信而不只是感受了，"
                        f"你可以用 hold(content=\"...\", pinned=True) 升级它。"
                        f"不急，你自己决定。\n"
                    )
    except Exception as e:
        rt.logger.warning(f"Dream crystallization hint failed: {e}")
    return ""


@dataclass
class SelfCandidate:
    """一条待沉淀的「我觉得……」，和这次梦里跟它撞上的材料。"""

    bucket: dict
    passes: list[str]
    collisions: list[tuple[dict, float]] = field(default_factory=list)


@dataclass
class SelfReview:
    """dream 里的 I 候选段。

    ``rendered_ids`` 由 output.py 在实际渲染出某条候选后回写，dream 的
    dispatch 只给这些候选记「被见证过一次」——没被看见的不算经历过。
    """

    candidates: list[SelfCandidate] = field(default_factory=list)
    vectors_available: bool = False
    threshold: int = I_PROMOTE_THRESHOLD
    rendered_ids: list[str] = field(default_factory=list)


def _timestamp_key(bucket: dict) -> str:
    meta = bucket.get("metadata") or {}
    return str(meta.get("last_active") or meta.get("created") or "")


async def collect_self_candidates(all_buckets: list) -> SelfReview:
    """收集待沉淀的 I 候选，并为每条取几条语义上最挨着的对照材料。

    材料只是材料：支持、反驳、撞车都可能，这里不做任何判定。
    """
    pending = [b for b in all_buckets if is_pending_candidate(b)]
    if not pending:
        return SelfReview()

    pending.sort(key=lambda b: str((b.get("metadata") or {}).get("created") or ""))
    review = SelfReview(
        candidates=[
            SelfCandidate(
                bucket=b,
                passes=[
                    str(d)[:10]
                    for d in ((b.get("metadata") or {}).get("i_dream_dates") or [])
                    if str(d).strip()
                ],
            )
            for b in pending
        ]
    )

    if not (rt.embedding_engine and rt.embedding_engine.enabled):
        return review

    try:
        pending_ids = {b["id"] for b in pending}
        # 正式 I 条目和其它候选永远参与——候选最该撞的就是「我已经认下的东西」
        # 和「我另一个还没想清楚的念头」。
        pool = [
            b for b in all_buckets
            if b["id"] not in pending_ids
            and (b.get("metadata") or {}).get("type") == "i"
        ]
        ordinary = [
            b for b in all_buckets
            if b["id"] not in pending_ids
            and (b.get("metadata") or {}).get("type") not in ("i", "letter")
        ]
        ordinary.sort(key=_timestamp_key, reverse=True)
        pool.extend(ordinary[:_COLLISION_POOL_RECENT])
        # 候选之间也要能撞上：两个还没想清楚的念头互相矛盾，正是最该被看见的。
        pool.extend(pending)

        embeddings: dict = {}
        for b in pool:
            bid = b["id"]
            if bid in embeddings:
                continue
            emb = await rt.embedding_engine.get_embedding(bid)
            if emb is not None:
                embeddings[bid] = emb
        if not embeddings:
            return review
        review.vectors_available = True

        by_id = {b["id"]: b for b in pool}
        for candidate in review.candidates:
            cid = candidate.bucket["id"]
            cemb = embeddings.get(cid)
            if cemb is None:
                continue
            scored: list[tuple[dict, float]] = []
            for other_id, oemb in embeddings.items():
                if other_id == cid or other_id not in by_id:
                    continue
                sim = rt.embedding_engine._cosine_similarity(cemb, oemb)
                if sim >= _COLLISION_MIN_SIM:
                    scored.append((by_id[other_id], sim))
            scored.sort(key=lambda pair: pair[1], reverse=True)
            candidate.collisions = scored[:_COLLISION_TOP_K]
    except Exception as e:
        rt.logger.warning(f"Dream self candidate collisions failed: {e}")

    return review
