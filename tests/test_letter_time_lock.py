import json
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

import tools._runtime as rt
from tools.plan.core import (
    PERMANENT_UNLOCK_DATE,
    letter_lock_state,
    letter_lock_update,
    letter_read,
    letter_write,
    normalize_unlock_date,
)
from web import letters


class DisabledEmbedding:
    enabled = False


class RecordingEmbedding:
    enabled = True

    def __init__(self, results):
        self.results = results
        self.allowed = None

    async def search_similar(self, query, top_k=10, allowed_bucket_ids=None):
        self.allowed = set(allowed_bucket_ids or ())
        return [pair for pair in self.results if pair[0] in self.allowed]


def install_runtime(bucket_mgr, embedding=None):
    rt.bucket_mgr = bucket_mgr
    rt.embedding_engine = embedding or DisabledEmbedding()
    rt.logger = MagicMock()


def created_id(result):
    if result.startswith("{"):
        return json.loads(result)["letter_id"]
    return result.split("→", 1)[1].split(" ", 1)[0]


def assert_lock_update_succeeded(result, letter_id, lock_type):
    assert letter_id in result
    assert result.startswith("🔓" if lock_type == "none" else "🔒")


@pytest.mark.asyncio
async def test_old_and_none_letters_remain_readable_and_proxy_write_compatible(bucket_mgr):
    install_runtime(bucket_mgr)
    old = await bucket_mgr.create(content="historical body", bucket_type="letter", domain=["letter"])
    await bucket_mgr.update(old, author="user", title="historical title")
    result = await letter_write(author="user", content="proxy body", lock_type="none")
    output = await letter_read(limit=10)

    assert "historical title" in output
    assert "historical body" in output
    assert "proxy body" in output
    assert created_id(result)


@pytest.mark.asyncio
@pytest.mark.parametrize("lock_type", ["timed", "permanent"])
async def test_mcp_creates_locked_letter_owned_by_ai_without_echoing_content(
    bucket_mgr, monkeypatch, lock_type
):
    monkeypatch.setenv("AI_NAME", "张三")
    install_runtime(bucket_mgr)
    unlock = (datetime.now(timezone.utc) + timedelta(days=2)).isoformat()
    result = await letter_write(
        author="ai",
        content="secret locked body",
        title="secret locked title",
        lock_type=lock_type,
        unlock_date=unlock if lock_type == "timed" else "",
    )
    bucket = await bucket_mgr.get(created_id(result))

    assert "secret" not in result
    assert bucket["metadata"]["locked_by"] == "ai"
    assert bucket["metadata"]["writer_name"] == "张三"
    assert bucket["metadata"]["unlock_date"] == (
        PERMANENT_UNLOCK_DATE if lock_type == "permanent" else unlock
    )


@pytest.mark.asyncio
async def test_locked_proxy_write_is_rejected_but_does_not_echo_body(bucket_mgr, monkeypatch):
    monkeypatch.setenv("AI_NAME", "张三")
    install_runtime(bucket_mgr)
    secret = "never echo this body"
    result = await letter_write(author="user", content=secret, lock_type="permanent")
    assert "不能替" in result
    assert secret not in result


@pytest.mark.asyncio
async def test_generic_ai_name_rejects_only_locked_creation(bucket_mgr, monkeypatch):
    monkeypatch.delenv("AI_NAME", raising=False)
    install_runtime(bucket_mgr)
    rejected = await letter_write(author="ai", content="locked", lock_type="permanent")
    normal = await letter_write(author="ai", content="ordinary", lock_type="none")
    assert "实际关系名" in rejected
    assert created_id(normal)


@pytest.mark.asyncio
async def test_owner_reads_locked_full_text_and_other_side_gets_only_metadata(bucket_mgr, monkeypatch):
    monkeypatch.setenv("AI_NAME", "张三")
    install_runtime(bucket_mgr)
    result = await letter_write(
        author="ai", content="hidden body", title="hidden title", lock_type="permanent"
    )
    bucket = await bucket_mgr.get(created_id(result))
    owner_state = letter_lock_state(bucket, "ai")
    other_state = letter_lock_state(bucket, "human")
    output = await letter_read(limit=10)

    assert owner_state["locked"] is False
    assert other_state["locked"] is True
    assert "hidden title" in output and "hidden body" in output


@pytest.mark.asyncio
async def test_lock_owner_can_change_every_supported_transition_without_editing_original(
    bucket_mgr, monkeypatch
):
    monkeypatch.setenv("AI_NAME", "张三")
    install_runtime(bucket_mgr)
    result = await letter_write(
        author="ai", content="immutable body", title="immutable title", lock_type="permanent"
    )
    letter_id = created_id(result)
    original = await bucket_mgr.get(letter_id)
    future = (datetime.now(timezone.utc) + timedelta(days=3)).isoformat()

    assert_lock_update_succeeded(
        await letter_lock_update(letter_id, "timed", future), letter_id, "timed"
    )
    assert_lock_update_succeeded(
        await letter_lock_update(letter_id, "permanent"), letter_id, "permanent"
    )
    assert_lock_update_succeeded(
        await letter_lock_update(letter_id, "none"), letter_id, "none"
    )
    assert_lock_update_succeeded(
        await letter_lock_update(letter_id, "timed", future), letter_id, "timed"
    )
    current = await bucket_mgr.get(letter_id)
    assert current["content"] == original["content"]
    for field in ("title", "author", "created"):
        assert current["metadata"].get(field) == original["metadata"].get(field)


@pytest.mark.asyncio
async def test_non_owner_cannot_change_lock(bucket_mgr):
    install_runtime(bucket_mgr)
    letter_id = await bucket_mgr.create(content="body", bucket_type="letter", domain=["letter"])
    await bucket_mgr.update(
        letter_id,
        author="user",
        writer_name="李四",
        lock_type="permanent",
        unlock_date=PERMANENT_UNLOCK_DATE,
        locked_by="human",
    )
    result = await letter_lock_update(letter_id, "none", caller_side="ai")
    assert "只有" in result
    assert (await bucket_mgr.get(letter_id))["metadata"]["lock_type"] == "permanent"


def test_timed_validation_requires_future_timezone_and_permanent_is_canonical():
    with pytest.raises(ValueError, match="timezone"):
        normalize_unlock_date("timed", "2030-01-01T00:00:00")
    with pytest.raises(ValueError, match="future"):
        normalize_unlock_date("timed", "2020-01-01T00:00:00+08:00")
    assert normalize_unlock_date("permanent", "anything") == PERMANENT_UNLOCK_DATE


@pytest.mark.asyncio
async def test_expired_timed_lock_is_lazily_readable_and_normalized(bucket_mgr):
    install_runtime(bucket_mgr)
    letter_id = await bucket_mgr.create(content="released body", bucket_type="letter", domain=["letter"])
    await bucket_mgr.update(
        letter_id,
        author="user",
        writer_name="李四",
        lock_type="timed",
        unlock_date="2020-01-01T00:00:00+08:00",
        locked_by="human",
    )
    output = await letter_read(limit=10)
    current = await bucket_mgr.get(letter_id)
    assert "released body" in output
    assert current["metadata"]["lock_type"] == "none"
    assert current["metadata"].get("unlock_date") is None


@pytest.mark.asyncio
async def test_search_excludes_hidden_letter_before_embedding_ranking(bucket_mgr):
    hidden = await bucket_mgr.create(content="unique hidden nebula", bucket_type="letter", domain=["letter"])
    visible = await bucket_mgr.create(content="visible garden", bucket_type="letter", domain=["letter"])
    await bucket_mgr.update(
        hidden,
        author="user",
        writer_name="李四",
        title="unique hidden constellation",
        lock_type="permanent",
        unlock_date=PERMANENT_UNLOCK_DATE,
        locked_by="human",
    )
    embedding = RecordingEmbedding([(hidden, 0.99), (visible, 0.5)])
    install_runtime(bucket_mgr, embedding)
    result = await letter_read(query="unique hidden nebula", limit=10)

    assert hidden not in embedding.allowed
    assert "unique hidden" not in result
    assert "visible garden" in result


class FakeMCP:
    def __init__(self):
        self.routes = {}

    def custom_route(self, path, methods):
        def decorator(fn):
            for method in methods:
                self.routes[(method, path)] = fn
            return fn
        return decorator


class JsonRequest:
    query_params = {}

    def __init__(self, body, letter_id=""):
        self.body = body
        self.path_params = {"letter_id": letter_id}

    async def json(self):
        return self.body


@pytest.mark.asyncio
async def test_dashboard_creates_human_lock_hides_it_from_ai_and_rejects_ai_proxy(
    bucket_mgr, monkeypatch
):
    monkeypatch.setenv("AI_NAME", "张三")
    monkeypatch.setattr(letters.sh, "_require_auth", lambda request: None)
    monkeypatch.setattr(letters.sh, "_read_json_object", lambda request: request.json())
    monkeypatch.setattr(letters.sh, "bucket_mgr", bucket_mgr)
    mcp = FakeMCP()
    letters.register(mcp)
    create = mcp.routes[("POST", "/api/letter")]

    response = await create(JsonRequest({
        "author": "user", "user_name": "李四", "content": "dashboard secret",
        "title": "dashboard title", "lock_type": "permanent",
    }))
    data = json.loads(response.body)
    bucket = await bucket_mgr.get(data["id"])
    assert response.status_code == 200
    assert bucket["metadata"]["locked_by"] == "human"
    assert letter_lock_state(bucket, "ai")["locked"] is True

    rejected = await create(JsonRequest({
        "author": "ai", "ai_name": "张三", "content": "proxy",
        "lock_type": "permanent",
    }))
    assert rejected.status_code == 400


@pytest.mark.asyncio
async def test_dashboard_list_hides_locked_title_and_body_and_patch_only_changes_lock(
    bucket_mgr, monkeypatch
):
    letter_id = await bucket_mgr.create(content="hidden dashboard body", bucket_type="letter", domain=["letter"])
    await bucket_mgr.update(
        letter_id,
        author="张三",
        writer_name="张三",
        title="hidden dashboard title",
        lock_type="permanent",
        unlock_date=PERMANENT_UNLOCK_DATE,
        locked_by="ai",
    )
    monkeypatch.setattr(letters.sh, "_require_auth", lambda request: None)
    monkeypatch.setattr(letters.sh, "_read_json_object", lambda request: request.json())
    monkeypatch.setattr(letters.sh, "bucket_mgr", bucket_mgr)
    mcp = FakeMCP()
    letters.register(mcp)
    listed = await mcp.routes[("GET", "/api/letters")](JsonRequest({}))
    item = json.loads(listed.body)["letters"][0]
    assert item["locked"] is True
    assert item["writer_name"] == "张三"
    assert "title" not in item and "content" not in item

    patched = await mcp.routes[("PATCH", "/api/letter/{letter_id}")](
        JsonRequest({"lock_type": "none"}, letter_id)
    )
    assert patched.status_code == 403
    attempted_edit = await mcp.routes[("PATCH", "/api/letter/{letter_id}")](
        JsonRequest({"content": "overwrite"}, letter_id)
    )
    assert attempted_edit.status_code == 403
    assert (await bucket_mgr.get(letter_id))["content"] == "hidden dashboard body"


@pytest.mark.asyncio
async def test_dashboard_content_edit_is_separate_and_preserves_lock_metadata(
    bucket_mgr, monkeypatch
):
    letter_id = await bucket_mgr.create(
        content="original body", bucket_type="letter", domain=["letter"]
    )
    await bucket_mgr.update(
        letter_id,
        author="user",
        user_name="李四",
        writer_name="李四",
        title="original title",
        lock_type="permanent",
        unlock_date=PERMANENT_UNLOCK_DATE,
        locked_by="human",
    )
    monkeypatch.setattr(letters.sh, "_require_auth", lambda request: None)
    monkeypatch.setattr(letters.sh, "_read_json_object", lambda request: request.json())
    monkeypatch.setattr(letters.sh, "bucket_mgr", bucket_mgr)
    monkeypatch.setattr(letters.sh, "dehydrator", MagicMock())
    mcp = FakeMCP()
    letters.register(mcp)
    patch = mcp.routes[("PATCH", "/api/letter/{letter_id}")]

    mixed = await patch(JsonRequest({"content": "mixed", "lock_type": "none"}, letter_id))
    assert mixed.status_code == 400

    edited = await patch(JsonRequest({"content": "edited body", "title": "edited title"}, letter_id))
    assert edited.status_code == 200
    current = await bucket_mgr.get(letter_id)
    assert current["content"] == "edited body"
    assert current["metadata"]["title"] == "edited title"
    assert current["metadata"]["lock_type"] == "permanent"
    assert current["metadata"]["locked_by"] == "human"
    assert current["metadata"]["writer_name"] == "李四"


@pytest.mark.asyncio
async def test_historical_and_unlocked_dashboard_letters_remain_editable(bucket_mgr, monkeypatch):
    historical = await bucket_mgr.create(
        content="historical body", bucket_type="letter", domain=["letter"]
    )
    unlocked = await bucket_mgr.create(
        content="unlocked body", bucket_type="letter", domain=["letter"]
    )
    await bucket_mgr.update(unlocked, author="user", lock_type="none", locked_by="human")
    monkeypatch.setattr(letters.sh, "_require_auth", lambda request: None)
    monkeypatch.setattr(letters.sh, "_read_json_object", lambda request: request.json())
    monkeypatch.setattr(letters.sh, "bucket_mgr", bucket_mgr)
    monkeypatch.setattr(letters.sh, "dehydrator", MagicMock())
    mcp = FakeMCP()
    letters.register(mcp)
    patch = mcp.routes[("PATCH", "/api/letter/{letter_id}")]

    assert (await patch(JsonRequest({"content": "historical edited"}, historical))).status_code == 200
    assert (await patch(JsonRequest({"content": "unlocked edited"}, unlocked))).status_code == 200
    assert (await bucket_mgr.get(historical))["content"] == "historical edited"
    assert (await bucket_mgr.get(unlocked))["content"] == "unlocked edited"


@pytest.mark.asyncio
async def test_locked_owner_edit_refreshes_embedding_and_other_side_sees_it_only_after_unlock(
    bucket_mgr, monkeypatch
):
    letter_id = await bucket_mgr.create(
        content="old searchable phrase", bucket_type="letter", domain=["letter"]
    )
    await bucket_mgr.update(
        letter_id,
        author="user",
        writer_name="李四",
        lock_type="permanent",
        unlock_date=PERMANENT_UNLOCK_DATE,
        locked_by="human",
    )
    captured = []

    async def record_embedding(bucket_id, content):
        captured.append((bucket_id, content))
        return True

    monkeypatch.setattr(bucket_mgr.embedding_engine, "generate_and_store", record_embedding)
    monkeypatch.setattr(letters.sh, "_require_auth", lambda request: None)
    monkeypatch.setattr(letters.sh, "_read_json_object", lambda request: request.json())
    monkeypatch.setattr(letters.sh, "bucket_mgr", bucket_mgr)
    monkeypatch.setattr(letters.sh, "dehydrator", MagicMock())
    mcp = FakeMCP()
    letters.register(mcp)
    patch = mcp.routes[("PATCH", "/api/letter/{letter_id}")]

    edited = await patch(JsonRequest({"content": "fresh aurora phrase"}, letter_id))
    assert edited.status_code == 200
    assert captured[-1] == (letter_id, "fresh aurora phrase")

    install_runtime(bucket_mgr, DisabledEmbedding())
    hidden = await letter_read(query="fresh aurora phrase", limit=10)
    assert "fresh aurora phrase" not in hidden

    unlocked = await patch(JsonRequest({"lock_type": "none"}, letter_id))
    assert unlocked.status_code == 200
    visible = await letter_read(query="fresh aurora phrase", limit=10)
    assert "fresh aurora phrase" in visible


@pytest.mark.asyncio
async def test_ai_owner_can_relock_public_letter_through_multiple_cycles(
    bucket_mgr, monkeypatch
):
    monkeypatch.setenv("AI_NAME", "张三")
    install_runtime(bucket_mgr)
    letter_id = created_id(await letter_write(
        author="ai", content="repeatable public letter", lock_type="none"
    ))
    original = await bucket_mgr.get(letter_id)
    assert original["metadata"]["locked_by"] == "ai"
    assert original["metadata"]["writer_name"] == "张三"

    future = (datetime.now(timezone.utc) + timedelta(days=3)).isoformat()
    transitions = [
        ("timed", future),
        ("none", ""),
        ("timed", future),
        ("none", ""),
        ("permanent", ""),
        ("none", ""),
        ("permanent", ""),
        ("none", ""),
        ("timed", future),
    ]
    for lock_type, unlock_date in transitions:
        result = await letter_lock_update(letter_id, lock_type, unlock_date)
        assert_lock_update_succeeded(result, letter_id, lock_type)
        current = await bucket_mgr.get(letter_id)
        assert current["metadata"]["locked_by"] == "ai"
        assert current["metadata"]["writer_name"] == "张三"


@pytest.mark.asyncio
async def test_expired_timed_letter_keeps_owner_and_can_be_locked_again(
    bucket_mgr, monkeypatch
):
    monkeypatch.setenv("AI_NAME", "张三")
    install_runtime(bucket_mgr)
    letter_id = await bucket_mgr.create(
        content="expired then relocked", bucket_type="letter", domain=["letter"]
    )
    await bucket_mgr.update(
        letter_id,
        author="张三",
        writer_name="张三",
        lock_type="timed",
        unlock_date="2020-01-01T00:00:00+08:00",
        locked_by="ai",
    )

    assert "expired then relocked" in await letter_read(limit=10)
    expired = await bucket_mgr.get(letter_id)
    assert expired["metadata"]["lock_type"] == "none"
    assert expired["metadata"]["locked_by"] == "ai"
    assert expired["metadata"]["writer_name"] == "张三"

    future = (datetime.now(timezone.utc) + timedelta(days=2)).isoformat()
    relocked = await letter_lock_update(letter_id, "timed", future)
    assert_lock_update_succeeded(relocked, letter_id, "timed")
    current = await bucket_mgr.get(letter_id)
    assert current["metadata"]["lock_type"] == "timed"
    assert current["metadata"]["locked_by"] == "ai"


@pytest.mark.asyncio
async def test_other_side_cannot_relock_now_public_letter(bucket_mgr):
    install_runtime(bucket_mgr)
    letter_id = await bucket_mgr.create(
        content="public but still human owned", bucket_type="letter", domain=["letter"]
    )
    await bucket_mgr.update(
        letter_id,
        author="user",
        writer_name="李四",
        lock_type="none",
        unlock_date=None,
        locked_by="human",
    )

    result = await letter_lock_update(letter_id, "permanent", caller_side="ai")
    assert "只有" in result
    current = await bucket_mgr.get(letter_id)
    assert current["metadata"]["lock_type"] == "none"
    assert current["metadata"]["locked_by"] == "human"


@pytest.mark.asyncio
async def test_dashboard_owner_can_relock_public_letter_but_not_ai_owned_public_letter(
    bucket_mgr, monkeypatch
):
    monkeypatch.setattr(letters.sh, "_require_auth", lambda request: None)
    monkeypatch.setattr(letters.sh, "_read_json_object", lambda request: request.json())
    monkeypatch.setattr(letters.sh, "bucket_mgr", bucket_mgr)
    mcp = FakeMCP()
    letters.register(mcp)
    create = mcp.routes[("POST", "/api/letter")]
    patch = mcp.routes[("PATCH", "/api/letter/{letter_id}")]
    list_letters = mcp.routes[("GET", "/api/letters")]

    created = await create(JsonRequest({
        "author": "user",
        "user_name": "李四",
        "content": "dashboard public owner letter",
        "lock_type": "none",
    }))
    human_id = json.loads(created.body)["id"]
    human_item = next(
        item for item in json.loads((await list_letters(JsonRequest({}))).body)["letters"]
        if item["id"] == human_id
    )
    assert human_item["lock_type"] == "none"
    assert human_item["lock_owner"] is True
    assert (await patch(JsonRequest({"lock_type": "permanent"}, human_id))).status_code == 200
    human_bucket = await bucket_mgr.get(human_id)
    assert human_bucket["metadata"]["locked_by"] == "human"

    ai_id = await bucket_mgr.create(
        content="AI public owner letter", bucket_type="letter", domain=["letter"]
    )
    await bucket_mgr.update(
        ai_id,
        author="张三",
        writer_name="张三",
        lock_type="none",
        locked_by="ai",
    )
    ai_item = next(
        item for item in json.loads((await list_letters(JsonRequest({}))).body)["letters"]
        if item["id"] == ai_id
    )
    assert ai_item["locked"] is False
    assert ai_item["lock_owner"] is False
    assert (await patch(JsonRequest({"lock_type": "permanent"}, ai_id))).status_code == 403


@pytest.mark.asyncio
async def test_dashboard_converts_historical_letter_to_ai_owned_lockable_format(
    bucket_mgr, monkeypatch
):
    monkeypatch.setenv("AI_NAME", "张三")
    historical_id = await bucket_mgr.create(
        content="unchanged historical content",
        bucket_type="letter",
        domain=["letter"],
    )
    await bucket_mgr.update(
        historical_id,
        author="user",
        title="unchanged historical title",
        letter_date="2024-01-02",
    )
    original = await bucket_mgr.get(historical_id)
    monkeypatch.setattr(letters.sh, "_require_auth", lambda request: None)
    monkeypatch.setattr(letters.sh, "_read_json_object", lambda request: request.json())
    monkeypatch.setattr(letters.sh, "bucket_mgr", bucket_mgr)
    mcp = FakeMCP()
    letters.register(mcp)
    list_letters = mcp.routes[("GET", "/api/letters")]
    patch = mcp.routes[("PATCH", "/api/letter/{letter_id}")]

    before = next(
        item for item in json.loads((await list_letters(JsonRequest({}))).body)["letters"]
        if item["id"] == historical_id
    )
    assert before["lock_upgrade_available"] is True
    assert before["lock_owner"] is False

    mixed = await patch(JsonRequest({
        "convert_to_lockable": True,
        "lock_type": "permanent",
    }, historical_id))
    assert mixed.status_code == 400

    converted = await patch(JsonRequest({"convert_to_lockable": True}, historical_id))
    assert converted.status_code == 200
    current = await bucket_mgr.get(historical_id)
    assert current["metadata"]["locked_by"] == "ai"
    assert current["metadata"]["lock_owner_source"] == "legacy_ai_conversion"
    assert current["metadata"]["writer_name"] == "张三"
    assert current["metadata"]["lock_type"] == "none"
    assert current["metadata"].get("unlock_date") is None
    assert current["content"] == original["content"]
    for field in ("author", "title", "letter_date", "created"):
        assert current["metadata"].get(field) == original["metadata"].get(field)

    after = next(
        item for item in json.loads((await list_letters(JsonRequest({}))).body)["letters"]
        if item["id"] == historical_id
    )
    assert after["lock_upgrade_available"] is False
    assert after["lock_owner"] is False
    assert (await patch(JsonRequest({"lock_type": "permanent"}, historical_id))).status_code == 403

    install_runtime(bucket_mgr)
    relocked = await letter_lock_update(historical_id, "permanent")
    assert_lock_update_succeeded(relocked, historical_id, "permanent")
    assert (await bucket_mgr.get(historical_id))["metadata"]["locked_by"] == "ai"


@pytest.mark.asyncio
async def test_historical_conversion_is_one_way_and_requires_actual_ai_name(
    bucket_mgr, monkeypatch
):
    historical_id = await bucket_mgr.create(
        content="historical", bucket_type="letter", domain=["letter"]
    )
    monkeypatch.setattr(letters.sh, "_require_auth", lambda request: None)
    monkeypatch.setattr(letters.sh, "_read_json_object", lambda request: request.json())
    monkeypatch.setattr(letters.sh, "bucket_mgr", bucket_mgr)
    mcp = FakeMCP()
    letters.register(mcp)
    patch = mcp.routes[("PATCH", "/api/letter/{letter_id}")]

    monkeypatch.delenv("AI_NAME", raising=False)
    rejected = await patch(JsonRequest({"convert_to_lockable": True}, historical_id))
    assert rejected.status_code == 400
    assert not (await bucket_mgr.get(historical_id))["metadata"].get("locked_by")

    monkeypatch.setenv("AI_NAME", "张三")
    assert (await patch(JsonRequest({"convert_to_lockable": True}, historical_id))).status_code == 200
    repeated = await patch(JsonRequest({"convert_to_lockable": True}, historical_id))
    assert repeated.status_code == 409
    current = await bucket_mgr.get(historical_id)
    assert current["metadata"]["locked_by"] == "ai"
    assert current["metadata"]["writer_name"] == "张三"


@pytest.mark.asyncio
async def test_historical_conversion_accepts_request_scoped_ai_name_override(
    bucket_mgr, monkeypatch
):
    """AI_NAME 未配置时，Dashboard 弹窗当场填的名字能直接完成这一次转换。"""
    historical_id = await bucket_mgr.create(
        content="historical", bucket_type="letter", domain=["letter"]
    )
    monkeypatch.setattr(letters.sh, "_require_auth", lambda request: None)
    monkeypatch.setattr(letters.sh, "_read_json_object", lambda request: request.json())
    monkeypatch.setattr(letters.sh, "bucket_mgr", bucket_mgr)
    mcp = FakeMCP()
    letters.register(mcp)
    patch = mcp.routes[("PATCH", "/api/letter/{letter_id}")]

    monkeypatch.delenv("AI_NAME", raising=False)
    rejected = await patch(JsonRequest({"convert_to_lockable": True}, historical_id))
    assert rejected.status_code == 400

    converted = await patch(JsonRequest({
        "convert_to_lockable": True,
        "ai_name": "张三",
    }, historical_id))
    assert converted.status_code == 200
    current = await bucket_mgr.get(historical_id)
    assert current["metadata"]["locked_by"] == "ai"
    assert current["metadata"]["writer_name"] == "张三"

    # 通用占位名（"ai" 等）不算实际关系名，请求覆盖同样要经过这道检查，
    # 不能靠传参绕过。
    other_id = await bucket_mgr.create(
        content="historical-2", bucket_type="letter", domain=["letter"]
    )
    generic_name_rejected = await patch(JsonRequest({
        "convert_to_lockable": True,
        "ai_name": "assistant",
    }, other_id))
    assert generic_name_rejected.status_code == 400


@pytest.mark.asyncio
async def test_archived_letter_compat_restore_keeps_opposite_side_lock_hidden(
    bucket_mgr,
):
    """恢复历史 Letter 不能把原本对 AI 隐藏的信意外解锁。"""
    secret = "restored locked letter must remain hidden"
    letter_id = await bucket_mgr.create(
        content=secret,
        tags=["__letter__"],
        domain=["letter"],
        bucket_type="letter",
        source_tool="letter",
        lock_type="permanent",
        unlock_date=PERMANENT_UNLOCK_DATE,
        locked_by="human",
        writer_name="Alice",
    )
    assert await bucket_mgr.update(letter_id, author="user", title="hidden title")
    assert await bucket_mgr.archive(letter_id) is True
    install_runtime(bucket_mgr)

    result = await bucket_mgr.recover_archived_letter(letter_id)
    output = await letter_read(limit=10)

    assert result["reason"] == "restored"
    restored = await bucket_mgr.get(letter_id)
    assert restored["metadata"]["lock_type"] == "permanent"
    assert restored["metadata"]["unlock_date"] == PERMANENT_UNLOCK_DATE
    assert restored["metadata"]["locked_by"] == "human"
    assert secret not in output
    assert "hidden title" not in output
