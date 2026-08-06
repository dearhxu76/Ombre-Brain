"""Spark R1 严格输入、深冻结与 Unicode 来源契约。"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import FrozenInstanceError
import json

import pytest

from tests.spark_r1_support import cue, event_payload, scenario_payload

from spark_r1 import (
    EvaluationAxes,
    InstrumentMetadata,
    SparkInputError,
    SparkIntegrityError,
    SparkPolicyError,
    parse_scenario,
    source_hash,
    strict_json_loads,
)
from spark_r1.schema import MAX_INPUT_BYTES


def test_parse_scenario_copies_mutable_input_into_frozen_contracts():
    raw = scenario_payload()
    original_text = raw["events"][0]["text"]

    parsed = parse_scenario(raw)
    raw["events"][0]["text"] = "调用方随后篡改的文本"
    raw["tension_structure"]["entities"].append("新增")

    assert parsed.snapshot.events[0].text == original_text
    assert parsed.request.tension_structure.entities == ("项目",)
    assert parsed.request.inspiration_requested is True
    with pytest.raises(FrozenInstanceError):
        parsed.snapshot.events[0].text = "不能修改"


@pytest.mark.parametrize("field", ["owner", "owner_id", "vault", "vault_path", "buckets_dir", "output"])
def test_scenario_rejects_owner_vault_and_output_path_fields(field):
    raw = scenario_payload()
    raw[field] = "synthetic-looking-but-forbidden"

    with pytest.raises(SparkInputError):
        parse_scenario(raw)


def test_strict_json_rejects_duplicate_keys_nan_and_excessive_depth():
    with pytest.raises(SparkInputError):
        strict_json_loads('{"schema_version": 1, "schema_version": 1}')
    with pytest.raises(SparkInputError):
        strict_json_loads('{"seed": NaN}')

    nested: object = "leaf"
    for _ in range(20):
        nested = {"nested": nested}
    with pytest.raises(SparkInputError):
        parse_scenario(nested)


@pytest.mark.parametrize("invalid_number", [float("nan"), float("inf"), float("-inf")])
def test_dict_scenario_rejects_non_finite_json_numbers(invalid_number):
    raw = scenario_payload()
    raw["seed"] = invalid_number

    with pytest.raises(SparkInputError, match="strict serializable UTF-8 JSON"):
        parse_scenario(raw)


def test_dict_scenario_rejects_cycles_and_unserializable_values_without_echoing_input():
    class UnsupportedPrivateValue:
        pass

    private_marker = "do-not-echo-this-dict-input"
    cyclic: list[object] = [private_marker]
    cyclic.append(cyclic)
    cases = (cyclic, {private_marker: UnsupportedPrivateValue()})

    for invalid_value in cases:
        raw = scenario_payload()
        raw["events"] = invalid_value
        with pytest.raises(SparkInputError) as exc_info:
            parse_scenario(raw)
        assert private_marker not in str(exc_info.value)


def test_dict_scenario_applies_utf8_byte_budget_before_character_contracts():
    emoji_text = "🧠" * 7_900
    events = [
        event_payload(f"emoji-source-{index}", emoji_text)
        for index in range(63)
    ]
    raw = scenario_payload(events=events)
    compact_utf8 = json.dumps(
        raw,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    ).encode("utf-8")

    assert sum(len(event["text"]) for event in events) == 497_700
    assert len(compact_utf8) > MAX_INPUT_BYTES
    with pytest.raises(SparkInputError, match="input size"):
        parse_scenario(raw)


def test_json_bytes_path_still_accepts_valid_utf8_payload():
    encoded = json.dumps(scenario_payload(), ensure_ascii=False).encode("utf-8")

    parsed = parse_scenario(encoded)

    assert parsed.snapshot.synthetic is True
    assert parsed.snapshot.events[0].source_id == "source-a"


@pytest.mark.parametrize("invalid", [False, None, 0, 1, "true"])
def test_scenario_requires_an_explicit_true_inspiration_request(invalid):
    raw = scenario_payload()
    raw["inspiration_requested"] = invalid

    with pytest.raises(SparkInputError, match="explicit inspiration request"):
        parse_scenario(raw)


def test_scenario_rejects_a_missing_inspiration_request_field():
    raw = scenario_payload()
    del raw["inspiration_requested"]

    with pytest.raises(SparkInputError, match="documented fields"):
        parse_scenario(raw)


@pytest.mark.parametrize(
    ("mutate", "error"),
    [
        (lambda raw: raw.__setitem__("seed", True), SparkInputError),
        (lambda raw: raw.__setitem__("max_candidates", 4), SparkInputError),
        (lambda raw: raw.__setitem__("channels", [{}]), SparkInputError),
        (lambda raw: raw.__setitem__("risk_domain", []), SparkInputError),
        (
            lambda raw: raw.__setitem__("data_cutoff", "2026-07-30T00:00:00"),
            SparkInputError,
        ),
        (lambda raw: raw["events"][0].__setitem__("resolved", 0), SparkPolicyError),
        (lambda raw: raw["events"][0].__setitem__("unknown", False), SparkInputError),
        (
            lambda raw: raw["events"][0]["structure"].__setitem__("source_hash", "0" * 64),
            SparkIntegrityError,
        ),
    ],
)
def test_scenario_rejects_bool_as_int_unknown_fields_and_bad_hash(mutate, error):
    raw = scenario_payload()
    mutate(raw)

    with pytest.raises(error):
        parse_scenario(raw)


def test_utf8_hash_and_spans_use_exact_unnormalized_unicode_code_points():
    text = "\ufeff甲🙂e\u0301\r\n乙"
    emoji_start = text.index("🙂")
    payload = scenario_payload(
        events=[
            event_payload(
                "unicode-source",
                text,
                structure={
                    "entities": [cue(text, "emoji", "🙂")],
                    "roles_actions": [],
                    "causal_constraints": [],
                    "transformations_outcomes": [],
                },
            )
        ],
        channels=["structural_distant"],
        tension_structure={
            "entities": ["emoji"],
            "roles_actions": [],
            "causal_constraints": [],
            "transformations_outcomes": [],
        },
    )

    parsed = parse_scenario(payload)
    evidence = parsed.snapshot.events[0].structure.entities[0]

    assert evidence.start == emoji_start
    assert evidence.end == emoji_start + 1
    assert text[evidence.start : evidence.end] == "🙂"
    assert parsed.snapshot.events[0].content_hash == source_hash(text)
    assert source_hash("é") != source_hash("e\u0301")


def test_invalid_unicode_is_rejected_without_normalization_or_replacement():
    raw = scenario_payload()
    raw["events"][0]["text"] = "\ud800"
    raw["events"][0]["structure"]["source_hash"] = "0" * 64

    with pytest.raises(SparkInputError):
        parse_scenario(raw)


@pytest.mark.parametrize(
    "field",
    ["tension", "cue_value", "extractor", "prompt_version"],
)
def test_all_model_facing_text_rejects_escaped_isolated_surrogates(field):
    raw = scenario_payload()
    if field == "tension":
        raw["tension"] = "\ud800"
    elif field == "cue_value":
        raw["events"][0]["structure"]["entities"][0]["value"] = "\ud800"
    else:
        raw["events"][0]["structure"][field] = "\ud800"

    escaped_json = json.dumps(raw, ensure_ascii=True)
    with pytest.raises(SparkInputError):
        parse_scenario(escaped_json)


def test_structure_cue_span_must_contain_non_whitespace_source_text():
    text = "有效内容  "
    raw = scenario_payload(
        events=[
            event_payload(
                "blank-cue-source",
                text,
                structure={
                    "entities": [cue(text, "抽象实体", "  ")],
                    "roles_actions": [],
                    "causal_constraints": [],
                    "transformations_outcomes": [],
                },
            )
        ]
    )

    with pytest.raises(SparkIntegrityError):
        parse_scenario(raw)


def test_json_round_trip_remains_finite_and_does_not_use_default_stringification():
    parsed = parse_scenario(json.dumps(scenario_payload(), ensure_ascii=False))

    assert parsed.snapshot.synthetic is True
    with pytest.raises(SparkInputError):
        EvaluationAxes(source_fidelity=float("nan"))
    with pytest.raises(SparkInputError):
        EvaluationAxes(risk=float("inf"))


def test_instrument_metadata_only_persists_allowlisted_typed_parameters():
    metadata = InstrumentMetadata(
        provider="test-provider",
        requested_model="model-v1",
        actual_model="model-v1",
        prompt_hash=source_hash("prompt-v1"),
        parameters=(
            ("max_tokens", "512"),
            ("temperature", "0.2"),
            ("top_p", "1"),
        ),
    )

    assert metadata.to_dict()["parameters"] == {
        "max_tokens": "512",
        "temperature": "0.2",
        "top_p": "1",
    }
    with pytest.raises(SparkInputError):
        InstrumentMetadata(
            provider="test-provider",
            requested_model="model-v1",
            actual_model="model-v1",
            prompt_hash=source_hash("prompt-v1"),
            parameters=(("temperature", "sk-secret-value"),),
        )
    with pytest.raises(SparkInputError):
        InstrumentMetadata(
            provider="sk-secret-provider",
            requested_model="model-v1",
            actual_model="model-v1",
            prompt_hash=source_hash("prompt-v1"),
        )


def test_parser_does_not_keep_nested_input_references():
    raw = scenario_payload()
    copied = deepcopy(raw)
    parsed = parse_scenario(raw)

    assert raw == copied
    assert isinstance(parsed.snapshot.events, tuple)
    assert isinstance(parsed.snapshot.events[0].structure.roles_actions, tuple)
