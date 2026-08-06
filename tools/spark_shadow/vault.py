"""Spark R2 的自持有临时 test_data Markdown vault。

本模块不接受任何调用方路径。每个 adapter 只读回自己在系统临时目录中新建的
一次性测试桶，并在离开上下文前清理整个根目录。
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import secrets
import stat
from tempfile import TemporaryDirectory
from types import TracebackType
from typing import Any

import yaml

from spark_r1 import (
    ParsedScenario,
    SparkBoundaryError,
    SparkInputError,
    parse_scenario,
    strict_json_loads,
)


SHADOW_SCHEMA_VERSION = 1
_MARKER_NAME = ".spark-shadow-owner.json"
_DATA_DIR_NAME = "test_data"
_PROVENANCE = {
    "kind": "test",
    "created_by": "spark-shadow-r2",
    "erasable": True,
}
_MARKER_FIELDS = frozenset(
    {
        "capability",
        "expected_event_count",
        "kind",
        "scenario",
        "shadow_schema_version",
    }
)
_SCENARIO_HEADER_FIELDS = frozenset(
    {
        "boundary_id",
        "channels",
        "data_cutoff",
        "inspiration_requested",
        "max_candidates",
        "risk_domain",
        "schema_version",
        "seed",
        "synthetic",
        "tension",
        "tension_structure",
    }
)
_BUCKET_METADATA_FIELDS = frozenset(
    {"id", "provenance", "spark_shadow_payload", "type"}
)
_MAX_MARKER_BYTES = 128_000
_MAX_BUCKET_BYTES = 512_000
_MAX_VAULT_BYTES = 4_000_000


class _StrictYamlLoader(yaml.SafeLoader):
    """拒绝 YAML alias 与重复键，避免测试 frontmatter 扩张或覆盖。"""

    def compose_node(self, parent: Any, index: Any) -> Any:
        if self.check_event(yaml.AliasEvent):
            raise yaml.constructor.ConstructorError(
                None,
                None,
                "YAML aliases are not allowed",
                self.peek_event().start_mark,
            )
        return super().compose_node(parent, index)


def _construct_unique_mapping(
    loader: _StrictYamlLoader,
    node: yaml.nodes.MappingNode,
    deep: bool = False,
) -> dict[Any, Any]:
    if not isinstance(node, yaml.nodes.MappingNode):
        raise yaml.constructor.ConstructorError(
            None,
            None,
            "expected a mapping node",
            node.start_mark,
        )
    result: dict[Any, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        try:
            duplicate = key in result
        except TypeError as exc:
            raise yaml.constructor.ConstructorError(
                None,
                None,
                "unhashable YAML mapping key",
                key_node.start_mark,
            ) from exc
        if duplicate:
            raise yaml.constructor.ConstructorError(
                None,
                None,
                "duplicate YAML mapping key",
                key_node.start_mark,
            )
        result[key] = loader.construct_object(value_node, deep=deep)
    return result


_StrictYamlLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_unique_mapping,
)


def _is_within(path: Path, root: Path) -> bool:
    return path == root or root in path.parents


def _is_reparse_point(path_stat: os.stat_result) -> bool:
    attributes = getattr(path_stat, "st_file_attributes", 0)
    reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    return bool(reparse_flag and attributes & reparse_flag)


def _assert_plain_path(
    path: Path,
    *,
    root: Path,
    directory: bool,
) -> None:
    try:
        path_stat = os.lstat(path)
        resolved = path.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise SparkBoundaryError("shadow vault path cannot be resolved safely") from exc
    if not _is_within(resolved, root):
        raise SparkBoundaryError("shadow vault path escaped its owned root")
    if stat.S_ISLNK(path_stat.st_mode) or _is_reparse_point(path_stat):
        raise SparkBoundaryError("shadow vault cannot contain links or reparse points")
    if directory and not stat.S_ISDIR(path_stat.st_mode):
        raise SparkBoundaryError("shadow vault expected a plain directory")
    if not directory and not stat.S_ISREG(path_stat.st_mode):
        raise SparkBoundaryError("shadow vault expected a regular file")


def _assert_safe_new_root(root: Path) -> None:
    repository_root = Path(__file__).resolve().parents[2]
    _assert_plain_path(root, root=root, directory=True)
    if _is_within(root, repository_root):
        raise SparkBoundaryError("shadow temporary root cannot be inside the repository")
    for ancestor in root.parents:
        if ancestor.name.casefold() == "buckets":
            raise SparkBoundaryError("shadow temporary root cannot be inside a buckets tree")
        if (ancestor / "config.yaml").is_file() and (
            (ancestor / "embeddings.db").exists()
            or (ancestor / "_app").exists()
        ):
            raise SparkBoundaryError("shadow temporary root cannot be inside an OB vault")
    if any(root.iterdir()):
        raise SparkBoundaryError("shadow temporary root must be newly created and empty")


def _write_new_file(path: Path, payload: bytes, *, root: Path) -> None:
    if not _is_within(path.parent.resolve(strict=True), root):
        raise SparkBoundaryError("shadow write target escaped its owned root")
    flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    except OSError as exc:
        raise SparkBoundaryError("shadow test file could not be created safely") from exc
    _assert_plain_path(path, root=root, directory=False)


def _read_regular_file(path: Path, *, root: Path, maximum: int) -> bytes:
    _assert_plain_path(path, root=root, directory=False)
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
        with os.fdopen(descriptor, "rb") as handle:
            payload = handle.read(maximum + 1)
    except OSError as exc:
        raise SparkBoundaryError("shadow test file could not be read safely") from exc
    if len(payload) > maximum:
        raise SparkInputError("shadow test file exceeds its byte budget")
    return payload


def _exact_object(value: Any, fields: frozenset[str], label: str) -> dict[str, Any]:
    if type(value) is not dict or set(value) != fields:
        raise SparkInputError(f"{label} fields do not match the shadow schema")
    if any(type(key) is not str for key in value):
        raise SparkInputError(f"{label} keys must be strings")
    return value


def _event_payload(event: Any) -> dict[str, Any]:
    return {
        "source_id": event.source_id,
        "event_type": event.event_type,
        "status": event.status,
        "visibility": event.visibility,
        "resolved": event.resolved,
        "anchor": event.anchor,
        "dont_surface": event.dont_surface,
        "digested": event.digested,
        "pinned": event.pinned,
        "protected": event.protected,
        "permanent": event.permanent,
        "structure": event.structure.to_dict(),
    }


def _scenario_header(scenario: ParsedScenario) -> dict[str, Any]:
    request = scenario.request
    snapshot = scenario.snapshot
    return {
        "schema_version": snapshot.schema_version,
        "synthetic": snapshot.synthetic,
        "boundary_id": snapshot.boundary_id,
        "data_cutoff": snapshot.data_cutoff,
        "tension": request.tension,
        "tension_structure": request.tension_structure.to_dict(),
        "channels": list(request.channels),
        "seed": request.seed,
        "inspiration_requested": request.inspiration_requested,
        "max_candidates": request.max_candidates,
        "risk_domain": request.risk_domain,
    }


def _markdown_bytes(metadata: dict[str, Any], body: str) -> bytes:
    yaml_text = yaml.safe_dump(
        metadata,
        allow_unicode=True,
        default_flow_style=False,
        sort_keys=True,
    )
    return (f"---\n{yaml_text}---\n{body}").encode("utf-8")


def _parse_markdown(payload: bytes) -> tuple[dict[str, Any], str]:
    try:
        text = payload.decode("utf-8", errors="strict")
    except UnicodeDecodeError as exc:
        raise SparkInputError("shadow bucket must be strict UTF-8") from exc
    if not text.startswith("---\n"):
        raise SparkInputError("shadow bucket is missing YAML frontmatter")
    header, delimiter, body = text[4:].partition("\n---\n")
    if not delimiter:
        raise SparkInputError("shadow bucket frontmatter is not closed")
    loader = _StrictYamlLoader(header)
    try:
        metadata = loader.get_single_data()
    except yaml.YAMLError as exc:
        raise SparkInputError("shadow bucket frontmatter is invalid") from exc
    finally:
        loader.dispose()
    return _exact_object(metadata, _BUCKET_METADATA_FIELDS, "shadow bucket"), body


class _EphemeralShadowVault:
    """仅供 R2 runner 使用的临时 vault capability。"""

    __slots__ = (
        "_active",
        "_capability",
        "_cleaned",
        "_root",
        "_scenario",
        "_temporary_directory",
    )

    def __init__(self, scenario: ParsedScenario) -> None:
        if not isinstance(scenario, ParsedScenario):
            raise SparkInputError("shadow vault requires a ParsedScenario")
        self._scenario = scenario
        self._capability = secrets.token_hex(32)
        self._temporary_directory: TemporaryDirectory | None = None
        self._root: Path | None = None
        self._active = False
        self._cleaned = False

    @property
    def root(self) -> Path:
        if not self._active or self._root is None:
            raise SparkBoundaryError("shadow vault root is not active")
        return self._root

    @property
    def cleaned(self) -> bool:
        return self._cleaned

    def __enter__(self) -> "_EphemeralShadowVault":
        if self._active or self._temporary_directory is not None:
            raise SparkBoundaryError("shadow vault cannot be entered twice")
        temporary_directory = TemporaryDirectory(prefix="ombre-spark-shadow-")
        self._temporary_directory = temporary_directory
        self._root = Path(temporary_directory.name).resolve(strict=True)
        try:
            _assert_safe_new_root(self._root)
            self._write_fixture()
            self._active = True
            return self
        except BaseException:
            self._cleanup()
            raise

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exc_type, exc_value, traceback
        self._cleanup()

    def _write_fixture(self) -> None:
        if self._root is None:
            raise SparkBoundaryError("shadow vault root is unavailable")
        marker = {
            "shadow_schema_version": SHADOW_SCHEMA_VERSION,
            "kind": "spark_shadow_test_vault",
            "capability": self._capability,
            "expected_event_count": len(self._scenario.snapshot.events),
            "scenario": _scenario_header(self._scenario),
        }
        marker_bytes = json.dumps(
            marker,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
        if len(marker_bytes) > _MAX_MARKER_BYTES:
            raise SparkInputError("shadow marker exceeds its byte budget")
        _write_new_file(self._root / _MARKER_NAME, marker_bytes, root=self._root)

        data_root = self._root / _DATA_DIR_NAME
        data_root.mkdir(mode=0o700)
        _assert_plain_path(data_root, root=self._root, directory=True)
        total_bytes = len(marker_bytes)
        for index, event in enumerate(self._scenario.snapshot.events, start=1):
            payload_json = json.dumps(
                _event_payload(event),
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
            metadata = {
                "id": f"spark-shadow-{index:04d}",
                "type": "dynamic",
                "provenance": dict(_PROVENANCE),
                "spark_shadow_payload": payload_json,
            }
            bucket_bytes = _markdown_bytes(metadata, event.text)
            if len(bucket_bytes) > _MAX_BUCKET_BYTES:
                raise SparkInputError("shadow bucket exceeds its byte budget")
            total_bytes += len(bucket_bytes)
            if total_bytes > _MAX_VAULT_BYTES:
                raise SparkInputError("shadow vault exceeds its total byte budget")
            path = data_root / f"bucket-{index:04d}.md"
            _write_new_file(path, bucket_bytes, root=self._root)

    def read_scenario(self) -> ParsedScenario:
        root = self.root
        _assert_plain_path(root, root=root, directory=True)
        expected_root_entries = {_MARKER_NAME, _DATA_DIR_NAME}
        if {entry.name for entry in root.iterdir()} != expected_root_entries:
            raise SparkBoundaryError("shadow vault contains an unexpected root entry")

        marker_raw = _read_regular_file(
            root / _MARKER_NAME,
            root=root,
            maximum=_MAX_MARKER_BYTES,
        )
        marker = _exact_object(
            strict_json_loads(marker_raw),
            _MARKER_FIELDS,
            "shadow marker",
        )
        if marker["shadow_schema_version"] != SHADOW_SCHEMA_VERSION:
            raise SparkInputError("shadow marker schema version is unsupported")
        if marker["kind"] != "spark_shadow_test_vault":
            raise SparkBoundaryError("shadow marker kind is invalid")
        if marker["capability"] != self._capability:
            raise SparkBoundaryError("shadow marker capability does not match this run")
        expected_count = marker["expected_event_count"]
        if type(expected_count) is not int or expected_count != len(
            self._scenario.snapshot.events
        ):
            raise SparkBoundaryError("shadow marker event count is invalid")
        header = _exact_object(
            marker["scenario"],
            _SCENARIO_HEADER_FIELDS,
            "shadow scenario header",
        )

        data_root = root / _DATA_DIR_NAME
        _assert_plain_path(data_root, root=root, directory=True)
        expected_names = {
            f"bucket-{index:04d}.md"
            for index in range(1, expected_count + 1)
        }
        if {entry.name for entry in data_root.iterdir()} != expected_names:
            raise SparkBoundaryError("shadow vault bucket set does not match its marker")

        events: list[dict[str, Any]] = []
        total_bytes = len(marker_raw)
        for index in range(1, expected_count + 1):
            path = data_root / f"bucket-{index:04d}.md"
            bucket_raw = _read_regular_file(
                path,
                root=root,
                maximum=_MAX_BUCKET_BYTES,
            )
            total_bytes += len(bucket_raw)
            if total_bytes > _MAX_VAULT_BYTES:
                raise SparkInputError("shadow vault exceeds its total byte budget")
            metadata, body = _parse_markdown(bucket_raw)
            if metadata["id"] != f"spark-shadow-{index:04d}":
                raise SparkBoundaryError("shadow bucket ID does not match its fixed slot")
            if metadata["type"] != "dynamic":
                raise SparkBoundaryError("shadow bucket type is invalid")
            if metadata["provenance"] != _PROVENANCE:
                raise SparkBoundaryError("shadow bucket lacks immutable test_data provenance")
            encoded_event = metadata["spark_shadow_payload"]
            if type(encoded_event) is not str:
                raise SparkInputError("shadow bucket payload must be JSON text")
            event_payload = strict_json_loads(encoded_event)
            if "text" in event_payload:
                raise SparkBoundaryError("shadow payload cannot duplicate bucket body text")
            events.append({**event_payload, "text": body})

        parsed = parse_scenario({**header, "events": events})
        if parsed != self._scenario:
            raise SparkBoundaryError("shadow vault round-trip changed the approved scenario")
        return parsed

    def _cleanup(self) -> None:
        temporary_directory = self._temporary_directory
        root = self._root
        self._active = False
        if temporary_directory is None:
            self._cleaned = True
            return
        try:
            temporary_directory.cleanup()
        except OSError as exc:
            raise SparkBoundaryError("shadow test vault cleanup failed") from exc
        finally:
            self._temporary_directory = None
        if root is not None and os.path.lexists(root):
            raise SparkBoundaryError("shadow test vault still exists after cleanup")
        self._cleaned = True
