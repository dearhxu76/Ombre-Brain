from __future__ import annotations

import importlib.util
from pathlib import Path


def _load_hook_module():
    path = Path(__file__).resolve().parents[1] / ".claude" / "hooks" / "session_breath.py"
    spec = importlib.util.spec_from_file_location("session_breath_hook", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_configure_stream_utf8_for_redirected_windows_output():
    module = _load_hook_module()

    class FakeStream:
        configured: dict[str, str] | None = None

        def reconfigure(self, **kwargs):
            self.configured = kwargs

    stream = FakeStream()
    module._configure_stream_utf8(stream)

    assert stream.configured == {"encoding": "utf-8", "errors": "replace"}


def test_configure_stream_utf8_tolerates_streams_without_reconfigure():
    module = _load_hook_module()
    module._configure_stream_utf8(object())
