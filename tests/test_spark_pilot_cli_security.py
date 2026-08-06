"""Spark Pilot CLI 的安全失败、产品隔离与 stdin/stdout 边界。"""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parent.parent
CLI = ROOT / "tools" / "evaluate_spark_pilot.py"


def _run(*args: str, stdin: str = "") -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(CLI), *args],
        input=stdin,
        text=True,
        capture_output=True,
        cwd=ROOT,
        check=False,
        timeout=30,
    )


def test_prepare_cli_emits_only_response_packet() -> None:
    completed = _run("prepare")
    payload = json.loads(completed.stdout)

    assert completed.returncode == 0
    assert payload["status"] == "ready_for_blind_human_review"
    assert payload["sample_count"] == 280
    assert payload["persistent"] is False
    assert payload["retrievable"] is False
    assert all("condition" not in sample for sample in payload["samples"])


def test_prepare_cli_rejects_stdin_without_echoing_it() -> None:
    secret = "sk-do-not-echo-this"
    completed = _run("prepare", stdin=secret)
    payload = json.loads(completed.stdout)

    assert completed.returncode == 2
    assert payload["status"] == "failed"
    assert secret not in completed.stdout
    assert secret not in completed.stderr


def test_analyze_cli_marks_partial_input_with_distinct_exit_code() -> None:
    sys.path.insert(0, str(ROOT / "tools"))
    from spark_pilot.core import assignment_table
    from spark_pilot.protocol import RATING_AXES

    identifier = assignment_table()[0][2]
    rating = {axis: 2 for axis in RATING_AXES}
    stdin = json.dumps(
        {
            "schema_version": 1,
            "phase": "analyze",
            "ratings": [
                {"sample_id": identifier, "rater_id": "rater-a", **rating}
            ],
        }
    )
    completed = _run("analyze", stdin=stdin)
    payload = json.loads(completed.stdout)

    assert completed.returncode == 3
    assert payload["status"] == "incomplete_descriptive_pilot"
    assert identifier not in completed.stdout
    assert "rater-a" not in completed.stdout


def test_cli_rejects_duplicate_json_keys_and_unknown_arguments_safely() -> None:
    duplicate = '{"schema_version":1,"schema_version":1,"phase":"analyze","ratings":[]}'
    duplicate_result = _run("analyze", stdin=duplicate)
    secret = "--output=C:/secret/location"
    argument_result = _run("prepare", secret)

    assert duplicate_result.returncode == 2
    assert json.loads(duplicate_result.stdout)["status"] == "failed"
    assert argument_result.returncode == 2
    assert secret not in argument_result.stdout
    assert secret not in argument_result.stderr


def test_pilot_remains_outside_product_and_docker_context() -> None:
    dockerignore = (ROOT / ".dockerignore").read_text(encoding="utf-8")
    src_text = "\n".join(
        path.read_text(encoding="utf-8", errors="ignore")
        for path in (ROOT / "src").rglob("*.py")
    )

    assert "tools/" in dockerignore.splitlines()
    assert "spark_pilot" not in src_text
    assert "evaluate_spark_pilot" not in src_text
