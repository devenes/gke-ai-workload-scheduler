"""Unit tests for the T00..T19 test runner evidence schema and safety invariants."""

from __future__ import annotations

import json
from pathlib import Path

from scripts.run_test_suite import TestSuiteRunner


def test_evidence_schema_and_immutability(tmp_path: Path) -> None:
    runner = TestSuiteRunner(mode="repro", run_dir=tmp_path / "run-test")
    rec = runner.record_test(
        test_id="T00",
        hypothesis="Sample hypothesis",
        preconditions=["Precondition 1"],
        acceptance_criteria=["Criterion 1"],
        limitations=["Limitation 1"],
        fn=lambda ev: ("PASS", {"metric_a": 1.0}),
    )
    assert rec["test_id"] == "T00"
    assert rec["status"] == "PASS"
    assert rec["execution_status"] == "COMPLETED"
    assert (tmp_path / "run-test" / "T00.json").exists()

    saved = json.loads((tmp_path / "run-test" / "T00.json").read_text(encoding="utf-8"))
    for field in [
        "test_id",
        "status",
        "execution_status",
        "hypothesis",
        "preconditions",
        "acceptance_criteria",
        "measured_outcomes",
        "evidence_paths",
        "limitations",
        "started_at",
        "finished_at",
        "environment",
        "software_versions",
    ]:
        assert field in saved
