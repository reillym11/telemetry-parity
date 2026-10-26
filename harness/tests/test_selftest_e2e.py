"""Runs the self-test matrix end to end. Needs Docker, .deps/ and the sample goldens."""
import json
import subprocess
import sys

import pytest

from harness.cli import FAULT_FAILS
from harness.config import ROOT, RUNS

pytestmark = pytest.mark.e2e

CASES = [("charging-session-builder", "sample", f, f == "none") for f in FAULT_FAILS if f != "reorder"]
CASES += [(j, "sample", "none", True) for j in ("ota-status-tracker", "battery-health-aggregator",
                                                "diagnostics-dtc-router")]
# the sample has no VIN with two sessions, so ordering can only be broken on the full fixture
CASES += [("charging-session-builder", "full", "reorder", False)]


@pytest.mark.parametrize("job,fixture,fault,chaos", CASES)
def test_fault_fails_exactly_its_check(job, fixture, fault, chaos):
    before = set(RUNS.glob("*")) if RUNS.exists() else set()
    args = [sys.executable, "-m", "harness", "--job", job, "--fixture", fixture, "--self-test", fault,
            "--legacy-from-golden"] + (["--chaos"] if chaos else [])
    proc = subprocess.run(args, cwd=ROOT, capture_output=True, text=True)
    new = sorted(set(RUNS.glob("*")) - before)
    assert new, proc.stdout + proc.stderr
    report = json.loads((new[-1] / "parity-report.json").read_text())
    st = report["self_test"]
    assert st["actual_fail"] == FAULT_FAILS[fault], (st, proc.stdout[-2000:])
    assert proc.returncode == (0 if fault == "none" else 1)
    for c in report["checks"]:
        if c["status"] == "FAIL":
            assert c["explanation"], c
