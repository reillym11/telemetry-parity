"""Runs the self-test matrix end to end. Needs Docker, .deps/ and the sample goldens."""
import json
import subprocess
import sys

import pytest

from harness.cli import FAULT_FAILS
from harness.config import ROOT, RUNS

pytestmark = pytest.mark.e2e
CASE_TIMEOUT = 1200

CASES = [("charging-session-builder", "sample", f, f == "none") for f in FAULT_FAILS if f != "reorder"]
CASES += [(j, "sample", "none", True) for j in ("ota-status-tracker", "battery-health-aggregator",
                                                "diagnostics-dtc-router")]
# the sample has no VIN with two sessions, so ordering can only be broken on the full fixture
CASES += [("charging-session-builder", "full", "reorder", False)]


def run_harness(*extra):
    before = set(RUNS.glob("*")) if RUNS.exists() else set()
    args = [sys.executable, "-m", "harness", "--legacy-from-golden", *extra]
    try:
        proc = subprocess.run(args, cwd=ROOT, capture_output=True, text=True, timeout=CASE_TIMEOUT)
    except subprocess.TimeoutExpired as e:
        new = sorted(set(RUNS.glob("*")) - before)
        log = (new[-1] / "harness.log").read_text(errors="replace")[-3000:] if new else ""
        job = subprocess.run(["docker", "logs", "--tail", "40", f"parity-rust-{extra[1]}"], capture_output=True,
                             text=True)
        pytest.fail(f"harness did not finish in {CASE_TIMEOUT} s\nstdout: {e.stdout}\nharness.log: {log}\n"
                    f"rust job: {job.stdout}{job.stderr}")
    new = sorted(set(RUNS.glob("*")) - before)
    assert new, proc.stdout + proc.stderr
    path = new[-1] / "parity-report.json"
    assert path.exists(), (proc.returncode, proc.stdout[-1500:], proc.stderr[-3000:])
    return proc, json.loads(path.read_text())


@pytest.mark.parametrize("job,fixture,fault,chaos", CASES)
def test_fault_fails_exactly_its_check(job, fixture, fault, chaos):
    proc, report = run_harness("--job", job, "--fixture", fixture, "--self-test", fault,
                               *(["--chaos"] if chaos else []))
    st = report["self_test"]
    assert st["actual_fail"] == FAULT_FAILS[fault], (st, proc.stdout[-2000:])
    assert proc.returncode == (0 if fault == "none" else 1)
    for c in report["checks"]:
        if c["status"] == "FAIL":
            assert c["explanation"], c


def test_failpoint_aborts_state_records_without_duplicates():
    proc, report = run_harness("--job", "charging-session-builder", "--fixture", "sample", "--self-test", "none",
                               "--chaos", "--chaos-mode", "failpoint")
    assert proc.returncode == 0, proc.stdout[-2000:]
    f = next(c for c in report["checks"] if c["id"] == "F")["metrics"]
    assert f["chaos_mode"] == "failpoint"
    assert 0 < f["kill_committed_offsets"] < f["input_total"]
    state = {t: n for t, n in f["aborted_records_by_topic"].items() if t.startswith("rust.charging-session-builder.state")}
    assert state and all(n > 0 for n in state.values()), f
    assert f["duplicates_after_restart"] == 0
