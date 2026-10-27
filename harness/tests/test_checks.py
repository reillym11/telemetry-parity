import json
from decimal import Decimal

import pytest

from harness import checks, config
from harness.cli import failpoint_exit_error
from harness.checks import Output, Rec, parse
from harness.config import JOBS
from harness.kafka_io import Message
from harness.murmur2 import java_partition, murmur2

CHARGING = JOBS["charging-session-builder"]
OTA = JOBS["ota-status-tracker"]
BATTERY = JOBS["battery-health-aggregator"]

SCHEMA = {"properties": {
    "session_id": {"x-privacy": "vehicle-id"},
    "vin": {"x-privacy": "vehicle-id"},
    "start_time": {"$ref": "../common.schema.json#/$defs/timestamp", "x-privacy": "none"},
    "energy_added_kwh": {"x-privacy": "none"},
    "charger_lat": {"x-privacy": "precise-location", "x-precision": 5},
}}


def rec(p, o, **v):
    base = {"session_id": "s1", "vin": "SYNAAAAAAAAAAAAA1", "start_time": "2026-10-01T00:00:00.000Z",
            "energy_added_kwh": "1.500", "charger_lat": "37.12345"}
    base.update(v)
    raw = json.dumps(base).replace('"1.500"', "1.500")
    for f in ("energy_added_kwh", "charger_lat"):
        raw = raw.replace(f'"{f}": "{base[f]}"', f'"{f}": {base[f]}')
    return Rec(p, o, base["vin"], parse(raw), raw)


def out(side, *recs):
    return Output(side, "kafka", list(recs))


def status(r):
    return r["status"]


# ---------- murmur2 ----------

@pytest.mark.parametrize("key,expected", [
    # values from org.apache.kafka.common.utils.Utils.murmur2
    (b"21", -973932308), (b"foobar", -790332482), (b"a-little-bit-long-string", -985981536),
    (b"a-little-bit-longer-string", -1486304829), (b"lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8", -58897971),
    (b"abc", 479470107),
])
def test_murmur2_matches_java(key, expected):
    assert murmur2(key) == expected & 0xFFFFFFFF


def test_java_partition_is_positive_mod():
    assert all(0 <= java_partition(f"SYN{i:014d}".encode(), 6) < 6 for i in range(100))


# ---------- A ----------

def test_a_pass_on_identical():
    r, _ = checks.check_a(CHARGING, out("l", rec(0, 0)), out("r", rec(0, 0)), SCHEMA)
    assert status(r) == "PASS"


def test_a_missing_extra_and_value_diff():
    legacy = out("l", rec(0, 0), rec(1, 0, session_id="s2"))
    rust = out("r", rec(0, 0, energy_added_kwh="1.6"), rec(2, 0, session_id="s3"))
    r, _ = checks.check_a(CHARGING, legacy, rust, SCHEMA)
    m = r["metrics"]
    assert status(r) == "FAIL" and m["missing_in_rust"] == 1 and m["extra_in_rust"] == 1 and m["value_diffs"] == 1


def test_a_numbers_compare_as_decimal():
    r, _ = checks.check_a(CHARGING, out("l", rec(0, 0, energy_added_kwh="1.500")),
                          out("r", rec(0, 0, energy_added_kwh="1.5")), SCHEMA)
    assert status(r) == "PASS"


def test_a_timestamp_format_only_is_its_own_category():
    r, _ = checks.check_a(CHARGING, out("l", rec(0, 0)), out("r", rec(0, 0, start_time="2026-10-01T00:00:00Z")),
                          SCHEMA)
    assert status(r) == "FAIL" and r["metrics"]["timestamp_format_diffs"] == 1 and r["metrics"]["value_diffs"] == 0


def test_a_identical_repeats_are_deduped_but_different_values_fail():
    ok, _ = checks.check_a(CHARGING, out("l", rec(0, 0)), out("r", rec(0, 0), rec(0, 1)), SCHEMA)
    bad, _ = checks.check_a(CHARGING, out("l", rec(0, 0)), out("r", rec(0, 0), rec(0, 1, energy_added_kwh="2.0")),
                            SCHEMA)
    assert status(ok) == "PASS"
    assert status(bad) == "FAIL" and bad["metrics"]["nondeterministic_rust"] == 1


def test_a_ota_uses_last_record_per_key():
    schema = {"properties": {"vin": {}, "campaign_id": {}, "state": {}}}

    def o(p, off, state):
        v = {"vin": "SYNAAAAAAAAAAAAA1", "campaign_id": "OTA-2026.10.1", "state": state}
        return Rec(p, off, "SYNAAAAAAAAAAAAA1|OTA-2026.10.1", v, json.dumps(v))

    legacy = out("l", o(0, 0, "installed"))
    rust = out("r", o(0, 0, "downloaded"), o(0, 1, "installed"))
    assert status(checks.check_a(OTA, legacy, rust, schema)[0]) == "PASS"
    rust_wrong = out("r", o(0, 0, "installed"), o(0, 1, "downloaded"))
    assert status(checks.check_a(OTA, legacy, rust_wrong, schema)[0]) == "FAIL"


def test_a_ignores_undeclared_fields():
    r, _ = checks.check_a(CHARGING, out("l", rec(0, 0)), out("r", rec(0, 0, debug="x")), SCHEMA)
    assert status(r) == "PASS"


# ---------- B ----------

def test_b_counts_rust_repeats_only_as_failure():
    assert status(checks.check_b(CHARGING, out("l", rec(0, 0), rec(0, 1)), out("r", rec(0, 0)))) == "PASS"
    r = checks.check_b(CHARGING, out("l", rec(0, 0)), out("r", rec(0, 0), rec(0, 1)))
    assert status(r) == "FAIL" and r["metrics"]["rust_duplicates"] == 1


def test_b_ota_identical_key_value_is_duplicate_but_update_is_not():
    def o(off, state):
        v = {"vin": "V", "campaign_id": "C", "state": state, "last_event_id": f"e-{state}"}
        return Rec(0, off, "V|C", v, json.dumps(v))
    assert checks.repeats(OTA, out("r", o(0, "downloaded"), o(1, "installed"))) == 0
    assert checks.repeats(OTA, out("r", o(0, "installed"), o(1, "installed"))) == 1
    v = {"vin": "V", "campaign_id": "C", "state": "installed", "last_event_id": "e"}
    a, b = Rec(0, 0, "V|C", v, json.dumps(v)), Rec(0, 1, "V|C", dict(v), json.dumps(v, separators=(",", ":")))
    assert checks.repeats(OTA, out("r", a, b)) == 1


# ---------- C ----------

def test_c_decrease_within_partition_and_vin_fails():
    a = rec(0, 0, session_id="s1", start_time="2026-10-01T02:00:00.000Z")
    b = rec(0, 1, session_id="s2", start_time="2026-10-01T01:00:00.000Z")
    r = checks.check_c(CHARGING, out("l", a), out("r", a, b))
    assert status(r) == "FAIL" and r["metrics"]["rust_violations"] == 1


def test_c_different_vins_or_partitions_are_independent():
    a = rec(0, 0, session_id="s1", start_time="2026-10-01T02:00:00.000Z")
    b = rec(0, 1, session_id="s2", vin="SYNBBBBBBBBBBBBB2", start_time="2026-10-01T01:00:00.000Z")
    c = rec(1, 0, session_id="s3", start_time="2026-10-01T01:00:00.000Z")
    assert status(checks.check_c(CHARGING, out("l"), out("r", a, b, c))) == "PASS"


def test_c_not_applicable_to_parquet():
    assert status(checks.check_c(BATTERY, out("l"), out("r"))) == "N/A"


# ---------- D ----------

def test_d_partition_mismatch_fails_with_explanation():
    r = checks.check_d(CHARGING, out("l", rec(0, 0)), out("r", rec(3, 0)))
    assert status(r) == "FAIL" and "murmur2_random" in r["explanation"]
    assert status(checks.check_d(CHARGING, out("l", rec(2, 0)), out("r", rec(2, 5)))) == "PASS"


def test_d_wrong_kafka_key_fails():
    good = rec(2, 0)
    bad = Rec(2, 0, "other", good.value, good.raw)
    r = checks.check_d(CHARGING, out("l", good), out("r", bad))
    assert status(r) == "FAIL" and r["metrics"]["wrong_key"] == 1


def test_d_rust_only_key_must_use_java_murmur2():
    vin = "SYNAAAAAAAAAAAAA1"
    right = java_partition(vin.encode(), 6)
    assert status(checks.check_d(CHARGING, out("l"), out("r", rec(right, 0)))) == "PASS"
    assert status(checks.check_d(CHARGING, out("l"), out("r", rec((right + 1) % 6, 0)))) == "FAIL"


def test_job_kafka_keys_match_registry():
    reg = config.topics()
    for job in config.JOBS.values():
        if not job.is_parquet:
            assert "|".join(job.kafka_key) == reg[job.output]["key"]


# ---------- E ----------

def test_e1_undeclared_field_fails():
    assert status(checks.check_e1(CHARGING, out("l", rec(0, 0)), out("r", rec(0, 0)), SCHEMA)) == "PASS"
    r = checks.check_e1(CHARGING, out("l", rec(0, 0)), out("r", rec(0, 0, debug="x")), SCHEMA)
    assert status(r) == "FAIL" and r["metrics"]["undeclared_fields"] == ["debug"]


def test_e2_precision_over_limit_or_different_from_legacy_fails():
    legacy = out("l", rec(0, 0))
    assert status(checks.check_e2(CHARGING, legacy, out("r", rec(0, 0)), SCHEMA, 0)) == "PASS"
    over = checks.check_e2(CHARGING, legacy, out("r", rec(0, 0, charger_lat="37.123456")), SCHEMA, 0)
    coarser = checks.check_e2(CHARGING, legacy, out("r", rec(0, 0, charger_lat="37.1235")), SCHEMA, 1)
    assert status(over) == "FAIL" and over["metrics"]["values_over_precision"] == 1
    assert status(coarser) == "FAIL" and coarser["metrics"]["rust_max_decimals"]["charger_lat"] == 4


def test_decimals_counts_literal_places():
    assert checks.decimals(Decimal("37.10")) == 2 and checks.decimals(Decimal("30")) == 0 and checks.decimals(4) == 0


def test_e3_only_declared_output_and_state_topics_allowed():
    stores = [{"kind": "topic", "name": "rust.out"}, {"kind": "topic", "name": "rust.job.state.sessions"}]
    assert status(checks.check_e3(stores, "rust.out", "rust.job.state", {})) == "PASS"
    stores.append({"kind": "topic", "name": "rust.job.cache"})
    r = checks.check_e3(stores, "rust.out", "rust.job.state", {})
    assert status(r) == "FAIL" and r["metrics"]["undeclared"] == [{"kind": "topic", "name": "rust.job.cache"}]


def test_scan_sensitive_finds_nested_fields():
    found = checks.scan_sensitive(['{"a": {"vin": "x", "lat": 1}}', "not json"],
                                  {"vin": "vehicle-id", "lat": "coarse-location"})
    assert found == {"lat": "coarse-location", "vin": "vehicle-id"}


def test_e4_manifest_mismatch():
    observed = {"inputs": ["in"], "outputs": ["out"], "state_stores": [], "sensitive_fields": {"out": ["vin"]}}
    assert status(checks.check_e4(None, observed)) == "N/A"
    assert status(checks.check_e4(dict(observed), observed)) == "PASS"
    r = checks.check_e4({**observed, "state_stores": ["rust.job.state.x"]}, observed)
    assert status(r) == "FAIL" and r["metrics"]["mismatches"] == 1


def test_e4_parquet_output_name_is_run_independent():
    run_id = "20261027T224103Z-battery-health-aggregator-769e"
    assert checks.manifest_name(f"{run_id}/rust/battery_health_hourly", run_id) == "rust/battery_health_hourly"
    assert checks.manifest_name("rust.job.state.x", run_id) == "rust.job.state.x"


class FakeKafkaMessage:
    def __init__(self, key, value):
        self._key, self._value = key, value

    def partition(self):
        return 1

    def offset(self):
        return 7

    def key(self):
        return self._key

    def value(self):
        return self._value


def test_capture_keeps_tombstones():
    m = Message.from_kafka(FakeKafkaMessage(b'"evt-1"', None))
    assert (m.partition, m.offset, m.key, m.raw) == (1, 7, '"evt-1"', None)
    assert checks.scan_sensitive([m.raw, '{"vin": "x"}'], {"vin": "vehicle-id"}) == {"vin": "vehicle-id"}


def test_failpoint_requires_sigkill_exit():
    assert failpoint_exit_error(137, 10) is None
    assert "never fired" in str(failpoint_exit_error(0, 10))
    assert "exited 1" in str(failpoint_exit_error(1, 10))
