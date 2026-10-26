import json
from dataclasses import dataclass
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
DEPS = ROOT / ".deps"
RUNS = ROOT / "runs"
GOLDEN = ROOT / "golden"
DATA_CONTRACTS = DEPS / "data-contracts"
CONTRACTS = DATA_CONTRACTS / "contracts"
BUCKET = "telemetry-lake"


@dataclass(frozen=True)
class Job:
    name: str
    input_topic: str
    output: str
    spark_class: str
    business_key: tuple
    time_field: str | None
    order_by: str

    @property
    def is_parquet(self):
        return self.output == "battery.health.hourly"

    def key_of(self, value: dict) -> str:
        return "|".join(str(value[f]) for f in self.business_key)


JOBS = {
    j.name: j
    for j in [
        Job("ota-status-tracker", "vehicle.ota.status.v1", "ota.status.latest.v1",
            "telemetry.jobs.ota.OtaStatusTracker", ("vin", "campaign_id"), "last_event_time", "key"),
        Job("charging-session-builder", "vehicle.charging.events.v1", "charging.sessions.v1",
            "telemetry.jobs.charging.ChargingSessionBuilder", ("session_id",), "start_time", "vin"),
        Job("battery-health-aggregator", "vehicle.battery.health.v1", "battery.health.hourly",
            "telemetry.jobs.battery.BatteryHealthAggregator", ("vin", "window_start"), None, "vin"),
        Job("diagnostics-dtc-router", "vehicle.diagnostics.dtc.v1", "service.dtc.alerts.v1",
            "telemetry.jobs.dtc.DiagnosticsDtcRouter", ("alert_id",), "event_time", "vin"),
    ]
}


def deps_lock() -> dict:
    out = {}
    for line in (ROOT / "deps.lock").read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            out[k] = v
    return out


def topics() -> dict:
    doc = yaml.safe_load((CONTRACTS / "topics.yaml").read_text())
    entries = doc if isinstance(doc, list) else next(v for v in doc.values() if isinstance(v, list))
    return {t["name"]: t for t in entries}


def schema(name: str) -> dict:
    return json.loads((CONTRACTS / "topics" / f"{name}.schema.json").read_text())


def all_schemas() -> dict:
    return {p.name.removesuffix(".schema.json"): json.loads(p.read_text())
            for p in sorted((CONTRACTS / "topics").glob("*.schema.json"))}


def sensitive_fields() -> dict:
    """field name -> privacy tag, over every schema (used to scan unknown stores)."""
    out = {}
    for s in all_schemas().values():
        for f, p in s["properties"].items():
            if p.get("x-privacy", "none") != "none":
                out[f] = p["x-privacy"]
    return out
