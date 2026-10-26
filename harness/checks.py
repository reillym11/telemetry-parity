"""Pure comparison functions. Each check returns a dict: id, title, status (PASS/FAIL/N/A), metrics, details."""
import datetime as dt
import json
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from decimal import Decimal

from harness.murmur2 import java_partition

PASS, FAIL, NA = "PASS", "FAIL", "N/A"
LOCATION_TAGS = ("precise-location", "coarse-location")
MAX_DIFFS = 20


@dataclass
class Rec:
    partition: int | None
    offset: int | None
    key: str | None
    value: dict
    raw: str | None = None


@dataclass
class Output:
    side: str
    kind: str
    records: list
    columns: dict = field(default_factory=dict)


def parse(raw: str) -> dict:
    return json.loads(raw, parse_float=Decimal)


def result(cid, title, status, metrics=None, details=None, explanation=None):
    return {"id": cid, "title": title, "status": status, "metrics": metrics or {}, "details": details or [],
            "explanation": explanation}


def decimals(v) -> int:
    if isinstance(v, Decimal):
        return max(0, -v.as_tuple().exponent)
    return 0


def declared(schema) -> list:
    return list(schema["properties"])


def timestamp_fields(schema) -> set:
    return {f for f, p in schema["properties"].items() if str(p.get("$ref", "")).endswith("/timestamp")}


def location_fields(schema) -> dict:
    return {f: p["x-precision"] for f, p in schema["properties"].items()
            if p.get("x-privacy") in LOCATION_TAGS and "x-precision" in p}


def _instant(s):
    try:
        return dt.datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except ValueError:
        return None


def canon(value: dict, fields) -> str:
    return json.dumps({f: str(value.get(f)) for f in fields}, sort_keys=True)


# ---------- effective output ----------

def effective(job, out: Output, schema):
    """key -> value per the job's rule, plus keys seen with different values (non-deterministic duplicates)."""
    fields = declared(schema)
    groups = defaultdict(list)
    for r in out.records:
        groups[job.key_of(r.value)].append(r)
    eff, nondet = {}, []
    for k, rs in groups.items():
        if job.name == "ota-status-tracker":
            eff[k] = max(rs, key=lambda r: (r.partition, r.offset)).value
            continue
        variants = {canon(r.value, fields) for r in rs}
        if len(variants) > 1:
            nondet.append(k)
        eff[k] = rs[0].value
    return eff, sorted(nondet)


def same(va, vb) -> bool:
    nums = (int, Decimal)
    if isinstance(va, nums) and isinstance(vb, nums) and not isinstance(va, bool) and not isinstance(vb, bool):
        return Decimal(va) == Decimal(vb)
    return type(va) is type(vb) and va == vb


def compare_values(a: dict, b: dict, schema):
    ts = timestamp_fields(schema)
    diffs = []
    for f in declared(schema):
        va, vb = a.get(f), b.get(f)
        if same(va, vb):
            continue
        if f in ts and va is not None and vb is not None and _instant(va) and _instant(va) == _instant(vb):
            diffs.append((f, va, vb, "timestamp_format"))
        else:
            diffs.append((f, va, vb, "value"))
    return diffs


def check_a(job, legacy: Output, rust: Output, schema, title="A. Effective output equality"):
    le, lnd = effective(job, legacy, schema)
    re_, rnd = effective(job, rust, schema)
    missing = sorted(set(le) - set(re_))
    extra = sorted(set(re_) - set(le))
    loc = set(location_fields(schema))
    diffs, cats = [], Counter()
    for k in sorted(set(le) & set(re_)):
        for f, va, vb, cat in compare_values(le[k], re_[k], schema):
            cats[cat] += 1
            if f in loc:
                cats["location_value"] += 1
            diffs.append({"key": k, "field": f, "legacy": str(va), "rust": str(vb), "category": cat})
    details = ([{"key": k, "category": "missing_in_rust"} for k in missing[:MAX_DIFFS]]
               + [{"key": k, "category": "extra_in_rust"} for k in extra[:MAX_DIFFS]]
               + [{"key": k, "category": "nondeterministic_duplicate", "side": "legacy"} for k in lnd[:MAX_DIFFS]]
               + [{"key": k, "category": "nondeterministic_duplicate", "side": "rust"} for k in rnd[:MAX_DIFFS]]
               + diffs)[:MAX_DIFFS]
    metrics = {"legacy_keys": len(le), "rust_keys": len(re_), "missing_in_rust": len(missing),
               "extra_in_rust": len(extra), "field_diffs": len(diffs),
               "timestamp_format_diffs": cats["timestamp_format"], "value_diffs": cats["value"],
               "location_value_diffs": cats["location_value"],
               "nondeterministic_legacy": len(lnd), "nondeterministic_rust": len(rnd)}
    bad = missing or extra or diffs or lnd or rnd
    return result("A", title, FAIL if bad else PASS, metrics, details,
                  "Keys missing/extra, field values differ, or a key has different values in one output."
                  if bad else None), le


# ---------- B. duplicates ----------

def repeats(job, out: Output) -> int:
    if job.name == "ota-status-tracker":
        seen = Counter((r.key, r.raw if r.raw is not None else canon(r.value, sorted(r.value))) for r in out.records)
    else:
        seen = Counter(job.key_of(r.value) for r in out.records)
    return sum(c - 1 for c in seen.values() if c > 1)


def check_b(job, legacy: Output, rust: Output):
    rd, ld = repeats(job, rust), repeats(job, legacy)
    return result("B", "B. Duplicates", FAIL if rd else PASS,
                  {"rust_duplicates": rd, "legacy_duplicates_info": ld},
                  explanation=(f"Rust output repeats {rd} business key(s) in read_committed. A transactional "
                               "producer with send_offsets_to_transaction prevents this.") if rd else None)


# ---------- C. ordering ----------

def order_violations(job, out: Output) -> list:
    groups = defaultdict(list)
    for r in sorted(out.records, key=lambda r: (r.partition, r.offset)):
        g = r.key if job.order_by == "key" else r.value.get("vin")
        groups[(r.partition, g)].append(r)
    bad = []
    for (p, g), rs in groups.items():
        for a, b in zip(rs, rs[1:]):
            ta, tb = _instant(a.value[job.time_field]), _instant(b.value[job.time_field])
            if ta and tb and tb < ta:
                bad.append({"partition": p, "group": g, "offset": b.offset,
                            "previous": str(a.value[job.time_field]), "value": str(b.value[job.time_field])})
    return bad


def check_c(job, legacy: Output, rust: Output):
    if job.is_parquet:
        return result("C", "C. Ordering", NA, explanation="Parquet output has no offset order.")
    rb, lb = order_violations(job, rust), order_violations(job, legacy)
    return result("C", "C. Ordering", FAIL if rb else PASS,
                  {"rust_violations": len(rb), "legacy_violations_info": len(lb), "field": job.time_field},
                  rb[:MAX_DIFFS],
                  f"{job.time_field} decreases in offset order within a partition for the same "
                  f"{'key' if job.order_by == 'key' else 'VIN'}." if rb else None)


# ---------- D. partition placement ----------

def check_d(job, legacy: Output, rust: Output, partitions=6):
    if job.is_parquet:
        return result("D", "D. Partition placement", NA, explanation="Parquet output has no partitions.")
    lp, rp = defaultdict(set), defaultdict(set)
    for r in legacy.records:
        lp[r.key].add(r.partition)
    for r in rust.records:
        rp[r.key].add(r.partition)
    bad = []
    for k in sorted(set(lp) & set(rp)):
        if lp[k] != rp[k]:
            bad.append({"key": k, "legacy": sorted(lp[k]), "rust": sorted(rp[k]),
                        "java_murmur2": java_partition(k.encode(), partitions)})
    legacy_off = sum(1 for k, ps in lp.items() if k is not None and ps != {java_partition(k.encode(), partitions)})
    return result("D", "D. Partition placement", FAIL if bad else PASS,
                  {"keys_compared": len(set(lp) & set(rp)), "misplaced": len(bad),
                   "legacy_not_murmur2_info": legacy_off}, bad[:MAX_DIFFS],
                  ("Keys land on different partitions. Java clients (the legacy Spark sink) use murmur2 by default; "
                   "librdkafka's default is consistent_random (CRC32). Set partitioner=murmur2_random in the Rust "
                   "producer.") if bad else None)


# ---------- E. privacy ----------

def field_sets(out: Output) -> Counter:
    return Counter(tuple(sorted(r.value)) for r in out.records)


def check_e1(job, legacy: Output, rust: Output, schema):
    decl = set(declared(schema))
    rsets, lsets = field_sets(rust), field_sets(legacy)
    r_union = set().union(*map(set, rsets)) if rsets else set()
    l_union = set().union(*map(set, lsets)) if lsets else set()
    bad_records = sum(c for fs, c in rsets.items() if set(fs) != decl)
    undeclared = sorted(r_union - decl)
    missing = sorted(decl - r_union) if rust.records else []
    ok = not bad_records and r_union == l_union == decl if rust.records else l_union in (decl, set())
    return result("E1", "E1. Field set", PASS if ok else FAIL,
                  {"rust_records_with_other_fields": bad_records, "undeclared_fields": undeclared,
                   "missing_fields": missing, "legacy_matches_schema": l_union == decl},
                  explanation=(f"Rust output has fields outside the schema: {undeclared}" if undeclared
                               else "Rust field set differs from legacy or the schema.") if not ok else None)


def max_decimals(out: Output, fields) -> dict:
    return {f: max((decimals(r.value.get(f)) for r in out.records), default=0) for f in fields}


def check_e2(job, legacy: Output, rust: Output, schema, location_value_diffs: int):
    loc = location_fields(schema)
    if not loc:
        return result("E2", "E2. Location precision", NA, explanation="No location-tagged fields in this output.")
    over = [{"field": f, "value": str(r.value[f]), "precision": p} for r in rust.records
            for f, p in loc.items() if decimals(r.value.get(f)) > p]
    rmax, lmax = max_decimals(rust, loc), max_decimals(legacy, loc)
    bad = over or rmax != lmax or location_value_diffs
    return result("E2", "E2. Location precision", FAIL if bad else PASS,
                  {"x_precision": loc, "rust_max_decimals": rmax, "legacy_max_decimals": lmax,
                   "values_over_precision": len(over), "location_value_mismatches": location_value_diffs},
                  over[:MAX_DIFFS],
                  "Location fields differ from legacy in precision or value." if bad else None)


def scan_sensitive(messages, sensitive: dict) -> dict:
    found = {}

    def walk(v):
        if isinstance(v, dict):
            for k, x in v.items():
                if k in sensitive:
                    found[k] = sensitive[k]
                walk(x)
        elif isinstance(v, list):
            for x in v:
                walk(x)

    for raw in messages:
        try:
            walk(json.loads(raw))
        except (ValueError, TypeError):
            continue
    return dict(sorted(found.items()))


def check_e3(stores: list, declared_output: str, state_prefix: str, state_scan: dict):
    """stores: [{"kind": "topic"|"s3", "name": ...}] written by the Rust job."""
    undeclared = [s for s in stores if s["name"] != declared_output and
                  not (s["kind"] == "topic" and s["name"].startswith(state_prefix))]
    return result("E3", "E3. No new data stores", FAIL if undeclared else PASS,
                  {"stores": stores, "undeclared": undeclared, "state_store_sensitive_fields": state_scan},
                  undeclared,
                  f"The Rust job wrote to undeclared store(s): {[s['name'] for s in undeclared]}"
                  if undeclared else None)


def check_e4(manifest: dict | None, observed: dict):
    if manifest is None:
        return result("E4", "E4. Privacy manifest", NA, explanation="No --manifest given.")
    mism = []
    for k in ("inputs", "outputs", "state_stores"):
        if sorted(manifest.get(k, [])) != sorted(observed.get(k, [])):
            mism.append({"item": k, "declared": sorted(manifest.get(k, [])), "observed": sorted(observed.get(k, []))})
    ds, os_ = manifest.get("sensitive_fields", {}), observed.get("sensitive_fields", {})
    for store in sorted(set(ds) | set(os_)):
        if sorted(ds.get(store, [])) != sorted(os_.get(store, [])):
            mism.append({"item": f"sensitive_fields[{store}]", "declared": sorted(ds.get(store, [])),
                         "observed": sorted(os_.get(store, []))})
    return result("E4", "E4. Privacy manifest", FAIL if mism else PASS, {"mismatches": len(mism)}, mism,
                  "Observed inputs, outputs, state stores or sensitive fields differ from the manifest."
                  if mism else None)
