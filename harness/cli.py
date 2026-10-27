import argparse
import contextlib
import datetime as dt
import hashlib
import json
import math
import os
import secrets
import shlex
import shutil
import subprocess
import sys
import time
import traceback
from pathlib import Path

import yaml

from harness import checks, config, report
from harness.checks import Output, Rec, parse
from harness.config import BUCKET, DATA_CONTRACTS, GOLDEN, JOBS, ROOT, RUNS
from harness.docker_env import Docker, HarnessError
from harness.kafka_io import Kafka
from harness.storage import Lake, read_parquet

FAULTS = ["none", "duplicate", "precision", "partitioner", "extra-field", "new-sink", "reorder"]
FAULT_FAILS = {"none": [], "duplicate": ["B"], "precision": ["A", "E2"], "partitioner": ["D"],
               "extra-field": ["E1"], "new-sink": ["E3"], "reorder": ["C"]}


def parse_args(argv):
    p = argparse.ArgumentParser(prog="run.sh", description="Legacy vs Rust parity harness")
    p.add_argument("--job", required=True, choices=sorted(JOBS))
    p.add_argument("--fixture", required=True, choices=["sample", "full", "edge"])
    p.add_argument("--legacy-only", action="store_true")
    p.add_argument("--chaos", action="store_true")
    p.add_argument("--chaos-at", type=float, default=0.4)
    p.add_argument("--chaos-mode", choices=["kill", "failpoint"], default="kill",
                   help="kill: SIGKILL from outside once committed offsets reach --chaos-at; "
                        "failpoint: the job kills itself before committing (TELEMETRY_FAILPOINT_AT_OFFSETS)")
    p.add_argument("--self-test", choices=FAULTS)
    p.add_argument("--determinism", action="store_true")
    p.add_argument("--manifest", type=Path, help="privacy-manifest.yaml for check E4")
    p.add_argument("--legacy-from-golden", action="store_true",
                   help="use golden/<fixture>/<job> as the legacy side instead of running Spark")
    a = p.parse_args(argv)
    if not 0 < a.chaos_at < 1:
        p.error("--chaos-at must be between 0 and 1")
    if a.self_test == "duplicate":
        a.chaos = True
    return a


def main(argv=None):
    args = parse_args(argv)
    run = None
    try:
        run = Run(args, argv if argv is not None else sys.argv[1:])
        return run.execute()
    except Exception as e:
        msg = f"harness error: {e}" if isinstance(e, HarnessError) else traceback.format_exc()
        print(msg, file=sys.stderr)
        if run is not None:
            (run.dir / "harness-error.txt").write_text(msg + "\n")
        return 2


def failpoint_exit_error(code, target):
    if code == 137:
        return None
    if code == 0:
        return HarnessError(f"rust job exited 0 with TELEMETRY_FAILPOINT_AT_OFFSETS={target}, so the failpoint "
                            "never fired; the image must implement it (docs/RUST_JOB_CONTRACT.md).")
    return HarnessError(f"rust job exited {code} with TELEMETRY_FAILPOINT_AT_OFFSETS={target}, not 137 (SIGKILL), "
                        "so it failed for another reason before the failpoint; see rust-job.log.")


class Run:
    def __init__(self, args, argv):
        self.a = args
        self.job = JOBS[args.job]
        stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        self.run_id = f"{stamp}-{self.job.name}-{secrets.token_hex(2)}"
        self.dir = RUNS / self.run_id
        self.dir.mkdir(parents=True)
        self.log = self.dir / "harness.log"
        host = os.environ.get("KAFKA_EXTERNAL_HOST", "localhost")
        self.kafka = Kafka(f"{host}:9094")
        self.lake = Lake(f"http://{host}:9000")
        self.docker = Docker(host)
        self.deps = config.deps_lock()
        self.durations = {}
        self.command = "./run.sh " + " ".join(shlex.quote(x) for x in argv)
        self.schema = config.schema(self.job.output)
        self.input_records = None
        print(f"run {self.run_id} -> {self.dir}")

    @contextlib.contextmanager
    def step(self, name):
        t0 = time.monotonic()
        print(f"  {name} ...", flush=True)
        yield
        self.durations[name] = self.durations.get(name, 0) + time.monotonic() - t0

    # ---------- setup ----------

    def execute(self):
        if not (DATA_CONTRACTS / ".git").exists() or not (ROOT / ".deps/telemetry-spark-jobs/.git").exists():
            raise HarnessError("dependencies missing: run scripts/fetch-deps.sh")
        t0 = time.monotonic()
        with self.step("fixture"):
            self.fixture = self.prepare_fixture()
        self.manifest_sha = hashlib.sha256((self.fixture / "MANIFEST.json").read_bytes()).hexdigest()
        if self.a.determinism:
            status = self.determinism()
        elif self.a.legacy_only:
            status = self.legacy_only()
        else:
            status = self.parity()
        print(f"{status}: {self.dir / 'parity-report.md'} ({time.monotonic() - t0:.0f} s)")
        return 0 if status == "PASS" else 1

    def prepare_fixture(self) -> Path:
        if self.a.fixture in ("sample", "edge"):
            return DATA_CONTRACTS / "fixtures" / self.a.fixture
        out = self.dir / "fixture"
        subprocess.run([sys.executable, str(DATA_CONTRACTS / "generator" / "gen.py"), "--out", str(out)],
                       check=True, stdout=subprocess.DEVNULL)
        return out

    def output_topic(self, side):
        return f"{side}.{self.job.output}"

    def s3_prefix(self, side):
        return f"{self.run_id}/{side}/battery_health_hourly"

    def setup(self):
        with self.step("clean slate"):
            self.docker.clean_slate(self.log)
            self.kafka.wait_ready()
        with self.step("topics and fixture load"):
            registry = config.topics()
            t = registry[self.job.input_topic]
            self.kafka.create_topic(t["name"], t["partitions"], t["cleanup_policy"])
            if not self.job.is_parquet:
                o = registry[self.job.output]
                for side in ("legacy", "rust"):
                    self.kafka.create_topic(self.output_topic(side), o["partitions"], o["cleanup_policy"])
            self.wait_bucket()
            loaded = self.kafka.load_fixture(self.fixture / f"{self.job.input_topic}.jsonl", t["name"],
                                             t["partitions"])
            if loaded["misplaced"]:
                raise HarnessError(f"loader placed keys off murmur2: {loaded['misplaced'][:5]}")
            self.input_records = loaded["records"]

    def wait_bucket(self):
        for _ in range(60):
            try:
                if BUCKET in self.lake.buckets():
                    return
            except Exception:
                pass
            time.sleep(1)
        raise HarnessError(f"bucket {BUCKET} not available")

    # ---------- legacy ----------

    def run_legacy(self, side="legacy") -> Output:
        with self.step("spark-runner image"):
            self.docker.ensure_spark_runner(self.deps["TELEMETRY_SPARK_JOBS_SHA"], self.log)
        with self.step("legacy job"):
            # every legacy run starts from a clean slate, so a second run reuses the legacy topic and prefix
            out = f"s3a://{BUCKET}/{self.s3_prefix('legacy')}" if self.job.is_parquet else self.output_topic("legacy")
            self.docker.spark_submit(self.job, out, self.dir / f"{side}-spark.log", self.job.is_parquet)
        with self.step("capture"):
            return self.capture(side, source="legacy")

    def capture(self, side, source=None) -> Output:
        source = source or side
        dest = self.dir / side
        dest.mkdir(exist_ok=True)
        if self.job.is_parquet:
            prefix = self.s3_prefix(source)
            self.lake.download(prefix, dest / "parquet")
            rows, types = read_parquet(dest / "parquet")
            return Output(side, "parquet", [Rec(None, None, None, r) for r in rows], types)
        msgs = self.kafka.capture(self.output_topic(source))
        with open(dest / f"{self.job.output}.jsonl", "w") as f:
            for m in msgs:
                f.write(json.dumps(m.as_json()) + "\n")
        return Output(side, "kafka", [Rec(m.partition, m.offset, m.key, parse(m.raw), m.raw) for m in msgs])

    def golden_dir(self):
        return GOLDEN / self.a.fixture / self.job.name

    def load_golden(self) -> Output:
        g = self.golden_dir()
        if not (g / "summary.json").exists():
            raise HarnessError(f"no golden at {g}; run --legacy-only first")
        if self.job.is_parquet:
            rows, types = read_parquet(g / "parquet")
            return Output("legacy", "parquet", [Rec(None, None, None, r) for r in rows], types)
        recs = [json.loads(line) for line in (g / "output.jsonl").read_text().splitlines() if line]
        return Output("legacy", "kafka", [Rec(r["partition"], r["offset"], r["key"], parse(r["value"]), r["value"])
                                          for r in recs])

    def legacy_side(self) -> Output:
        self.setup()
        if self.a.legacy_from_golden:
            with self.step("load golden"):
                return self.load_golden()
        return self.run_legacy()

    def summary(self, out: Output) -> dict:
        eff, nondet = checks.effective(self.job, out, self.schema)
        s = {"job": self.job.name, "fixture": self.a.fixture, "manifest_sha256": self.manifest_sha,
             "deps": self.deps, "input_records": self.input_records, "output_records": len(out.records),
             "effective_records": len(eff), "duplicates": checks.repeats(self.job, out),
             "nondeterministic_keys": len(nondet)}
        for f in ("close_reason", "state", "severity"):
            if f in self.schema["properties"]:
                vals = sorted(str(v.get(f)) for v in eff.values())
                s[f"by_{f}"] = {v: vals.count(v) for v in sorted(set(vals))}
        return s

    def legacy_only(self):
        legacy = self.legacy_side()
        summary = self.summary(legacy)
        (self.dir / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
        schema_bad = [r.raw or r.value for r in legacy.records if self.schema_errors(r)]
        res2 = checks.result("S", "Legacy output validates against the schema", "FAIL" if schema_bad else "PASS",
                             {"invalid_records": len(schema_bad)}, [str(x)[:200] for x in schema_bad[:20]])
        if schema_bad:
            res = checks.result("G", "Golden written", "FAIL", explanation="Golden left unchanged: output is invalid.")
            return self.finish("legacy-only", [res, res2])
        g = self.golden_dir()
        shutil.rmtree(g, ignore_errors=True)
        g.mkdir(parents=True)
        if self.job.is_parquet:
            shutil.copytree(self.dir / "legacy" / "parquet", g / "parquet")
        else:
            shutil.copy(self.dir / "legacy" / f"{self.job.output}.jsonl", g / "output.jsonl")
        (g / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
        res = checks.result("G", "Golden written", "PASS", {k: v for k, v in summary.items() if k != "deps"},
                            explanation=None)
        return self.finish("legacy-only", [res, res2])

    def schema_errors(self, rec):
        from jsonschema import Draft202012Validator
        from referencing import Registry, Resource
        if not hasattr(self, "_validator"):
            res = [Resource.from_contents(json.loads(p.read_text()))
                   for p in [config.CONTRACTS / "common.schema.json", *(config.CONTRACTS / "topics").glob("*.json")]]
            reg = Registry().with_resources([(r.contents["$id"], r) for r in res])
            self._validator = Draft202012Validator(self.schema, registry=reg)
        v = json.loads(rec.raw) if rec.raw else {k: (float(x) if isinstance(x, checks.Decimal) else x)
                                                 for k, x in rec.value.items()}
        return list(self._validator.iter_errors(v))

    def determinism(self):
        first = self.legacy_side()
        self.setup()
        second = self.run_legacy("legacy-2")
        a, _ = checks.check_a(self.job, first, second, self.schema, "A. Determinism: run 1 vs run 2")
        b = checks.result("B", "B. Duplicates (info)", "PASS",
                          {"run1_duplicates": checks.repeats(self.job, first),
                           "run2_duplicates": checks.repeats(self.job, second)})
        (self.dir / "summary.json").write_text(json.dumps(self.summary(first), indent=2, sort_keys=True) + "\n")
        return self.finish("determinism", [a, b])

    # ---------- rust ----------

    def rust_image(self):
        if self.a.self_test:
            tag = f"telemetry-rs/{self.job.name}:selftest"
            files = [ROOT / "docker/mirror.Dockerfile", ROOT / "harness/__init__.py", ROOT / "harness/murmur2.py",
                     *sorted((ROOT / "harness/mirror").glob("*.py"))]
            h = hashlib.sha256(b"".join(f.read_bytes() for f in files)).hexdigest()[:12]
            base = f"telemetry-parity/mirror:{h}"
            with self.step("mirror image"):
                if not self.docker.has_image(base):
                    self.docker.build("docker/mirror.Dockerfile", [base], self.log)
                self.docker.docker("tag", base, tag)
            return tag
        image = os.environ.get("RUST_IMAGE")
        if not image:
            raise HarnessError("RUST_IMAGE is not set (or use --self-test)")
        return image

    def stores(self) -> tuple:
        return self.kafka.list_topics(), set(self.lake.keys())

    def parity(self):
        legacy = self.legacy_side()
        image = self.rust_image()
        group, txid = f"rust.{self.job.name}", f"rust.{self.job.name}.tx"
        state_prefix = f"rust.{self.job.name}.state"
        env = {"KAFKA_BOOTSTRAP": "kafka:9092", "INPUT_TOPICS": self.job.input_topic,
               "S3_ENDPOINT": "http://minio:9000", "CONSUMER_GROUP": group, "TRANSACTIONAL_ID": txid,
               "STATE_TOPIC_PREFIX": state_prefix, "EXIT_WHEN_CAUGHT_UP": "true"}
        if self.job.is_parquet:
            env["OUTPUT_PATH"] = f"s3a://{BUCKET}/{self.s3_prefix('rust')}"
        else:
            env["OUTPUT_TOPIC"] = self.output_topic("rust")
        if self.a.self_test:
            src = f"{self.run_id}/selftest"
            if self.job.is_parquet:
                root = (self.golden_dir() / "parquet") if self.a.legacy_from_golden else self.dir / "legacy" / "parquet"
                for f in root.rglob("*.parquet"):
                    self.lake.put(f"{src}/parquet/{f.relative_to(root)}", f)
            else:
                path = self.dir / "selftest-legacy.jsonl"
                path.write_text("".join(json.dumps({"partition": r.partition, "offset": r.offset, "key": r.key,
                                                    "value": r.raw}) + "\n" for r in legacy.records))
                self.lake.put(f"{src}/legacy.jsonl", path)
            env.update({"MIRROR_SOURCE": f"s3://{BUCKET}/{src}", "MIRROR_FAULT": self.a.self_test})

        before = self.stores()
        name = f"parity-rust-{self.job.name}"
        self.docker.remove(name)
        chaos = None
        failpoint = self.a.chaos and self.a.chaos_mode == "failpoint"
        with self.step("rust job"):
            first = dict(env)
            if failpoint:
                first["TELEMETRY_FAILPOINT_AT_OFFSETS"] = str(self.chaos_target()[1])
            self.docker.start_rust(name, image, first, self.log)
            try:
                if failpoint:
                    chaos = self.failpoint(name, group, image, env)
                elif self.a.chaos:
                    chaos = self.chaos(name, group)
                code = self.docker.wait(name)
            finally:
                self.docker.logs(name, self.dir / "rust-job.log")
                self.docker.remove(name)
        ends = self.kafka.end_offsets(self.job.input_topic)
        committed = self.kafka.committed(group, self.job.input_topic)
        caught_up = all(committed.get(p, 0) >= e for p, e in ends.items())
        why = []
        if code != 0:
            tail = (self.dir / "rust-job.log").read_text(errors="replace").splitlines()[-5:]
            why.append(f"Rust job exited with {code}; see rust-job.log. Last lines: " + " | ".join(tail))
        if not caught_up:
            why.append("Committed offsets are behind the input end offsets.")
        r = checks.result("R", "R. Rust job exited 0 and caught up", "FAIL" if why else "PASS",
                          {"exit_code": code, "input_end_offsets": sum(ends.values()),
                           "committed": sum(committed.values())}, explanation=" ".join(why) or None)

        with self.step("capture"):
            rust = self.capture("rust")
            aborted = None
            if not self.job.is_parquet:
                aborted = len(self.kafka.capture(self.output_topic("rust"), "read_uncommitted")) - len(rust.records)
        after = self.stores()
        aborted_by_topic = {}
        if self.a.chaos:
            counted = [t for t in sorted(after[0]) if t.startswith(state_prefix)]
            if not self.job.is_parquet:
                counted.insert(0, self.output_topic("rust"))
            for t in counted:
                aborted_by_topic[t] = (len(self.kafka.capture(t, "read_uncommitted"))
                                       - len(self.kafka.capture(t, "read_committed")))
        new_topics = sorted(after[0] - before[0])
        new_keys = sorted(after[1] - before[1])
        declared_out = env.get("OUTPUT_TOPIC") or self.s3_prefix("rust")
        stores = [{"kind": "topic", "name": t} for t in new_topics if not t.startswith("__")]
        prefixes = sorted({k[:len(self.s3_prefix('rust'))] if k.startswith(self.s3_prefix("rust") + "/")
                           else "/".join(k.split("/")[:3]) for k in new_keys})
        stores += [{"kind": "s3", "name": p} for p in prefixes]
        out_store = {"kind": "s3" if self.job.is_parquet else "topic", "name": declared_out}
        if out_store not in stores and rust.records:
            stores.append(out_store)
        sensitive = config.sensitive_fields()
        state_scan = {}
        for s in stores:
            if s["kind"] == "topic" and s["name"] != declared_out:
                state_scan[s["name"]] = checks.scan_sensitive([m.raw for m in self.kafka.capture(s["name"])],
                                                              sensitive)

        a, _ = checks.check_a(self.job, legacy, rust, self.schema)
        results = [
            r, a,
            checks.check_b(self.job, legacy, rust),
            checks.check_c(self.job, legacy, rust),
            checks.check_d(self.job, legacy, rust),
            checks.check_e1(self.job, legacy, rust, self.schema),
            checks.check_e2(self.job, legacy, rust, self.schema, a["metrics"]["location_value_diffs"]),
            checks.check_e3(stores, declared_out, state_prefix, state_scan),
        ]
        out_sensitive = {f: p["x-privacy"] for f, p in self.schema["properties"].items()
                         if p.get("x-privacy", "none") != "none"}
        name = lambda n: checks.manifest_name(n, self.run_id)
        observed = {"inputs": [self.job.input_topic], "outputs": [name(declared_out)],
                    "state_stores": sorted(name(s["name"]) for s in stores if s["name"] != declared_out),
                    "sensitive_fields": {name(declared_out): sorted(out_sensitive),
                                         **{name(k): sorted(v) for k, v in state_scan.items()}}}
        manifest = yaml.safe_load(self.a.manifest.read_text()) if self.a.manifest else None
        results.append(checks.check_e4(manifest, observed))
        if chaos:
            for t, n in aborted_by_topic.items():
                chaos["timeline"].append({"event": f"aborted-transaction records on `{t}` "
                                                   "(read_uncommitted - read_committed)", "detail": str(n)})
            dup = results[2]["metrics"]["rust_duplicates"]
            chaos["timeline"].append({"event": "duplicates after restart (check B)", "detail": str(dup)})
            results.append(checks.result("F", "F. Chaos summary (informational)", "PASS",
                                         {"chaos_mode": chaos["mode"],
                                          "kill_committed_offsets": chaos["kill_committed_total"],
                                          "input_total": chaos["input_total"], "kill_time": chaos["kill_time"],
                                          "restart_time": chaos["restart_time"], "aborted_records": aborted,
                                          "aborted_records_by_topic": aborted_by_topic,
                                          "duplicates_after_restart": dup}))
        else:
            results.append(checks.result("F", "F. Chaos", "N/A", explanation="--chaos not set"))
        privacy = [{"name": declared_out, "kind": out_store["kind"], "sensitive_fields": out_sensitive}]
        privacy += [{"name": k, "kind": "state topic", "sensitive_fields": v} for k, v in state_scan.items()]
        mode = f"self-test:{self.a.self_test}" if self.a.self_test else "parity"
        extra = {"rust_image": image, "chaos": chaos, "privacy": privacy, "aborted_records": aborted,
                 "aborted_records_by_topic": aborted_by_topic}
        if self.a.self_test:
            failed = sorted(c["id"] for c in results if c["status"] == "FAIL")
            extra["self_test"] = {"fault": self.a.self_test, "expected_fail": FAULT_FAILS[self.a.self_test],
                                  "actual_fail": failed, "as_expected": failed == FAULT_FAILS[self.a.self_test]}
        return self.finish(mode + (f"+chaos:{self.a.chaos_mode}" if self.a.chaos else ""), results, **extra)

    def chaos_target(self):
        total = sum(self.kafka.end_offsets(self.job.input_topic).values())
        return total, math.ceil(self.a.chaos_at * total)

    def exited_early(self, target, total):
        return HarnessError(f"rust job exited before committed offsets reached {target}/{total}, so the kill "
                            "could not land. The job finished too fast for kill mode; rerun with --chaos-mode "
                            "failpoint (TELEMETRY_FAILPOINT_AT_OFFSETS in docs/RUST_JOB_CONTRACT.md).")

    def running(self, name):
        r = self.docker.docker("inspect", "-f", "{{.State.Running}}", name, capture=True, check=False)
        return r.stdout.strip() == "true"

    def chaos(self, name, group):
        total, target = self.chaos_target()
        timeline = [{"event": "rust job started", "detail": f"input end offsets total={total}, kill at >= {target}"}]
        i = 0
        reader = self.kafka.offset_reader(group, "read_uncommitted")
        while True:
            try:
                c = self.kafka.committed(group, self.job.input_topic, reader)
            except Exception:
                c = {}
            if sum(c.values()) >= target:
                try:
                    self.docker.kill(name)
                except HarnessError:
                    if self.running(name):
                        raise
                    reader.close()
                    raise self.exited_early(target, total) from None
                t_kill = dt.datetime.now(dt.timezone.utc)
                at_kill = self.kafka.committed(group, self.job.input_topic, reader)
                reader.close()
                break
            i += 1
            if i % 10 == 0 and not self.running(name):
                reader.close()
                raise self.exited_early(target, total)
            time.sleep(0.02)
        timeline.append({"event": "SIGKILL", "detail": f"{t_kill.isoformat()} committed={sum(at_kill.values())}/"
                                                      f"{total} per partition={dict(sorted(at_kill.items()))}"})
        self.docker.restart(name)
        t_restart = dt.datetime.now(dt.timezone.utc)
        timeline.append({"event": "restart (same container, group.id and transactional.id)",
                         "detail": t_restart.isoformat()})
        return {"mode": "kill", "chaos_at": self.a.chaos_at, "input_total": total, "kill_target": target,
                "kill_committed_total": sum(at_kill.values()), "kill_committed": at_kill,
                "kill_time": t_kill.isoformat(), "restart_time": t_restart.isoformat(), "timeline": timeline}

    def failpoint(self, name, group, image, env):
        total, target = self.chaos_target()
        timeline = [{"event": "rust job started", "detail": f"input end offsets total={total}, "
                                                           f"TELEMETRY_FAILPOINT_AT_OFFSETS={target}"}]
        code = self.docker.wait(name)
        t_crash = dt.datetime.now(dt.timezone.utc)
        reader = self.kafka.offset_reader(group, "read_uncommitted")
        try:
            at_crash = self.kafka.committed(group, self.job.input_topic, reader)
        finally:
            reader.close()
        if err := failpoint_exit_error(code, target):
            self.docker.logs(name, self.dir / "rust-job.log")
            raise err
        timeline.append({"event": f"failpoint crash (exit {code})",
                         "detail": f"{t_crash.isoformat()} committed={sum(at_crash.values())}/{total} "
                                   f"per partition={dict(sorted(at_crash.items()))}"})
        self.docker.logs(name, self.dir / "rust-job.log")
        self.docker.remove(name)
        self.docker.start_rust(name, image, env, self.log)
        t_restart = dt.datetime.now(dt.timezone.utc)
        timeline.append({"event": "restart (new container, same group.id and transactional.id, "
                                  "no TELEMETRY_FAILPOINT_AT_OFFSETS)", "detail": t_restart.isoformat()})
        return {"mode": "failpoint", "chaos_at": self.a.chaos_at, "input_total": total, "kill_target": target,
                "kill_committed_total": sum(at_crash.values()), "kill_committed": at_crash,
                "kill_time": t_crash.isoformat(), "restart_time": t_restart.isoformat(), "timeline": timeline}

    # ---------- report ----------

    def finish(self, mode, results, **extra):
        data = {"run_id": self.run_id, "job": self.job.name, "mode": mode, "fixture": self.a.fixture,
                "manifest_sha256": self.manifest_sha, "deps": self.deps, "input_records": self.input_records,
                "checks": results, "durations": self.durations, "command": self.command, **extra}
        return report.write(self.dir, data)
