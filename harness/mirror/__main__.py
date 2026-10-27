"""Self-test stand-in for a Rust job (docs/RUST_JOB_CONTRACT.md).

It consumes the input topics as CONSUMER_GROUP and, in each transaction, re-emits the share of the legacy output
that matches the fraction of input committed so far, together with the input offsets
(send_offsets_to_transaction). A restart after SIGKILL resumes from the committed fraction, so output is
exactly-once. Each transaction also writes one progress record to a state topic under STATE_TOPIC_PREFIX.
TELEMETRY_FAILPOINT_AT_OFFSETS works as in the contract. MIRROR_FAULT injects one deliberate defect so the harness can prove each check fails.
"""
import json
import os
import re
import signal
import sys
import tempfile
import time
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

import boto3
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

TIME_FIELD = {"charging.sessions.v1": "start_time", "service.dtc.alerts.v1": "event_time",
              "ota.status.latest.v1": "last_event_time"}
LOCATION = {"charging.sessions.v1": ("charger_lat", 5), "service.dtc.alerts.v1": ("lat", 2)}
FAULTS = {"none", "duplicate", "precision", "partitioner", "extra-field", "new-sink", "reorder"}

env = os.environ


def log(msg):
    print(f"[mirror] {msg}", flush=True)


def s3():
    return boto3.client("s3", endpoint_url=env["S3_ENDPOINT"], region_name="us-east-1",
                        aws_access_key_id=env.get("AWS_ACCESS_KEY_ID", "minioadmin"),
                        aws_secret_access_key=env.get("AWS_SECRET_ACCESS_KEY", "minioadmin"))


def split_s3(uri):
    path = uri.split("://", 1)[1]
    bucket, _, prefix = path.partition("/")
    return bucket, prefix.rstrip("/")


def contract_topic(topic):
    return next(t for t in TIME_FIELD if topic == t or topic.endswith("." + t))


def field_text(raw, f):
    m = re.search(rf'"{re.escape(f)}":"([^"]*)"', raw)
    return m.group(1) if m else ""


def load_kafka_source(topic):
    bucket, prefix = split_s3(env["MIRROR_SOURCE"])
    body = s3().get_object(Bucket=bucket, Key=f"{prefix}/legacy.jsonl")["Body"].read().decode()
    recs = [json.loads(line) for line in body.splitlines() if line.strip()]
    tf = TIME_FIELD[contract_topic(topic)]
    recs.sort(key=lambda r: (field_text(r["value"], tf), r["partition"], r["offset"]))
    return [(r["key"], r["value"]) for r in recs]


def apply_faults(recs, fault, topic):
    topic = contract_topic(topic)
    if fault == "precision" and topic in LOCATION:
        f, p = LOCATION[topic]
        q = Decimal(1).scaleb(-(p - 1))

        def coarser(m):
            return f'"{f}":{Decimal(m.group(1)).quantize(q, rounding=ROUND_HALF_UP)}'

        recs = [(k, re.sub(rf'"{f}":(-?[0-9]+\.[0-9]+)', coarser, v, count=1)) for k, v in recs]
    if fault == "extra-field":
        recs = [(k, '{"debug_source":"mirror",' + v[1:]) for k, v in recs]
    if fault == "reorder":
        seen = {}
        for i, (_, v) in enumerate(recs):
            vin = field_text(v, "vin")
            if vin in seen:
                j = seen[vin]
                recs = list(recs)
                recs[i], recs[j] = recs[j], recs[i]
                log(f"reorder: swapped two records of {vin}")
                break
            seen[vin] = i
        else:
            log("reorder: no VIN has two records; nothing to reorder")
    return recs


def retry(fn, seconds=60):
    # a fresh broker answers NOT_COORDINATOR / COORDINATOR_LOAD_IN_PROGRESS until __consumer_offsets is ready
    deadline = time.monotonic() + seconds
    while True:
        try:
            return fn()
        except KafkaException as e:
            if time.monotonic() > deadline:
                raise
            log(f"retrying after {e}")
            time.sleep(1)


def main():
    fault = env.get("MIRROR_FAULT", "none")
    if fault not in FAULTS:
        log(f"unknown MIRROR_FAULT {fault}")
        return 2
    inputs = env["INPUT_TOPICS"].split(",")
    group, batch = env["CONSUMER_GROUP"], int(env.get("MIRROR_BATCH", "10"))
    boot = env["KAFKA_BOOTSTRAP"]
    consumer = Consumer({"bootstrap.servers": boot, "group.id": group, "enable.auto.commit": False,
                         "auto.offset.reset": "earliest", "isolation.level": "read_committed"})
    tps = []
    for t in inputs:
        for p in sorted(consumer.list_topics(t, timeout=10).topics[t].partitions):
            tps.append(TopicPartition(t, p))
    ends = {(tp.topic, tp.partition): consumer.get_watermark_offsets(tp, timeout=10)[1] for tp in tps}
    total = sum(ends.values())
    if env.get("OUTPUT_TOPIC"):
        # fence a killed run first: its open transaction holds pending offsets, and a read_committed committed()
        # waits on those until transaction.timeout.ms aborts it
        Producer({"bootstrap.servers": boot, "transactional.id": env["TRANSACTIONAL_ID"]}).init_transactions(30)
    committed = {(tp.topic, tp.partition): max(tp.offset, 0) for tp in retry(lambda: consumer.committed(tps, timeout=10))}
    c0 = sum(committed.values())
    restart = c0 > 0
    log(f"start: input end offsets={total} committed={c0} restart={restart} fault={fault}")
    if c0 >= total:
        return 0
    failpoint = int(env["TELEMETRY_FAILPOINT_AT_OFFSETS"]) if env.get("TELEMETRY_FAILPOINT_AT_OFFSETS") else None

    out_topic, out_path = env.get("OUTPUT_TOPIC"), env.get("OUTPUT_PATH")
    if out_topic:
        recs = apply_faults(load_kafka_source(out_topic), fault, out_topic)
    else:
        table = load_parquet_source()
        recs = None
    n_total = len(recs) if recs is not None else table.num_rows

    def upto(c):
        return n_total if c >= total else c * n_total // total

    n_prev = upto(c0)
    transactional = not (fault == "duplicate" and restart)
    pconf = {"bootstrap.servers": boot, "partitioner": "consistent_random" if fault == "partitioner" else "murmur2_random",
             "enable.idempotence": True, "linger.ms": 1}
    if transactional and out_topic:
        pconf["transactional.id"] = env["TRANSACTIONAL_ID"]
    producer = Producer(pconf)
    if out_topic and transactional:
        producer.init_transactions(30)
    state = None
    if out_topic and transactional:
        state = env["STATE_TOPIC_PREFIX"] + ".progress"
        admin = AdminClient({"bootstrap.servers": boot})
        try:
            admin.create_topics([NewTopic(state, 6, 1, config={"cleanup.policy": "compact"})])[state].result(30)
        except KafkaException as e:
            if e.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise

    side = None
    if fault == "new-sink" and out_topic:
        side = env["STATE_TOPIC_PREFIX"].removesuffix(".state") + ".cache"
        admin = AdminClient({"bootstrap.servers": boot})
        admin.create_topics([NewTopic(side, 6, 1)])[side].result(30)
        log(f"new-sink: writing a copy of every record to {side}")

    if fault == "duplicate" and restart and out_topic:
        # the killed producer is already fenced above; misbehave without transactions
        log(f"duplicate: re-emitting {n_prev} records without a transaction")
        for k, v in recs[:n_prev]:
            producer.produce(out_topic, key=k.encode(), value=v.encode())
        producer.flush(30)

    consumer.subscribe(inputs)
    pos = dict(committed)
    while True:
        msgs = consumer.consume(batch, timeout=1.0)
        for m in msgs:
            if m.error():
                raise RuntimeError(m.error())
            pos[(m.topic(), m.partition())] = m.offset() + 1
        if msgs:
            n = upto(sum(pos.values()))
            offsets = [TopicPartition(t, p, o) for (t, p), o in sorted(pos.items())]
            crash = failpoint is not None and sum(pos.values()) >= failpoint
            if out_topic:
                emit_kafka(producer, consumer, transactional, out_topic, side, state, recs[n_prev:n], offsets, crash)
            else:
                emit_parquet(table, n_prev, n, out_path)
                if crash:
                    die()
                consumer.commit(offsets=offsets, asynchronous=False)
            n_prev = n
        if all(pos[k] >= e for k, e in ends.items()):
            break
    consumer.close()
    log(f"done: emitted up to {n_prev} of {n_total}")
    return 0


def die():
    log("failpoint: SIGKILL before commit")
    os.kill(os.getpid(), signal.SIGKILL)


def emit_kafka(producer, consumer, transactional, topic, side, state, recs, offsets, crash):
    if transactional:
        producer.begin_transaction()
    for k, v in recs:
        producer.produce(topic, key=k.encode(), value=v.encode())
        if side:
            producer.produce(side, key=k.encode(), value=v.encode())
    if state:
        producer.produce(state, key=b"progress", value=json.dumps({"committed": sum(o.offset for o in offsets)}).encode())
    if transactional:
        producer.send_offsets_to_transaction(offsets, consumer.consumer_group_metadata(), 30)
        if crash:
            producer.flush(30)
            die()
        producer.commit_transaction(30)
    else:
        producer.flush(30)
        consumer.commit(offsets=offsets, asynchronous=False)


def load_parquet_source():
    bucket, prefix = split_s3(env["MIRROR_SOURCE"])
    client, tables = s3(), []
    with tempfile.TemporaryDirectory() as d:
        keys = [o["Key"] for o in client.list_objects_v2(Bucket=bucket, Prefix=f"{prefix}/parquet/").get("Contents", [])]
        for i, k in enumerate(sorted(k for k in keys if k.endswith(".parquet"))):
            local = Path(d) / f"{i}.parquet"
            client.download_file(bucket, k, str(local))
            t = pq.ParquetFile(local).read()
            date = re.search(r"date=([0-9-]+)", k).group(1)
            tables.append(t.append_column("__date", pa.array([date] * t.num_rows)))
    table = pa.concat_tables(tables)
    return table.sort_by([("window_start", "ascending"), ("vin", "ascending")])


def emit_parquet(table, a, b, out_path):
    if b <= a:
        return
    bucket, prefix = split_s3(out_path)
    part = table.slice(a, b - a)
    client = s3()
    for date in sorted(set(part.column("__date").to_pylist())):
        rows = part.filter(pc.equal(part.column("__date"), date)).drop_columns(["__date"])
        with tempfile.NamedTemporaryFile(suffix=".parquet") as f:
            pq.write_table(rows, f.name)
            client.upload_file(f.name, bucket, f"{prefix}/date={date}/part-{a:06d}-{b:06d}.parquet")


if __name__ == "__main__":
    sys.exit(main())
