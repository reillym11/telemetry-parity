# Rust job contract

A Rust port of a legacy job is a container image that the parity harness starts once, may kill with SIGKILL, and restarts with identical configuration. This document is the contract it must follow.

## Image

`telemetry-rs/<job>:<tag>`, where `<job>` is one of `ota-status-tracker`, `charging-session-builder`, `battery-health-aggregator`, `diagnostics-dtc-router`. The harness takes the image from `RUST_IMAGE`.

## Environment

| Variable | Value set by the harness |
|---|---|
| `KAFKA_BOOTSTRAP` | `kafka:9092` |
| `INPUT_TOPICS` | Comma-separated input topics, for example `vehicle.charging.events.v1` |
| `OUTPUT_TOPIC` | Kafka jobs only: `rust.<output topic>`, for example `rust.charging.sessions.v1` |
| `OUTPUT_PATH` | Battery job only: `s3a://telemetry-lake/<run_id>/rust/battery_health_hourly` |
| `S3_ENDPOINT` | `http://minio:9000` (credentials in `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`) |
| `CONSUMER_GROUP` | `rust.<job>` |
| `TRANSACTIONAL_ID` | `rust.<job>.tx` |
| `STATE_TOPIC_PREFIX` | `rust.<job>.state` |
| `EXIT_WHEN_CAUGHT_UP` | `true` |
| `TELEMETRY_FAILPOINT_AT_OFFSETS` | `--chaos-mode failpoint` only, first start only: `ceil(--chaos-at * input end offsets)` |

## Behavior

1. At startup, read the end offsets of every partition of `INPUT_TOPICS`.
2. Consume as `CONSUMER_GROUP`, resuming from its committed offsets.
3. Kafka outputs: produce with `transactional.id = TRANSACTIONAL_ID` and commit input offsets in the same transaction (`send_offsets_to_transaction`, then `commit_transaction`). Output read with `isolation.level=read_committed` must contain no duplicates, including after a SIGKILL and restart.
4. Partitioning: key outputs as declared in `topics.yaml` and use `partitioner=murmur2_random`, so every key lands on the partition the Java client would pick.
5. Parquet output: write under `OUTPUT_PATH`, partitioned by `date`, with the `x-parquet-type` column types. A restart must not produce duplicate `(vin, window_start)` rows.
6. Output values must match the schema and the legacy job exactly. Fields are the declared schema fields, no more and no fewer.
7. Any Kafka topic the job creates must start with `STATE_TOPIC_PREFIX`. The job writes nowhere else.
8. Exit 0 once committed offsets reach the end offsets recorded at startup.
9. Failpoint (test only): when `TELEMETRY_FAILPOINT_AT_OFFSETS=N` is set, in the first transaction whose committed input offsets would reach `N`, produce the outputs and state records, call `send_offsets_to_transaction`, flush the producer, then SIGKILL the process before `commit_transaction`. Unset means no failpoint. The harness runs the image with an init process (`init: true`), so the job is not PID 1 and its own SIGKILL terminates it. It treats exit code 137 (SIGKILL) as the kill and restarts the job without the variable. Any other exit is a harness error.

The harness checks completion itself through the consumer group's committed offsets; it does not trust the exit code alone.

## What the harness checks

| Check | Rule |
|---|---|
| A | Effective output equals legacy: `session_id`, `alert_id`, last record per `vin\|campaign_id`, or `(vin, window_start)`. Numbers compare as decimals, timestamps as exact strings. |
| B | No repeated business key in the read_committed output. |
| C | The event-time field never decreases in offset order within a partition, per VIN (per key for OTA). |
| D | Every key is on the same partition as in the legacy output. |
| E1 | Field set equals the schema and legacy. |
| E2 | Location fields have at most `x-precision` decimals and the same maximum as legacy. |
| E3 | No topics or S3 prefixes other than the declared output and `STATE_TOPIC_PREFIX*`. Sensitive fields found in state topics are reported. |
| E4 | With `--manifest`, declared inputs, outputs, state stores and sensitive fields equal what was observed. |
| F | With `--chaos`: mode, kill or crash point, restart time, aborted-transaction records (read_uncommitted minus read_committed) per topic for the output topic and every topic under `STATE_TOPIC_PREFIX`, duplicates after restart. |

## Privacy manifest (optional, check E4)

```yaml
inputs: [vehicle.charging.events.v1]
outputs: [rust.charging.sessions.v1]
state_stores: [rust.charging-session-builder.state.sessions]
sensitive_fields:
  rust.charging.sessions.v1: [charger_lat, charger_lon, session_id, vin]
  rust.charging-session-builder.state.sessions: [vin]
```
