# telemetry-parity

Mock project for a demo. All data is synthetic.

Replays a data-contracts fixture through a legacy Spark job and its Rust port, then checks that the effective outputs match, the Rust job writes no duplicates even when killed mid-run, ordering and partition placement are preserved, and the privacy footprint is unchanged. The contract a Rust job must follow is in [docs/RUST_JOB_CONTRACT.md](docs/RUST_JOB_CONTRACT.md).

## Setup

```
scripts/fetch-deps.sh                      # clones the commits pinned in deps.lock into .deps/
python3.11 -m venv .venv && .venv/bin/pip install -r requirements.txt
```

Needs Docker with compose. The first run builds the spark-runner image from the pinned telemetry-spark-jobs commit.

## Usage

```
./run.sh --job <name> --fixture sample|full [--legacy-only] [--chaos] [--chaos-at 0.4]
         [--self-test <fault>] [--determinism] [--manifest privacy-manifest.yaml] [--legacy-from-golden]
```

- `--legacy-only` runs the Spark job and writes `golden/<fixture>/<job>/`.
- `--determinism` runs the legacy job twice from a clean slate and compares the effective output.
- `RUST_IMAGE=telemetry-rs/<job>:<tag> ./run.sh --job <job> --fixture sample --chaos` compares a Rust port.
- `--self-test <fault>` uses `harness/mirror`, a Python stand-in that follows the Rust contract, to prove each check can fail: `none`, `duplicate`, `precision`, `partitioner`, `extra-field`, `new-sink`, `reorder`.
- `--legacy-from-golden` uses the committed golden as the legacy side instead of running Spark.

Each run writes `runs/<run_id>/parity-report.md` and `parity-report.json`. Exit code: 0 all checks pass, 1 a check failed, 2 harness error.

## Tests

```
.venv/bin/python -m pytest -q harness/tests -m "not e2e"   # unit tests
.venv/bin/python -m pytest -q harness/tests -m e2e         # self-test matrix, needs Docker
```
