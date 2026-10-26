#!/usr/bin/env bash
# Clones the commits pinned in deps.lock into .deps/. In CI the clone authenticates with CI_JOB_TOKEN.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"
# shellcheck disable=SC1091
source deps.lock

if [[ -n "${CI_JOB_TOKEN:-}" ]]; then
  base="${GIT_BASE:-https://gitlab-ci-token:${CI_JOB_TOKEN}@gitlab.com/congition-demo-group}"
else
  base="${GIT_BASE:-https://gitlab.com/congition-demo-group}"
fi

fetch() {
  local name="$1" sha="$2" dir=".deps/$1"
  [[ -z "$sha" ]] && return 0
  if [[ -d "$dir/.git" ]] && [[ "$(git -C "$dir" rev-parse HEAD)" == "$sha" ]]; then
    echo "$name@$sha (cached)"
    return 0
  fi
  rm -rf "$dir"
  mkdir -p .deps
  git clone -q "$base/$name.git" "$dir"
  git -C "$dir" checkout -q "$sha"
  echo "$name@$sha"
}

fetch data-contracts "$DATA_CONTRACTS_SHA"
fetch telemetry-spark-jobs "$TELEMETRY_SPARK_JOBS_SHA"
fetch telemetry-rs "${TELEMETRY_RS_SHA:-}"
