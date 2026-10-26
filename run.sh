#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
py="${PYTHON:-python3}"
[[ -x .venv/bin/python ]] && py=.venv/bin/python
exec "$py" -m harness "$@"
