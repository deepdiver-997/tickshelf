#!/usr/bin/env bash
# Optional external JSON mock -> isolated four-service stack -> SQLite.
# No shared Redis, fixed work directory, or fabricated freshness metric.
# Default: 54 symbols x 10 batches. Stress: --suite stress.
# BRIDGE_BIN / BUILD remain supported. Missing prerequisites fail, not PASS/SKIP.
# Legacy positional seconds now set the source timeout, not a throughput divisor.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
if [[ $# -gt 0 && "$1" != -* ]]; then
  TIMEOUT="$1"
  shift
  exec python3 "$ROOT/scripts/ifind_sim_bench.py" --timeout "$TIMEOUT" "$@"
fi
exec python3 "$ROOT/scripts/ifind_sim_bench.py" "$@"
