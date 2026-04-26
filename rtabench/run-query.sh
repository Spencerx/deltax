#!/bin/bash
# Wrapper invoked by the Makefile's `query` / `query-cold` / `query-ts` targets.
# Runs ONE query in two phases:
#   1. Plain query with `\timing on` — accurate wall clock, no instrumentation.
#   2. EXPLAIN (ANALYZE, BUFFERS, TIMING OFF) — plan + actual rows.
#
# Usage: run-query.sh <query-file> <psql command...>
#   e.g. run-query.sh queries/0030_*.sql sudo -u postgres psql test
#
# An optional GUC override can be passed via $SET, e.g. SET="SET work_mem='4GB'".

set -euo pipefail

QUERY_FILE="$1"
shift
PSQL=("$@")

SET_CMD="${SET:-}"
sql=$(cat "$QUERY_FILE")

echo "$QUERY_FILE"
echo '---'
cat "$QUERY_FILE"
echo '---'
echo '----- WALL CLOCK -----'
"${PSQL[@]}" -X --no-psqlrc \
    -c '\timing on' \
    -c "${SET_CMD:-SELECT 1}" \
    -c "$sql" 2>&1 \
  | grep -E '^Time:|^ERROR|^FATAL' || true
echo '----- PLAN -----'
"${PSQL[@]}" -X --no-psqlrc \
    -c "${SET_CMD:-SELECT 1}" \
    -c "EXPLAIN (ANALYZE, BUFFERS, TIMING OFF) $sql"
