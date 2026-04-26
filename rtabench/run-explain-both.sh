#!/bin/bash
# Two-phase per-query measurement:
#   1. Plain query with `\timing on` for wall-clock — no per-row instrumentation
#      overhead, this is the number you should trust for "how slow is the query."
#   2. EXPLAIN (ANALYZE, BUFFERS, TIMING OFF) for plan shape + actual row counts.
#      Per-row counter overhead can still inflate this on plans that filter
#      billions of rows (Q0030 NL+Materialize is ~20–60 sec just for nfiltered
#      counters), but plan_rows-vs-actual_rows is the main signal we use to
#      identify optimization opportunities.
#
# Both clusters are run side-by-side; per-query files contain wall-time first,
# then the plan. The summary uses wall-time, not EXPLAIN's Execution Time.
#
# Re-runnable: nukes /tmp/rtabench_plans/ on each invocation.

set -euo pipefail

OUT_DIR=/tmp/rtabench_plans
DELTAX_DIR=$OUT_DIR/deltax
TS_DIR=$OUT_DIR/timescale
SUMMARY=$OUT_DIR/summary.tsv
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
QUERIES_DIR="$SCRIPT_DIR/queries"

DELTAX_PSQL="sudo -u postgres psql -p 5432 test"
TS_PSQL="sudo -u postgres /usr/lib/postgresql/16/bin/psql -p 5433 test"

rm -rf "$OUT_DIR"
mkdir -p "$DELTAX_DIR" "$TS_DIR"

printf 'query\tdeltax_ms\tts_ms\tratio\n' > "$SUMMARY"

# Pull the first `Time: N.NNN ms` from a `\timing on` query run.
extract_wall_time() {
    grep -oE 'Time: [0-9.]+ ms' "$1" | head -1 | grep -oE '[0-9.]+' | head -1
}

run_one() {
    local label="$1"   # "pg_deltax (PG18, port 5432)"
    local psql="$2"    # "$DELTAX_PSQL"
    local f="$3"       # path to query .sql
    local out="$4"     # output file
    local sql
    sql=$(cat "$f")

    {
        echo "-- $(basename "$f" .sql) on $label"
        echo "-- $f"
        echo
        cat "$f"
        echo
        echo "----- WALL CLOCK (\\timing on, no instrumentation) -----"
    } > "$out"

    # Discard query rows (\o /dev/null) so we measure work, not output formatting.
    if ! $psql -X --no-psqlrc \
        -c "\\timing on" \
        -c "\\o /dev/null" \
        -c "$sql" \
        >> "$out" 2>&1; then
        echo "  $label: query FAILED" >&2
    fi

    {
        echo
        echo "----- EXPLAIN (ANALYZE, BUFFERS, TIMING OFF) -----"
    } >> "$out"
    if ! $psql -X --no-psqlrc -c "EXPLAIN (ANALYZE, BUFFERS, TIMING OFF) $sql" \
        >> "$out" 2>&1; then
        echo "  $label: explain FAILED" >&2
    fi
}

for f in "$QUERIES_DIR"/*.sql; do
    base=$(basename "$f" .sql)
    echo "=== $base ==="

    run_one "pg_deltax (PG18, port 5432)"   "$DELTAX_PSQL" "$f" "$DELTAX_DIR/$base.txt"
    run_one "TimescaleDB (PG16, port 5433)" "$TS_PSQL"     "$f" "$TS_DIR/$base.txt"

    dms=$(extract_wall_time "$DELTAX_DIR/$base.txt" || echo "")
    tms=$(extract_wall_time "$TS_DIR/$base.txt" || echo "")
    if [ -n "$dms" ] && [ -n "$tms" ] && [ "$tms" != "0" ]; then
        ratio=$(awk -v d="$dms" -v t="$tms" 'BEGIN { printf "%.2f", d/t }')
    else
        ratio=""
    fi
    printf '%s\t%s\t%s\t%s\n' "$base" "${dms:-?}" "${tms:-?}" "${ratio:-?}" \
        | tee -a "$SUMMARY"
done

echo
echo "=== Summary (deltax_ms / ts_ms / ratio) — wall clock ==="
column -t -s $'\t' "$SUMMARY"
echo
echo "Plans saved to $OUT_DIR"
