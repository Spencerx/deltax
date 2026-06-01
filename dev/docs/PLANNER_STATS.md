# Planner statistics for compressed partitions — roadmap & testing

pg_deltax compresses each partition's heap to (near) empty and stores the data
in companion blob tables, so PostgreSQL's own `ANALYZE` can't sample it. We
therefore **synthesize `pg_statistic` by hand** from the per-segment / per-
partition metadata we already keep (min/max, HLL sketches, value maps, null
counts). `src/stats.rs` is the implementation; `dev/docs/RTABENCH_QUERY_ANALYSIS.md`
§2–4 has the original motivation and the per-query wins.

## Why this is delicate: wrong stats are worse than no stats

With **no** stats the planner is conservative (defaults, no histograms) and
tends to pick safe-but-slow plans. With **confidently wrong** stats it commits
hard to a bad plan. Every regression we hit during this work was wrong/missing
stats, not absent stats:

| Symptom | Root cause | Impact |
|---|---|---|
| Q19 871 s | `event_type='Shipped'` (absent value) estimated `1/ndistinct` not ~0 | NestLoop over a 224K Materialize, ×9255 |
| Q06 17.6 s | parent `nullfrac` hardcoded 0 → `<> ''` on a 98%-NULL col estimated ~100% | per-partition Append + 3.6M-row Sort instead of parent DeltaXAppend Top-N |
| Q30 254 s | `event_created` range estimated `rows=8` vs 4.2M (no histogram) | NestLoop-over-Materialize join flip |
| Q17 6.3 s | parent `order_id` n_distinct too low (MAX heuristic) → join card 45M vs 6.3M | hash-join a 105M build instead of nested loop |
| histogram silently ignored | `stacoll` written as NULL (`Oid::into_datum(InvalidOid)`) | range estimates never improved |
| histogram neutralised | `stanullfrac=1.0` on the order-by col (its `_nonnull_count` read 0) | range selectivity `(1−nullfrac)` → 0 |
| n_distinct = 264 (9 real) | standalone analyze SUMmed per-segment ndistinct | equality selectivity wrong |
| MCV n_distinct = 12 (634 real) | MCV written from an incomplete valmap union | high-card col looked low-card → bad Top-N |

The lesson: **any new stat we write needs a way to verify it matches reality
before it ships**, because a benchmark only catches it if a query happens to
exercise the bad estimate, and only catastrophically-bad plans are obvious by
eye. See "Testing strategy" below.

## Current state — what we fill

Per compressed partition (`stainherit=false`) and once on the parent
(`stainherit=true`, merged across partitions):

| Field / kind | Written? | Source | Notes |
|---|---|---|---|
| `stanullfrac` | ✅ | child: `_colstats._nonnull_count`; parent: row-count-weighted avg of children | NOT NULL cols forced to 0 |
| `stawidth` | ✅ | `pg_attribute.attlen` | varlena → flat 32 |
| `stadistinct` | ✅ | child: merged-HLL / valmap count; parent: merged-HLL | |
| MCV (`stakind=1`) | ✅ (partial) | `column_valmap` for low-card cols | **uniform** freqs (no real counts); only when valmap covers all distinct |
| Histogram (`stakind=2`) | ✅ (partial) | per-partition `column_minmax` (parent: per-partition mins) | int/date/timestamp only; child is 2-point |
| `reltuples` / `relpages` | ✅ | `deltax_partition.row_count` | |

## Backlog — gaps, prioritized

Each item: what, why it matters, effort, whether it needs a data **reload**
(stats computed only at compress time can't be back-filled).

### P1 — Real MCV frequencies  (effort: M, reload: yes)
MCV frequencies are uniform (`1/ndistinct`). Correct for absent values (~0) but
flat for present values: `event_type='Delivered'` estimates 11% vs 4.6% actual;
`'Approved'` 11% vs 41%. Fix: count low-cardinality values at compress time
(cheap — we have the rows) and persist per-value counts alongside `column_valmap`;
write `most_common_freqs` from them. **Most likely to recover the residual Q17
overestimate** and tighten every skewed-enum filter. Highest value.

### P2 — MCV *and* histogram on the same column  (effort: S, reload: no)
We write at most one slot-1 stat (MCV **or** histogram). Real ANALYZE writes MCV
(hot values) + histogram (the tail) + correlation. A column with a few hot
values and a long tail loses one or the other. Let a column carry an MCV in
slot 1 and a histogram in slot 2 when both apply.

### P3 — Finer child histograms  (effort: M, reload: no)
Child histograms are 2-point `[min,max]` (one bucket → uniform assumption). We
have per-segment min/max to build a multi-bucket child histogram (as the parent
already does). Improves range selectivity *within* a partition for skewed
distributions. Lower value — partition pruning + the parent histogram cover most
range queries.

### P4 — `STATISTIC_KIND_CORRELATION` (kind 3)  (effort: S, reload: no)
Never written. PG uses it for ordered/index-scan costing. The leading order-by
column (`order_id`) is physically ~perfectly correlated; we could assert ~1.0.
Marginal here because scans go through custom nodes, not btree index scans —
but could nudge MergeAppend / ordered-path costs.

### P5 — Float histograms  (effort: S, reload: no)
`FLOAT4/8` are excluded from histogram eligibility (decode→Datum precision
hassle), so `satisfaction` (real) gets only `stadistinct`. Low value — the
satisfaction queries hit the metadata fast path.

### P6 — Extended statistics (`pg_statistic_ext`)  (effort: L, reload: n/a)
Multi-column n_distinct + functional dependencies (e.g. `state` ⇒ `country`).
Mostly relevant to the plain dimension tables (`customers`/`products`), which
PG's own `CREATE STATISTICS`/ANALYZE already handle — not pg_deltax's columns.
Little value for `order_events`.

### P7 — Array / range / JSONB element stats (kinds 4–7)  (effort: L, reload: yes)
`event_payload` is JSONB; PG does little with JSONB element stats anyway.
Negligible.

Suggested order: **P1 → P2 → P3 → (P4/P5 if a query points at them)**. P6/P7
are unlikely to be worth it.

## Testing strategy

The disasters above were ultimately caught by a full RTABench/ClickBench run —
slow, manual, EC2-bound, and only loud when a plan is *catastrophically* bad. We
need cheaper, automated checks that catch a wrong stat *value* before it ever
reaches a benchmark. Five layers, cheapest first:

### L1 · Unit tests (pure functions) — have, keep expanding
`histogram_eligible`, `merge_ndistinct`, `parent_histogram_bounds`, the HLL
serialize/merge round-trip, etc. Every new stat-computation helper gets one.
Catches encoding/merge logic bugs. Fast, no PG.

### L2 · Stat-value validation (the highest-leverage new investment)
A `#[pg_test]` (or python integration test) that:
1. builds a small synthetic table with a **known** distribution (controlled
   null fraction, skewed low-card column with a value that never appears, an
   ordered column, a high-cardinality column),
2. compresses it through pg_deltax,
3. reads back `pg_statistic` and asserts each field is within tolerance of the
   ground truth computed directly from the data:
   - `stanullfrac` ≈ true null fraction (would have caught Q06's 0 / the 1.0 bug),
   - `stadistinct` ≈ true distinct (would have caught 264-vs-9 and 12-vs-634),
   - MCV `stavalues` == the true distinct set, `stacoll`/`staop` non-null & correct
     (would have caught the NULL-`stacoll` and incomplete-valmap bugs),
   - histogram bounds bracket the true min/max and are strictly ascending.

This directly targets the failure mode "a stat is written but wrong," which is
where almost every bug lived. It's the layer we were missing.

### L3 · Estimate-accuracy assertions
For a set of representative predicates on a known table, compare the planner's
estimated rows (`EXPLAIN`) to the actual count, asserting the ratio is bounded
(say within 10×), with special cases:
- **absent value ⇒ estimate ≈ 0** (would have caught Q19's 20M),
- **range fully inside a partition ⇒ ~all rows** (would have caught the rows=8 collapse).
Run on a small dataset in integration tests; also worth a `make … query`-style
spot check.

### L4 · Plain-PG oracle (free, in the local RTABench harness)
`make bench-rtabench` already loads the *same* data into both `order_events`
(pg_deltax) and `order_events_plain` (plain PostgreSQL, properly ANALYZEd) and
asserts **result-set equality**. Extend it to also compare **row estimates**:
plain PG's ANALYZE is the oracle — for each query's scan/filter, assert
pg_deltax's `EXPLAIN` estimate is within Nx of plain PG's. This would have
flagged Q19 (plain ≈ 0 vs deltax 20M) and Q06 (plain 2% vs deltax 100%) on a
sub-GB local run, no EC2 needed.

### L5 · Disaster guard in the benchmark (cheap backstop)
The bench harness should **fail loudly** instead of relying on a human noticing
a 17-second query. Add, per query, either a wall-clock ceiling (e.g. flag any
query whose warm time regressed >3× vs the recorded baseline) or an
estimate-vs-actual ratio check from `EXPLAIN ANALYZE`. Converts "the total
looks a bit worse" into a named failing query.

**Priority:** L2 (stat-value validation) and L4 (plain-PG oracle) give the most
coverage for the least cost and should land alongside the next stats change
(P1). L1 is ongoing; L3/L5 are incremental hardening.

## Operational notes (carried over)
- Stats auto-populate on load (COPY/backfill end + background-worker compaction);
  `deltax_analyze_table` is a manual refresh. See RTABENCH_QUERY_ANALYSIS.md §4.3.
- Stats computed only at compress time (HLL n_distinct, real MCV counts once P1
  lands) need a reload to back-fill; histogram/MCV-from-valmap and nullfrac are
  reload-free (re-run `deltax_analyze_table`).
- Don't run a plain `ANALYZE <deltax table>` — it samples the empty compressed
  heaps and clobbers the synthesized inheritance-tree stats.
