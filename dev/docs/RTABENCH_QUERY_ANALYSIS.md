# RTABench join-query planner work

Current-state analysis of pg_deltax's planner behaviour on the join-heavy
RTABench queries, and the estimator work to fix it. (The long historical
progress log was dropped on 2026-05-31; this is the fresh start.)

Benchmark box: EC2 `c6a.4xlarge`, PG18, 181M `order_events`, `order_items`
105M, `orders` 10M, `customers` 1,102, `products` 9,255. `order_events` is
pg_deltax-managed: `order_by = [order_id, event_created]`, no segment-by,
30K-row segments, 3-day partitions.

## 1 · Harness fixes (done, committed)

Two RTABench-harness handicaps were removed (commits `2661f0a`, `7b6b9fd`):

- **`SET enable_nestloop = off` removed.** A pre-parallel-era guard against
  a mis-estimated NestLoop blow-up. With today's parallel-safe
  `DeltaXAppend` + PK-index probes into the dimension tables, nested loops
  are the *best* plan for the join queries; forcing them off cost 2–11×.
- **`work_mem` 8GB → 50MB.** Only ever load-bearing for the old 105M-row
  hash builds; the nested-loop plans use negligible memory. 50MB matches
  the TimescaleDB harness exactly (apples-to-apples).

Result (full benchmark, lukewarm-cold, 3×/query): suite **21.6 s → 12.7 s**;
vs TimescaleDB 29.3 s → pg_deltax **2.3× faster** overall. Only Q03
(tiny EXISTS) regressed (+91 ms). `run.sh` drops OS caches per query, so
these numbers are ~2× the fully-warm isolated times.

## 2 · The estimator gap (issues #20 + #21)

`DeltaXAppend` row estimates on compressed partitions are badly low, which
mis-drives join planning. Canonical case:

```
event_type='Delivered' AND event_created IN [2024-01-01, 2024-07-01)
  estimated rows = 8        actual = 4,207,526
```

Two independent causes:

1. **No histogram for `event_created`** (the dominant error). `src/stats.rs`
   writes only `stadistinct`/`stanullfrac`/`stawidth`, so range-predicate
   selectivity falls back to PG's default and collapses — even for
   partitions fully inside the WHERE range, where the true selectivity is
   1.0. → **issue #20.**
2. **`event_type` n_distinct was wrong** via the standalone-analyze path:
   `analyze_partition_from_catalog` SUMmed per-segment `_colstats._ndistinct`
   → 264 for a 9-value column. → **issue #21.**

### Why this is dangerous, not just slow — the Q30 NestLoop trap

With nested loops on (§1), a `rows=8` estimate doesn't just cost a little;
it flips the join order catastrophically. Q30 with stats present
(n_distinct fix, no histogram):

```
Nested Loop  (Join Filter: c.customer_id = o.customer_id)
  -> Index Scan customers c            (1,102 rows)            ← outer
  -> Materialize                       (est 61, actual 4.2M)  ← inner, re-scanned 1,102×
       -> Gather -> Nested Loop
            -> Parallel DeltaXAppend order_events  rows=8 (actual 4.2M)
            -> Index Scan orders_pkey
```

1,102 × 4.2M ≈ 4.6B comparisons → **254 seconds** (vs 1.7 s healthy).

**Key lesson:** the no-stats baseline (12.7 s) is only safe by luck — its
`rows=97` estimate stays just high enough to avoid the flip. Writing
accurate n_distinct *without* the histogram pushes the estimate to `rows=8`
and triggers the trap. So **#21 must not be enabled without #20** — they
ship together. The histogram is the load-bearing fix.

## 3 · #21 — n_distinct fix (done)

`analyze_partition_from_catalog` reads the authoritative merged-HLL counts
persisted at compression time in `deltax.deltax_partition.column_ndistinct`
(keyed by column name), instead of re-deriving a SUM from `_colstats`.
Columns missing from the persisted map are left for PG to default. Validated:
`event_type` n_distinct **264 → 9**; `event_type='Delivered'` estimate
**475K → 20M** (actual 8.4M).

## 4 · #20 — histograms + parent stats (done; one follow-up)

Implemented in `src/stats.rs`:

- **Per-column histograms** (`stakind=2`) for order-by / time columns whose
  min/max is an order-preserving i64 (INT2/4/8, DATE, TIMESTAMP, TIMESTAMPTZ),
  built from the persisted `column_minmax`. `pg_statistic.stavalues` is an
  `anyarray` pseudo-type column that **cannot** be populated via SQL INSERT,
  so the tuple is formed in C (`form_and_insert_pg_statistic` →
  `heap_form_tuple` + `CatalogTupleInsert`), like PG's own ANALYZE.
- **Parent-relation stats** (`write_table_stats`, `stainherit=true`): the
  partitions are scanned through one `DeltaXAppend`, so the planner reads
  join/range selectivity from the *parent's* pg_statistic. Per-partition
  stats are merged onto the parent — n_distinct via a disjoint/overlap
  heuristic (`merge_ndistinct`), plus a multi-bucket histogram from the
  sorted per-partition minimums.

**Three bugs found and fixed during on-EC2 validation:**
1. `anyarray` rejects SQL INSERT of a concrete array → form the tuple in C.
2. `pgrx`'s `Oid::into_datum()` maps `InvalidOid`→SQL NULL, so `stacoll1`
   became NULL and PG silently ignored the histogram → build the 0 Datum
   directly (same guard for the int2/float4 catalog columns).
3. **`stanullfrac = 1.0` on `event_created`** — the order-by/time column's
   `_colstats._nonnull_count` reads 0, and `(rows−0)/rows = 1.0` told PG the
   column was all-NULL, zeroing its range selectivity `(1−nullfrac)` and
   neutralising the histogram → fixed via `attnotnull` + guarding the 0 case.

**Results (EC2, full suite vs the no-stats 12.7 s baseline):** Q30 254s-trap
**eliminated** (1.36 s); Q3 −49%, Q4 −24%, Q25 −45%, Q27 −52%, Q30 −21%.
`clippy` clean, 547 unit tests pass.

### 4.1 · Q17 — the one regression, needs a global HLL (follow-up)

`top_selling_month_product` went 928 ms → 6.3 s. Forcing the nested loop
runs it in **710 ms** (9× faster than the hash join the planner now picks),
so it's a pure plan-choice problem, not spill (`work_mem=8GB` is *worse*).

Root cause: the `oe ⋈ order_items` join cardinality is overestimated (45M
vs 6.3M actual). `card = rows1·rows2 / max(nd1, nd2)`, and **both** order_id
n_distincts are too low: `order_items.order_id` = 458 808 (PG ANALYZE
underestimate; true ~10M), and the merged parent `oe.order_id` = 19 767
because `merge_ndistinct` took MAX — order_id's per-partition ranges all
overlap `[1, large]`, so the min/max overlap heuristic can't see that the
*values* are disjoint sets across time partitions (additive). Previously NL
won only because `oe` was drastically *under*-estimated (accidental); accurate
stats removed that accident.

The fix is a **global HyperLogLog**: accumulate per-segment HLL sketches into
a table-level sketch at load time (both the compress and COPY/backfill paths
currently discard them), persist it, and read it in `write_table_stats` for an
accurate parent `order_id` distinct (~10M) → `max(10M, 458K)=10M` → correct
join card → nested loop. Requires a data reload to populate (HLL can't be
rebuilt from the stored estimates). Tracked as a follow-up to this commit.
