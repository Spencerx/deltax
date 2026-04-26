# RTABench: pg_deltax vs TimescaleDB plan comparison

Side-by-side EXPLAIN (ANALYZE, BUFFERS) of all 31 RTABench queries on a single
EC2 c6a.4xlarge running both clusters concurrently:

- pg_deltax: PG18, port 5432
- TimescaleDB 2.18.1: PG16, port 5433 (same data, same hardware, same chunk
  layout — 3-day chunks, `order_id, event_created` orderby)

Plans are in `rtabench/plans/{deltax,timescale}/`. Reproduce with
`bash rtabench/run-explain-both.sh` on the EC2 box.

## Results summary

ratio = pg_deltax_ms / timescale_ms (>1 means pg_deltax slower)

```
query                                        deltax_ms   ts_ms     ratio
0000_terminal_hourly_stats                   498.0       326.1     1.53
0001_count_orders_from_terminal              253.9       225.0     1.13
0002_global_agg                              5.8         107.0     0.05  ★
0003_exists_order_delivered_from_terminal    236.0       124.0     1.90
0004_count_delayed_orders_per_day            630.0       725.7     0.87
0005_search_events_for_processor             146.8       81.8      1.80
0006_order_events_without_backups            34.5        23.2      1.49
0007_last_order_event_for_order              31.2        3.9       8.03
0008_most_week_delayed_order                 311.4       529.1     0.59
0009_departed_orders_count                   16.3        0.6       25.86
0010_last_event_for_an_order                 20.6        1.9       11.09
0011_events_for_an_order                     9.1         0.3       26.55
0012_max_satisfaction_for_order_per_day      33.1        1.0       34.78
0013_satisfaction_with_without_backup        31.3        0.9       35.03
0014_sum_prod_stock_price_per_category       1.7         3.0       0.57
0015_exists_order_delivered_for_customer     27.5        6.8       4.04
0016_customers_with_most_orders              255.7       484.1     0.53
0017_top_selling_month_product               3267.2      4887.4    0.67
0018_customer_month_value                    13862.9     2996.3    4.63  ⚠
0019_out_of_stock_products                   1279.7      1306.4    0.98
0020_customers_outstanding                   1312.2      616.9     2.13
0021_sales_volume_by_country                 2434.6      2986.0    0.82
0022_sales_volume_by_country_state           1108.1      1236.3    0.90
0023_top_sales_volume_product_from_terminal  13035.2     5894.2    2.21  ⚠
0024_top_customer_by_revenue                 303.9       494.2     0.61
0025_product_category_performance            181.6       1955.4    0.09  ★
0026_average_order_value                     650.5       890.1     0.73
0027_country_category_performance            378.9       423.1     0.90
0028_sales_volume_by_age_group               480.9       794.2     0.61
0029_top_product_in_age_group                349.2       827.5     0.42
0030_customers_with_most_orders_delivered    408787.8    3058.9    133.64 🔥
```

## Three patterns explain almost all losses

### 1. DeltaXAppend row underestimate → NestedLoop disaster (Q0030, Q0023, Q0003)

Q0030 is the biggest offender — **134x slower (408s vs 3s)**. It's a 3-way
join: `customers ⨝ orders ⨝ order_events` with a filter on
`event_created` + `event_type = 'Delivered'`. TimescaleDB plans a hash join
(7s of work, parallelized 6-way → 3s). pg_deltax picks a Nested Loop with
the customers (1102 rows) on the outside and a `Materialize`d 4.2M-row
DeltaXAppend on the inside, then probes `orders_pkey` 4.2M times:

```
Nested Loop (cost=... rows=61) (actual rows=4_207_526)
  Rows Removed by Join Filter: 4_632_486_126   ← 4.6 BILLION
  -> Index Scan customers_pkey (rows=1102)
  -> Materialize (rows=4_207_526)
       -> Gather
            -> Parallel Custom Scan (DeltaXAppend) on order_events
                 (cost=... rows=8 width=4)            ← estimate
                 (actual rows=467_502.89 loops=9)     ← reality
                 Filter: event_created in [Jan, Jul) AND event_type = 'Delivered'
```

DeltaXAppend reports `rows=8` to the planner. Reality is 4.2M rows. That's a
**~525,000x underestimate**. With 8 estimated rows on one side, a Nested Loop
is "cheap" — except the side you join against is actually 4.2M rows, and the
Materialize blows up to 197 MB.

Q0023 has the same shape on a smaller scale: estimate 8, actual ~65K rows
out of DeltaXAppend → Nested Loop with order_items.pkey lookups → 2.2x
slower than TimescaleDB's hash-join-everything-then-aggregate plan.

Q0003 is the same again: estimate 1, actual 11,380 → Nested Loop with
orders.pkey lookups (this one's only 1.9x slower because the absolute work
is small).

**Why** — the DeltaXAppend custom scan is not exposing realistic row
estimates after its filter. The planner uses these estimates to size joins;
when they're orders of magnitude off, it picks the wrong join algorithm.
TimescaleDB's DecompressChunk reports per-chunk row counts that are roughly
right (they're based on actual chunk row counts, not heuristics), so the
planner consistently picks hash joins.

This is the highest-leverage fix on the board. Even a coarse, conservative
estimate (e.g. "expect 10% of total scanned rows after filter") would knock
Q0030 from 408s down to ~3s.

### 2. Point lookup by order_id with no chunk-level index (Q0007, Q0009-Q0013, Q0015)

Q0011 is canonical: `WHERE order_id = 512 AND event_created BETWEEN ... 24h`.

- TimescaleDB: 0.3 ms. The compressed chunk has a btree index over
  `(_ts_meta_min_1, _ts_meta_max_1, _ts_meta_min_2, _ts_meta_max_2)` —
  i.e. per-segment min/max for both order_id and event_created. The index
  pinpoints the one segment that could contain `order_id=512` in the time
  window. One segment fetched, 3 rows returned.
- pg_deltax: 9 ms. The chunk is selected by partition pruning, but within
  it pg_deltax scans all segments and filters by min/max metadata
  sequentially. `segments=N segments_skipped=M` shows skipping is working,
  but it's a linear pass over the segment metadata rather than an indexed
  one.

Absolute numbers are tiny (≤35 ms) but the multiplicative gap is huge
(8–35x). A btree (or any sub-linear lookup) over per-segment min/max would
close it.

### 3. Q0018 — pg_deltax cluster is just slower on a query that doesn't touch order_events

Q0018 only joins `customers / orders / order_items / products` — pg_deltax
isn't involved at all. Both clusters pick the same Hash-Join-everything
plan. But:

- pg_deltax cluster: `Parallel Seq Scan on order_items` — **11.8s** for
  11.7M rows (~1M rows/sec/worker)
- TimescaleDB cluster: `Parallel Seq Scan on order_items` — **0.86s** for
  15M rows (~17M rows/sec/worker)

Same hardware, same data shape, plain Postgres on both. The ~17x throughput
gap is a system-level difference, not a pg_deltax issue. Most likely
suspect: OS page cache state (TS data was loaded ~30 min before this run,
so it sits in the page cache; pg_deltax data is older and was evicted).
Buffer counts support this — TS shows `shared hit=224 read=567488` (almost
nothing in shared_buffers, lots from disk) yet runs fast, so the kernel
must be serving those reads from page cache.

Re-running Q0018 alone after a `make query EC2=... Q=18` warmup (or after
a `drop_caches`) would tell us. This shouldn't drive any pg_deltax
engineering decisions until that's verified.

## Where pg_deltax wins big (sanity check)

- **Q0002 — global aggregate** (0.05x, 20x faster). pg_deltax's vectorized
  evaluator over compressed segments dominates a global `count()`/`avg()`
  at 5.8ms vs TS 107ms (TS still has to materialize per-chunk and sum).
- **Q0025 — product category performance** (0.09x, 11x faster). Aggregation
  over the full event range with a join — pg_deltax's batch-eval pipeline
  shows its strength when the access pattern is "scan most of a wide,
  highly-compressed range and aggregate."
- **Q0029, Q0028, Q0024, Q0017, Q0016, Q0008** — full-range aggregations
  where pg_deltax's compression + vectorized batch path is faster than
  TS's per-chunk decompress + aggregate.

These are the exact workloads ClickBench-style benchmarks reward, and they
match where we already win that benchmark. RTABench's twist is the join
patterns in pattern (1) and the tight point-lookup patterns in (2) —
neither rewards raw scan throughput.

## ⚠ Q0030's "408s catastrophe" was an EXPLAIN ANALYZE artifact

The plan does emit `Rows Removed by Join Filter: 4,632,486,126` from a NL
over a `Materialize(4.2M rows)` × 1102 customers — a real ~3s query. With
plain `EXPLAIN ANALYZE`, every one of those 4.6 B comparisons gets
per-tuple `gettimeofday()` instrumentation, and the wall clock balloons
to 408 s. In a regular query (or `make bench`'s 3-run harness, no
EXPLAIN), Q0030 cold is 3.4 s and warm is 3.0 s — close to TimescaleDB's
3.1 s. **There is no real-world Q0030 disaster to fix.**

This was discovered after landing a row-estimator fix that traded ~0.7 s
on Q0030-warm for +6.5 s of regressions on Q0017 / Q0023 / Q0025 / Q0003
in the actual `make bench` warm totals. The fix was reverted.

To avoid this trap going forward, the EXPLAIN harness in
`rtabench/run-explain-both.sh` and `make query` / `make query-both` now
uses `EXPLAIN (ANALYZE, BUFFERS, TIMING OFF)` — disables the per-tuple
timer so `Execution Time:` reflects real wall-clock work, at the cost of
losing per-node `actual time=` breakdowns. Plan shape, row counts, and
buffer hits are all preserved, which is what we actually use for
identifying optimization opportunities.

## Recommended fixes, ranked by impact

1. **Indexed (or hashed) segment skipping by order_id** — fixes the
   Q0007/Q0009-Q0013/Q0015 cluster of 8–35× ratios. TS has a btree over
   `(_ts_meta_min_1, _ts_meta_max_1, _ts_meta_min_2, _ts_meta_max_2)`
   that pinpoints one segment in microseconds for queries like
   `WHERE order_id = N AND ts BETWEEN ...`. pg_deltax linearly scans
   segment metadata. Absolute gap is small (≤ 35 ms) but the
   multiplicative gap is huge.

2. **Selectivity for skewed equality (Q0017, Q0023)** — `event_type =
   'Delivered'` matches 0.13% of rows in this dataset but our ndistinct
   says 1/5 = 20%. Without MCV histograms, we have no way to know.
   Adding column-level frequency stats (cheap to collect at compression
   time — top-N most common values) would let us return realistic
   selectivities without an over-estimate cap.

3. **Verify Q0018 reproduces after cache warmup** — Q0018 is plain-PG
   tables; the gap is OS page-cache state.
2. **Indexed (or hashed) segment skipping by order_id** — fixes the
   Q0007/Q0009-Q0013/Q0015 cluster. Bloom filters help only with
   high-cardinality equality; for skipping, a btree over per-segment
   `(min_order_id, max_order_id, min_ts, max_ts)` mirrors what TS does and
   wins these point-lookup queries cleanly.
3. **Verify Q0018 reproduces after cache warmup** — if it doesn't, ignore.
   If it does, it's a pure-PG18 vs PG16 perf question and probably out of
   scope for pg_deltax.

## Reproducing

```bash
# Set up the side-by-side TimescaleDB cluster (one-time, ~15 min)
make -C rtabench setup-ts EC2=$(grep -oE '[0-9.]+' rtabench/.env)

# Re-run all 31 queries on both clusters, save plans + summary
ssh -i ~/.ssh/tsg.pem ubuntu@<ip> 'cd ~/rtabench && bash run-explain-both.sh'

# Pull plans locally
rsync -az -e "ssh -i ~/.ssh/tsg.pem" \
    ubuntu@<ip>:/tmp/rtabench_plans/ rtabench/plans/

# Spot-compare a single query
make -C rtabench query-both EC2=<ip> Q=30
```
