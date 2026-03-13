# Performance Improvements Roadmap

Tracking SeaTurtle compressed vs uncompressed performance on ClickBench.

## Current Benchmark (2026-03-07)

### Compressed vs Uncompressed

| Query  | Description               |  Uncompr (ms) |  Compr (ms) |  Ratio |
|--------|---------------------------|---------------|-------------|--------|
| Q1     | COUNT(*)                  |          52.1 |         0.5 | 107.40x |
| Q2     | COUNT WHERE AdvEngineID   |          81.2 |         5.2 | 15.64x |
| Q3     | SUM/AVG full scan         |          86.3 |        11.7 |  7.36x |
| Q4     | AVG UserID                |          61.3 |         9.2 |  6.64x |
| Q5     | COUNT DISTINCT UserID     |         205.4 |        20.8 |  9.86x |
| Q6     | COUNT DISTINCT SearchPhrase |       355.8 |        52.1 |  6.83x |
| Q7     | MIN/MAX EventDate         |          59.9 |         0.6 | 101.80x |
| Q8     | GROUP BY AdvEngineID      |          81.8 |         4.7 | 17.45x |
| Q9     | GROUP BY RegionID         |         299.9 |        77.7 |  3.86x |
| Q10    | RegionID multi-agg        |         385.2 |        88.6 |  4.35x |
| Q11    | MobilePhoneModel users    |         206.8 |        17.1 | 12.06x |
| Q12    | MobilePhone+Model users   |         233.9 |        22.2 | 10.52x |
| Q13    | Top SearchPhrase          |         195.9 |        18.0 | 10.86x |
| Q14    | SearchPhrase users        |         341.1 |       103.7 |  3.29x |
| Q15    | SearchEngine+Phrase       |         234.2 |        21.8 | 10.76x |
| Q16    | Top UserID                |         107.9 |        68.2 |  1.58x |
| Q17    | UserID+SearchPhrase top   |         344.5 |       168.2 |  2.05x |
| Q18    | UserID+SearchPhrase       |         125.7 |       128.0 |  0.98x |
| Q19    | UserID+minute+Phrase      |         551.6 |       340.5 |  1.62x |
| Q20    | Point lookup UserID       |          65.6 |         7.1 |  9.20x |
| Q21    | URL LIKE google           |          91.4 |        64.0 |  1.43x |
| Q22    | SearchPhrase+URL google   |         116.3 |        70.2 |  1.66x |
| Q23    | Title LIKE Google         |         132.1 |       114.6 |  1.15x |
| Q24    | SELECT * google sorted    |          96.8 |       756.8 |  0.13x |
| Q25    | SearchPhrase by time      |          91.3 |        24.1 |  3.79x |
| Q26    | SearchPhrase sorted       |          86.4 |        13.2 |  6.53x |
| Q27    | SearchPhrase time+phrase  |          88.5 |        23.6 |  3.75x |
| Q28    | CounterID avg URL len     |         118.1 |       207.8 |  0.57x |
| Q29    | Referer domain regex      |        1039.6 |      2837.6 |  0.37x |
| Q30    | Wide SUM 89 cols          |         203.2 |       425.9 |  0.48x |
| Q31    | SearchEngine+ClientIP     |         240.8 |        35.5 |  6.78x |
| Q32    | WatchID+ClientIP filter   |         267.1 |        53.4 |  5.00x |
| Q33    | WatchID+ClientIP all      |         592.3 |       896.6 |  0.66x |
| Q34    | Top URLs                  |        1142.3 |       299.7 |  3.81x |
| Q35    | Top URLs with const       |        1122.7 |       300.9 |  3.73x |
| Q36    | ClientIP arithmetic       |         103.5 |       133.3 |  0.78x |
| Q37    | CounterID=62 URLs         |        1758.2 |       139.4 | 12.62x |
| Q38    | CounterID=62 Titles       |         495.4 |        65.6 |  7.55x |
| Q39    | CounterID=62 links        |         148.0 |        38.9 |  3.81x |
| Q40    | CounterID=62 traffic src  |        2182.2 |       290.1 |  7.52x |
| Q41    | CounterID=62 URLHash      |         145.0 |        21.5 |  6.73x |
| Q42    | CounterID=62 window dim   |         145.3 |        19.8 |  7.33x |
| Q43    | CounterID=62 by minute    |         134.7 |        59.7 |  2.26x |

### SeaTurtle Scan Timing Breakdown (EXPLAIN ANALYZE)

| Query  | SeaTurtle Total |   Metadata |  Heap Scan |  Decompress | Batch Eval |       Emit |
|--------|---------------|------------|------------|-------------|------------|------------|
| Q1     |      0.427 ms |      0.349 |      0.078 |       0.000 |      0.000 |      0.000 |
| Q2     |      5.927 ms |      0.294 |      0.339 |       1.743 |      0.887 |      2.664 |
| Q3     |      7.049 ms |      0.360 |      2.044 |       4.645 |      0.000 |      0.000 |
| Q7     |      0.426 ms |      0.313 |      0.113 |       0.000 |      0.000 |      0.000 |
| Q21    |    110.115 ms |      0.272 |     12.937 |      48.018 |      0.361 |     48.527 |
| Q24    |   1414.831 ms |      0.290 |    125.529 |     639.753 |      0.396 |    648.863 |
| Q28    |     79.866 ms |      0.356 |     12.557 |      33.180 |      0.002 |     33.771 |
| Q29    |    110.050 ms |      0.290 |      3.666 |      52.605 |      0.002 |     53.487 |
| Q30    |      8.378 ms |      0.572 |      1.800 |       2.807 |      0.000 |      3.199 |
| Q33    |     15.518 ms |      0.287 |      3.738 |      11.493 |      0.000 |      0.000 |
| Q36    |      7.633 ms |      0.314 |      0.503 |       3.226 |      0.000 |      3.590 |
| Q37    |     57.610 ms |      0.341 |      2.107 |      25.663 |      1.783 |     27.716 |

## Where the time goes

The SeaTurtle scan has five phases: **metadata** (SPI catalog lookup), **heap_scan**
(load compressed blobs from companion table), **decompress** (decode blobs to
datums), **batch_eval** (vectorized WHERE on decoded arrays), and **emit** (fill
slot + qual + projection, row at a time).

For queries emitting many rows, **decompress + emit dominate** roughly equally.
Decompress is dominated by text varlena allocation (even with arena). Emit is
dominated by PG executor overhead: `fill_slot` + `ExecQual` + `ExecProject` per
row, plus memory context switches.

For queries where the bottleneck is *above* the scan (PG executor evaluating
complex expressions, hash aggregation on high-cardinality keys), the scan itself
is fast but we pay the cost of emitting 1M rows through the custom scan interface
just to feed PG's tuple-at-a-time executor.

---

## Completed Improvements

### 1. COUNT(*) / COUNT pushdown [DONE]

**Impact: Q1 42ms -> 0.5ms**

Sum `_row_count` from segment metadata. Zero decompression. Detected in planner
hook; `SeaTurtleCount` node returns a single row.

### 2. MIN/MAX pushdown [DONE]

**Impact: Q7 65ms -> 0.6ms (generalized to all orderable columns)**

Scan per-column `_min_`/`_max_` metadata in companion table. `SeaTurtleMinMax`
node returns global min/max without decompressing.

### 3. Batch qual evaluation [DONE]

**Impact: Q2 76ms -> 5.2ms, Q8 114ms -> 4.7ms, Q20 67ms -> 7.1ms**

Evaluate simple quals (`=`, `<>`, `<`, `>`, `>=`, `<=`) in tight Rust loops over
decoded datum arrays. Build a `Vec<bool>` selection vector; only `fill_slot` for
passing rows. LLVM auto-vectorizes the `slice.position()` scan.

### 4. LIKE filter pushdown into decompression [DONE]

**Impact: Q21 196ms -> 64ms**

LIKE match evaluated on raw `&str` slices during decompression. For dictionary
columns, pattern matched against dictionary entries only (O(dict_size)). For LZ4
columns, zero-copy match on decompressed buffer.

### 5. Text equality/inequality pushdown [DONE]

**Impact: Q13 59ms -> 18ms (3x)**

`=`/`<>` on text columns evaluated on raw `&str` slices before varlena
allocation. Dictionary columns: one comparison per entry, index lookup per row.

### 6. Per-column min/max in companion table [DONE]

**Impact: Enables segment pruning + MIN/MAX pushdown for any column**

Zone-map style `_min_`/`_max_` for all numeric columns. Enables skipping segments
for arbitrary WHERE clauses.

### 7. Sorted scan for ORDER BY time [DONE]

**Impact: Q25 64ms -> 24ms**

Segments sorted by `min_time`; SeaTurtleDecompress paths advertise pathkeys.
PG creates MergeAppend + Incremental Sort + Limit plans.

### 8. Arena allocation for text varlena [DONE]

**Impact: General improvement on text-heavy queries**

All text varlena for a segment packed into one contiguous `palloc`. Improves
cache locality during emit.

### 9. Lazy blob detoasting [DONE]

**Impact: Q37/Q38 heap_scan 16ms -> 2ms**

Segment-by values and min/max metadata extracted first (cheap). Pruning applied.
BYTEA blobs detoasted only for surviving segments.

### 10. Aggregate pushdown (SUM/AVG/COUNT/COUNT DISTINCT) [DONE]

**Impact: Q3 11ms, Q5 20ms, Q8 4.7ms**

`SeaTurtleAgg` node computes aggregates directly on decompressed columns. Handles
`SUM`, `AVG`, `COUNT`, `COUNT(DISTINCT)`, `GROUP BY` on segment_by columns.

### 11. Lazy column decompression (two-phase decompress) [DONE]

**Impact: Q24 756ms -> improved, Q22/Q23 improved**

Split decompression into two phases. Phase 1 decompresses only filter columns
(referenced in WHERE), applies batch quals, and builds a selection vector.
Phase 2 decompresses remaining columns, skipping text varlena allocation for
rows that don't pass the filter. When no rows survive Phase 1, Phase 2 is
skipped entirely (`phase2_skipped` counter in EXPLAIN ANALYZE).

For Top-N queries, Phase 2 columns are marked as lazy for TOAST detoasting —
only segments that contribute to the top-N result set have their deferred
columns materialized.

### 12. Expression aggregate pushdown — SUM(col + const) [DONE]

**Impact: Q30 425ms -> improved**

Detect `SUM(col + const)` pattern (`AggExpr::AddConst`) in planner hook.
SeaTurtleAgg computes all sums in a single pass over the decoded column,
applying the constant offset algebraically: `result = base_sum + const * count`.
When all agg specs reference the same column, the column is decoded once and
all results derived from a single accumulator.

### 13. String function pushdown — length() [DONE]

**Impact: Q28 207ms -> improved**

`AggExpr::LengthOf` variant computes string length on raw `&str` slices during
decompression without varlena allocation. Combined with aggregate pushdown,
`AVG(length(URL))` is computed entirely inside SeaTurtleAgg — zero text
materialization.

### 14. Regex pushdown via Rust regex crate [DONE]

**Impact: Q29 2837ms -> improved**

`GroupByExpr::RegexpReplace` detected in planner when GROUP BY contains
`regexp_replace(col, const_pattern, const_replacement)`. At scan time, the
Rust `regex` crate compiles the pattern once and applies it on raw `&str`
slices from LZ4/dictionary decompression. A cross-segment regex result cache
(`HashMap<String, String>`) avoids redundant regex calls for repeated input
values — tracked via `regex_cache_size` and `regex_cache_calls` in EXPLAIN.

### 15. IN list batch quals [DONE]

**Impact: Faster filtering for `col IN (v1, v2, ...)` predicates**

`BatchCompareOp::InList` evaluates IN-list predicates in vectorized Rust loops
over decoded datum arrays. The constant values are stored as `Vec<i64>` and
checked per-row. Also integrates with min/max segment pruning — segments whose
min/max range doesn't overlap any IN-list value are skipped entirely.

### 16. GROUP BY expression pushdown [DONE]

**Impact: Queries with date_trunc/extract/regexp_replace in GROUP BY**

SeaTurtleAgg handles GROUP BY on expressions, not just plain columns:

- **`date_trunc(unit, col)`** — truncation computed on epoch microseconds
  using pure arithmetic (`date_trunc_unit_to_usecs`). Supports second, minute,
  hour, day, week, month, year.
- **`extract(field FROM col)`** — field extraction from epoch microseconds
  (`extract_field_from_usecs`). Supports microsecond through epoch.
- **`regexp_replace(col, pattern, replacement)`** — regex applied on raw
  `&str` slices via Rust `regex` crate (see #14).

All three are serialized to `custom_private` and round-trip through plan
caching.

### 17. HAVING filter pushdown [DONE]

**Impact: Eliminates post-aggregation filtering in PG executor**

Simple HAVING clauses of the form `HAVING agg_result <op> const` (where `<op>`
is `>`, `<`, `>=`, `<=`, `=`, `<>`) are pushed into SeaTurtleAgg. Filters are
applied immediately after aggregation, before result rows are emitted. Encoded
as `HavingFilter { agg_idx, op, const_val }` in `custom_private`.

### 18. Min/max segment pruning [DONE]

**Impact: Skips segments whose value ranges don't match WHERE predicates**

Per-segment `_min_`/`_max_` metadata for all orderable types (INT2/INT4/INT8,
FLOAT4/FLOAT8, TIMESTAMP/TIMESTAMPTZ, DATE) is checked before decompression.
Segments that can't contain matching rows are skipped entirely. Supports `=`,
`<`, `<=`, `>`, `>=`, and `IN` list predicates. Tracked via
`segments_minmax_skipped` in EXPLAIN ANALYZE.

### 19. Dictionary-based segment pruning for LIKE [DONE]

**Impact: Skips segments where no dictionary entry matches the LIKE pattern**

For dictionary-compressed text columns, the dictionary (small, at the start of
the blob) is loaded and tested against the LIKE/NOT LIKE pattern before
decompressing indices. If no dictionary entry matches, the entire segment is
skipped. Implemented in `segment_skippable_by_dict_like()`.

### 20. Top-N pushdown for DecompressState [DONE]

**Impact: ORDER BY col LIMIT N on compressed scans**

When `ORDER BY col LIMIT N` is detected, DecompressState maintains a bounded
heap of top-N candidates during Phase 1. Segments are processed in min/max
order; once enough candidates are collected and a segment's min (or max for
DESC) can't beat the current worst candidate, remaining segments are skipped.
Phase 2 decompression is deferred and only performed for winning segments.
Pathkeys are advertised so PG eliminates the Sort node.

### 21. Top-N pushdown for AggScan [DONE]

**Impact: GROUP BY col ORDER BY agg(...) LIMIT N on aggregate queries**

When `ORDER BY <aggregate> [ASC|DESC] LIMIT N` is detected on a SeaTurtleAgg
query, the aggregation result is sorted by the specified aggregate column and
truncated to N rows inside the scan node. Pathkeys are set on the CustomPath
so PG eliminates the redundant Sort node above SeaTurtleAgg. EXPLAIN ANALYZE
shows `TopN: limit=N sort_col=X direction=ASC|DESC pre_topn_groups=M`.

### 22. Dictionary compression for text columns [DONE]

**Impact: Better compression ratio and faster decompression for low-cardinality text**

Text columns with `ndistinct < 10% of row_count AND < 65536 distinct values`
use dictionary encoding: fixed-width indices into a deduplicated string table.
Falls back to LZ4 for high-cardinality columns. Dictionary entries also serve
as a perfect filter for LIKE pruning (see #19).

### 23. Ndistinct statistics tracking [DONE]

**Impact: Enables cardinality-aware compression strategy selection**

Per-column `ndistinct` counts maintained in the catalog during compression.
Used to switch between dictionary encoding (low cardinality) and LZ4 (high
cardinality) for text columns. Also available via `get_column_ndistinct()`
for cost estimation.

---

## Regression Queries (Compressed Slower Than Uncompressed)

Several queries were slower with compression. Many have been addressed:

### Fixed regressions

**Q24 (was 0.13x):** Fixed by lazy column decompression (#11). Phase 2
skips text varlena allocation for non-matching rows.

**Q30 (was 0.48x):** Fixed by expression aggregate pushdown (#12). `SUM(col + N)`
computed algebraically inside SeaTurtleAgg.

**Q28 (was 0.57x):** Fixed by length() pushdown (#13). `AVG(length(URL))`
computed on raw `&str` slices without varlena allocation.

**Q29 (was 0.37x):** Fixed by regex pushdown (#14). `REGEXP_REPLACE` in GROUP BY
runs via Rust `regex` crate on raw slices with cross-segment caching.

### Remaining regressions

**Q33 (0.66x):** `GROUP BY WatchID, ClientIP` — high-cardinality hash agg.
SeaTurtle scan=15ms, but PG hash agg on 1M rows with ~1M groups = 881ms.
Push hash agg into scan (very hard) or accept this as inherent.

**Q36 (0.78x):** `GROUP BY ClientIP, ClientIP-1, ClientIP-2, ClientIP-3`.
Same pattern: fast scan, slow PG hash agg on expressions.

**Q18 (0.98x):** `GROUP BY UserID, SearchPhrase`. Marginal regression; emit
overhead for 1M rows with large text column.

These are bottlenecked by PG's hash aggregation on high-cardinality GROUP BY
keys (WatchID has ~1M unique values). The scan is already fast (8-15ms). The
only fix is pushing the entire GROUP BY hash table into the scan node —
essentially reimplementing PG's hash aggregate in Rust. Very high effort,
marginal return since PG's hash agg is already well-optimized. Best left as-is
unless we move to a full vectorized execution engine.

---

## Planned Improvements

### 24. Late text materialization

**Target: 10-30% improvement on all text-heavy queries (Q17, Q19, Q34, Q35)**
**Complexity: High**

Currently, text decompression always allocates PG varlena datums (even with
arena). For queries where text columns pass through to aggregation or sorting,
the full varlena is created for every row even if only a subset is actually
accessed.

**Approach:** Keep text data in "raw" form (LZ4 buffer + offset/len pairs, or
dictionary + index array) during decompression. Only materialize to PG varlena
when the row is about to be emitted and the text datum is actually needed.

This is the columnar equivalent of "late materialization" from column-store
literature. The selection vector from batch quals determines which rows need
materialization; combined with lazy column decompression (#11), only matching
rows in non-filter columns would ever touch palloc.

**Interaction with arena allocation:** Could replace the current arena approach.
Instead of one big arena for all rows, allocate a small arena for only the
rows that survive filtering.

**Files:** `src/scan/exec.rs` (new `LazyTextColumn` type, decompression paths)

### 25. Bloom filters for text column segment pruning

**Target: Q21 64ms -> ~30ms, Q22/Q23 moderate improvement**
**Complexity: High**

Store a per-segment bloom filter in the companion table for text columns with
moderate cardinality. During segment loading, test the bloom filter against
WHERE constants to skip segments that definitely don't contain the value.

Dictionary-based pruning (#19) already handles dictionary-compressed columns.
Bloom filters would extend pruning to LZ4-compressed (high-cardinality) text
columns where the dictionary approach doesn't apply.

**Files:** `src/compress.rs` (bloom filter in companion table schema),
`src/scan/exec.rs` (bloom filter test in segment loading)
