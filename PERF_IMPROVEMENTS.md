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

---

## Regression Queries (Compressed Slower Than Uncompressed)

Seven queries are slower with compression. Root causes fall into three categories:

### Category A: Decompressing columns that aren't needed for filtering

**Q24 (0.13x):** `SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10`
- 95 rows match, but ALL ~100 columns decompressed for ALL 1M rows
- Decompress=640ms (all columns), Emit=649ms (all columns in slot)
- Fix: **Lazy column decompression** (improvement #11)

### Category B: PG executor overhead above the scan

**Q30 (0.48x):** `SUM(ResolutionWidth + 0..89)` — 89 SUM expressions
- SeaTurtle scan=8ms, but PG evaluates 89 exprs × 1M rows = 417ms overhead
- Fix: **Expression aggregate pushdown** (improvement #12)

**Q33 (0.66x):** `GROUP BY WatchID, ClientIP` — high-cardinality hash agg
- SeaTurtle scan=15ms, but PG hash agg on 1M rows with ~1M groups = 881ms
- Fix: Push hash agg into scan (very hard) or accept this as inherent

**Q36 (0.78x):** `GROUP BY ClientIP, ClientIP-1, ClientIP-2, ClientIP-3`
- Same pattern: fast scan, slow PG hash agg on expressions
- Fix: Same as Q33

**Q18 (0.98x):** `GROUP BY UserID, SearchPhrase`
- Marginal regression; emit overhead for 1M rows with large text column

### Category C: Expensive text operations on many rows

**Q28 (0.57x):** `AVG(length(URL)) GROUP BY CounterID`
- Full URL varlena allocated for 999K rows just to compute `length()`
- Fix: **length() pushdown** — compute on raw `&str`, return int (improvement #13)

**Q29 (0.37x):** `REGEXP_REPLACE(Referer, ...) GROUP BY`
- SeaTurtle scan=110ms (921K text rows), but PG REGEXP_REPLACE=2727ms
- PG's regex engine is slow; varlena allocation overhead on top
- Fix: **Regex pushdown** via Rust `regex` crate (improvement #14)

---

## Planned Improvements

### 11. Lazy column decompression (two-phase decompress)

**Target: Q24 756ms -> ~100ms (0.13x -> ~1x), also helps Q22/Q23**
**Complexity: Medium-High**

Currently all needed columns are decompressed for all rows before filtering
(`exec.rs:2977` loop). For Q24, that's ~100 columns × 1M rows, but only 95 rows
match the LIKE filter.

Split decompression into two phases:

1. **Phase 1 — Filter columns:** Decompress only columns referenced in WHERE
   (URL for Q24). Apply LIKE/batch quals. Build selection vector with ~95 true
   entries out of 1M.

2. **Phase 2 — Remaining columns:** Decompress non-filter columns, but only
   allocate datums for rows passing the selection vector.

For **pass-by-value types** (int, float, timestamp): full decode is cheap
(gorilla/varint are sequential), but skip `Datum` creation for non-matching rows
— minor saving.

For **text types** (the big win): decode LZ4/dictionary to get `&str` slices
(cheap), but only call `str_slices_to_text_datums_arena()` for the ~95 matching
rows. This eliminates the dominant cost: varlena allocation for ~100 text columns
× 1M rows.

**Implementation sketch:**
```rust
// Phase 1: decompress filter columns, build selection vector
let mut selection = vec![true; row_count];
for col_idx in filter_columns {
    decompress_column(col_idx, &seg);
    apply_batch_qual(col_idx, &mut selection);
}

// Phase 2: decompress remaining columns with selection
for col_idx in non_filter_columns {
    if is_text_type(col_idx) {
        decompress_text_with_selection(col_idx, &seg, &selection);
    } else {
        decompress_column(col_idx, &seg); // cheap, full decode ok
    }
}
```

**Expected impact on other queries:**
- Q22 (1.66x -> ~3x): URL+SearchPhrase decompressed for 1M rows, only 2 match
- Q23 (1.15x -> ~2x): Title+URL+SearchPhrase for 1M rows, only 217 match
- Q21 (1.43x -> ~2x): URL for 1M rows, 95 match (single column, smaller win)

**Files:** `src/scan/exec.rs` (decompression loop at line 2977)

### 12. Expression aggregate pushdown (SUM/AVG with arithmetic)

**Target: Q30 425ms -> ~10ms (0.48x -> ~20x)**
**Complexity: Medium**

Q30 computes 89 expressions of the form `SUM(ResolutionWidth + N)`. The
SeaTurtle scan emits 1M rows in 8ms, then PG spends 417ms evaluating 89
expressions per row through its tuple-at-a-time executor.

Extend `SeaTurtleAgg` to detect `SUM(col + const)` and `SUM(col * const)`
patterns. Inside the scan node, compute all sums in a single pass:

```rust
let values = decode_i32(blob);  // decode ResolutionWidth once
let mut sums = vec![0i64; 89];
for &v in &values {
    for i in 0..89 {
        sums[i] += v as i64 + i as i64;
    }
}
// Emit 1 row with 89 columns
```

This eliminates 89M expression evaluations in PG. The pattern detection happens
in `seaturtle_create_upper_paths` by walking the Aggregate target list for
`SUM(OpExpr(Var, Const))`.

More generally, this extends to any aggregate over a simple expression on a
single column. Patterns to detect:
- `SUM(col + const)`, `SUM(col * const)`, `SUM(col - const)`
- `AVG(col + const)` (sum + count, divide at end)
- Could extend to `SUM(col1 + col2)` later

**Files:** `src/scan/hook.rs` (upper path detection), `src/scan/exec.rs`
(aggregate computation in SeaTurtleAgg)

### 13. String function pushdown (length, lower, upper)

**Target: Q28 207ms -> ~50ms (0.57x -> ~2.5x)**
**Complexity: Medium**

Q28 is `SELECT CounterID, AVG(length(URL)) ... GROUP BY CounterID`. The scan
decompresses full URL text (LZ4, 33ms) and allocates varlena for 999K rows
(33ms emit), just so PG can call `length()` on each — which only needs the
byte count.

Push `length()` into the scan: during LZ4/dictionary decompression, we already
have `(offset, len)` ranges or `&str` slices. Emit `len as i32` as an integer
datum instead of the full text.

**Detection:** In the planner, look for `FuncExpr` wrapping a `Var` on a text
column. If the function is `length` (oid 1317/1318/1319), replace the text
column in the scan output with a synthetic integer column computed during
decompression.

**Combined with aggregate pushdown (#12):** If the full expression is
`AVG(length(URL))`, push the entire computation into SeaTurtleAgg. Compute
string lengths on raw `&str` slices → accumulate sum + count → emit single row.
Zero text varlena allocation.

**Generalization:** Same approach works for `lower()`, `upper()`, `substr()`,
`position()` — any function that can operate on `&str` slices and return a
simple value.

**Files:** `src/scan/hook.rs` (function detection), `src/scan/exec.rs`
(decompression with function application)

### 14. Regex pushdown via Rust regex crate

**Target: Q29 2837ms -> ~500ms (0.37x -> ~2x)**
**Complexity: High**

Q29 is `SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1')
AS k, AVG(length(Referer)), COUNT(*), MIN(Referer) FROM hits WHERE Referer <> ''
GROUP BY k ...`. The SeaTurtle scan emits 921K text rows in 110ms; PG's regex
engine spends ~2727ms on REGEXP_REPLACE.

Rust's `regex` crate is typically 5-10x faster than PG's regex engine and can
operate on raw `&str` slices without varlena allocation.

**Approach:** Detect `REGEXP_REPLACE(Var, const_pattern, const_replacement)` in
the target list during planning. At scan time:
1. Compile the regex once with `regex::Regex::new()`
2. During decompression, apply regex on raw `&str` slices from LZ4/dictionary
3. Emit the replacement string as a text datum (only for GROUP BY key)

**Combined with `length()` pushdown:** The `AVG(length(Referer))` can also be
computed on raw slices. And `MIN(Referer)` can use byte-level comparison on
slices. The entire query could potentially run without emitting any rows
through PG's executor.

**Adds dependency:** `regex` crate (widely used, no-std compatible).

**Files:** `src/scan/hook.rs` (detect REGEXP_REPLACE in target list),
`src/scan/exec.rs` (regex evaluation during decompression)

### 15. Late text materialization

**Target: 10-30% improvement on all text-heavy queries (Q17, Q19, Q29, Q34, Q35)**
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

### 16. Bloom filters for text column segment pruning

**Target: Q21 64ms -> ~30ms, Q22/Q23 moderate improvement**
**Complexity: High**

Store a per-segment bloom filter in the companion table for text columns with
moderate cardinality. During segment loading, test the bloom filter against
WHERE constants to skip segments that definitely don't contain the value.

For Q21 (`WHERE URL LIKE '%google%'`), a substring bloom filter could prune
segments where no URL contains "google". For dictionary-compressed columns,
the dictionary itself serves as a perfect filter — if the pattern doesn't
match any dictionary entry, the entire segment can be skipped.

**Quick win — dictionary-based pruning:** For dictionary-compressed text columns,
load just the dictionary (small, at the start of the blob) and test the LIKE
pattern against it before decompressing indices. If no dictionary entry matches,
skip the segment entirely. No bloom filter storage needed.

**Files:** `src/compress.rs` (bloom filter in companion table schema),
`src/scan/exec.rs` (bloom filter test in segment loading),
`src/compression/dictionary.rs` (dictionary-only decode for pruning)

---

## Priority Order

Ranked by estimated time savings × feasibility:

1. **Lazy column decompression (#11)** — Fixes the worst regression (Q24 0.13x).
   Medium-high effort but clear implementation path. Also improves Q22/Q23.

2. **Expression aggregate pushdown (#12)** — Fixes Q30 (0.48x → ~20x).
   Medium effort, extends existing SeaTurtleAgg infrastructure.

3. **String function pushdown (#13)** — Fixes Q28 (0.57x → ~2.5x).
   Medium effort, pairs well with aggregate pushdown.

4. **Regex pushdown (#14)** — Fixes Q29 (0.37x → ~2x). Largest absolute time
   savings (2300ms) but adds a dependency and high complexity.

5. **Late text materialization (#15)** — Broad 10-30% improvement across many
   queries. High effort, architectural change to decompression.

6. **Bloom/dictionary pruning (#16)** — Moderate improvement on LIKE queries.
   Dictionary-based pruning is a quick win; full bloom filters are high effort.

### Queries that remain hard to optimize

**Q33 (0.66x), Q36 (0.78x), Q18 (0.98x):** These are bottlenecked by PG's hash
aggregation on high-cardinality GROUP BY keys (WatchID has ~1M unique values).
The scan is already fast (8-15ms). The only fix is pushing the entire GROUP BY
hash table into the scan node — essentially reimplementing PG's hash aggregate
in Rust. Very high effort, marginal return since PG's hash agg is already
well-optimized. These are best left as-is unless we move to a full vectorized
execution engine (VECTORIZE.md Phase 4).
