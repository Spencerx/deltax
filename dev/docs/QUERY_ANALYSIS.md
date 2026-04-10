# ClickBench Query-by-Query Analysis

Investigation of all 43 ClickBench queries on the full 100M row dataset
(c6a.4xlarge EC2, PostgreSQL 18, pg_deltax). Each section records the
EXPLAIN ANALYZE output, what dominates execution, and any improvement
ideas that haven't already been tried (see `PERF_IMPROVEMENTS.md`).

> Environment: 18 partitions, ~3338 compressed segments total,
> `pg_deltax.parallel_workers=0` (auto, capped at 16), `max_parallel_workers=8`.

## Top-level findings (actionable)

Several cross-cutting issues affect many queries and are worth fixing
before any query-specific work:

### F1. Planning overhead of ~30 ms on every DeltaXAgg query with GROUP BY

**Impact: 43 queries × up to 30 ms = ~1.3 s wasted cumulatively. Huge on
 short queries (Q36, Q37, Q41, Q42 where planning ≈ execution).**

`plan_agg_path` calls `cost::get_column_ndistinct(oid)` once per
companion table during planning. That helper runs
`SELECT MAX(_ndistinct_col1), MAX(_ndistinct_col2), ... FROM <companion>`
which is a full SeqScan of the companion metadata table (4000+ buffer
hits). With 18 partitions, that's 18 SeqScans per plan.

`cost::estimate_cost` also issues an SPI query
(`SELECT row_count FROM deltax_partition ...`) once per partition
per plan.

**Fix (trivial):** Add a thread_local cache keyed on companion OID:
`HashMap<Oid, HashMap<String, i64>>` for ndistinct and
`HashMap<Oid, i64>` for row_count. Invalidate via the existing
`invalidate_compressed_cache` hook. Same pattern as
`TIME_COLUMN_CACHE` and `SEGMENT_BY_CACHE` in `hook.rs`.

**Expected win:** ~30 ms off planning time for every aggregate query —
 brings Q36/Q37/Q41/Q42 from 80–100 ms to 50 ms, makes the fast
 queries 2× closer to ClickHouse.

### F2. Metadata-only "heap_scan" takes 50–70 ms for 3338 segments

**Impact: Q0 (19 ms → ~2 ms), Q2 (68 ms → ~15 ms), Q3, Q6, Q29.
 All queries pay this as a fixed startup tax.**

Every scan runs `load_segments_heap` across all companion tables to
build a `Vec<SegmentInfo>`. Even when the rest of the query is
fully metadata-resolvable (`segments_metadata_resolved=3338`,
`rows_processed=0`), reading 3338 wide companion rows takes
55 ms on Q2/Q3/Q6/Q29.

The companion rows are wide (~100 ndistinct/min/max/sum columns each)
so reading a few fields still touches every page.

**Fix ideas (in order of effort):**

1. **Session-level segment metadata cache.** `thread_local!
   HashMap<Oid, Arc<Vec<SegmentInfo>>>`, invalidated when the companion
   table's xmin changes or via the existing invalidation hook.
   After the first query, all subsequent queries get 0 ms heap_scan.
   This is a very cheap fix and covers all metadata-only queries.
2. **Parent-level aggregate summary.** Maintain `_row_count`,
   per-column `_min`, `_max`, `_sum`, `_nonnull_count` at the
   companion-relation level (one row per companion, updated on
   compression). Q0/Q2/Q3/Q6/Q29 collapse to an O(1) catalog lookup.
   Higher implementation cost; interacts with DML and concurrent
   compression.

The first option is a 1-hour fix and likely recovers ~40 ms on every
query that touches the metadata.

### F3. PG detoast is the dominant cost on most DeltaXAgg queries

**Impact: Q7, Q9, Q10, Q11, Q12, Q13, Q14, Q20, Q21, Q22, Q27, Q28,
 Q30, Q31, Q32, Q33, Q34, Q35 — basically every query that touches
 text or wide integer columns.**

Detoast time (in ms) dominates execution on: Q20 (2150), Q22 (2380),
Q21 (1343), Q28 (1148), Q9 (899), Q32 (2129), Q27 (1031), Q33/Q34
(1041/1047), Q13 (529), Q12 (384).

This is already tracked as **#39 Pipelined detoast + parallel
aggregation** in PERF_IMPROVEMENTS.md. Not yet implemented — and is
probably the single biggest win across the whole benchmark.

Additional angle not in #39: **aggressive projection pruning**.
Several agg queries detoast columns they don't actually need. Check
that `needed_cols` is tight for agg paths.

### F4. `merge` phase is slow for per-group COUNT DISTINCT and high-card GROUP BY

**Impact: Q8 (merge=1347 ms), Q13 (2883), Q15 (1075), Q22, Q32 (5771),
 Q35 (837).**

The per-worker merge for `COUNT(DISTINCT ...) GROUP BY ...` queries
and high-cardinality GROUP BY queries (WatchID, UserID) is the
dominant cost. Q32 in particular spends 5.7 s merging
~100 M near-unique WatchID groups across workers.

This is adjacent to **#36 Two-level hash aggregation** in
PERF_IMPROVEMENTS.md. #36 would help both the per-worker accumulation
and the merge. Still planned, not implemented.

Also worth considering: **cheaper distinct sketches** for the
`COUNT(DISTINCT)` inner set (HyperLogLog-style approximate sketches
when exactness isn't required, or at least a more compact bitset for
low-cardinality groups).

---

## Query-by-query details

Format per query:
- **Query**
- **CH / deltax / ratio**
- **Dominant cost** (from EXPLAIN ANALYZE `DeltaX Timing`)
- **Analysis + potential improvements**

### Q0 — COUNT(*)

- CH 0.001 s / deltax 0.022 s / **22×**
- `DeltaXCount`, metadata=1.7 ms, heap_scan=17.5 ms, planning=3 ms.
- Dominant: heap_scan of 3338 segment rows just to sum
  `_row_count`.
- **Improvements:** F2 (cached segments or parent-level total).
  A per-companion summary row with `total_rows` would reduce this
  to an O(18) catalog lookup, ~0 ms. Currently wasting ~17 ms on
  every COUNT(*).

### Q1 — COUNT(*) WHERE AdvEngineID <> 0

- CH 0.006 s / deltax 0.088 s / **15×**
- `DeltaXAgg`, metadata=1.7, heap_scan=68.7, batch_quals=1,
  rows_processed=0 (metadata-fast-path resolved all 2271 surviving
  segments via #32).
- Dominant: heap_scan of 2271 segment rows (incl. AdvEngineID min/max
  and row_count). 30 µs per segment — wide rows.
- **Improvements:** F2. This is purely metadata-bound; a session
  cache would drop heap_scan to ~0. Would go from 88 ms to ~20 ms.
  Remaining 20 ms is planning + framework overhead.

### Q2 — SUM/AVG full-scan

- CH 0.021 s / deltax 0.079 s / **3.8×**
- metadata=1.7, heap_scan=56, `segments_metadata_resolved=3338`,
  `segments_decompressed=0`. Fully resolved from per-segment SUM
  metadata (#22).
- Dominant: heap_scan of 3338 rows.
- **Improvements:** F2. With cached metadata this is ~25 ms total,
  i.e. competitive.

### Q3 — AVG(UserID)

- CH 0.027 s / deltax 0.077 s / **2.9×**
- Same profile as Q2. Metadata-only. Same fix.

### Q4 — COUNT(DISTINCT UserID)

- CH 0.353 s / deltax 3.441 s / **9.7×**
- heap_scan=404 ms (loading UserID blobs), agg=528 ms (building global
  distinct set).
- Dominant: aggregation of 100 M UserIDs into a global hash set.
- **Improvements:**
  - Per-segment pre-computed `_distinct_hll_<col>` (HyperLogLog sketch
    with e.g. 16 KB) would let COUNT(DISTINCT) be metadata-resolvable
    when the query asks for it plain (no filter, no GROUP BY). Exact
    path still available when needed. ClickHouse uses this heavily.
  - **Known pattern in ClickHouse:** `uniqHLL12` / `uniqCombined`.
    Store the HLL bytes as part of segment metadata at compression
    time, merge them at query time. For 3338 segments × 16 KB = 52 MB
    of metadata total — not cheap, but this query is one of the
    expensive outliers.
  - Alternative: make the per-segment distinct sketches optional / on
    a handful of high-value columns (UserID, SearchPhrase) rather than
    all columns.

### Q5 — COUNT(DISTINCT SearchPhrase)

- CH 0.623 s / deltax 1.996 s / **3.2×**
- heap_scan=546 ms, agg=380 ms.
- SearchPhrase is dictionary-encoded low-cardinality text. Each
  segment's dictionary already encodes its distinct values.
- **Improvements:** when the target column is dictionary-encoded and
  the query is COUNT(DISTINCT col) with no filter, load only the
  dict portion of each segment blob (tiny) and union dict entries
  across segments. For 3338 segments × ~500 dict entries, that's
  ~1.7 M strings to dedupe — far less than 100 M row values.
  Should drop from 2 s to ~200 ms.

### Q6 — MIN/MAX EventDate

- CH 0.010 s / deltax 0.061 s / **6.1×**
- `DeltaXMinMax`, heap_scan=54 ms across 3338 segments.
- Same F2 story. Fixing metadata caching turns this into the
  single-segment-style lookup it should be. Could also materialize
  per-companion min/max directly.

### Q7 — GROUP BY AdvEngineID

- CH 0.009 s / deltax 0.118 s / **13×**
- `DeltaXAgg` with 18 groups. heap_scan=15, detoast=36, decompress=2,
  agg=30, planning=33.
- Dominant: detoast (36) + planning (33) + agg (30). About a third of
  the query is the planning overhead described in F1.
- **Improvements:** F1 (planning) + F3 (detoast pipelining) both apply.
  With both, this query gets to ~20 ms. Still ~2× CH but much better.

### Q8 — GROUP BY RegionID COUNT(DISTINCT UserID)

- CH 0.452 s / deltax 2.187 s / **4.8×**
- detoast=632 ms, agg=33, **merge=1347 ms**, finalize=2.
- Dominant: merge phase for per-group UserID distinct sets.
- 9040 pre_topn_groups × ~11 K distinct UserIDs each on average.
- **Improvements:** F4 (approximate sketches or two-level merge). A
  16 KB HLL per group merges in O(groups), not O(groups × distincts).
  Could go from 2.2 s to ~600 ms.

### Q9 — RegionID multi-agg

- CH 0.522 s / deltax 1.469 s / **2.8×**
- detoast=899, decompress=15, agg=53, merge=0, finalize=308,
  topn_select=106.
- Dominant: detoast (60% of time).
- **Improvements:** F3 pipelined detoast. finalize=308 ms is the
  COUNT(DISTINCT UserID) finalization — F4 applies too.

### Q10 — MobilePhoneModel users

- CH 0.147 s / deltax 0.434 s / **3.0×**
- detoast=289, decompress=47, agg=35, merge=25.
- Dominant: detoast. F3.

### Q11 — MobilePhone + Model users

- CH 0.143 s / deltax 0.568 s / **4.0×**
- detoast=303, decompress=114, agg=43, merge=27.
- Dominant: detoast. F3.

### Q12 — Top 10 SearchPhrase

- CH 0.599 s / deltax 1.178 s / **2.0×**
- detoast=384, decompress=145, agg=563.
- Dominant: agg on 55 M rows with 4.77 M groups (dictionary text).
- **Improvements:** F3 for detoast; #36 two-level hash for the agg
  merge (though this is single-pass, merge=0). The agg work itself
  is dictionary-encoded SearchPhrase hashing at 10 ns/row — already
  near the limit. Modest improvement possible.

### Q13 — SearchPhrase users (COUNT DISTINCT)

- CH 0.804 s / deltax 5.001 s / **6.2×**
- detoast=529, decompress=165, agg=752, **merge=2883**,
  finalize=295.
- Dominant: merge phase for per-group distinct UserID sets.
  3.85 M groups × distinct UserIDs each.
- **Improvements:** F4 (HLL sketches) is a 4–5× win here. F3 also.

### Q14 — SearchEngine + SearchPhrase

- CH 0.597 s / deltax 1.270 s / **2.1×**
- detoast=409, decompress=157, agg=624.
- Similar to Q12. F3 + modest agg improvement.

### Q15 — Top 10 UserID

- CH 0.384 s / deltax 2.045 s / **5.3×**
- detoast=570, decompress=4 (!), agg=325, **merge=1075**.
- Dominant: merge. 22 M groups merged across parallel workers.
- **Improvements:** F4 / #36 two-level hash aggregation.
  The decompress is 4 ms because UserID is bitpacked; almost all
  the cost is detoast + agg + merge.

### Q16 — UserID + SearchPhrase top

- CH 1.709 s / deltax 2.060 s / **1.2×**
- detoast=512, decompress=191, agg=1137.
- Already competitive. Modest improvement with F3.

### Q17 — UserID + SearchPhrase (no order)

- CH 0.999 s / deltax 1.733 s / **1.7×**
- detoast=511, decompress=181, agg=854.
- Competitive. F3.

### Q18 — UserID + extract(minute) + SearchPhrase

- CH 3.041 s / deltax 3.722 s / **1.2×**
- detoast=1037, decompress=327, agg=2053.
- Already competitive. agg dominates; needs better hash agg (#36).

### Q19 — Point lookup UserID = const

- CH 0.003 s / deltax 0.164 s / **55×**
- `DeltaXAppend`. segments=40, segments_skipped=3298
  (1863 by minmax + 1435 by bloom).
- metadata=1.9, **heap_scan=137.9**, decompress=10, batch_eval=1.4.
- Dominant: heap_scan loading *all 3338 segments' metadata* so it can
  check the bloom filter on each. Even skipped segments require their
  row read.
- **Improvements:**
  1. **Two-tier metadata layout.** Split companion into a narrow
     "hot" metadata table (row_count, min/max for all cols + bloom
     filter bytes) and a wider table for everything else. Currently
     everything is in one wide row. A narrow bloom-only scan would
     be 10× faster because the rows fit in one page per 10–50
     segments.
  2. **Partition-level bloom filter / catalog-level min-max.**
     First check a partition-level bloom. If the partition can't
     contain the value, skip loading any segment metadata from it.
     For point lookups this is a huge win.
  3. **F2 metadata caching** also helps: after the first query, this
     drops to framework overhead (~20 ms).
- **Expected with #3 alone:** 164 ms → ~25 ms.
- **Expected with #1+#2:** closer to 5 ms, near CH.

### Q20 — COUNT(*) WHERE URL LIKE '%google%'

- CH 0.312 s / deltax 6.744 s / **22×**
- `DeltaXAgg`, segments=1690 (after dict pruning, #19 worked).
  rows_processed=15911 (very few!).
- heap_scan=2281, **detoast=2150**, **decompress=2600**, agg=1828.
- Dominant: detoast URL blobs (2.1 s) + decompress (2.6 s) for 1690
  segments before we can confirm which rows match.
- **Improvements:**
  - **#40 Dict-accelerated LIKE filtering** in PERF_IMPROVEMENTS.md.
    This is the biggest listed improvement for Q20. Currently
    row-by-row. Dict-level match drops it to `dict_entries ×
    string.contains`, typically 500 vs 30 000 per segment.
  - **Two-phase column decompression** (same entry): if dict
    filter eliminates all rows in a segment, don't decompress
    the other columns.
  - **Dict-level bloom** (#33) for segments where no dict entries
    match at all, skipping the dict load.
  - With #40 the detoast + decompress cost drops because we decompress
    far fewer segments and only the dict header in the rest. Could
    go from 6.7 s to ~300 ms.

### Q21 — SearchPhrase MIN(URL) WHERE URL LIKE '%google%'

- CH 0.098 s / deltax 1.980 s / **20×**
- detoast=1343, decompress=344, agg=329.
- Dominant: detoast URL blobs.
- Same fix as Q20: **#40**. Dict pruning is active (segments=2403)
  but URL is LZ4 so dict-accelerated filter doesn't apply — URL is
  always LZ4. That means #40 alone doesn't help here.
- **Additional improvement:** **#33 Trigram bloom** for LZ4 URL
  columns. Prune segments where 'google' trigrams aren't present.
  Expected to eliminate >99% of segments for queries like this.
  Q21: 2.0 s → ~0.3 s.

### Q22 — Title LIKE Google + URL NOT LIKE

- CH 0.717 s / deltax 3.741 s / **5.2×**
- detoast=2380, decompress=713, agg=823.
- Dominant: detoast Title + URL.
- Title is dict-encoded (should benefit from #40). URL is LZ4
  (needs #33 trigram bloom).
- **Improvements:** #40 + #33 combined. Expected 3.7 s → ~1 s.

### Q23 — SELECT * WHERE URL LIKE ... ORDER BY EventTime LIMIT 10

- CH 0.393 s / deltax 0.471 s / **1.2×**
- `DeltaXAppend` TopN, 12 surviving segments.
- heap_scan=163, decompress=240.
- Already competitive. Nothing urgent.

### Q24 — SearchPhrase ORDER BY EventTime LIMIT 10

- CH 0.147 s / deltax 0.114 s / **0.78×**
- **Already faster than CH.** DeltaXAppend TopN with sorted-by-time
  pathkeys. 21 segments, 97 K candidates.

### Q25 — ORDER BY SearchPhrase LIMIT 10

- CH 0.192 s / deltax 1.915 s / **10×**
- `DeltaXAppend`, 3330 segments (no pruning), decompress=**2337 ms**.
- Parallel text top-N (#37) is active but the bottleneck is
  decompressing SearchPhrase across all segments.
- **Improvement:** SearchPhrase is dictionary-encoded. For
  ORDER BY text_col LIMIT N, we can do a **dict-only scan**:
  for each segment, load just the dict header (<1 KB each),
  find the lexicographically smallest dict entries, apply
  strcoll only to the top-N merged candidates. Skip the main LZ4
  block entirely.
  - For a segment with 500 dict entries, finding the min K is
    O(dict_size) per segment vs O(rows).
  - 3330 segments × 500 entries = 1.66 M items to min-heap,
    vs 1.5 M rows currently.
  - With byte-order pre-prune then strcoll for the final merge,
    this is near-instant.
- Expected: 2.0 s → ~100–200 ms. Close to CH.

### Q26 — ORDER BY EventTime, SearchPhrase LIMIT 10

- CH 0.149 s / deltax 0.116 s / **0.78×**
- Already faster. Same story as Q24 — time-ordered pathkey kicks in.

### Q27 — CounterID AVG(length(URL)) HAVING c > 100K

- CH 0.083 s / deltax 1.835 s / **22×**
- detoast=1031, decompress=270, agg=558.
- Dominant: detoast + agg of URL.
- `length()` pushdown (#13) is active — agg runs on `&str` slices.
  But we still detoast all URL blobs.
- **Improvement:** store **per-segment `_sum_length_<textcol>`** and
  **`_nonnull_count_<textcol>`** at compression time (same pattern as
  #22 for numeric SUM). Then `AVG(length(col))` with no filter
  (or a filter on another column that's already in metadata) is
  fully metadata-resolvable — Q27 drops from 1.8 s to ~50 ms.
  Low complexity, high impact. This is a **new idea not in
  PERF_IMPROVEMENTS.md**.

### Q28 — Referer REGEXP_REPLACE GROUP BY

- CH 9.582 s / deltax 9.515 s / **1.0×**
- decompress=3503, agg=2036, merge=1529, finalize=1408,
  detoast=1149.
- Already tied with ClickHouse. Not a priority.

### Q29 — Wide SUM 89 cols

- CH 0.029 s / deltax 0.151 s / **5.2×**
- heap_scan=56, agg=0, everything metadata-resolved (#22).
- Dominant: heap_scan (F2) + planning (~30 ms, F1).
- With F1 + F2 fixes, this becomes a pure O(partitions) operation
  at ~30 ms. Close to CH.

### Q30 — SearchEngine + ClientIP multi-agg

- CH 0.342 s / deltax 1.555 s / **4.5×**
- detoast=653, decompress=230, agg=606.
- Dominant: detoast + agg. F3.

### Q31 — WatchID + ClientIP with SearchPhrase filter

- CH 0.562 s / deltax 2.260 s / **4.0×**
- detoast=990, decompress=296, agg=885.
- Dominant: detoast + agg. F3 + #36.

### Q32 — WatchID + ClientIP all

- CH 3.793 s / deltax 9.604 s / **2.5×**
- detoast=2129, decompress=15 (bitpacked), agg=1610,
  **merge=5771**.
- Dominant: merge phase. 100 M pre_topn_groups — essentially
  unique. Merging per-worker hash tables is O(100 M).
- **Improvement:** This is **#36 two-level hash agg** exactly —
  partition merges into 256 buckets for parallel lock-free merging.
  Expected: 9.6 s → ~3 s. Already planned.

### Q33 — GROUP BY URL ORDER BY c DESC LIMIT 10

- CH 2.782 s / deltax 2.748 s / **1.0×**
- Already competitive.

### Q34 — GROUP BY 1, URL

- CH 2.851 s / deltax 2.710 s / **0.95×**
- **Already faster than CH.** Nothing to do.

### Q35 — GROUP BY ClientIP, IP−1, IP−2, IP−3

- CH 0.297 s / deltax 1.753 s / **5.9×**
- detoast=467, decompress=3, agg=375, **merge=837**.
- #34 "redundant GROUP BY" already pushes ClientIP-1/2/3 out. Still,
  merge phase dominates for 21 M distinct ClientIPs.
- **Improvement:** #36 two-level hash. Expected: 1.75 s → ~500 ms.

### Q36 — Top URLs for CounterID=62

- CH 0.043 s / deltax 0.120 s / **2.8×**
- Segments=26 (min/max pruning), rows_processed=671 K.
- detoast=22, decompress=14, agg=54, planning=33.
- **~40% of total time is the F1 planning overhead.**
- **Improvements:** F1 cache brings this to 50–60 ms, within 2× CH.

### Q37 — Top Titles for CounterID=62

- CH 0.021 s / deltax 0.066 s / **3.1×**
- Segments=26. Execution 29 ms, planning 33 ms.
- **Planning overhead > execution.** F1 is the entire fix.
  After F1: ~30 ms total, close to CH.

### Q38 — CounterID=62 links OFFSET 1000

- CH 0.017 s / deltax 0.110 s / **6.5×**
- detoast=24, decompress=29, agg=27, planning=32.
- F1 is the dominant improvement.

### Q39 — CounterID=62 traffic src

- CH 0.077 s / deltax 0.414 s / **5.4×**
- detoast=42, decompress=33, agg=114, merge=170, finalize=28.
- Uses CASE expression pushed into GROUP BY (recent Q39
  optimization). merge is the biggest post-F1 cost.
- **Improvement:** #36 two-level hash would help. But this is
  already close enough that F1 alone gets within ~3×.

### Q40 — CounterID=62 URLHash

- CH 0.013 s / deltax 0.149 s / **12×**
- detoast=13, decompress=75 (!), agg=23, planning=32.
- decompress 75 ms for only 89 K rows and 26 segments is surprising.
  URLHash is int8, bitpacked. Worth investigating separately; may
  point at a per-segment fixed cost.
- F1 helps significantly.
- **Additional:** check whether URLHash is doing something non-trivial
  in decompress path (maybe full blob load despite narrow predicate).

### Q41 — CounterID=62 window dim

- CH 0.009 s / deltax 0.068 s / **7.6×**
- detoast=8, decompress=8, agg=10, planning=33.
- **Pure planning overhead.** After F1: ~35 ms. Close to CH.

### Q42 — CounterID=62 by minute

- CH 0.008 s / deltax 0.042 s / **5.3×**
- All phases small; planning=4 (this query doesn't trip the text
  ndistinct code path — planning is already fast!)
- Execution 34 ms. Reasonable. Metadata heap_scan (F2) is 10 ms.

---

## Prioritized improvement list

The same improvements come up across many queries. Sorted by
estimated combined wallclock benefit (across the whole benchmark):

| # | Improvement                                 | Queries helped                  | Est. benefit    | Complexity | In PERF_IMPROVEMENTS.md? |
|---|---------------------------------------------|---------------------------------|-----------------|------------|---------------------------|
| **1** | **F1: cache ndistinct + row_count in planner** | Q7–Q18, Q27, Q30–Q42 (almost all) | ~0.5 s aggregate | Low | **No — new idea**     |
| **2** | **F2: cache segment metadata per session**    | Q0, Q2, Q3, Q6, Q19, Q29, all    | ~0.3 s aggregate | Low | **No — new idea**     |
| **3** | **#39 Pipelined detoast**                      | Q7, Q9–Q14, Q20–Q22, Q27, Q30–Q34| ~5–8 s          | Medium  | Yes                    |
| **4** | **#40 Dict-accelerated LIKE filtering**        | Q20, Q21, Q22                    | ~5 s            | Medium  | Yes                    |
| **5** | **#33 Trigram bloom filters**                  | Q20, Q21, Q22                    | ~3 s            | Medium  | Yes                    |
| **6** | **#36 Two-level hash aggregation**             | Q15, Q16, Q17, Q18, Q32, Q35     | ~8 s            | Med-High| Yes                    |
| **7** | **New: per-segment SUM(length(text_col))**     | Q27                              | ~1.7 s          | Low     | **No — new idea**     |
| **8** | **New: dict-only scan for ORDER BY text LIMIT**| Q25                              | ~1.8 s          | Low-Med | **No — new idea**     |
| **9** | **New: HLL sketch per segment for COUNT(DISTINCT)** | Q4, Q8, Q13                 | ~8 s            | Medium  | **No — new idea**     |
|**10** | **New: dict-only COUNT(DISTINCT)**             | Q5                               | ~1.8 s          | Low-Med | **No — new idea**     |
|**11** | **Q19: two-tier metadata / partition bloom**   | Q19                              | ~0.15 s         | Med     | **No — new idea**     |
|**12** | **Q40: investigate decompress=75ms**           | Q40                              | ~60 ms          | Investigation | —                    |

### Recommended order of attack

1. **F1 and F2 first.** Both are < 1 day of work and affect
   every query. F1 alone clears ~30 ms from 30+ queries. F2
   eliminates the metadata heap_scan tax.
2. **#7 per-segment `SUM(length(text))` metadata.** Q27 alone goes
   from 1.8 s to 50 ms. Small, self-contained change in `compress.rs`
   + `load_segments_heap`.
3. **#8 dict-only ORDER BY text LIMIT.** Q25 drops 10×.
4. **#9 HLL sketches for COUNT(DISTINCT).** Biggest-bang-for-buck on
   the Q4/Q8/Q13 triad; ClickHouse beats us mostly on these.
5. **#3 #39 pipelined detoast.** Biggest overall hit but high
   implementation complexity.
6. **#4/#5 #40 / #33 LIKE optimizations.** Solve the Q20/Q21/Q22
   cluster.
7. **#6 #36 two-level hash.** Solves the high-cardinality GROUP BY
   cluster.

### New ideas proposed (not in PERF_IMPROVEMENTS.md)

- **F1** Session cache for planner SPI calls
- **F2** Session cache for segment metadata (or parent-level summary)
- **#7** Per-segment `SUM(length(text_col))` for metadata fast path
- **#8** Dict-only scan for `ORDER BY text_col LIMIT N`
- **#9** Per-segment HLL sketches for distinct count metadata fast path
- **#10** Dict-only scan for `COUNT(DISTINCT dict_text_col)`
- **#11** Two-tier metadata layout (narrow hot table + wide cold)
  or partition-level bloom filter for point lookups
- **#12** Investigate Q40 decompress=75 ms for 26 segments
