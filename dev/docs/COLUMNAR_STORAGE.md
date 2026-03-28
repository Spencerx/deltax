# Proposal: Columnar Blob Storage for Cold I/O Performance

## Problem Statement

DeltaX stores compressed column blobs alongside segment metadata in the same
companion table row. Each segment occupies one row with ~N `BYTEA` columns
(one per non-segment-by column). PostgreSQL automatically TOASTs these blobs
into a shared TOAST heap.

For the ClickBench `hits` table (105 compressed columns, ~3338 segments across
7 partitions), the TOAST table for a single partition is ~2.8 GB. The chunks
for different columns are interleaved in insertion order:

```
TOAST heap physical layout (current):
  seg1_col0_chunks  seg1_col1_chunks  ...  seg1_col104_chunks
  seg2_col0_chunks  seg2_col1_chunks  ...  seg2_col104_chunks
  ...
  segN_col0_chunks  segN_col1_chunks  ...  segN_col104_chunks
```

When a query needs only one column (e.g., Q7: `AdvEngineID`), PostgreSQL
detoasts that column's blob from each segment independently. Each detoast is
an index lookup on the TOAST B-tree followed by chunk page reads. Because the
chunks for one column are scattered across the entire TOAST table (spaced
every 1/105th), the I/O pattern is random.

**Measured impact on gp2 EBS (ClickBench 100M rows, r7i.4xlarge, cold cache):**

We instrumented `load_segments_heap` to separately measure `heap_getnext`
(reading companion table heap pages), `heap_deform_tuple` (extracting
datums), and `pg_detoast_datum` (TOAST I/O). Results across all 43 queries:

- **`heap_getnext` + `heap_deform_tuple`**: ~1-2ms per partition — negligible
- **`pg_detoast_datum`**: 99-100% of `heap_scan` time for every DeltaXAgg query
- **All blobs are TOASTed** (0 inline blobs observed)

The entire cold-run bottleneck is TOAST random I/O. See "Appendix: Full Cold
Run Measurements" for the complete per-query breakdown.

Example queries:

| Query | Columns | Cold Total | detoast | detoast % of total |
|-------|---------|------------|---------|-------------------|
| Q7    | 1       | 3.3s       | 3131ms  | 96%               |
| Q21   | 3       | 30.1s      | 28679ms | 95%               |
| Q22   | 5       | 53.4s      | 50308ms | 94%               |
| Q32   | 4       | 36.8s      | 27905ms | 76%               |

**`posix_fadvise` does not help**: We tested prefetching TOAST table files
and TOAST indexes. On gp2, prefetching 12 GB at 250 MB/s takes ~48s —
slower than the random reads themselves. The fundamental issue is that
sequential prefetch reads 100× more data than needed.

## How Data is Organized into Segments

A DeltaX compressed table partitions data in two dimensions: **time-based
partitions** (PostgreSQL declarative partitioning) and **segments** within
each partition (groups of rows compressed together).

```
Original table: hits (100M rows, 105 columns)
│
├── Partition 1 (2013-07-01 to 2013-07-08) ── ~20M rows
│   ├── Segment 1 ── 30,000 rows, ordered by EventTime
│   │   ├── _advengineid_compressed  BYTEA  (~14 KB)
│   │   ├── _searchphrase_compressed BYTEA  (~95 KB)
│   │   ├── _url_compressed          BYTEA  (~1 MB)
│   │   ├── ... (105 columns total)
│   │   ├── _row_count = 30000
│   │   ├── _min_eventtime, _max_eventtime
│   │   └── _min_<col>, _max_<col> for each column
│   │
│   ├── Segment 2 ── 30,000 rows
│   │   └── ... (same structure)
│   │
│   ├── ... (~667 segments per partition)
│   │
│   └── Segment 667 ── remaining rows
│
├── Partition 2 (2013-07-08 to 2013-07-15) ── ~20M rows
│   └── ... (~667 segments)
│
├── ... (5 partitions total)
│
└── Partition 5
    └── ...

Current companion table layout (one row per segment):
┌──────────────────────────────────────────────────────────────────────┐
│ Row 1: seg_by | _row_count | _min/max_time | _min/max_col0..104 |  │
│        _col0_compressed (BYTEA→TOAST) | ... | _col104_compressed   │
├──────────────────────────────────────────────────────────────────────┤
│ Row 2: ... same 105 BYTEA blobs, all TOASTed ...                   │
├──────────────────────────────────────────────────────────────────────┤
│ ...                                                                 │
└──────────────────────────────────────────────────────────────────────┘

TOAST table (physical disk order = insertion order):
┌─────────────────────────────────────────────────────────┐
│ seg1_col0 | seg1_col1 | ... | seg1_col104 |            │  ← all cols
│ seg2_col0 | seg2_col1 | ... | seg2_col104 |            │    interleaved
│ ...                                                     │
│ seg667_col0 | seg667_col1 | ... | seg667_col104 |      │
└─────────────────────────────────────────────────────────┘
  ↑ Reading AdvEngineID = every 105th blob = random I/O
```

Each segment's compressed blobs are stored as BYTEA columns in one companion
table row. PostgreSQL TOASTs them into a shared TOAST heap. The segment size
defaults to 30,000 rows (configurable via `segment_size` parameter).

For ClickBench: 100M rows / 30K rows per segment ≈ 3,338 segments total,
distributed across 5 partitions (~667 segments each).

## ClickBench Query Patterns

Analysis of all 43 ClickBench queries shows most queries are highly selective
in which columns they read:

| Columns Referenced | Queries     | Fraction |
|-------------------|-------------|----------|
| 1–3 columns       | 21 queries  | 49%      |
| 4–10 columns      | 20 queries  | 47%      |
| All columns       | 1 query     | 2%       |

The 10 most-referenced columns (SearchPhrase, EventDate, CounterID, URL,
UserID, IsRefresh, ResolutionWidth, AdvEngineID, ClientIP, EventTime) account
for the vast majority of column accesses. Most queries use a small fraction
of the ~105 available columns.

**Key insight**: The current layout optimizes for reading all columns (single
row = single heap tuple → sequential TOAST per row). But nearly all real
queries read a small subset of columns. The layout should optimize for the
common case: reading one or a few columns across many segments.

## Proposed Layout

Split the current monolithic companion table into two tables per partition:

### 1. Segment Metadata Table

Stores all scalar per-segment metadata. No BYTEA columns, so no TOAST I/O.

```sql
CREATE TABLE "_deltax_compressed"."<partition>_meta" (
    _segment_id   SERIAL PRIMARY KEY,

    -- Segment-by columns (original types)
    "<seg_by_col>" <type>,

    -- Per-segment row count
    _row_count    INT,

    -- Time bounds
    _min_<time_col> TIMESTAMPTZ,
    _max_<time_col> TIMESTAMPTZ,

    -- Per-column min/max (orderable types only)
    _min_<col>    <type>,
    _max_<col>    <type>,

    -- Per-column sum + nonnull count (numeric types only)
    _sum_<col>    DOUBLE PRECISION,  -- or NUMERIC
    _nonnull_count_<col> INT,

    -- Per-column cardinality estimate
    _ndistinct_<col> INT
);
```

For ClickBench (105 columns): each row is ~2 KB (all small scalars). With
~667 segments per partition, the entire metadata table fits in ~1.3 MB. A full
sequential scan takes <1 ms even on cold storage.

### 2. Column Blob Table

Stores compressed column data. One row per (column, segment) pair.

```sql
CREATE TABLE "_deltax_compressed"."<partition>_blobs" (
    _col_idx     SMALLINT NOT NULL,
    _segment_id  INT NOT NULL REFERENCES "<partition>_meta"(_segment_id),
    _data        BYTEA,
    PRIMARY KEY (_col_idx, _segment_id)
);
```

The key design decision is **column-major insertion order**: blobs are
inserted sorted by `(_col_idx, _segment_id)`. Because PostgreSQL writes TOAST
chunks in insertion order, this naturally produces a columnar physical layout
with no post-processing:

```
Proposed layout (two tables):

Metadata table (no TOAST, ~1.3 MB):
┌──────────────────────────────────────────────────────┐
│ seg 1: seg_by | _row_count | _min/max_time | min/max │
│ seg 2: ...                                           │
│ ...                                                  │
│ seg 667: ...                                         │
└──────────────────────────────────────────────────────┘

Blob table (column-major insertion → columnar TOAST):
┌──────────────────────────────────────────────────────┐
│ (col=0, seg=1, blob) | (col=0, seg=2, blob) | ...   │  ← col 0 blobs
│ (col=0, seg=667, blob) |                             │    contiguous
│ (col=1, seg=1, blob) | (col=1, seg=2, blob) | ...   │  ← col 1 blobs
│ ...                                                  │    contiguous
│ (col=104, seg=1, blob) | ... | (col=104, seg=667)   │
└──────────────────────────────────────────────────────┘

TOAST heap (follows insertion order):
┌──────────────────────────────────────────────────────┐
│ col0_seg1 | col0_seg2 | ... | col0_seg667 |         │  ← sequential
│ col1_seg1 | col1_seg2 | ... | col1_seg667 |         │    for each
│ ...                                                  │    column
│ col104_seg1 | ... | col104_seg667 |                  │
└──────────────────────────────────────────────────────┘
  ↑ Reading AdvEngineID = first 1/105th of TOAST = sequential I/O
```

Reading one column = sequential I/O on a contiguous ~1/105th slice of the
TOAST table. The kernel's readahead (128 KB default on Linux) prefetches
upcoming chunks automatically.

## Read Path

The read path becomes a two-phase process:

### Phase 1: Metadata Scan (unchanged pattern, faster execution)

Scan the metadata table with `heap_getnext()`. Apply pruning:

1. **Segment-by filters**: skip segments with non-matching segment_by values
2. **Time range filters**: skip segments outside query time range
3. **MinMax filters**: skip segments where min/max metadata proves no rows
   can match

Collect surviving `_segment_id` values into an array. This phase involves
zero TOAST I/O — the metadata table has no BYTEA columns.

### Phase 2: Column Blob Reads (new, sequential I/O)

For each needed column, read blobs from the blob table:

```
For each needed col_idx:
    Index scan: _col_idx = X AND _segment_id = ANY(surviving_ids)
    Detoast each blob → sequential TOAST I/O (contiguous region)
    Store in SegmentData.compressed_blobs[blob_idx]
```

Because blobs were inserted in column-major order, the TOAST chunks for one
column are contiguous on disk.

**Index scan vs sequential scan**: For columns needed across all segments,
a sequential scan with `_col_idx = X` filter is optimal. For selective
queries (many segments pruned), an index scan on the PK is better. The query
planner handles this automatically.

### Parallel Workers

The current parallel dispatch pattern is preserved:
1. Main thread: Phase 1 (metadata) + Phase 2 (blob reads) → `Vec<SegmentData>`
2. Dispatch segments to parallel workers for decompression + aggregation

Phase 2 runs on the main thread because `pg_detoast_datum` requires a valid
PostgreSQL backend context. However, the I/O is now sequential per column,
so the main thread can saturate the storage bandwidth.

## Write Path

### During Compression

Compression currently processes one segment at a time (all columns for
segment 1, then all columns for segment 2, etc.). To achieve column-major
insertion into the blob table, we buffer compressed blobs in memory and flush
them after all segments are processed.

```
Phase 1: Compress all segments, buffer blobs in memory

for each batch of 30,000 rows:
    compress all columns → 105 blobs
    INSERT metadata into meta table immediately
    buffer compressed blobs in memory (keyed by segment_id)

Phase 2: Flush blobs in column-major order

for col_idx in 0..num_cols:
    for seg_id in segment_order:
        INSERT INTO blobs (_col_idx, _segment_id, _data)
        VALUES (col_idx, seg_id, buffered_blob)

ANALYZE blobs
ANALYZE meta
```

### Memory Impact

Buffering requires holding all compressed blobs for one partition in memory.
For ClickBench (105 columns, ~667 segments per partition), the total
compressed data is ~2.8 GB per partition. This is the worst case — a typical
time-series table with 10-20 narrower columns and smaller partitions would
buffer tens of MB.

This is acceptable because:
- Compression is a batch operation (not latency-sensitive)
- The server already needs substantial memory for the uncompressed data
  during compression
- Peak memory can be reduced by flushing one column at a time: after all
  segments are compressed, iterate columns and flush each column's blobs,
  freeing them immediately after insertion

## Performance Projections

### gp2 EBS (500 GB, ~3000 IOPS, ~250 MB/s sequential)

Projections based on measured data. Current detoast times are from cold runs
on r7i.4xlarge. Projected times assume sequential reads at ~250 MB/s after
columnar storage eliminates random I/O.

**Single-column queries:**

| Query | Cols | Current Cold | Measured detoast | Data read | Projected detoast | Projected Total | Speedup |
|-------|------|-------------|------------------|-----------|-------------------|-----------------|---------|
| Q7  | 1 | 3.3s  | 3131ms  | ~14MB  | ~56ms  | ~0.2s  | **16×** |
| Q1  | 1 | 3.7s  | 3119ms  | ~14MB  | ~56ms  | ~0.6s  | **6×** |
| Q4  | 1 | 15.6s | 10160ms | ~95MB  | ~380ms | ~5.8s  | **3×** |
| Q5  | 1 | 14.5s | 11319ms | ~95MB  | ~380ms | ~3.6s  | **4×** |
| Q15 | 1 | 11.5s | 10220ms | ~95MB  | ~380ms | ~1.7s  | **7×** |

**Multi-column queries:**

| Query | Cols | Current Cold | Measured detoast | Projected detoast | Projected Total | Speedup |
|-------|------|-------------|------------------|-------------------|-----------------|---------|
| Q9  | 5 | 21.7s | 20452ms | ~400ms  | ~1.6s  | **14×** |
| Q22 | 5 | 53.4s | 50308ms | ~1000ms | ~4.1s  | **13×** |
| Q32 | 4 | 36.8s | 27905ms | ~600ms  | ~9.5s  | **4×** |
| Q33 | 1 | 25.1s | 21673ms | ~380ms  | ~3.8s  | **7×** |

**Filtered queries (segment pruning reduces data):**

| Query | Cols | Current Cold | Measured detoast | Projected Total | Speedup |
|-------|------|-------------|------------------|-----------------|---------|
| Q36 | 5 | 0.6s | 494ms  | ~0.2s | **3×** |
| Q37 | 3 | 0.5s | 399ms  | ~0.1s | **5×** |
| Q42 | 3 | 0.5s | 380ms  | ~0.2s | **3×** |

**Metadata-only queries (Q0, Q2, Q3, Q6, Q29): no change** — already fast (<0.2s).

### Metadata scan improvement

| Metric              | Current            | Proposed             |
|---------------------|--------------------|--------------------- |
| Metadata table size | ~2 MB + TOAST pointers | ~1.3 MB (no TOAST) |
| Cold metadata scan  | ~15-20ms           | ~5 ms                |
| Pruning I/O cost    | Essentially free (inline metadata) | Essentially free |

## Implementation Plan

### Step 0: Instrument TOAST I/O timing ✓ DONE

Instrumentation added to `load_segments_heap` in `segments.rs`. Separately
measures `heap_getnext`, `heap_deform_tuple`, and `pg_detoast_datum` timing,
plus blob counts (toasted vs inline) and total bytes. The `detoast_us` field
is propagated through `AggScanState` and surfaced in both logs and EXPLAIN
ANALYZE output (`[detoast=X.XXX]` in the DeltaX Timing line).

**Key finding**: TOAST I/O accounts for 99%+ of `heap_scan` time on every
DeltaXAgg query. The companion table heap pages themselves load in ~1-2ms
per partition. See appendix for full data.

### Step 1: Schema changes in compress.rs

Modify companion table creation (`compress_partition`) to create two tables:
- `<partition>_meta`: all metadata columns (no BYTEA)
- `<partition>_blobs`: `(_col_idx, _segment_id, _data)`

Update the INSERT path:
- Phase 1: compress segments sequentially, INSERT metadata immediately,
  buffer compressed blobs in memory
- Phase 2: after all segments are compressed, INSERT blobs in column-major
  order (`_col_idx` ascending, then `_segment_id` ascending within each column)
- Run ANALYZE on both tables

### Step 2: Read path changes in segments.rs

Split `load_segments_heap` into two functions:

- `load_segment_metadata(meta_oid, ...)` → scan metadata table, apply
  pruning, return `(Vec<SegmentMetadata>, skipped, minmax_skipped)` where
  `SegmentMetadata` contains segment_id, segment_by values, row_count,
  min/max, sums.

- `load_segment_blobs(blob_oid, segment_ids, needed_cols, col_names, ...)` →
  read blobs for needed columns for surviving segments. Return blobs indexed
  by (segment_idx, col_idx).

Assemble `SegmentData` from metadata + blobs.

### Step 3: Update callers in agg.rs and decompress.rs

Update all `load_segments_heap` call sites to use the new two-phase API.
The callers already separate metadata usage from blob usage, so the
refactoring is mechanical.

### Step 4: Catalog changes

Update `catalog.rs` to track both table OIDs (meta + blobs) per partition.
Update `deltax_partition_info` to report the new schema.

### Step 5: Decompression/cleanup path

Update `deltax_decompress_partition` to drop both tables.
Update the background worker's `drop_after` logic if applicable.

## Alternatives Considered

### CLUSTER after segment-major insertion
Instead of buffering blobs for column-major insertion, insert in segment
order (natural compression order) and then run `CLUSTER blobs USING pkey`
to rewrite the heap + TOAST in column-major order. Rejected because CLUSTER
writes the data twice (2× write amplification) and takes
`AccessExclusiveLock`. Column-major insertion achieves the same physical
layout with no extra I/O.

### One table per column
Perfect I/O locality but creates N tables per partition (105 × 7 = 735
tables for ClickBench). Management overhead is high, catalog bloat affects
planning time, and DDL operations (DROP PARTITION) become expensive.

### Column-chunk concatenation
Store all segments' data for one column in a single large BYTEA with an
offsets array. Perfect locality (one TOAST detoast per column) but:
- Cannot skip individual segments (must read entire column blob)
- Any modification requires rewriting the entire blob
- Maximum BYTEA size is 1 GB (could be hit for large columns)

### posix_fadvise prefetching
Tested and found ineffective on gp2 EBS. The prefetch reads 100× more data
than needed for selective queries. On high-IOPS storage (NVMe), the random
reads aren't a bottleneck in the first place.

### Separate files (outside PostgreSQL)
Would give full control over I/O layout but breaks PostgreSQL replication,
pg_dump, and crash recovery. Incompatible with the requirement that standard
PostgreSQL replication must work.

## Open Questions

1. **TOAST chunk size**: PostgreSQL's default TOAST chunk size is ~2000 bytes.
   For blobs that are 50-100 KB, this means 25-50 chunks per blob. We could
   investigate `toast_tuple_target` to reduce chunk overhead, but this is a
   minor optimization.

2. **Blob table without TOAST**: If we ensure blob sizes stay under ~2 KB
   (by chunking at our level), we could use `ALTER COLUMN _data SET STORAGE
   MAIN` to prevent TOAST entirely. This eliminates the TOAST index lookup
   overhead but requires managing our own chunking. Worth investigating if
   TOAST lookup overhead is significant.

3. **Insertion order durability**: TOAST physical layout depends on insertion
   order, which PostgreSQL does not formally guarantee across restarts or
   `VACUUM FULL`. In practice, since compressed data is write-once (immutable
   after compression) and never updated/deleted, the layout should be stable.
   `VACUUM` won't reorder existing pages. However, if we ever need to
   re-guarantee ordering (e.g., after a pg_dump/restore), we could add a
   `CLUSTER` as a repair step.

4. **Backward compatibility**: Existing compressed partitions use the old
   single-table layout. We need a migration path or version flag to handle
   both layouts during the transition period.

## Appendix: Full Cold Run Measurements

Measured on r7i.4xlarge, gp2 500GB EBS, ClickBench 100M rows, PostgreSQL 18.
Each query run after `systemctl restart postgresql && echo 3 > /proc/sys/vm/drop_caches`.

Sorted by detoast % of total execution time (descending).

### DeltaXAgg Path (32 queries — detoast instrumented)

| Query | Total (s) | heap_scan (ms) | detoast (ms) | detoast % of heap_scan | detoast % of total | Description |
|-------|-----------|----------------|--------------|------------------------|--------------------|-------------|
| Q10 | 12.5 | 12266 | 12239 | 100% | 98% | MobilePhoneModel, COUNT(DISTINCT UserID) WHERE MobilePhoneModel <> '' |
| Q11 | 14.7 | 14401 | 14371 | 100% | 98% | MobilePhone+Model, COUNT(DISTINCT UserID) |
| Q7 | 3.3 | 3155 | 3131 | 99% | 96% | AdvEngineID, COUNT(*) WHERE <> 0 |
| Q21 | 30.1 | 28709 | 28679 | 100% | 95% | SearchPhrase, MIN(URL), COUNT(*) WHERE URL LIKE google |
| Q9 | 21.7 | 20482 | 20452 | 100% | 94% | 5 aggs GROUP BY RegionID, COUNT(DISTINCT UserID) |
| Q22 | 53.4 | 50340 | 50308 | 100% | 94% | SearchPhrase+URL+Title, 5 aggs, 3 LIKE filters |
| Q14 | 16.9 | 15664 | 15637 | 100% | 93% | SearchEngineID+SearchPhrase, COUNT(*) |
| Q8 | 17.8 | 16396 | 16367 | 100% | 92% | RegionID, COUNT(DISTINCT UserID) |
| Q17 | 21.1 | 19394 | 19365 | 100% | 92% | UserID+SearchPhrase, COUNT(*) LIMIT |
| Q12 | 12.4 | 11350 | 11323 | 100% | 91% | SearchPhrase, COUNT(*) WHERE <> '' |
| Q16 | 21.6 | 19342 | 19314 | 100% | 90% | UserID+SearchPhrase, COUNT(*) ORDER BY |
| Q15 | 11.5 | 10248 | 10220 | 100% | 89% | UserID, COUNT(*) |
| Q18 | 35.3 | 30932 | 30901 | 100% | 88% | UserID+minute+SearchPhrase, COUNT(*) |
| Q41 | 0.7 | 598 | 590 | 99% | 88% | Filtered: CounterID=62, date range, URLHash match |
| Q33 | 25.1 | 21701 | 21673 | 100% | 86% | URL, COUNT(*) (full table) |
| Q34 | 25.1 | 21714 | 21686 | 100% | 86% | 1+URL, COUNT(*) (full table) |
| Q1 | 3.7 | 3142 | 3119 | 99% | 84% | COUNT(*) WHERE AdvEngineID <> 0 |
| Q38 | 0.6 | 510 | 502 | 98% | 84% | Filtered: CounterID=62, date+flags, URL GROUP BY |
| Q20 | 26.3 | 21725 | 21697 | 100% | 83% | COUNT(*) WHERE URL LIKE google |
| Q37 | 0.5 | 407 | 399 | 98% | 80% | Filtered: CounterID=62, date range, Title |
| Q36 | 0.6 | 502 | 494 | 98% | 79% | Filtered: CounterID=62, date range, URL |
| Q5 | 14.5 | 11347 | 11319 | 100% | 78% | COUNT(DISTINCT SearchPhrase) |
| Q30 | 35.7 | 27646 | 27615 | 100% | 77% | SearchEngineID+ClientIP, 3 aggs, WHERE SearchPhrase <> '' |
| Q31 | 46.8 | 35414 | 35382 | 100% | 76% | WatchID+ClientIP, 3 aggs, WHERE SearchPhrase <> '' |
| Q32 | 36.8 | 27937 | 27905 | 100% | 76% | WatchID+ClientIP, 3 aggs (full table) |
| Q13 | 26.2 | 19355 | 19326 | 100% | 74% | SearchPhrase, COUNT(DISTINCT UserID) |
| Q40 | 0.8 | 571 | 564 | 99% | 74% | Filtered: CounterID=62, TraficSourceID IN, RefererHash= |
| Q42 | 0.5 | 388 | 380 | 98% | 74% | Filtered: CounterID=62, narrow date, DATE_TRUNC agg |
| Q27 | 33.4 | 24003 | 23976 | 100% | 72% | CounterID, AVG(length(URL)), HAVING >100K |
| Q4 | 15.6 | 10187 | 10160 | 100% | 65% | COUNT(DISTINCT UserID) |
| Q28 | 33.1 | 20788 | 20761 | 100% | 63% | REGEXP_REPLACE(Referer), AVG(length), HAVING >100K |
| Q35 | 32.6 | 9310 | 9284 | 100% | 29% | ClientIP expressions, COUNT(*) (agg-heavy) |

### DeltaXAgg — Sum/Count Pushdown (3 queries — no TOAST, uses metadata only)

| Query | Total (s) | heap_scan (ms) | Description |
|-------|-----------|----------------|-------------|
| Q2 | 0.1 | 101 | SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) |
| Q3 | 0.1 | 99 | AVG(UserID) |
| Q29 | 0.2 | 101 | 90 × SUM(ResolutionWidth + N) |

### DeltaXCount / DeltaXMinMax (2 queries — metadata only)

| Query | Total (s) | heap_scan (ms) | Description |
|-------|-----------|----------------|-------------|
| Q0 | 0.1 | 82 | COUNT(*) |
| Q6 | 0.1 | 100 | MIN(EventDate), MAX(EventDate) |

### DeltaXDecompress Path (6 queries — detoast not yet instrumented separately)

| Query | Total (s) | heap_scan (ms) | Description |
|-------|-----------|----------------|-------------|
| Q19 | 8.6 | 8054 | WHERE UserID = specific value (point lookup) |
| Q23 | 0.9 | 604 | WHERE URL LIKE google ORDER BY EventTime LIMIT 10 |
| Q24 | 0.4 | 241 | WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 10 |
| Q25 | 16.9 | 11335 | WHERE SearchPhrase <> '' ORDER BY SearchPhrase LIMIT 10 |
| Q26 | 0.3 | 239 | WHERE SearchPhrase <> '' ORDER BY EventTime, SearchPhrase LIMIT 10 |
| Q39 | 1.8 | 922 | Filtered: CounterID=62, CASE expression, multi-GROUP BY |

### Summary

- **32 of 43 queries** go through DeltaXAgg and have detoast instrumentation
- Of those, **29 queries** spend 63-98% of total execution time in TOAST I/O
- Only 3 queries (Q2, Q3, Q29) avoid TOAST entirely via sum/count pushdown
- The median detoast % of total is **86%**
- Total cold-run time across all 43 queries: **~636s**, of which **~520s** is TOAST I/O
