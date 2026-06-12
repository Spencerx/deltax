# Transparent DML on Compressed Partitions

Status: P1 (INSERT-only, §4) is implemented — transparent INSERT into
compressed partitions, heap-tail union on every read path
(`DeltaXDecompress`/`DeltaXAppend` Phase 3, exec-time folding in
`DeltaXCount`/`DeltaXMinMax`, plan-time gating + stale-plan guard for
`DeltaXAgg`/Top-N/pathkeys), `deltax_compact_partition()` + worker
compaction with valmap append. P1 deviations from this
design: `DeltaXAgg` bails to plain Agg over heap-tail-aware scans instead of
ingesting the tail (steady state in §4.3 is future work), Top-N disables
instead of merging heap candidates, and compaction takes an
AccessExclusive lock + TRUNCATE instead of `FOR UPDATE SKIP LOCKED` (§4.5
step 1 refinement deferred to P3). Logical-replication origin filtering
(§5.6) is not implemented yet.

P2 (UPDATE/DELETE via option (a), segment-level decompose-on-write, §5) is
implemented: the ExecutorStart interceptor walks every ModifyTable node
(including data-modifying CTEs and MERGE targets), locates candidate
segments with the read path's pruning pipeline
(`dml_candidate_segments` → `load_segments_heap`), decomposes them into
heap rows (`decompose_segments_for_dml`, per-segment refactor of the
decompress loop) under `DML_BYPASS`, bumps the statement snapshot's command
id, and lets the planned heap scan run the DML normally. The whole-segment
direct DELETE (§5.4) is implemented behind three guards (no RETURNING on
that ModifyTable — CTE-level, not just top-level; no row DELETE triggers or
OLD TABLE transition relations on the leaf or the named target; every qual
batch-provable AllPass) with an ExecutorRun hook folding dropped rows into
`es_processed` so command tags stay truthful (top-level statements only —
CTE-deleted rows never count toward the outer tag, matching PostgreSQL). The row trigger no longer rejects UPDATE/DELETE (rows it
sees are by construction heap rows); ON CONFLICT stays rejected.
P2 deviations/details vs this design:
- Locking is an AccessExclusive partition lock (same as compaction) taken
  before the meta deletes, held to end of transaction — concurrent DML on
  the same compressed partition serializes (or one statement is cancelled
  by the deadlock detector; never silent wrongness), and concurrent
  readers block until the writing transaction commits. The meta-row
  delete-first protocol (§5.5) is implemented on top: losers see 0 deleted
  meta rows and skip (READ COMMITTED) or get a serialization error (RR+).
- `pg_deltax.max_segments_decomposed_per_dml` GUC: deferred to P3 (no
  decompose cap; an unprunable UPDATE decomposes the whole partition).
- Segment-id reuse guard: decompose records `max_segment_id` in the
  catalog before deleting meta rows; compaction allocates above it. The
  shared blob/decomp caches and the colstats cache are keyed by
  `(companion_oid, segment_id, ...)`, so id recycling within a companion's
  lifetime would poison them.
- Sentinels/column_minmax are NOT rebuilt on decompose (over-coverage =
  false positives only, §6); valbitmap/_counts/colstats/blooms/
  text_lengths rows are deleted with their segment, keeping live-segment
  metadata exact; catalog row_count/compressed_size are decremented.
- Cross-partition UPDATE works via PG's delete + routed insert (the insert
  lands in the target's loose region per P1).

Provenance note: this document covers the whole DML program and was written
on a longer perf-session branch. Some infrastructure it references lives in
sibling PRs and may not be merged yet — dual-mode segment files
(`STORAGE_V2.md`, the `blob_file` catalog column), partition-level bloom
sentinels (`_segment_id = -1` rows, PERF #47), and per-value count
sidecars. Where those are absent the corresponding interaction rules (§4.4
sentinel fold-in, §6 matrix rows) are vacuously satisfied; whichever PR
lands second must implement them — compaction in `compact_partition_impl`
carries an inline NOTE marking the sentinel fold-in obligation.

Today compressed partitions are read-only: an ExecutorStart hook
(`deltax_executor_start`, `src/scan/hook.rs:4609`) rejects INSERT/UPDATE/DELETE
when a result relation is a compressed partition, and a belt-and-suspenders
row trigger (`deltax_reject_compressed_partition_dml`, `src/lib.rs:247`,
installed by `install_compressed_dml_trigger`, `src/catalog.rs:1057`) catches
tuple-routed inserts that arrive through the parent. To change one row the
user must `deltax_decompress_partition()` (full round-trip: restore all rows
to the heap, drop all six companion tables, unlink the blob file), modify,
and recompress the whole partition.

This document designs **transparent DML**: the user runs plain
INSERT/UPDATE/DELETE and the extension does whatever is needed internally,
with full transactional semantics.

## 1. The mechanics we are working with

Facts about the current implementation that constrain the design:

| Component | Current behavior | Relevance |
|---|---|---|
| Partition heap | `TRUNCATE`d at compress time (`compress.rs:980`); assumed empty forever after | The natural landing zone for new/decomposed rows |
| Scan path | `DeltaXDecompress` **replaces** the SeqScan (`path.rs:348` nulls the pathlist) and reads only companion tables; heap rows would be invisible | Must learn to union heap rows |
| `DeltaXAppend` | Bails if any uncompressed partition heap is non-empty (`hook.rs:4556`) — "any uncompressed data would be silently dropped" | Must include per-partition heap tails |
| `DeltaXCount` / `DeltaXMinMax` / `DeltaXAgg` | Computed purely from `_meta._row_count` / `_colstats` min-max / decoded segments | Wrong the moment heap rows exist or rows are logically deleted |
| Segment identity | `_meta._segment_id` is a `SERIAL` PK; one meta row per segment, sidecar rows in `_colstats`, `_blobs`, `_blooms`, `_text_lengths`, `_valbitmap` keyed `(_col_idx, _segment_id)` | A segment is fully described by ordinary, MVCC-visible heap tuples |
| Segment meta scan | Normal `heap_getnext` under `GetActiveSnapshot()` (`segments.rs:2030`) | Deleting a meta row transactionally hides the segment — MVCC for free |
| Blob bytes | TOAST rows in `_blobs`, optionally mirrored in an immutable write-once segment file (`blob_storage = 'dual'`, `segment_file.rs`); per-segment fallback to TOAST when a file index entry is missing | New segments cannot be appended to an existing file; orphaned file entries are harmless |
| Partition bloom sentinels | `_segment_id = -1` rows in `_blooms` (PERF #47). Probe runs as Phase 0pre and, on reject, **skips the colstats probes and meta scan entirely**. Correctness today relies on "companion tables are always built fresh + DML rejected + decompress drops the table" | New segments must be folded into (or invalidate) sentinels; sentinel reject must never skip the heap tail |
| Decompressed / blob caches | Keyed `(companion_oid, segment_id, col_idx)`; invalidation is "companion table is dropped+recreated so keys become unreachable, LRU ages them out" | Holds as long as segment ids are never reused within a companion table's lifetime (SERIAL guarantees this) |
| pg_statistic | Synthesized at compress time from colstats/HLL/valmap; partition has `autovacuum_enabled = off` so PG never overwrites them | Loose rows make stats slightly stale; refresh on compaction |
| Background worker | 60s cycle: drain default, premake, `auto_compress_partitions`, stats merge, auto-drop. Skips replicas. No recompress/merge path exists | The obvious home for compaction |

## 2. Prior art: TimescaleDB

TimescaleDB's compressed chunks are the closest production system and validate
the overall shape:

- **INSERT (2.3+):** rows land in the original uncompressed chunk heap next to
  the compressed companion table; the chunk is flagged "partially compressed";
  scans plan an Append of the decompress node plus a plain heap scan; a
  recompression job folds loose rows back into batches (segmentwise
  recompression decompresses only affected batches).
- **UPDATE/DELETE (2.11+): decompress-on-write.** Segmentby and orderby
  min/max predicates locate candidate batches; matching batches are
  decompressed into the chunk heap, the compressed batch tuple is **deleted**,
  then the ordinary UPDATE/DELETE runs over row-form tuples. Granularity is
  the whole ~1,000-row batch.
- **MVCC:** batches are ordinary heap tuples, so delete-batch + insert-rows +
  DML is plain PostgreSQL transactionality. Concurrency serializes on the
  batch tuple (REPEATABLE READ gets "could not serialize access" on
  conflict). A GUC caps tuples decompressed per DML transaction (2.14).
- **Pain points & fixes:** write amplification (1-row update rewrites a
  batch), dead-tuple bloat from decompress-then-delete (issue #6196),
  tuple filtering during decompression (2.16, up to 500x faster DML), direct
  whole-batch DELETE without decompression when predicates provably cover the
  batch (2.17/2.21), per-batch bloom pruning for DML (2.27).

Notably, Timescale did **not** ship delete-bitmaps; they doubled down on
decompose-on-write and spent subsequent releases shrinking how much gets
decomposed. That matches our analysis below.

## 3. Design overview

Two cooperating mechanisms, mirroring the storage's existing two-tier shape:

1. **Loose-row region (INSERT, P1).** The partition heap — empty today — holds
   newly inserted rows alongside the segments. Every scan node unions
   "segments ∪ heap tail". The worker periodically compacts loose rows into
   new segments.
2. **Decompose-on-write (UPDATE/DELETE, P2).** The ExecutorStart interceptor
   locates the candidate segments via existing pruning machinery (minmax,
   blooms, valbitmap), decompresses *just those segments* into the partition
   heap, deletes their meta+sidecar rows, and lets PostgreSQL's normal DML run
   over ordinary heap tuples. The worker recompacts later (P3 policy).

The unifying invariant, which the whole scan/agg architecture already depends
on, is preserved: **a live segment's metadata is exact** — `_row_count`,
colstats min/max/sum, blooms, valbitmaps always describe exactly the rows in
that segment. Rows are either fully in a segment or fully in the heap; never
half-and-half.

## 4. P1: INSERT into compressed partitions

### 4.1 Write path

Cheapest possible: drop the insert rejection (remove INSERT from the row
trigger and from the ExecutorStart check) and let tuple routing put the row in
the partition heap. Indexes defined on the partition are maintained normally
for loose rows. No companion table is touched at insert time. COPY and
multi-row INSERT work unchanged.

Caveat to document: unique constraints are only enforced among loose rows and
pre-compression data that had index entries — segment rows are not in any
index, so a duplicate of a compressed row is not detected. (Timescale solved
this in 2.11/2.27 with decompress-on-conflict + bloom-assisted checks; that is
explicitly out of scope until P3+.)

### 4.2 Scan path: the heap tail

`DeltaXDecompress` gains a **Phase 3 heap tail**: after the last segment, open
the partition heap with `table_beginscan` under the active snapshot and emit
tuples through the existing `ExecQual`/`ExecProject` path (batch quals don't
apply; rows are already row-form — plain qual evaluation is fine for what is
expected to be a small tail). The pathlist replacement at `path.rs:348` stays;
the custom scan simply becomes responsible for both sources. `DeltaXAppend`
does the same per child instead of bailing.

Doing the union *inside* the node (rather than planning an Append of
CustomScan + SeqScan) is deliberate: every aggregate pushdown node can then
reuse one helper, Top-N can merge heap candidates into its pass-1 candidate
set, and EXPLAIN keeps a single node with a `heap_rows=N` counter.

Cheapness gate: `relation_heap_is_empty()` (`hook.rs:4507`) already exists and
is the right test — zero heap blocks means Phase 3 is free. Compaction ends
with `TRUNCATE` of the loose region (see 4.5), restoring the zero-block state.

### 4.3 Aggregate shortcuts

| Node | Change |
|---|---|
| `DeltaXCount` | `sum(_meta._row_count) + count(heap tail)` — heap counted at exec time under the query snapshot |
| `DeltaXMinMax` | fold heap-tail values into the metadata-derived min/max |
| `DeltaXAgg` | feed heap-tail rows through the existing accumulators after the segment loop (row-form ingest; no vectorization needed for a small tail) |
| Top-N two-pass | pass 1 also collects (heap, ctid, time) candidates; pass 2 fetches winners by ctid |
| `classify_meta_quals` fast paths (Phase 0a/0b) | unchanged for the segment side; heap tail is always scanned regardless of meta-qual classification |

First implementation may instead *disable* a shortcut when the heap is
non-empty (correct, slower); the table above is the steady state. Disabling is
acceptable only for `DeltaXMinMax`/`DeltaXCount`; `DeltaXAgg` must learn the
heap tail immediately or parent-level aggregates silently regress to row paths
whenever any partition has one loose row.

### 4.4 Bloom sentinel correctness

New rule replacing PERF #47's "fresh-build-only" invariant:

> **Sentinels cover live segments only. A sentinel reject skips the segment
> side (colstats probes + meta scan) but never the heap tail.**

Phase 0pre therefore short-circuits to Phase 3 instead of returning empty.
Loose rows need no sentinel maintenance at insert time — they are always
scanned.

When compaction creates new segments (4.5), each affected column's sentinel is
**folded into**, not rebuilt: sentinels store positions as `hash % 2^n`
(OR-halving fold), so inserting new value hashes at `hash % stored_size`
directly is exactly fold-compatible — no false negatives. If the updated
density exceeds `PARTITION_BLOOM_MAX_DENSITY` (0.6) the sentinel row is
deleted (saturated filters prune nothing; same self-pruning rule as build
time). If anything about the fold is in doubt, deleting the sentinel rows for
affected columns is always safe — false positives only.

### 4.5 Compaction (worker)

New worker step after `auto_compress_partitions`: for each compressed
partition whose heap is non-empty and crosses a threshold (P3 policy, §8),
in one transaction:

1. Read loose rows `FOR UPDATE SKIP LOCKED` (rows under concurrent DML stay
   loose until the next cycle), sorted by `segment_by + order_by`.
2. Reuse the existing `flush_segment` path to append new segments: fresh
   `_segment_id`s from the SERIAL sequence, full sidecar rows (colstats,
   blobs, blooms, text_lengths, valbitmap), **TOAST-only** — no blob-file
   append (the file is write-once; per-segment TOAST fallback in
   `segment_file.rs` already handles file-index misses).
3. Fold new values into partition bloom sentinels (4.4) and append any new
   low-cardinality values to `column_valmap` — appending keeps every existing
   valbitmap valid (missing trailing bits read as 0 = absent, which is
   correct). If a column's value count outgrows the valmap limits, drop its
   valbitmap rows + valmap entry.
4. Delete the compacted heap rows.
5. Refresh catalog `row_count`/sizes and re-run
   `stats::analyze_partition_from_catalog` so pg_statistic reflects the new
   segments; the worker's existing parent-stats merge picks it up.

Compaction may produce undersized trailing segments; merging small segments is
a P3 concern, not a correctness one.

## 5. P2: UPDATE / DELETE

### 5.1 Options considered

**(a) Segment-level decompose-on-write.** Locate candidate segments via
minmax + blooms + valbitmap; decompress only those into the partition heap;
delete their meta + sidecar rows; let the planned heap scan + ModifyTable
proceed normally. Worker recompacts later.

**(b) Delete-bitmap sidecar.** A `_delmap(_segment_id, _bits)` companion row
per touched segment; scans AND the bitmap into the selection vector; DELETE =
set bits; UPDATE = set bits + insert new version into the heap.

| | (a) decompose-on-write | (b) delete bitmap |
|---|---|---|
| MVCC / transactionality | **Free.** Meta-row delete + heap inserts + user DML are one transaction over ordinary tuples; rollback restores the segment untouched | Bitmap row is MVCC-visible, but the *combination* (bitmap version + heap row version) must be read consistently; UPDATE writes two places whose visibility must always agree |
| Scan-path cost | Zero for untouched partitions; touched segments become heap rows until recompaction | Every scan of every segment must probe `_delmap` forever (one PK probe + AND per segment); cost paid by all readers to benefit writers |
| Metadata-exactness invariant | **Preserved** — live segments stay exact | **Broken.** `_row_count`, min/max, `_sum`, `_nonnull_count`, ndistinct, valbitmaps, blooms, text-length sidecars all over-count when bits are set. min/max/sum cannot be "subtracted" without decompressing — so `DeltaXCount`, `DeltaXMinMax`, `DeltaXAgg`, Top-N, the Phase 0a/0b fast paths, and planner stats must all detect marks and bail or consult bitmaps. This is the entire performance architecture of the extension |
| Write amplification | High: 1-row update decomposes a 30,000-row segment (30x Timescale's batch size). Mitigations: candidate pruning usually hits few segments; whole-segment direct DELETE (5.4); future segment split | Low: one bitmap row write per touched segment |
| Concurrency | Serializes on the segment's meta row (delete vs delete); after decompose, normal per-tuple row locks | Serializes on the bitmap row — *every* DML pair touching the same segment conflicts, even on disjoint rows; read-modify-write merge of bits under contention |
| blob_file (storage v2) | File untouched; deleted segment's bytes become unreachable garbage, reclaimed at recompress (file is immutable, holes unsupported — by design) | Same (bitmaps live in a heap table), slight edge |
| Crash safety | WAL'd heap operations only | WAL'd heap operations only |
| Code surface | ExecutorStart interceptor + single-segment decompose (refactor of existing `decompress_partition_inner` loop) + sidecar deletes | New sidecar table + bitmap consult in segment load, batch-qual eval, all five custom nodes, agg pushdown, Top-N, count/minmax, stats synthesis, valbitmap semantics, EXPLAIN |
| Long-term | Matches Timescale's proven trajectory; improvements are localized (prune better, decompose less) | Bitmaps never go away; compaction must additionally rewrite marked segments to reclaim |

**Recommendation: (a) decompose-on-write.** The deciding factor is the
metadata-exactness invariant: pg_deltax derives far more from per-segment
metadata than a row-store does (count/minmax/agg pushdown, Phase 0 fast
paths, sentinel pruning, synthesized pg_statistic), and option (b) poisons
every one of those paths with a "but check the delete bitmap" qualifier — a
permanent correctness tax on the read side, which is the side this extension
exists to make fast. Option (a) costs more per write but writes to compressed
partitions are, by the product's own definition (compress_after), rare and
cold. Timescale's history is direct evidence that (a) is shippable and that
its weaknesses (amplification) are fixable incrementally.

### 5.2 Chosen flow

In `deltax_executor_start`, replace the error path for CMD_UPDATE/CMD_DELETE
(and CMD_INSERT ... ON CONFLICT later) with:

1. For each compressed result relation (leaf rels appear in
   `resultRelations` for both direct and parent-targeted DML; runtime-pruned
   leaves that survive are handled identically):
2. Extract pushable predicates from the plan's qual tree — reuse
   `extract_segment_filters` + the batch-qual extraction walker against the
   ModifyTable child scan's quals.
3. Run the existing Phase 0pre/0a/0b machinery to produce candidate segment
   ids: sentinel probe, colstats minmax, per-segment blooms, valbitmap. No
   pushable quals ⇒ all segments are candidates (document the perf cliff, add
   a `pg_deltax.max_segments_decomposed_per_dml` guard GUC like Timescale's
   2.14 limit, default generous, 0 = unlimited).
4. Under `DML_BYPASS`, for each candidate segment: decompress all columns
   (single-segment refactor of the `decompress_partition_inner` per-segment
   loop) and insert the rows into the partition heap; then `DELETE FROM
   _meta WHERE _segment_id = X` plus the sidecar rows in `_colstats`,
   `_blobs`, `_blooms` (segment rows only, not sentinels), `_text_lengths`,
   `_valbitmap`.
5. `CommandCounterIncrement()` + `UpdateActiveSnapshotCommandId()` so the
   already-taken statement snapshot sees the decomposed rows (the same
   mechanism Timescale uses before letting ModifyTable run).
6. Fall through to `standard_ExecutorStart`; the planned SeqScan/IndexScan
   over the (formerly empty) heap now finds the rows; UPDATE/DELETE proceeds
   with ordinary ctids, firing user triggers, RLS, and RETURNING normally.

The row-level rejection trigger is removed entirely once this ships (the
interceptor is the single enforcement point; `DML_BYPASS` keeps internal
compress/decompress working during the transition). Prerequisite check: the
pathlist hook must never have replaced a DML target scan with a CustomScan
(CustomScan can't supply target-rel ctids) — today this is moot because DML
was rejected; P2 must add an explicit `root->parse->resultRelation` guard.

Cross-partition UPDATE (row moves to another partition) works for free: PG
turns it into delete + routed insert, and the routed insert lands in the
target's loose region per P1.

### 5.3 MVCC, transactionality, crash safety

All effects — heap inserts of decomposed rows, meta/sidecar deletes, the user
DML itself — are ordinary WAL-logged heap operations in one transaction:

- **Abort:** every tuple version vanishes; the segment's meta row was never
  visibly deleted; concurrent readers never saw an intermediate state. The
  blob/decomp caches stay valid because the segment's bytes never changed.
- **Concurrent readers:** a snapshot taken before the DML commits sees the
  meta row (segment alive) and not the decomposed heap rows; after commit, the
  reverse. No reader can see both (same-transaction visibility) or neither
  (the meta delete and heap inserts commit atomically).
- **Crash:** standard WAL recovery; no extension-side state machine. The
  immutable blob file is untouched by P2, so there is nothing non-WAL'd to
  repair (orphaned file entries are dead weight until recompress).

### 5.4 Direct whole-segment DELETE (fast path)

When the statement is a DELETE and the pushed-down predicates provably cover a
candidate segment in full (e.g. time range `[min,max] ⊆` deleted range, with
no residual quals), skip decompose entirely: delete the meta + sidecar rows.
This is Timescale's 2.17/2.21 trick and removes both the amplification and the
dead-tuple bloat for the dominant bulk-retention pattern. `TRUNCATE`/`DROP` of
whole partitions continue to work as today.

### 5.5 Locking and concurrency

- Decompose serializes naturally on the `_meta` row delete. In READ COMMITTED,
  a second transaction targeting the same segment blocks, then finds the meta
  row gone (0 rows from its DELETE) — it must then re-run its candidate scan
  (the rows are now loose heap rows; its planned heap scan handles them via
  EvalPlanQual like any concurrent-update case). Implementation: perform the
  meta delete *first* with `SPI` and treat "0 rows deleted" as "segment
  already decomposed by someone else — skip its decompose step".
  In REPEATABLE READ/SERIALIZABLE this surfaces as a serialization error,
  matching Timescale and vanilla PG semantics.
- Compaction vs concurrent DML: compaction's `FOR UPDATE SKIP LOCKED` read
  (4.5) means it never waits on in-flight writers and never compacts a row
  mid-update. Compaction vs decompose on the same partition: compaction only
  touches loose rows + *inserts* new segments; decompose only deletes
  *existing* segments — disjoint except for sentinel-row updates, which both
  sides do via short row-level UPDATE/DELETE. A per-partition advisory lock
  (`pg_try_advisory_xact_lock`) around compaction keeps the worker from
  stacking up behind itself; DML never takes it.

### 5.6 Replicas

- **Physical standbys:** every P1/P2 effect is WAL'd heap data; standbys see
  consistent state. The blob file is not WAL'd (existing dual-mode caveat);
  since decompose never edits the file and compaction writes TOAST-only, the
  standby's TOAST fallback keeps working unchanged. The worker already skips
  replicas, so no compaction runs there.
- **Logical replication:** loose-row INSERTs and user UPDATE/DELETEs replicate
  as normal row changes — strictly better than today. Two hazards:
  1. *Decompose noise:* the decomposed-row inserts + meta deletes are internal.
     Companion tables are typically not in the publication (Scenario 1 in
     `tests/test_logical_replication.py`), so meta deletes don't leak; but the
     decomposed heap-row INSERTs *do* replicate, followed by the user DML —
     the subscriber (which holds the rows uncompressed or compressed on its
     own schedule) would duplicate them. 
  2. *Compaction deletes:* compaction's heap-row DELETEs would erase
     subscriber rows — same hazard class as the compress-time TRUNCATE that
     Scenario 1 already excludes from publication.
  Mitigation for both: maintenance/decompose transactions set a replication
  origin (`deltax_internal`); Scenario-1 subscriptions use `origin = NONE`
  (PG16+) so origin-tagged changes are filtered while user DML (origin-less)
  flows. Scenario 2 (companion schema replicated wholesale) is consistent
  as-is — both sides see the same meta/sidecar/heap mutations — but the
  decomposed-row inserts must then *not* be origin-filtered; the two scenarios
  need distinct documented publication recipes, and
  `test_logical_replication.py` grows cases for DML-on-compressed under each.

## 6. Sidecar / cache interaction matrix

| Component | INSERT (P1) | Decompose (P2) | Compaction (P3) |
|---|---|---|---|
| `_meta` row counts | untouched; `DeltaXCount` adds heap count at exec | rows deleted with segment — counts stay exact | new exact rows |
| `_colstats` min/max/sum | untouched | deleted with segment | new exact rows |
| Per-segment blooms / text-lengths / valbitmap | untouched | deleted with segment | new rows per new segment |
| Partition bloom sentinels | none (heap tail always scanned) | none (over-coverage ⇒ false positives only — safe) | fold-in new hashes; drop row if density > 0.6 |
| `column_valmap` / HLL / MCV catalog stats | stale w.r.t. loose rows (acceptable; planner-only) | stale (over-estimate; safe) | valmap appended (old bitmaps stay valid); stats re-synthesized |
| pg_statistic | slightly stale; partition autovacuum stays off | slightly stale | `analyze_partition_from_catalog` re-run + parent merge |
| Decomp/blob caches `(oid, seg_id, col)` | unaffected | dead ids unreachable, LRU evicts; **invariant: segment ids never reused** (SERIAL — holds) | new ids; no invalidation needed |
| `MAPPED_FILE_CACHE` / blob file | unaffected | file untouched; deleted segments = dead bytes until recompress | new segments TOAST-only; file untouched |
| Full recompress (`deltax_compress_partition` after decompress) | unchanged: still drops + recreates everything, rewrites blob file, rebuilds sentinels — the GC of last resort | | |

## 7. Phasing

**P1 — INSERT-only (~2–3 weeks).** Remove INSERT rejection; heap tail in
`DeltaXDecompress`/`DeltaXAppend`; extend `DeltaXCount`/`DeltaXMinMax`/
`DeltaXAgg`/Top-N (or gate the first two); sentinel covers-segments-only rule;
worker compaction reusing `flush_segment` + sentinel fold-in + valmap append +
stats refresh; integration tests incl. logical replication recipes. Biggest
user value (late-arriving data into compressed history) for the smallest
mechanism.

**P2 — UPDATE/DELETE via decompose-on-write (~3–4 weeks).** ExecutorStart
interceptor; qual reuse for candidate location; single-segment decompose
refactor; meta-delete-first concurrency protocol; CCI/snapshot dance;
`max_segments_decomposed_per_dml` GUC; whole-segment direct DELETE; remove the
rejection trigger; correctness suite (concurrent DML, rollback, RR/serializable,
RETURNING, triggers, cross-partition UPDATE).

**P3 — recompaction policy (~2 weeks).** Trigger thresholds: loose rows ≥
`segment_size`, or ≥ N% of partition row_count, or oldest loose row older than
a `compact_after` interval — whichever first. Churn guard: skip partitions
with write activity in the last cycle (hysteresis), so steady writers aren't
compacted into a stream of tiny segments that immediately re-fragment; merge
undersized segments opportunistically; blob-file garbage ratio metric +
recommendation to recompress. Later/optional: unique-constraint conflict
checking against segments (bloom-assisted), direct-compress on insert.

## 8. Open questions

- Should Phase 3 heap-tail rows flow through batch quals via a row-form shim
  for very large tails (e.g. right after a big decompose), or is plain
  `ExecQual` always fine? Measure before optimizing.
- Decompose emits up to 30k rows × N segments into the heap inside the user's
  transaction — memory is bounded (per-segment batching exists in the
  decompress path) but dead-tuple bloat after the DML commits argues for
  compaction prioritizing recently decomposed partitions.
- `ON CONFLICT` / UPSERT semantics on compressed partitions: reject until P3+,
  or silently treat segments as conflict-invisible? Rejecting is honest;
  silent is wrong. Reject.
- Parallel-worker scans (`pg_deltax.parallel_workers`): heap tail should be
  scanned by exactly one worker; simplest is leader-only Phase 3.

---

## P2.5 — Tombstone fast layer (added 2026-06-12, product requirement)

**Requirement (Alexis):** DML latency on compressed partitions must be
near-indistinguishable from normal Postgres. Decompose-on-write alone
cannot meet this: even a perfectly-pruned single-row UPDATE decodes a
~30K-row segment (~50–300 ms vs ~1 ms native).

**Architecture:** synchronous tombstones-as-rows + asynchronous rewrite.

- DELETE ⇒ insert `(segment_id, row_offset)` into a per-partition
  `_tombstones` companion table (ordinary heap rows ⇒ MVCC, rollback,
  replication all native). UPDATE ⇒ tombstone + heap-insert of the new
  version. O(1) per affected row, single-digit ms.
- Scan paths: segments with zero tombstones (steady state) are
  untouched — one indexed existence probe gates this. Tombstoned
  segments filter rows by offset (exact) or bail to the row path.
- Metadata fast paths (count/minmax/agg/valcounts): bail or subtract
  exact per-segment tombstone counts; never approximate.
- The P1/P3 compaction worker physically rewrites tombstone-bearing
  segments (decompose → drop dead rows → recompress), restoring
  pristine fast-path state; P2's decompose machinery is the tool.
- Why this differs from the REJECTED option (b) bitmap blobs: blobs
  broke MVCC/exactness; tombstone *rows* are MVCC-native and the
  zero-tombstone gate keeps the steady-state read path tax at one
  index probe per scanned partition (cacheable alongside the heap
  -emptiness check P1 added).

Sequencing: P2 (decompose-on-write) lands first as the correctness
foundation + compactor tool; P2.5 then makes the synchronous path
fast; P3 policy work follows.

### P2.5 implementation status (DELETE-only tombstones)

Implemented for **DELETE only**; UPDATE stays on P2 decompose-on-write.
Honesty check outcome: a tombstone-fast UPDATE must still materialize the
old row versions into the heap for the executor to apply SET expressions /
fire triggers / serve RETURNING, so its synchronous cost is dominated by
decoding **all** columns of the candidate segments and inserting rows — the
same decode the decompose performs. The clean incremental win there
(restore only the matching rows instead of the whole segment) is real but
bounded well below 10x for selective updates and adds a second restore
path; it is deferred. DELETE needs **no materialization at all** — only the
qual columns are decoded once to identify matching offsets — which is where
the order-of-magnitude win lives.

**Write path** (`deltax_executor_start` → `claim_segments_for_tombstone` /
`insert_dml_tombstones`, offsets via `dml_matching_offsets`):

- Eligibility (per statement): DELETE; no RETURNING; no user row DELETE
  triggers (same observation-free guards as the §5.4 whole-segment drop);
  the collected scan quals are PROVABLY the complete predicate AND every
  qual was batch-extracted (`all_quals_handled` — the same exactness
  contract production scans rely on when they null `ps.qual`); no BOOL
  ordering quals (the shared evaluator degrades those to equality).
  Anything else falls back to P2 decompose — never to skipping.
- Whole-segment drops (§5.4) still take precedence for AllPass segments;
  tombstones cover the partially-matching candidates.
- Per surviving candidate: decode ONLY the qual columns (blob cache
  applies), evaluate text quals explicitly + the shared
  `evaluate_batch_quals` (NULL fails, SQL semantics), collect matching
  offsets; segments with zero matches are left untouched (pruning false
  positives now cost a qual-column decode, not a decompose).
- Concurrency: `SELECT ... FOR UPDATE` on the candidates' `_meta` rows
  (ordered by segment_id) — concurrent decompose/compaction of those
  segments waits for our commit and then excludes our tombstones; segments
  already claimed by a concurrent decompose are absent and skipped (the P2
  "0 rows claimed" rule). Readers never block (no AccessExclusive on this
  path). Tombstone inserts use `ON CONFLICT DO NOTHING`: a row concurrently
  tombstoned elsewhere is skipped and not double-counted (READ COMMITTED
  "already deleted" semantics; RR surfaces a serialization error).
- Command tag: inserted-tombstone count folds into `es_processed` via the
  same ExecutorRun hook mechanism as whole-segment drops.
- Catalog `row_count` semantics (decision): it counts **live rows stored in
  segments** and stays exact — decremented by the tombstone insert in the
  same transaction (MVCC-rollback-safe), so the `DeltaXCount` catalog
  shortcut and synthesized planner stats need no tombstone awareness.

**Storage**: per-partition `_deltax_compressed.<part>_tombstones
(_segment_id int, _row_offset int, PRIMARY KEY(_segment_id, _row_offset))`.
Created EAGERLY (empty) at compress time so the OWNER/GRANT cascade covers
it (deviation from "lazily on first tombstone": permission-safety; an empty
table has zero blocks = zero read cost, which is what lazy bought), plus
`CREATE TABLE IF NOT EXISTS` at first tombstone for pre-P2.5 partitions.
Ordinary heap rows ⇒ MVCC / rollback / physical & logical replication all
native.

**Read side** — steady-state gate is `companion_may_have_tombstones`
(syscache probe + relcache nblocks, the same physical-truth trick as P1's
`relation_heap_is_empty`); exact map loaded only when the gate fires,
under the scan's active snapshot (atomic with the `_meta` scan):

- `load_segments_heap` (and the Phase 0a point-lookup path, and parallel
  workers rebuilding segments from the DSM wire) attach per-segment sorted
  offset lists to `SegmentData.tombstones`.
- Decode paths (`DeltaXDecompress`/`DeltaXAppend` main scan + all three
  Top-N variants) seed their selection vectors with
  `tombstone_preselection` — tombstoned rows are filtered exactly during
  decode; Top-N/pathkeys stay enabled (removing rows preserves order).
- `DeltaXCount`: catalog path exact by construction (see above); meta-scan
  fallback sums `live_row_count()` (exact: that path is only planned when
  quals provably cover every row of surviving segments).
- `DeltaXMinMax` and `DeltaXAgg` (metadata fast paths, AllPass shortcuts,
  valcounts GROUP BY) describe physical rows → they BAIL: plan-time gates
  (`any_compressed_rte_heap_nonempty` extended + tombstone check before
  `add_minmax_path`) push those queries to the row path while tombstones
  exist; exec-time stale-plan guards error on the residual race (same
  contract as P1's heap-tail guard; the tombstone writer fires a relcache
  invalidation so cached plans replan). Compaction restores the pushdowns.
- Sentinels/blooms/valbitmaps/colstats/pg_statistic: tombstones only ever
  REMOVE rows, so all existing structures over-cover → false positives
  only → safe unchanged (§6 column for tombstones ≙ the Decompose column).

**Drive-by fix uncovered by the P2.5 tests**: the per-segment / partition
bloom probes hashed the RAW PG-epoch datum for timestamp/date equality
constants while the build side hashes Unix-epoch-encoded values — every
segment was falsely bloom-rejected and `ts_col = const` (or `IN`) returned
zero rows on compressed data. `bloom_probe_encode` (segments.rs) now
converts probe constants into the build domain; `point_ts` joined the
twin-equality query set as the regression net.

**Rewrite**: every rematerialization path excludes tombstoned rows and
consumes their tombstone rows — full `deltax_decompress_partition`, P2
decompose (`restore_segment_rows(skip_offsets)`), and compaction.
`deltax_compact_partition()` + the worker (`auto_compact_partitions`, now
also triggered by a non-empty tombstones table) decompose tombstone-bearing
segments minus dead rows, recompress the survivors into fresh segments, and
`TRUNCATE` the tombstones table back to the zero-block steady state (also
clearing dead pages from rolled-back tombstone DML).

Measured (local Docker arm64, one partition, 60k rows in 2×30k-row
segments with an int + text payload, `\timing`):

| single-row DELETE            | latency  |
|------------------------------|----------|
| plain PostgreSQL (indexed)   | 0.42 ms  |
| P2.5 tombstone (first)       | 0.78 ms  |
| P2.5 tombstone (warm caches) | 0.53 ms  |
| P2 decompose (same predicate, forced via RETURNING) | 91.2 ms |

≈118–173x faster than decompose and within ~2x of native PostgreSQL —
the "near-indistinguishable" requirement. Guarded in CI by
`tests/test_compressed_dml.py::TestTombstoneDelete::
test_single_row_delete_latency` (asserts >10x vs decompose).
