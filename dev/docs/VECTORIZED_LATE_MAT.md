# Late materialization for point-selective scans

> Status: partially shipped + scoped-down proposal. The scan-path and top-N
> forms of late materialization already exist in `main`; this doc records
> what's there, what's genuinely left, and — importantly — why late mat is a
> **modest** win on rtabench Q9–Q13 rather than the order-of-magnitude win an
> earlier draft assumed. Measurement context: `RTABENCH_VS_TIMESCALE.md` and
> the EC2 box (`make -C rtabench query`).

## TL;DR

For point-selective queries, decompress *only* the columns needed to evaluate
the filter, build a selection vector, then decompress remaining needed columns
*only at rows the filter kept*. For pure-counting shapes (`count(*) WHERE …`),
popcount the mask instead of accumulating per row.

This is already done on the **scan path** (`load_next_segment`) and the
**top-N path** (`exec_topn_two_pass`). It is **not** done on the pushed-down
**aggregation path** (`DeltaXAgg`), which still decompresses every needed
column densely.

The catch — confirmed by measurement, see [Why the win is bounded](#why-the-win-is-bounded) —
is that on Q9–Q13 the columns that dominate `decompress` are encoded with
**sequential codecs** (Gorilla, DeltaVarint, Lz4Blocked) and are also the
filter / group / aggregate-input columns that late mat **must decode anyway**.
So late mat cannot skip the expensive decode; it can only skip per-row
*materialization* of rejected rows (cheap for fixed-width types) and the
aggregation accumulation. Realistic gain on these queries: ~10–20% plus a
`count(*)`→popcount win, not the 5–7× an earlier draft projected.

## Update (2026-05-30) — sequential-codec decode sped up (shipped)

Code-level tracing confirmed the agg late-mat path is sub-noise on Q9–Q13
(decode-bound on sequential codecs, which late mat can't skip). So the first
work landed against [the real levers](#the-real-levers-for-q9q13-bigger-than-late-mat),
lever #1 — faster decode — rather than late mat:

- **Gorilla `BitReader`** rewritten with a 64-bit refill buffer (serves any
  ≤57-bit read in one shift/mask vs the old ≤8-bits-per-iteration loop).
  Encoder and on-disk format unchanged. Speeds up `event_created` (all
  queries) and `satisfaction` (Q12/Q13).
- **DeltaVarint decode** uses an inlined `read_varint_at` that indexes the
  buffer directly instead of re-slicing per value. Speeds up `order_id` (the
  filter in all four queries).
- **Bonus fix:** a randomized roundtrip test surfaced a pre-existing
  `encode_timestamps` off-by-one — the delta-of-delta range guards
  (`-63..=64` etc.) didn't match the decoder's two's-complement
  sign-extension, so `dod` values of exactly 64/256/2048 µs were silently
  mis-encoded. Corrected the ranges; backward-compatible.

Measured on EC2 (warm), decompress bucket:

| Q | before | after | Δ |
|----|----|----|----|
| Q9  | 6.47 ms | 4.61 ms | −29% |
| Q11 | 4.72 ms | 3.84 ms | −19% |
| Q12 | 9.57 ms | 6.03 ms | −37% |
| Q13 | 9.46 ms | 6.76 ms | −29% |

Next: extend the same "tighter inner decode loop" treatment to the other
codecs and the datum-materialization step.

## What's already implemented

Three scan executors exist with different maturity. Don't treat them as one
loop.

### Scan path — `DeltaXAppend` (`src/scan/exec/decompress.rs::load_next_segment`)

Already does two-phase late materialization, per segment:

1. **Phase 1** — decompress *filter columns* (any column referenced by a
   `BatchQual`) plus `segment_by`. Other needed columns are pushed onto
   `phase2_cols` and deferred.
2. **Selection vector** — `evaluate_batch_quals` (`batch_qual.rs`) ANDs each
   filter result into a `Vec<bool>` selection vector. Text Eq/Ne/LIKE/IN quals
   are evaluated *during* decode via `decompress_text_blob_with_*_filter`,
   seeding `pre_selection`.
3. **Phase 2** — decompress the deferred columns, and **skip the whole phase
   if no row in the segment survives** (`phase2_skipped`). For TEXT and JSONB,
   `decompress_text_blob_with_selection` / `decompress_jsonb_blob_with_selection`
   skip the per-row varlena `palloc`/`memcpy` for rejected rows. Fixed-width
   deferred columns currently decode densely (see note below).
4. **Emit** — the per-row fill loop reads the selection vector and skips
   rejected rows before `ExecQual` / projection.

### Top-N path — `exec_topn_two_pass` / `exec_topn_text`

A second, independent late-mat implementation: Pass 1 decompresses filter +
sort columns and collects candidates (with row-level early-exit when rows are
time-ordered); Pass 2 sorts, truncates to N, and decompresses the remaining
columns **only for winning segments**. `detoast_lazy_blobs_selective` defers
TOAST detoasting of Phase-2 blobs until a segment wins. `compute_phase1_col_indices`
computes the filter+sort column set.

### Aggregation path — `DeltaXAgg` (`src/scan/exec/agg/`)

**No late materialization.** `agg/serial.rs` and `agg/parallel_*.rs`
decompress *all* needed columns densely into `Vec<Vec<(Datum, bool)>>`,
evaluate batch quals *after*, then filter per row. `count(*)` increments one
row at a time — there is no popcount fast path. The text/jsonb
`_with_selection` helpers exist but are **not called** from the agg path.

### Codec random-access support (`src/compression/`)

| Codec (tag) | Random access ("decode only set rows") |
|----|----|
| `Constant` (7) | Free — one value broadcast |
| `Dictionary` / `DictionaryLz4` (3/9) | After dict materialization, O(1) per index |
| `BooleanBitmap` (5) | Free — bit lookup |
| `ForBitpacked` (8) | O(1) per row, no serial dependency |
| `Lz4` / `Lz4Blocked` (4/6) | **None** — must inflate the whole frame |
| `Gorilla` (1) | **None** — XOR-delta serial dependency |
| `DeltaVarint` (2) | **None** — delta-chain serial dependency |

A masked sparse-decode variant (`decompress_blob_to_datums_masked`) currently
exists only as the text/jsonb `_with_selection` helpers. There is no masked
variant for fixed-width numeric codecs, and `load_next_segment`'s Phase 2 has
an explicit comment that fixed-width types see "no win" from masking because
the per-row datum cost is just an `as usize` cast. That comment is right *about
materialization*; it does not consider skipping *decode* for random-access
codecs (`ForBitpacked`, `Dictionary`) — which is a real but small win on
these queries (see below).

## Why the win is bounded

Measured on EC2 (`make -C rtabench query`, warm), with the codec each hot
column actually uses (read from the `_blobs` companion's tag byte):

| Q | Path | decompress | survivors / scanned |
|----|----|----|----|
| Q9  | `DeltaXAgg`               | 6.47 ms | 5 / ~90k (3 segs) |
| Q10 | top-N two-pass           | —       | already late-mat'd |
| Q11 | `DeltaXAppend`           | 4.72 ms | 3 / 30k |
| Q12 | `DeltaXAgg`               | 9.57 ms | 22 / ~120k (4 segs) |
| Q13 | `DeltaXAppend` + PG GroupAgg | 9.46 ms | 26 / 120k |

Note Q13's FILTER aggregation runs in PostgreSQL on top of a plain
`DeltaXAppend` scan — it is **not** a `DeltaXAgg` query.

Codecs of the `order_events` columns these queries touch:

| Column | Type | Codec | Random access? | Role |
|----|----|----|----|----|
| `order_id` | int | **DeltaVarint** | ❌ | filter (all four) |
| `event_created` | timestamptz | **Gorilla** | ❌ | time-range / group / sort |
| `satisfaction` | real | **Gorilla** | ❌ | agg arg (Q12), FILTER (Q13) |
| `backup_processor` | text | **Lz4Blocked** | ❌ (whole-frame) | FILTER (Q13) |
| `event_type` | text | Dictionary | ✅ | filter (Q9) |
| `counter` | int | ForBitpacked | ✅ | projected (Q11 `SELECT *`) |
| `event_payload` | jsonb | DictionaryLz4 | ✅-ish | projected (Q11) |

The decisive point: late mat **always** decodes the filter / group /
aggregate-input columns — you cannot build the selection vector or compute
`max(satisfaction)` without them — and skips only the *non-filter projected*
columns. On Q9–Q13 the dominant-cost columns (`order_id`, `event_created`,
`satisfaction`, `backup_processor`) are exactly the ones late mat must decode,
and they're all sequential codecs whose decode can't be skipped row-wise.

Consequences, per query:

- **Q9** (`count(*)`): no projected columns to defer at all. The 6.47 ms is
  decoding the three filter columns. The only late-mat win is
  `count`→`mask.count_ones()` replacing the `agg` bucket (0.92 ms). Floor is
  ~5.5 ms, not the ~1 ms an earlier draft claimed.
- **Q12** (`max(satisfaction) … WHERE order_id=…`): all three columns needed,
  two are Gorilla → unskippable sequential decode. Late mat saves only the
  per-row materialization of rejected rows (cheap, pass-by-value) and the
  accumulation. Realistic: ~10–25% (≈7–8.5 ms), not ~2 ms.
- **Q11 / Q13**: genuine but bounded. `counter` (ForBitpacked) and
  `event_type` (Dictionary) projected columns *can* use masked decode; text
  bodies already skip varlena alloc. But `event_created` / `satisfaction`
  (Gorilla) and `backup_processor` (Lz4Blocked) decode in full regardless.

So the honest ceiling for late mat on these queries is roughly the earlier
draft's own *Phase 1* estimate (10–20%), plus the `count(*)` popcount win.

## Work that is actually worth doing

Ordered by leverage-per-effort, given the measurements above.

### 1. Aggregation-path late materialization + `count(*)` popcount

The agg path (`agg/serial.rs`, `agg/parallel_*.rs`) is the one executor with
**no** deferral. Two concrete changes:

- **`count(*)` fast path.** When the only aggregate is `count(*)` (e.g. Q9),
  build the selection vector from the filter columns and return
  `selection.iter().filter(|&&b| b).count()` — never materialize survivor
  columns, never run the per-row accumulation loop. (Keep `Vec<bool>`; popcount
  via `filter().count()` — no `bitvec` dependency, consistent with the existing
  `agg/compact.rs` choice.)
- **Deferral mirror.** Decode filter columns first, build the selection vector,
  then accumulate aggregate inputs over set bits only. For sequential codecs
  this saves the accumulation pass and per-row materialization, not the decode.
  Reuse the scan path's `phase2_cols` partitioning and the `_with_selection`
  text/jsonb helpers (currently unused on the agg path).

Expected: the popcount path is most of Q9's `agg` bucket; the deferral mirror
is a ~10–20% trim on Q12. Modest, but it's the only path with low-hanging fruit
and it improves any future low-selectivity agg query, not just rtabench.

### 2. Masked sparse-decode for random-access codecs

Add `decompress_blob_to_datums_masked` for `Constant`, `Dictionary` /
`DictionaryLz4`, `BooleanBitmap`, `ForBitpacked` and wire it into Phase 2 of
both the scan and agg paths. On rtabench this helps `counter` (ForBitpacked)
and `event_type` (Dictionary) — real, but minor columns here. The bigger
payoff is on workloads where the *projected* (not filter) columns are
random-access and selectivity is low.

Sequential codecs (`Gorilla`, `DeltaVarint`, `Lz4*`) keep the dense path; their
only mask-aware saving is skipping output materialization (worthwhile for
pass-by-ref text via the already-existing `_with_selection`, marginal for
fixed-width).

### Non-goals for the late-mat work

- **BitVec.** An earlier draft proposed switching the selection vector to
  `BitVec`. Keep `Vec<bool>`: popcount is `iter().filter().count()`, and
  `agg/compact.rs` already documents a deliberate choice to avoid the `bitvec`
  dependency. Not worth the churn.
- Compress path is untouched (read-side capability only); existing data stays
  readable; no catalog or on-disk format change; no `extension.control` bump.

## The real levers for Q9–Q13 (bigger than late mat)

These dwarf late mat on the measured queries and deserve their own design
passes. An earlier draft listed them as "out of scope"; on the evidence they
are the main event.

1. **Faster sequential-codec decode.** Gorilla (timestamps, floats) and
   DeltaVarint (ids) decode is the bulk of the 4.7–9.6 ms. A SIMD / batched
   decode pass for these is where the largest single win lives — this is the
   gap to TimescaleDB's Arrow+SIMD decode that survives all the planner work.
2. **Vectorized filter on encoded data.** Evaluate filters on the encoded
   representation where it's comparable: `Dictionary` `event_type` can compare
   dict codes without materializing strings (partly done via
   `decompress_text_blob_with_eq_filter` and `segment_skippable_by_dict`).
   Limited for delta/Gorilla columns, which need reconstruction.
3. **Intra-segment zone maps / finer min-max.** Segment-level min/max already
   prunes to the surviving segments; the remaining waste is *inside* them —
   decoding a 30k-row segment to find 3 matching rows. Sub-segment min-max
   ("zone maps") over the sort/filter columns would let the decoder skip
   row-ranges that can't match, attacking the dominant decode cost directly.
   This composes with late mat but is a separate, larger mechanism.

## Edge cases & gotchas (for the work in §"actually worth doing")

- **Multi-column filter ordering.** Evaluate the cheapest / most selective qual
  first so later columns short-circuit. `evaluate_batch_quals` already ANDs into
  the selection vector; what's new is choosing order (numeric/fixed-width
  first, text/regex last).
- **Non-batch quals.** Predicates not representable as `BatchQual` still run via
  PG's `ExecQual` per row, after late mat. `all_quals_batch_handled` says
  whether the full row must be materialized regardless.
- **NULL handling.** `apply_batch_filter_*` drops nulls (correct for eq/range).
  Audit `IS NULL` / `IS NOT NULL` against the null bitmap, which sits at the
  front of the blob (see `CompressedColumn::to_bytes`) and can be read without
  decoding the payload.
- **Top-N + late mat compose.** Top-N decides *which segments* to visit; late
  mat decides *which columns* to materialize. Already true on the top-N path.
- **Parallel agg.** The selection vector is thread-local per worker/segment —
  the existing segment loop is already per-segment per-worker.

## File touch surface (for §"actually worth doing")

- `src/scan/exec/agg/serial.rs`, `agg/parallel_*.rs` — filter-first deferral +
  `count(*)` popcount fast path (item 1).
- `src/scan/exec/agg/compact.rs` — `count(*)` accumulation bypass.
- `src/scan/exec/datum_utils.rs` — `decompress_blob_to_datums_masked` for
  random-access codecs (item 2).
- `src/compression/{bitpacked,dictionary,boolean}.rs` — masked decode variants
  (item 2).

The sequential-codec speedups, encoded-data filtering, and zone maps in
§"real levers" are separate efforts tracked elsewhere.
