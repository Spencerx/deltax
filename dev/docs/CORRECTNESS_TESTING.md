# Correctness Testing

pg_deltax should treat plain PostgreSQL as the source of truth. The dedicated
correctness harness lives in `tests/correctness/` and compares a regular
PostgreSQL table against a pg_deltax-managed table loaded with the same logical
rows.

## Objectives

- Verify that compressed scans return the same answers as PostgreSQL.
- Exercise planner and executor paths that are easy to break: quals, Top-N,
  aggregate fast paths, JSON extraction, segment pruning, parallel scans, and
  mixed compressed/uncompressed layouts.
- Keep benchmark correctness checks useful, but avoid making benchmarks the
  only reference tests.
- Make failures easy to reproduce and promote into permanent regression cases.

## Harness Model

Each case has:

- A deterministic dataset.
- A deltax physical layout: partition interval, `segment_by`, `order_by`,
  segment size, compression path, and planner GUCs.
- One SQL statement with a table placeholder.
- A comparison policy.

The harness runs the query twice:

1. Against a plain PostgreSQL table.
2. Against the pg_deltax table.

The result rows are then compared using the case's policy.

## Comparison Policies

- `ordered_exact`: rows and order must match. This is the preferred policy.
- `unordered_exact`: row multiset must match. Use for queries without a
  deterministic `ORDER BY`.
- `limit_ties`: row count and overlap checks for non-unique `ORDER BY ... LIMIT`
  cases where PostgreSQL may legally choose different tied boundary rows.
- `float_tolerant`: ordered comparison with a small tolerance for floating point
  aggregates.

Tests should prefer adding deterministic tie-breakers over using relaxed
comparators.

## Initial Coverage Areas

The first expansion should focus on areas where pg_deltax has custom behavior:

- Qual evaluation: equality/range predicates, `IN`, `BETWEEN`, `IS NULL`,
  `LIKE`, nested boolean expressions, casts, and expression quals.
- NULL semantics: compressed columns, segment-by columns, order-by columns,
  aggregate inputs, and `ORDER BY ... NULLS FIRST/LAST`.
- Top-N: ascending/descending order, multi-column order, ties, filters, and
  projected columns not needed by the first Top-N pass.
- Aggregates: `count(*)`, `count(col)`, `min`, `max`, `sum`, `avg`, grouped
  aggregates, `HAVING`, and metadata-fast-path fallback boundaries.
- Joins: deltax table as outer/inner side, semi joins, anti joins, and
  RTABench-shaped dimension joins.
- JSONB: raw JSONB reads, `->`, `->>`, nested paths, casts, missing paths, type
  mismatches, and `pg_deltax.json_extract_mode` A/B tests.
- Storage/codecs: dictionary text, high-cardinality text, booleans, integers,
  floats, timestamps, repeated values, monotonic values, and segment-boundary
  row counts.

## Dataset Plan

Use deterministic generated datasets before large benchmark datasets:

- `tiny_edge`: small handpicked data with NULLs, ties, and extremes.
- `codec_matrix`: columns designed to trigger different compression codecs.
- `partition_edges`: timestamps exactly at partition boundaries.
- `segment_edges`: row counts around `segment_size - 1`, `segment_size`, and
  `segment_size + 1`.
- `rtabench_synthetic`: expanded version of the current RTABench-shaped fixture.
- `jsonbench_synthetic`: JSON-heavy fixture modeled after JSONBench.
- `wide_clickbench_like`: many columns with mixed types but small row counts.

## Generated Queries

Generated query coverage should be seeded and reproducible. The generator should
combine dimensions such as projection shape, predicate shape, grouping, ordering,
limit/offset, and planner GUCs. When a generated case fails, save enough metadata
to rerun it and then promote the minimized query into a curated suite.

## Running

```bash
make correctness-smoke
make correctness
```

As the suite grows, `correctness-smoke` should remain CI-friendly. Longer
generated or benchmark-derived checks should live behind separate targets such
as `make correctness-fuzz`.
