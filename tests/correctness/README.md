# pg_deltax Correctness Harness

This directory contains the dedicated plain-PostgreSQL-vs-pg_deltax correctness
test harness.

The rule for every test is simple: load identical logical data into a regular
PostgreSQL table and a pg_deltax table, run the same query against both, and
compare the results with an explicit comparison policy.

## Running

```bash
make correctness-smoke
make correctness
```

`correctness-smoke` runs the small deterministic suite in this directory.
`correctness` is the broader entry point; for now it points at the same suite
and is intended to grow as more datasets and query families are added.

## Layout

- `harness.py` executes one query case against the postgres and deltax tables.
- `comparators.py` defines result comparison policies.
- `datasets.py` creates deterministic table pairs.
- `querygen.py` holds curated cases now and seeded generated cases later.
- `suites/` is reserved for declarative query suites once the matrix grows.

## Comparison Policies

- `ordered_exact`: rows and row order must match exactly.
- `unordered_exact`: row multiset must match, order is ignored.
- `limit_ties`: relaxed policy for non-unique `ORDER BY ... LIMIT` cases.
- `float_tolerant`: ordered comparison with small numeric tolerance.

Prefer deterministic SQL and `ordered_exact` when possible. Relaxed policies
should be used only when PostgreSQL is allowed to choose a different but still
valid result, such as boundary rows in a non-unique Top-N query.

## Adding Coverage

Add coverage by introducing a deterministic dataset or a new query family, then
promote interesting generated failures into curated cases. Every failure should
print enough information to reproduce the case without rerunning the generator:
case name, SQL, comparator, row samples, and eventually seed/layout metadata.
