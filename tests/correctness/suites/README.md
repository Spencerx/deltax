# Declarative Suites

This directory is reserved for declarative correctness suites once the curated
and generated query matrix outgrows Python-only definitions.

Expected future shape:

```yaml
name: topn
dataset: tiny_edge
layout: compressed_small_segments
cases:
  - name: desc_limit_with_tiebreaker
    comparator: ordered_exact
    sql: |
      SELECT id, ts, val
      FROM {table}
      ORDER BY val DESC NULLS LAST, id
      LIMIT 10
```

