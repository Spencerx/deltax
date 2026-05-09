"""Seeded query generation helpers.

This module is intentionally a placeholder for the next phase. The initial
correctness suite is curated; generated cases should build QueryCase instances
here and keep their seed in the test id / failure output.
"""

from __future__ import annotations

from collections.abc import Iterable

from .harness import QueryCase


def curated_smoke_cases() -> Iterable[QueryCase]:
    yield QueryCase(
        "count_all",
        "SELECT count(*) FROM {table}",
    )
    yield QueryCase(
        "filtered_projection",
        """
        SELECT id, device_id, kind, val
        FROM {table}
        WHERE ts >= '2025-01-15 00:10:00+00'
          AND ts < '2025-01-15 01:10:00+00'
          AND (kind = 'alpha' OR device_id IS NULL)
        ORDER BY ts, id
        """,
    )
    yield QueryCase(
        "grouped_aggregate",
        """
        SELECT device_id, kind, count(*), sum(val), min(val), max(val)
        FROM {table}
        GROUP BY device_id, kind
        ORDER BY device_id NULLS LAST, kind NULLS LAST
        """,
    )
    yield QueryCase(
        "deterministic_topn",
        """
        SELECT id, ts, kind, val
        FROM {table}
        ORDER BY val DESC NULLS LAST, id
        LIMIT 10
        """,
    )
