"""Smoke coverage for the correctness harness itself."""

import pytest

from .datasets import create_tiny_events_pair
from .comparators import compare
from .harness import assert_query_case
from .querygen import curated_smoke_cases


pytestmark = pytest.mark.smoke


@pytest.fixture()
def tiny_events(db):
    return create_tiny_events_pair(db)


@pytest.mark.parametrize("case", list(curated_smoke_cases()), ids=lambda case: case.name)
def test_tiny_events_matches_plain_postgres(tiny_events, db, case):
    plain_table, deltax_table = tiny_events
    assert_query_case(
        db,
        case,
        plain_table=plain_table,
        deltax_table=deltax_table,
    )


def test_tiny_events_prepared_statement_matches_plain_postgres(tiny_events, db):
    plain_table, deltax_table = tiny_events
    db.execute(
        f"""
        PREPARE plain_tiny_range(timestamptz, timestamptz, integer) AS
        SELECT id, ts, device_id, kind, val, metric
        FROM {plain_table}
        WHERE ts >= $1
          AND ts < $2
          AND (device_id IS NOT DISTINCT FROM $3 OR val IS NULL)
        ORDER BY ts, id
        """
    )
    db.execute(
        f"""
        PREPARE deltax_tiny_range(timestamptz, timestamptz, integer) AS
        SELECT id, ts, device_id, kind, val, metric
        FROM {deltax_table}
        WHERE ts >= $1
          AND ts < $2
          AND (device_id IS NOT DISTINCT FROM $3 OR val IS NULL)
        ORDER BY ts, id
        """
    )

    params = (
        ("2025-01-15 00:00:00+00", "2025-01-15 00:45:00+00", "1"),
        ("2025-01-15 00:30:00+00", "2025-01-15 01:40:00+00", "NULL"),
    )
    for lo, hi, device_id in params:
        plain_rows = db.execute(
            f"EXECUTE plain_tiny_range('{lo}'::timestamptz, '{hi}'::timestamptz, {device_id})"
        ).fetchall()
        deltax_rows = db.execute(
            f"EXECUTE deltax_tiny_range('{lo}'::timestamptz, '{hi}'::timestamptz, {device_id})"
        ).fetchall()
        comparison = compare("float_tolerant", plain_rows, deltax_rows)
        assert comparison.ok, (lo, hi, device_id, comparison.detail)
