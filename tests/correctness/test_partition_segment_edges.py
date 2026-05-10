"""Partition and segment edge correctness coverage."""

import pytest

from .datasets import (
    create_partition_segment_edges_direct_backfill_pair,
    create_partition_segment_edges_pair,
)
from .harness import QueryCase, assert_query_case
from .querygen import (
    partition_segment_boundary_cases,
    partition_segment_direct_backfill_cases,
    partition_segment_edge_cases,
    partition_segment_fastpath_cases,
    partition_segment_plan_shape_cases,
)


pytestmark = pytest.mark.smoke

FASTPATH_MODES = (
    ("fastpath_on", "off"),
    ("fastpath_disabled", "on"),
)


@pytest.fixture()
def partition_segment_edges(db):
    return create_partition_segment_edges_pair(db)


@pytest.fixture(params=(1, 2, 5), ids=lambda size: f"segment_size_{size}")
def partition_segment_edges_by_segment_size(db, request):
    return create_partition_segment_edges_pair(
        db,
        deltax_table=f"partition_segment_edges_s{request.param}",
        segment_size=request.param,
    )


@pytest.fixture()
def direct_backfill_partition_segment_edges(db):
    return create_partition_segment_edges_direct_backfill_pair(db)


@pytest.mark.parametrize("case", list(partition_segment_edge_cases()), ids=lambda case: case.name)
def test_partition_segment_edges_match_plain_postgres(partition_segment_edges, db, case):
    plain_table, deltax_table = partition_segment_edges
    assert_query_case(
        db,
        case,
        plain_table=plain_table,
        deltax_table=deltax_table,
    )


@pytest.mark.parametrize(
    "case",
    list(partition_segment_boundary_cases()),
    ids=lambda case: case.name,
)
def test_partition_boundary_variants_match_plain_postgres(partition_segment_edges, db, case):
    plain_table, deltax_table = partition_segment_edges
    assert_query_case(
        db,
        case,
        plain_table=plain_table,
        deltax_table=deltax_table,
    )


@pytest.mark.parametrize(
    "case",
    list(partition_segment_plan_shape_cases()),
    ids=lambda case: case.name,
)
def test_partition_plan_shapes_match_plain_postgres(partition_segment_edges, db, case):
    plain_table, deltax_table = partition_segment_edges
    assert_query_case(
        db,
        case,
        plain_table=plain_table,
        deltax_table=deltax_table,
    )


@pytest.mark.parametrize(
    "case",
    [
        QueryCase(
            "all_rows_ordered",
            """
            SELECT id, ts, bucket, payload
            FROM {table}
            WHERE ts >= '2025-01-14 00:00:00+00'
              AND ts < '2025-01-18 00:00:00+00'
            ORDER BY ts, id
            """,
        ),
        QueryCase(
            "filtered_boundary_topn",
            """
            SELECT id, ts, bucket, val, payload
            FROM {table}
            WHERE val IS NULL OR val >= 35
            ORDER BY ts DESC, id DESC
            LIMIT 8
            """,
        ),
    ],
    ids=lambda case: case.name,
)
def test_partition_segment_size_variants_match_plain_postgres(
    partition_segment_edges_by_segment_size,
    db,
    case,
):
    plain_table, deltax_table = partition_segment_edges_by_segment_size
    assert_query_case(
        db,
        case,
        plain_table=plain_table,
        deltax_table=deltax_table,
    )


@pytest.mark.parametrize(
    "case",
    list(partition_segment_direct_backfill_cases()),
    ids=lambda case: case.name,
)
def test_direct_backfill_partition_segment_edges_match_plain_postgres(
    direct_backfill_partition_segment_edges,
    db,
    case,
):
    plain_table, deltax_table = direct_backfill_partition_segment_edges
    assert_query_case(
        db,
        case,
        plain_table=plain_table,
        deltax_table=deltax_table,
    )


@pytest.mark.parametrize(
    "case",
    list(partition_segment_fastpath_cases()),
    ids=lambda case: case.name,
)
@pytest.mark.parametrize("fastpath_mode", FASTPATH_MODES, ids=lambda mode: mode[0])
def test_partition_segment_fastpath_modes_match_plain_postgres(
    partition_segment_edges,
    db,
    case,
    fastpath_mode,
):
    _, guc_value = fastpath_mode
    plain_table, deltax_table = partition_segment_edges
    db.execute(f"SET pg_deltax.disable_meta_agg_fastpath = {guc_value}")
    assert_query_case(
        db,
        case,
        plain_table=plain_table,
        deltax_table=deltax_table,
    )


def test_prepared_time_ranges_match_plain_postgres(partition_segment_edges, db):
    plain_table, deltax_table = partition_segment_edges
    ranges = (
        ("compressed_only", "2025-01-15 00:00:00+00", "2025-01-16 00:00:00+00"),
        ("uncompressed_only", "2025-01-16 00:00:00+00", "2025-01-17 00:00:00+00"),
        ("default_old_only", "2025-01-13 00:00:00+00", "2025-01-14 00:00:00+00"),
        ("mixed_registered", "2025-01-15 18:00:00+00", "2025-01-17 02:00:00+00"),
    )

    db.execute(
        f"""
        PREPARE plain_partition_range(timestamptz, timestamptz) AS
        SELECT id, ts, bucket, val, payload
        FROM {plain_table}
        WHERE ts >= $1 AND ts < $2
        ORDER BY ts, id
        """
    )
    db.execute(
        f"""
        PREPARE deltax_partition_range(timestamptz, timestamptz) AS
        SELECT id, ts, bucket, val, payload
        FROM {deltax_table}
        WHERE ts >= $1 AND ts < $2
        ORDER BY ts, id
        """
    )

    for range_name, lo, hi in ranges:
        plain_rows = db.execute(
            f"EXECUTE plain_partition_range('{lo}'::timestamptz, '{hi}'::timestamptz)"
        ).fetchall()
        deltax_rows = db.execute(
            f"EXECUTE deltax_partition_range('{lo}'::timestamptz, '{hi}'::timestamptz)"
        ).fetchall()
        assert plain_rows == deltax_rows, range_name


def test_partition_edge_join_matches_plain_postgres(partition_segment_edges, db):
    plain_table, deltax_table = partition_segment_edges
    db.execute(
        """
        CREATE TABLE edge_devices (
            device_id integer PRIMARY KEY,
            label text NOT NULL
        )
        """
    )
    db.execute(
        """
        INSERT INTO edge_devices (device_id, label)
        VALUES (0, 'zero'), (1, 'one'), (2, 'two')
        """
    )

    case = QueryCase(
        "partition_edge_join",
        """
        SELECT t.id, t.ts, t.bucket, d.label, t.payload
        FROM {table} t
        JOIN edge_devices d ON d.device_id = t.device_id
        WHERE t.ts >= '2025-01-15 18:00:00+00'
          AND t.ts < '2025-01-19 00:00:00+00'
        ORDER BY t.ts, t.id
        """,
    )
    assert_query_case(
        db,
        case,
        plain_table=plain_table,
        deltax_table=deltax_table,
    )
