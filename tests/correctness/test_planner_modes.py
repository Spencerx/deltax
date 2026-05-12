"""Planner mode A/B correctness coverage."""

import pytest

from .comparators import compare
from .datasets import (
    create_aggregate_matrix_pair,
    create_partition_segment_edges_pair,
    create_rtabench_synthetic_pair,
)
from .harness import QueryCase, assert_query_case


pytestmark = pytest.mark.smoke


PLANNER_MODES = (
    (
        "serial_scan",
        (
            ("max_parallel_workers_per_gather", "0"),
            ("pg_deltax.max_parallel_workers_per_scan", "0"),
        ),
    ),
    (
        "parallel_auto",
        (
            ("max_parallel_workers", "8"),
            ("max_parallel_workers_per_gather", "4"),
            ("min_parallel_table_scan_size", "0"),
            ("parallel_setup_cost", "0"),
            ("parallel_tuple_cost", "0"),
            ("pg_deltax.max_parallel_workers_per_scan", "-1"),
        ),
    ),
    (
        "parallel_capped",
        (
            ("max_parallel_workers", "8"),
            ("max_parallel_workers_per_gather", "4"),
            ("min_parallel_table_scan_size", "0"),
            ("parallel_setup_cost", "0"),
            ("parallel_tuple_cost", "0"),
            ("pg_deltax.max_parallel_workers_per_scan", "2"),
        ),
    ),
)

FASTPATH_MODES = (
    ("fastpath_on", "off"),
    ("fastpath_disabled", "on"),
)

AGGREGATE_PLAN_CASES = (
    QueryCase(
        "serial_parallel_grouped_aggregate",
        """
        SELECT
            group_key,
            sub_key,
            count(*) AS rows,
            count(int_nullable) AS non_null_ints,
            sum(int_not_null) AS sum_not_null,
            avg(float_val) AS avg_float,
            min(ts) AS first_ts,
            max(ts) AS last_ts
        FROM {table}
        WHERE ts >= '2025-01-15 00:00:00+00'
          AND ts < '2025-01-24 00:00:00+00'
          AND (filter_val BETWEEN -4 AND 7 OR int_nullable IS NULL)
        GROUP BY group_key, sub_key
        HAVING count(*) >= 3
        ORDER BY group_key NULLS LAST, sub_key NULLS LAST
        """,
        comparator="float_tolerant",
    ),
    QueryCase(
        "serial_parallel_filtered_projection",
        """
        SELECT id, ts, group_key, sub_key, device_id, int_nullable, float_val
        FROM {table}
        WHERE (
            group_key IN (1, 3, 5)
            OR device_id IS NULL
            OR int_nullable BETWEEN -2 AND 2
        )
          AND ts >= '2025-01-16 00:00:00+00'
          AND ts < '2025-01-23 00:00:00+00'
        ORDER BY ts, id
        """,
        comparator="float_tolerant",
    ),
    QueryCase(
        "topn_remains_result_stable",
        """
        SELECT id, ts, group_key, int_not_null, int_nullable, float_val
        FROM {table}
        WHERE filter_val >= -5
          AND (group_key IS NOT NULL OR int_nullable IS NULL)
        ORDER BY ts DESC, id DESC
        LIMIT 37
        """,
        comparator="float_tolerant",
    ),
)

METADATA_FASTPATH_CASES = (
    QueryCase(
        "metadata_fastpath_full_partition_counts",
        """
        SELECT count(*), count(int_nullable), sum(int_not_null), min(ts), max(ts)
        FROM {table}
        WHERE ts >= '2025-01-15 00:00:00+00'
          AND ts < '2025-01-21 00:00:00+00'
        """,
        comparator="float_tolerant",
    ),
    QueryCase(
        "metadata_fastpath_grouped_segment_key",
        """
        SELECT group_key, count(*), count(int_nullable), sum(int_nullable), min(int_not_null), max(int_not_null)
        FROM {table}
        WHERE ts >= '2025-01-15 00:00:00+00'
          AND ts < '2025-01-21 00:00:00+00'
        GROUP BY group_key
        ORDER BY group_key NULLS LAST
        """,
        comparator="float_tolerant",
    ),
    QueryCase(
        "metadata_fastpath_partial_range_fallback",
        """
        SELECT group_key, count(*), sum(int_not_null), avg(float_val)
        FROM {table}
        WHERE ts >= '2025-01-16 05:20:00+00'
          AND ts < '2025-01-20 17:40:00+00'
        GROUP BY group_key
        ORDER BY group_key NULLS LAST
        """,
        comparator="float_tolerant",
    ),
)

PARTITION_PLAN_CASES = (
    QueryCase(
        "planner_mode_mixed_storage_projection",
        """
        SELECT id, ts, bucket, device_id, val, metric, payload
        FROM {table}
        WHERE ts >= '2025-01-14 00:00:00+00'
          AND ts < '2025-01-18 00:00:00+00'
          AND (device_id IS NULL OR val IS NULL OR val >= 24)
        ORDER BY ts, id
        """,
        comparator="float_tolerant",
    ),
    QueryCase(
        "planner_mode_mixed_storage_topn",
        """
        SELECT id, ts, bucket, payload, val
        FROM {table}
        WHERE ts >= '2025-01-14 00:00:00+00'
          AND ts < '2025-01-19 00:00:00+00'
        ORDER BY ts DESC, id DESC
        LIMIT 11
        """,
    ),
)

RTABENCH_PLAN_CASES = (
    QueryCase(
        "planner_mode_rtabench_join_aggregate",
        """
        SELECT
            c.country,
            oe.event_type,
            count(*) AS rows,
            count(DISTINCT oe.order_id) AS orders,
            round(avg(oe.satisfaction)::numeric, 4) AS avg_satisfaction
        FROM customers c
        JOIN orders o ON o.customer_id = c.customer_id
        JOIN {table} oe ON oe.order_id = o.order_id
        WHERE oe.event_created >= '2024-05-03 00:00:00+00'
          AND oe.event_created < '2024-05-12 00:00:00+00'
          AND oe.event_type IN ('Delivered', 'Returned', 'Cancelled')
        GROUP BY c.country, oe.event_type
        ORDER BY c.country, oe.event_type
        """,
        comparator="float_tolerant",
    ),
    QueryCase(
        "planner_mode_rtabench_filtered_projection",
        """
        SELECT order_id, counter, event_created, event_type, satisfaction, processor, backup_processor
        FROM {table}
        WHERE event_created >= '2024-05-04 00:00:00+00'
          AND event_created < '2024-05-10 00:00:00+00'
          AND (processor IN ('proc-a', 'proc-c') OR backup_processor IS NULL)
        ORDER BY event_created, order_id, counter NULLS LAST
        """,
        comparator="float_tolerant",
    ),
    QueryCase(
        "planner_mode_rtabench_topn",
        """
        SELECT order_id, counter, event_created, event_type, processor
        FROM {table}
        WHERE event_type <> 'Created'
        ORDER BY event_created DESC, order_id DESC, counter DESC NULLS LAST
        LIMIT 25
        """,
    ),
)


@pytest.fixture()
def aggregate_matrix_large(db):
    return create_aggregate_matrix_pair(
        db,
        deltax_table="planner_mode_aggregate",
        segment_by=("group_key",),
        order_by=("ts", "id"),
        segment_size=17,
        row_count=900,
    )


@pytest.fixture()
def partition_segment_edges(db):
    return create_partition_segment_edges_pair(
        db,
        deltax_table="planner_mode_partition_edges",
        segment_size=3,
    )


@pytest.fixture()
def rtabench_synthetic(db):
    return create_rtabench_synthetic_pair(
        db,
        deltax_table="planner_mode_order_events",
        load_path="copy_text",
        segment_size=11,
    )


def _apply_mode(conn, settings):
    for name, value in settings:
        conn.execute(f"SET {name} = {value}")


def _fetch(conn, sql):
    return conn.execute(sql).fetchall()


def _explain_text(conn, sql):
    plan_rows = conn.execute(f"EXPLAIN (FORMAT TEXT) {sql}").fetchall()
    return "\n".join(row[0] for row in plan_rows)


def _assert_modes_match_plain_and_each_other(conn, case, plain_table, deltax_table, modes):
    plain_sql = case.sql.format(table=plain_table)
    deltax_sql = case.sql.format(table=deltax_table)
    plain_rows = _fetch(conn, plain_sql)
    deltax_by_mode = {}

    for mode_name, settings in modes:
        _apply_mode(conn, settings)
        deltax_rows = _fetch(conn, deltax_sql)
        plain_comparison = compare(case.comparator, plain_rows, deltax_rows)
        assert plain_comparison.ok, (
            f"{case.name} failed against plain PostgreSQL in {mode_name}: "
            f"{plain_comparison.detail}\nSQL: {case.sql}\n"
            f"plain sample: {plain_rows[:5]!r}\ndeltax sample: {deltax_rows[:5]!r}"
        )
        deltax_by_mode[mode_name] = deltax_rows

    baseline_name, baseline_rows = next(iter(deltax_by_mode.items()))
    for mode_name, deltax_rows in list(deltax_by_mode.items())[1:]:
        mode_comparison = compare(case.comparator, baseline_rows, deltax_rows)
        assert mode_comparison.ok, (
            f"{case.name} changed between {baseline_name} and {mode_name}: "
            f"{mode_comparison.detail}\nSQL: {case.sql}\n"
            f"{baseline_name} sample: {baseline_rows[:5]!r}\n"
            f"{mode_name} sample: {deltax_rows[:5]!r}"
        )


@pytest.mark.parametrize("case", AGGREGATE_PLAN_CASES, ids=lambda case: case.name)
def test_aggregate_queries_match_across_serial_and_parallel_modes(
    aggregate_matrix_large,
    db,
    case,
):
    plain_table, deltax_table = aggregate_matrix_large
    _assert_modes_match_plain_and_each_other(
        db,
        case,
        plain_table,
        deltax_table,
        PLANNER_MODES,
    )


@pytest.mark.parametrize("case", METADATA_FASTPATH_CASES, ids=lambda case: case.name)
@pytest.mark.parametrize("fastpath_mode", FASTPATH_MODES, ids=lambda mode: mode[0])
@pytest.mark.parametrize("planner_mode", PLANNER_MODES, ids=lambda mode: mode[0])
def test_metadata_fastpath_matches_plain_under_planner_modes(
    aggregate_matrix_large,
    db,
    case,
    fastpath_mode,
    planner_mode,
):
    _, fastpath_value = fastpath_mode
    _, planner_settings = planner_mode
    plain_table, deltax_table = aggregate_matrix_large
    _apply_mode(db, planner_settings)
    db.execute(f"SET pg_deltax.disable_meta_agg_fastpath = {fastpath_value}")
    assert_query_case(db, case, plain_table=plain_table, deltax_table=deltax_table)


@pytest.mark.parametrize("case", PARTITION_PLAN_CASES, ids=lambda case: case.name)
def test_partition_segment_queries_match_across_serial_and_parallel_modes(
    partition_segment_edges,
    db,
    case,
):
    plain_table, deltax_table = partition_segment_edges
    _assert_modes_match_plain_and_each_other(
        db,
        case,
        plain_table,
        deltax_table,
        PLANNER_MODES,
    )


@pytest.mark.parametrize("case", RTABENCH_PLAN_CASES, ids=lambda case: case.name)
def test_rtabench_queries_match_across_serial_and_parallel_modes(
    rtabench_synthetic,
    db,
    case,
):
    plain_table, deltax_table = rtabench_synthetic
    _assert_modes_match_plain_and_each_other(
        db,
        case,
        plain_table,
        deltax_table,
        PLANNER_MODES,
    )


@pytest.mark.parametrize(
    ("fixture_name", "case"),
    (
        ("aggregate_matrix_large", AGGREGATE_PLAN_CASES[2]),
        ("partition_segment_edges", PARTITION_PLAN_CASES[1]),
        ("rtabench_synthetic", RTABENCH_PLAN_CASES[2]),
    ),
    ids=lambda param: param if isinstance(param, str) else param.name,
)
def test_topn_queries_do_not_use_parallel_custom_scan(
    request,
    db,
    fixture_name,
    case,
):
    _, deltax_table = request.getfixturevalue(fixture_name)
    _apply_mode(db, PLANNER_MODES[1][1])
    plan_text = _explain_text(db, case.sql.format(table=deltax_table))

    assert "Gather" not in plan_text
    assert "Parallel Custom Scan" not in plan_text


def test_parallel_mode_uses_gather_for_broad_parent_scan(
    rtabench_synthetic,
    db,
):
    _, deltax_table = rtabench_synthetic
    _apply_mode(db, PLANNER_MODES[1][1])
    plan_text = _explain_text(
        db,
        f"""
        SELECT order_id, counter, event_created, event_type, processor, backup_processor
        FROM {deltax_table}
        WHERE event_created >= '2024-05-01 00:00:00+00'
          AND event_created < '2024-05-16 00:00:00+00'
        """,
    )

    assert "Gather" in plan_text, plan_text
    assert "Custom Scan (DeltaXAppend)" in plan_text, plan_text
