"""Extended planner mode A/B correctness coverage."""

import pytest

from .comparators import compare
from .datasets import (
    create_aggregate_matrix_pair,
    create_partition_segment_edges_pair,
    create_rtabench_synthetic_pair,
)
from .harness import QueryCase
from .test_planner_modes import (
    AGGREGATE_PLAN_CASES,
    PARTITION_PLAN_CASES,
    PLANNER_MODES,
    RTABENCH_PLAN_CASES,
    _apply_mode,
    _assert_modes_match_plain_and_each_other,
)


pytestmark = pytest.mark.extended


PARALLEL_AUTO = dict(PLANNER_MODES)["parallel_auto"]
PARALLEL_SINGLE_WORKER = (
    ("max_parallel_workers", "8"),
    ("max_parallel_workers_per_gather", "4"),
    ("min_parallel_table_scan_size", "0"),
    ("parallel_setup_cost", "0"),
    ("parallel_tuple_cost", "0"),
    ("pg_deltax.max_parallel_workers_per_scan", "1"),
)
PARALLEL_LEADER_OFF = PARALLEL_AUTO + (("parallel_leader_participation", "off"),)
PARALLEL_LOW_WORK_MEM = PARALLEL_AUTO + (("work_mem", "'64kB'"),)
SERIAL_SCAN = dict(PLANNER_MODES)["serial_scan"]

LIMIT_OFFSET_CASES = (
    QueryCase(
        "ordered_subquery_limit_offset",
        """
        SELECT id, ts, group_key, int_not_null, float_val
        FROM (
            SELECT id, ts, group_key, int_not_null, float_val
            FROM {table}
            WHERE ts >= '2025-01-16 00:00:00+00'
              AND ts < '2025-01-24 00:00:00+00'
              AND (filter_val <= 3 OR group_key IS NULL)
        ) filtered
        ORDER BY group_key NULLS LAST, ts DESC, id DESC
        LIMIT 50 OFFSET 10
        """,
        comparator="float_tolerant",
    ),
    QueryCase(
        "ordered_join_limit_offset",
        """
        SELECT
            c.country,
            oe.order_id,
            oe.event_created,
            oe.event_type,
            oe.processor
        FROM customers c
        JOIN orders o ON o.customer_id = c.customer_id
        JOIN {table} oe ON oe.order_id = o.order_id
        WHERE oe.event_created >= '2024-05-03 00:00:00+00'
          AND oe.event_created < '2024-05-12 00:00:00+00'
          AND oe.event_type <> 'Created'
        ORDER BY c.country, oe.event_created DESC, oe.order_id DESC, oe.counter DESC NULLS LAST
        LIMIT 60 OFFSET 15
        """,
    ),
)

DISTINCT_AGGREGATE_CASES = (
    QueryCase(
        "parallel_count_distinct_global",
        """
        SELECT
            count(DISTINCT group_key),
            count(DISTINCT sub_key),
            count(DISTINCT int_nullable),
            count(DISTINCT repeat_val)
        FROM {table}
        WHERE ts >= '2025-01-15 00:00:00+00'
          AND ts < '2025-01-25 00:00:00+00'
          AND (filter_val BETWEEN -6 AND 8 OR int_nullable IS NULL)
        """,
    ),
    QueryCase(
        "parallel_count_distinct_grouped",
        """
        SELECT
            group_key,
            count(DISTINCT sub_key),
            count(DISTINCT device_id),
            count(DISTINCT int_nullable),
            sum(DISTINCT repeat_val)
        FROM {table}
        WHERE ts >= '2025-01-15 00:00:00+00'
          AND ts < '2025-01-25 00:00:00+00'
        GROUP BY group_key
        ORDER BY group_key NULLS LAST
        """,
    ),
    QueryCase(
        "rtabench_distinct_join_aggregate",
        """
        SELECT
            p.category,
            oe.event_type,
            count(DISTINCT oe.order_id) AS orders,
            count(DISTINCT o.customer_id) AS customers,
            count(*) AS event_item_rows
        FROM {table} oe
        JOIN orders o ON o.order_id = oe.order_id
        JOIN order_items oi ON oi.order_id = o.order_id
        JOIN products p ON p.product_id = oi.product_id
        WHERE oe.event_created >= '2024-05-02 00:00:00+00'
          AND oe.event_created < '2024-05-14 00:00:00+00'
          AND oe.event_type IN ('Delivered', 'Returned', 'Cancelled')
        GROUP BY p.category, oe.event_type
        ORDER BY p.category, oe.event_type
        """,
    ),
)

FORCE_PARALLEL_CASES = (
    QueryCase(
        "force_parallel_filtered_projection",
        """
        SELECT id, ts, group_key, sub_key, int_nullable, float_val
        FROM {table}
        WHERE ts >= '2025-01-16 00:00:00+00'
          AND ts < '2025-01-23 00:00:00+00'
          AND (group_key IN (0, 2, 4) OR int_nullable IS NULL)
        ORDER BY ts, id
        """,
        comparator="float_tolerant",
    ),
    QueryCase(
        "force_parallel_grouped_aggregate",
        """
        SELECT group_key, sub_key, count(*), sum(int_not_null), avg(float_val)
        FROM {table}
        WHERE ts >= '2025-01-15 00:00:00+00'
          AND ts < '2025-01-24 00:00:00+00'
        GROUP BY group_key, sub_key
        ORDER BY group_key NULLS LAST, sub_key NULLS LAST
        """,
        comparator="float_tolerant",
    ),
)


@pytest.fixture()
def aggregate_matrix_large(db):
    return create_aggregate_matrix_pair(
        db,
        deltax_table="planner_mode_extended_aggregate",
        segment_by=("group_key",),
        order_by=("ts", "id"),
        segment_size=13,
        row_count=1200,
    )


@pytest.fixture()
def partition_segment_edges(db):
    return create_partition_segment_edges_pair(
        db,
        deltax_table="planner_mode_extended_partition_edges",
        segment_size=3,
    )


@pytest.fixture(params=(False, True), ids=("compressed_only", "mixed_tail"))
def rtabench_synthetic_variant(db, request):
    return create_rtabench_synthetic_pair(
        db,
        deltax_table=f"pmx_evt_{'m' if request.param else 'c'}",
        load_path="copy_text",
        segment_size=9,
        mixed_uncompressed_tail=request.param,
    )


def _exec_prepared(conn, statement_name, params):
    event_type, lo, hi, ts_lo, ts_hi = params
    return conn.execute(
        f"""
        EXECUTE {statement_name}(
            '{event_type}',
            {lo},
            {hi},
            '{ts_lo}'::timestamptz,
            '{ts_hi}'::timestamptz
        )
        """
    ).fetchall()


def _assert_case_matches(conn, case, plain_table, deltax_table):
    plain_rows = conn.execute(case.sql.format(table=plain_table)).fetchall()
    deltax_rows = conn.execute(case.sql.format(table=deltax_table)).fetchall()
    comparison = compare(case.comparator, plain_rows, deltax_rows)
    assert comparison.ok, (
        f"{case.name} failed: {comparison.detail}\nSQL: {case.sql}\n"
        f"plain sample: {plain_rows[:5]!r}\ndeltax sample: {deltax_rows[:5]!r}"
    )
    return deltax_rows


def _fetch_cursor_chunks(conn, cursor_name, sql, chunk_sizes):
    rows = []
    conn.execute(f"DECLARE {cursor_name} NO SCROLL CURSOR FOR {sql}")
    try:
        chunk_index = 0
        while True:
            chunk_size = chunk_sizes[chunk_index % len(chunk_sizes)]
            chunk = conn.execute(f"FETCH FORWARD {chunk_size} FROM {cursor_name}").fetchall()
            if not chunk:
                return rows
            rows.extend(chunk)
            chunk_index += 1
    finally:
        conn.execute(f"CLOSE {cursor_name}")


@pytest.mark.parametrize("case", LIMIT_OFFSET_CASES, ids=lambda case: case.name)
def test_limit_offset_queries_match_across_serial_and_parallel_modes(
    aggregate_matrix_large,
    rtabench_synthetic_variant,
    db,
    case,
):
    if case.name.startswith("ordered_join"):
        plain_table, deltax_table = rtabench_synthetic_variant
    else:
        plain_table, deltax_table = aggregate_matrix_large

    _assert_modes_match_plain_and_each_other(
        db,
        case,
        plain_table,
        deltax_table,
        PLANNER_MODES,
    )


@pytest.mark.parametrize("case", DISTINCT_AGGREGATE_CASES, ids=lambda case: case.name)
def test_distinct_aggregates_match_across_serial_and_parallel_modes(
    aggregate_matrix_large,
    rtabench_synthetic_variant,
    db,
    case,
):
    if case.name.startswith("rtabench"):
        plain_table, deltax_table = rtabench_synthetic_variant
    else:
        plain_table, deltax_table = aggregate_matrix_large

    _assert_modes_match_plain_and_each_other(
        db,
        case,
        plain_table,
        deltax_table,
        PLANNER_MODES,
    )


@pytest.mark.parametrize("case", FORCE_PARALLEL_CASES, ids=lambda case: case.name)
def test_force_parallel_mode_matches_plain_postgres(
    aggregate_matrix_large,
    db,
    case,
):
    plain_table, deltax_table = aggregate_matrix_large
    _apply_mode(db, PARALLEL_AUTO)
    try:
        db.execute("SET force_parallel_mode = on")
    except Exception as exc:
        db.rollback()
        pytest.skip(f"force_parallel_mode is not available: {exc}")

    plain_rows = db.execute(case.sql.format(table=plain_table)).fetchall()
    deltax_rows = db.execute(case.sql.format(table=deltax_table)).fetchall()
    comparison = compare(case.comparator, plain_rows, deltax_rows)
    assert comparison.ok, (
        f"{case.name} failed with force_parallel_mode=on: {comparison.detail}\n"
        f"SQL: {case.sql}\nplain sample: {plain_rows[:5]!r}\n"
        f"deltax sample: {deltax_rows[:5]!r}"
    )


@pytest.mark.parametrize(
    "case",
    (
        AGGREGATE_PLAN_CASES[1],
        PARTITION_PLAN_CASES[0],
        RTABENCH_PLAN_CASES[1],
    ),
    ids=lambda case: case.name,
)
def test_parallel_single_worker_cap_matches_plain_postgres(
    aggregate_matrix_large,
    partition_segment_edges,
    rtabench_synthetic_variant,
    db,
    case,
):
    if case.name.startswith("planner_mode_rtabench"):
        plain_table, deltax_table = rtabench_synthetic_variant
    elif case.name.startswith("planner_mode_mixed_storage"):
        plain_table, deltax_table = partition_segment_edges
    else:
        plain_table, deltax_table = aggregate_matrix_large
    _apply_mode(db, PARALLEL_SINGLE_WORKER)
    _assert_case_matches(db, case, plain_table, deltax_table)


@pytest.mark.parametrize(
    "case",
    (
        AGGREGATE_PLAN_CASES[0],
        PARTITION_PLAN_CASES[0],
        RTABENCH_PLAN_CASES[0],
    ),
    ids=lambda case: case.name,
)
def test_parallel_without_leader_participation_matches_plain_postgres(
    aggregate_matrix_large,
    partition_segment_edges,
    rtabench_synthetic_variant,
    db,
    case,
):
    if case.name.startswith("planner_mode_rtabench"):
        plain_table, deltax_table = rtabench_synthetic_variant
    elif case.name.startswith("planner_mode_mixed_storage"):
        plain_table, deltax_table = partition_segment_edges
    else:
        plain_table, deltax_table = aggregate_matrix_large
    _apply_mode(db, PARALLEL_LEADER_OFF)
    _assert_case_matches(db, case, plain_table, deltax_table)


@pytest.mark.parametrize(
    "case",
    (
        AGGREGATE_PLAN_CASES[0],
        DISTINCT_AGGREGATE_CASES[1],
        DISTINCT_AGGREGATE_CASES[2],
    ),
    ids=lambda case: case.name,
)
def test_parallel_with_low_work_mem_matches_plain_postgres(
    aggregate_matrix_large,
    rtabench_synthetic_variant,
    db,
    case,
):
    plain_table, deltax_table = (
        rtabench_synthetic_variant if case.name.startswith("rtabench") else aggregate_matrix_large
    )
    _apply_mode(db, PARALLEL_LOW_WORK_MEM)
    _assert_case_matches(db, case, plain_table, deltax_table)


@pytest.mark.parametrize(
    "case",
    (
        RTABENCH_PLAN_CASES[0],
        RTABENCH_PLAN_CASES[1],
        RTABENCH_PLAN_CASES[2],
    ),
    ids=lambda case: case.name,
)
def test_mixed_compressed_uncompressed_rtabench_matches_planner_modes(
    rtabench_synthetic_variant,
    db,
    case,
):
    plain_table, deltax_table = rtabench_synthetic_variant
    _assert_modes_match_plain_and_each_other(
        db,
        case,
        plain_table,
        deltax_table,
        PLANNER_MODES,
    )


@pytest.mark.parametrize(
    "disabled_join",
    ("enable_hashjoin", "enable_mergejoin", "enable_nestloop"),
)
@pytest.mark.parametrize(
    "case",
    (
        RTABENCH_PLAN_CASES[0],
        DISTINCT_AGGREGATE_CASES[2],
    ),
    ids=lambda case: case.name,
)
def test_parallel_mode_with_disabled_join_methods_matches_plain_postgres(
    rtabench_synthetic_variant,
    db,
    case,
    disabled_join,
):
    plain_table, deltax_table = rtabench_synthetic_variant
    _apply_mode(db, PARALLEL_AUTO)
    db.execute(f"SET {disabled_join} = off")

    plain_rows = db.execute(case.sql.format(table=plain_table)).fetchall()
    deltax_rows = db.execute(case.sql.format(table=deltax_table)).fetchall()
    comparison = compare(case.comparator, plain_rows, deltax_rows)
    assert comparison.ok, (
        f"{case.name} failed with parallel mode and {disabled_join}=off: "
        f"{comparison.detail}\nSQL: {case.sql}\nplain sample: {plain_rows[:5]!r}\n"
        f"deltax sample: {deltax_rows[:5]!r}"
    )


def test_cursor_chunked_fetch_matches_plain_postgres(
    aggregate_matrix_large,
    db,
):
    plain_table, deltax_table = aggregate_matrix_large
    case = QueryCase(
        "cursor_chunked_fetch_projection",
        """
        SELECT id, ts, group_key, sub_key, int_nullable, float_val
        FROM {table}
        WHERE ts >= '2025-01-15 00:00:00+00'
          AND ts < '2025-01-27 00:00:00+00'
          AND (filter_val BETWEEN -8 AND 8 OR int_nullable IS NULL)
        ORDER BY ts, id
        """,
        comparator="float_tolerant",
    )

    _apply_mode(db, PARALLEL_AUTO)
    plain_rows = _fetch_cursor_chunks(
        db,
        "planner_plain_cursor",
        case.sql.format(table=plain_table),
        (7, 19, 3, 41),
    )
    deltax_rows = _fetch_cursor_chunks(
        db,
        "planner_deltax_cursor",
        case.sql.format(table=deltax_table),
        (7, 19, 3, 41),
    )
    comparison = compare(case.comparator, plain_rows, deltax_rows)
    assert comparison.ok, (
        f"{case.name} failed across cursor fetch chunks: {comparison.detail}\n"
        f"plain sample: {plain_rows[:5]!r}\ndeltax sample: {deltax_rows[:5]!r}"
    )


def test_parameterized_lateral_rescan_matches_plain_postgres(
    aggregate_matrix_large,
    db,
):
    plain_table, deltax_table = aggregate_matrix_large
    case = QueryCase(
        "parameterized_lateral_rescan",
        """
        WITH bounds(bucket, lo_filter, start_ts, end_ts) AS (
            VALUES
                (1, -9, '2025-01-15 00:00:00+00'::timestamptz, '2025-01-18 00:00:00+00'::timestamptz),
                (2, -4, '2025-01-18 00:00:00+00'::timestamptz, '2025-01-21 00:00:00+00'::timestamptz),
                (3, 0, '2025-01-21 00:00:00+00'::timestamptz, '2025-01-24 00:00:00+00'::timestamptz),
                (4, 5, '2025-01-24 00:00:00+00'::timestamptz, '2025-01-27 00:00:00+00'::timestamptz)
        )
        SELECT b.bucket, scan.group_key, scan.rows, scan.sum_int, scan.max_ts
        FROM bounds b
        CROSS JOIN LATERAL (
            SELECT group_key, count(*) AS rows, sum(int_not_null) AS sum_int, max(ts) AS max_ts
            FROM {table}
            WHERE ts >= b.start_ts
              AND ts < b.end_ts
              AND filter_val BETWEEN b.lo_filter AND b.lo_filter + 8
            GROUP BY group_key
            ORDER BY group_key NULLS LAST
            LIMIT 4
        ) scan
        ORDER BY b.bucket, scan.group_key NULLS LAST
        """,
    )

    _apply_mode(db, PARALLEL_AUTO)
    _assert_case_matches(db, case, plain_table, deltax_table)


@pytest.mark.parametrize("plan_cache_mode", ("force_generic_plan", "force_custom_plan"))
def test_prepared_parameterized_join_matches_across_serial_and_parallel_modes(
    rtabench_synthetic_variant,
    db,
    plan_cache_mode,
):
    plain_table, deltax_table = rtabench_synthetic_variant
    db.execute(f"SET plan_cache_mode = {plan_cache_mode}")
    db.execute(
        f"""
        PREPARE planner_plain_join(text, integer, integer, timestamptz, timestamptz) AS
        SELECT c.country, oe.event_type, count(*) AS rows, max(oe.event_created) AS latest_event
        FROM customers c
        JOIN orders o ON o.customer_id = c.customer_id
        JOIN {plain_table} oe ON oe.order_id = o.order_id
        WHERE oe.event_type = $1
          AND oe.order_id BETWEEN $2 AND $3
          AND oe.event_created >= $4
          AND oe.event_created < $5
        GROUP BY c.country, oe.event_type
        ORDER BY c.country, oe.event_type
        """
    )
    db.execute(
        f"""
        PREPARE planner_deltax_join(text, integer, integer, timestamptz, timestamptz) AS
        SELECT c.country, oe.event_type, count(*) AS rows, max(oe.event_created) AS latest_event
        FROM customers c
        JOIN orders o ON o.customer_id = c.customer_id
        JOIN {deltax_table} oe ON oe.order_id = o.order_id
        WHERE oe.event_type = $1
          AND oe.order_id BETWEEN $2 AND $3
          AND oe.event_created >= $4
          AND oe.event_created < $5
        GROUP BY c.country, oe.event_type
        ORDER BY c.country, oe.event_type
        """
    )

    params = (
        ("Delivered", 1, 80, "2024-05-01 00:00:00+00", "2024-05-10 00:00:00+00"),
        ("Returned", 40, 150, "2024-05-03 00:00:00+00", "2024-05-14 00:00:00+00"),
        ("Cancelled", 70, 180, "2024-05-05 00:00:00+00", "2024-05-15 00:00:00+00"),
    )

    serial_results = []
    for mode_name, settings in (("serial_scan", SERIAL_SCAN), ("parallel_auto", PARALLEL_AUTO)):
        _apply_mode(db, settings)
        mode_results = []
        for param_set in params:
            plain_rows = _exec_prepared(db, "planner_plain_join", param_set)
            deltax_rows = _exec_prepared(db, "planner_deltax_join", param_set)
            assert plain_rows == deltax_rows, (mode_name, param_set)
            mode_results.append(deltax_rows)
        serial_results.append((mode_name, mode_results))

    assert serial_results[0][1] == serial_results[1][1]


@pytest.mark.parametrize("plan_cache_mode", ("force_generic_plan", "force_custom_plan"))
def test_prepared_parameterized_projection_matches_across_serial_and_parallel_modes(
    aggregate_matrix_large,
    db,
    plan_cache_mode,
):
    plain_table, deltax_table = aggregate_matrix_large
    db.execute(f"SET plan_cache_mode = {plan_cache_mode}")
    db.execute(
        f"""
        PREPARE planner_plain_projection(integer, integer, timestamptz, timestamptz) AS
        SELECT id, ts, group_key, sub_key, int_nullable, float_val
        FROM {plain_table}
        WHERE filter_val BETWEEN $1 AND $2
          AND ts >= $3
          AND ts < $4
          AND (group_key IS NULL OR int_nullable IS NULL OR group_key IN (1, 3, 5))
        ORDER BY ts, id
        """
    )
    db.execute(
        f"""
        PREPARE planner_deltax_projection(integer, integer, timestamptz, timestamptz) AS
        SELECT id, ts, group_key, sub_key, int_nullable, float_val
        FROM {deltax_table}
        WHERE filter_val BETWEEN $1 AND $2
          AND ts >= $3
          AND ts < $4
          AND (group_key IS NULL OR int_nullable IS NULL OR group_key IN (1, 3, 5))
        ORDER BY ts, id
        """
    )

    params = (
        (-6, 2, "2025-01-15 00:00:00+00", "2025-01-20 00:00:00+00"),
        (-3, 6, "2025-01-18 00:00:00+00", "2025-01-25 00:00:00+00"),
        (0, 9, "2025-01-21 00:00:00+00", "2025-01-30 00:00:00+00"),
    )

    serial_results = []
    for mode_name, settings in (("serial_scan", SERIAL_SCAN), ("parallel_auto", PARALLEL_AUTO)):
        _apply_mode(db, settings)
        mode_results = []
        for lo, hi, ts_lo, ts_hi in params:
            plain_rows = db.execute(
                f"""
                EXECUTE planner_plain_projection(
                    {lo},
                    {hi},
                    '{ts_lo}'::timestamptz,
                    '{ts_hi}'::timestamptz
                )
                """
            ).fetchall()
            deltax_rows = db.execute(
                f"""
                EXECUTE planner_deltax_projection(
                    {lo},
                    {hi},
                    '{ts_lo}'::timestamptz,
                    '{ts_hi}'::timestamptz
                )
                """
            ).fetchall()
            comparison = compare("float_tolerant", plain_rows, deltax_rows)
            assert comparison.ok, (mode_name, lo, hi, ts_lo, ts_hi, comparison.detail)
            mode_results.append(deltax_rows)
        serial_results.append((mode_name, mode_results))

    assert serial_results[0][1] == serial_results[1][1]


@pytest.mark.parametrize("plan_cache_mode", ("force_generic_plan", "force_custom_plan"))
def test_prepared_parameterized_partition_pruning_matches_plain_postgres(
    aggregate_matrix_large,
    db,
    plan_cache_mode,
):
    plain_table, deltax_table = aggregate_matrix_large
    db.execute(f"SET plan_cache_mode = {plan_cache_mode}")
    db.execute(
        f"""
        PREPARE planner_plain_prune(timestamptz, timestamptz) AS
        SELECT count(*), min(id), max(id), sum(int_not_null), avg(float_val)
        FROM {plain_table}
        WHERE ts >= $1
          AND ts < $2
        """
    )
    db.execute(
        f"""
        PREPARE planner_deltax_prune(timestamptz, timestamptz) AS
        SELECT count(*), min(id), max(id), sum(int_not_null), avg(float_val)
        FROM {deltax_table}
        WHERE ts >= $1
          AND ts < $2
        """
    )

    params = (
        ("2025-01-15 00:00:00+00", "2025-01-26 00:00:00+00"),
        ("2025-01-18 00:00:00+00", "2025-01-19 00:00:00+00"),
        ("2025-03-01 00:00:00+00", "2025-03-02 00:00:00+00"),
    )

    serial_results = []
    for mode_name, settings in (("serial_scan", SERIAL_SCAN), ("parallel_auto", PARALLEL_AUTO)):
        _apply_mode(db, settings)
        mode_results = []
        for ts_lo, ts_hi in params:
            plain_rows = db.execute(
                f"""
                EXECUTE planner_plain_prune(
                    '{ts_lo}'::timestamptz,
                    '{ts_hi}'::timestamptz
                )
                """
            ).fetchall()
            deltax_rows = db.execute(
                f"""
                EXECUTE planner_deltax_prune(
                    '{ts_lo}'::timestamptz,
                    '{ts_hi}'::timestamptz
                )
                """
            ).fetchall()
            comparison = compare("float_tolerant", plain_rows, deltax_rows)
            assert comparison.ok, (plan_cache_mode, mode_name, ts_lo, ts_hi, comparison.detail)
            mode_results.append(deltax_rows)
        serial_results.append((mode_name, mode_results))

    assert serial_results[0][1] == serial_results[1][1]
