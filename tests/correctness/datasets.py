"""Deterministic datasets used by the correctness harness."""

from __future__ import annotations

import csv
import io


MOCK_NOW = "2025-01-15 12:00:00+00"
BASE_TS = "2025-01-15 00:00:00+00"

PARTITION_SEGMENT_EDGE_ROWS = (
    ("2025-01-13 23:58:00+00", 100, 0, "default_old", -100, 1.00, "before-start-a"),
    ("2025-01-13 23:59:59.999999+00", 101, 1, "default_old", -99, None, "before-start-b"),
    ("2025-01-13 12:00:00+00", 102, None, "default_old", -98, 1.25, "before-start-c"),
    ("2025-01-14 00:00:00+00", 0, 0, "compressed_4", 0, 0.00, "p14-start"),
    ("2025-01-14 00:00:00.000001+00", 1, 1, "compressed_4", 1, 0.10, "p14-after-start"),
    ("2025-01-14 12:00:00+00", 2, None, "compressed_4", None, 0.20, "p14-mid"),
    ("2025-01-14 23:59:59.999999+00", 3, 1, "compressed_4", 3, None, "p14-end-minus"),
    ("2025-01-15 00:00:00+00", 10, 0, "compressed_5", 10, 1.00, "p15-start"),
    ("2025-01-15 00:00:00.000001+00", 11, 1, "compressed_5", 11, 1.10, "p15-after-start"),
    ("2025-01-15 06:00:00+00", 12, 2, "compressed_5", 12, None, "p15-morning"),
    ("2025-01-15 18:00:00+00", 13, None, "compressed_5", None, 1.30, "p15-evening"),
    ("2025-01-15 23:59:59.999999+00", 14, 2, "compressed_5", 14, 1.40, "p15-end-minus"),
    ("2025-01-16 00:00:00+00", 20, 0, "uncompressed_6", 20, 2.00, "p16-start"),
    ("2025-01-16 00:00:00.000001+00", 21, 1, "uncompressed_6", 21, None, "p16-after-start"),
    ("2025-01-16 04:00:00+00", 22, 2, "uncompressed_6", 22, 2.20, "p16-early"),
    ("2025-01-16 08:00:00+00", 23, None, "uncompressed_6", None, 2.30, "p16-mid"),
    ("2025-01-16 12:00:00+00", 24, 1, "uncompressed_6", 24, 2.40, "p16-noon"),
    ("2025-01-16 23:59:59.999999+00", 25, 2, "uncompressed_6", 25, 2.50, "p16-end-minus"),
    ("2025-01-17 00:00:00+00", 30, 0, "compressed_10", 30, 3.00, "p17-00"),
    ("2025-01-17 01:00:00+00", 31, 1, "compressed_10", 31, 3.10, "p17-01"),
    ("2025-01-17 02:00:00+00", 32, 2, "compressed_10", 32, None, "p17-02"),
    ("2025-01-17 03:00:00+00", 33, None, "compressed_10", None, 3.30, "p17-03"),
    ("2025-01-17 04:00:00+00", 34, 1, "compressed_10", 34, 3.40, "p17-04"),
    ("2025-01-17 05:00:00+00", 35, 2, "compressed_10", 35, 3.50, "p17-05"),
    ("2025-01-17 06:00:00+00", 36, 0, "compressed_10", 36, None, "p17-06"),
    ("2025-01-17 07:00:00+00", 37, 1, "compressed_10", 37, 3.70, "p17-07"),
    ("2025-01-17 08:00:00+00", 38, None, "compressed_10", 38, 3.80, "p17-08"),
    ("2025-01-17 23:59:59.999999+00", 39, 2, "compressed_10", 39, 3.90, "p17-end-minus"),
    ("2025-01-19 00:00:00+00", 110, 0, "default_future", 110, 4.00, "after-end-a"),
    ("2025-01-19 00:00:00.000001+00", 111, 1, "default_future", None, None, "after-end-b"),
)

PARTITION_SEGMENT_EDGE_REGISTERED_ROWS = tuple(
    row
    for row in PARTITION_SEGMENT_EDGE_ROWS
    if "default_" not in row[3]
)


def _compress_non_default_partitions(conn, table_name: str) -> None:
    partitions = conn.execute(
        f"SELECT partition_name FROM deltax_partition_info('{table_name}') "
        "WHERE partition_name NOT LIKE '%default%' ORDER BY range_start"
    ).fetchall()
    for (partition_name,) in partitions:
        row_count = conn.execute(f'SELECT count(*) FROM "{partition_name}"').fetchone()[0]
        if row_count > 0:
            conn.execute("SELECT deltax_compress_partition(%s)", (partition_name,))
    conn.commit()


def _compress_partitions_by_start_date(
    conn,
    table_name: str,
    start_dates: tuple[str, ...],
) -> None:
    for start_date in start_dates:
        rows = conn.execute(
            f"""
            SELECT partition_name
            FROM deltax_partition_info('{table_name}')
            WHERE range_start::date = %s::date
            ORDER BY range_start
            """,
            (start_date,),
        ).fetchall()
        for (partition_name,) in rows:
            row_count = conn.execute(f'SELECT count(*) FROM "{partition_name}"').fetchone()[0]
            if row_count > 0:
                conn.execute("SELECT deltax_compress_partition(%s)", (partition_name,))
    conn.commit()


def _analyze_tables(conn, *table_names: str) -> None:
    conn.rollback()
    conn.autocommit = True
    for table_name in table_names:
        conn.execute(f"ANALYZE {table_name}")
    conn.autocommit = False


def _create_partition_segment_edges_schema(conn, table_name: str) -> None:
    conn.execute(
        f"""
        CREATE TABLE {table_name} (
            ts timestamptz NOT NULL,
            id integer NOT NULL,
            device_id integer,
            bucket text,
            val integer,
            metric double precision,
            payload text
        )
        """
    )


def _insert_partition_segment_edge_rows(conn, table_name: str, rows: tuple[tuple, ...]) -> None:
    with conn.cursor() as cur:
        cur.executemany(
            f"""
            INSERT INTO {table_name} (ts, id, device_id, bucket, val, metric, payload)
            VALUES (%s::timestamptz, %s, %s, %s, %s, %s, %s)
            """,
            rows,
        )


def _copy_partition_segment_edge_rows_deltax(
    conn,
    table_name: str,
    rows: tuple[tuple, ...],
) -> None:
    buf = io.StringIO()
    writer = csv.writer(buf)
    for row in rows:
        writer.writerow("" if value is None else value for value in row)

    with conn.cursor() as cur:
        with cur.copy(
            f"COPY {table_name} FROM STDIN WITH (FORMAT deltax_compress_csv)"
        ) as copy:
            copy.write(buf.getvalue().encode())


def create_tiny_events_pair(conn, *, segment_size: int = 16) -> tuple[str, str]:
    """Create a small postgres/deltax table pair and compress the deltax side."""
    conn.execute(f"SET pg_deltax.mock_now = '{MOCK_NOW}'")
    conn.execute(
        """
        CREATE TABLE events_plain (
            ts timestamptz NOT NULL,
            id integer NOT NULL,
            device_id integer,
            kind text,
            val integer,
            metric double precision
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE events (
            ts timestamptz NOT NULL,
            id integer NOT NULL,
            device_id integer,
            kind text,
            val integer,
            metric double precision
        )
        """
    )
    conn.execute("SELECT deltax_create_table('events', 'ts', '1 day'::interval, 3)")
    conn.execute(
        "SELECT deltax_enable_compression("
        "'events', segment_by => ARRAY['device_id'], "
        "order_by => ARRAY['ts', 'id'], segment_size => %s)",
        (segment_size,),
    )
    conn.commit()

    insert_sql = f"""
        INSERT INTO {{table}} (ts, id, device_id, kind, val, metric)
        SELECT
            '{BASE_TS}'::timestamptz + (i * interval '1 minute') AS ts,
            i AS id,
            CASE WHEN i % 11 = 0 THEN NULL ELSE i % 5 END AS device_id,
            CASE
                WHEN i % 13 = 0 THEN NULL
                WHEN i % 3 = 0 THEN 'alpha'
                WHEN i % 3 = 1 THEN 'beta'
                ELSE 'gamma'
            END AS kind,
            CASE WHEN i % 17 = 0 THEN NULL ELSE (i % 23) - 11 END AS val,
            CASE WHEN i % 19 = 0 THEN NULL ELSE (i::float8 / 10.0) END AS metric
        FROM generate_series(0, 95) AS g(i)
    """
    conn.execute(insert_sql.format(table="events_plain"))
    conn.execute(insert_sql.format(table="events"))
    conn.commit()

    _compress_non_default_partitions(conn, "events")
    _analyze_tables(conn, "events_plain", "events")

    return "events_plain", "events"


def create_predicate_matrix_pair(
    conn,
    *,
    deltax_table: str = "predicate_events",
    order_by: tuple[str, ...] = ("ts", "id"),
    segment_size: int = 8,
) -> tuple[str, str]:
    """Create a deterministic scalar predicate dataset and compress it."""
    plain_table = f"{deltax_table}_plain"
    order_by_sql = ", ".join(f"'{column}'" for column in order_by)

    conn.execute(f"SET pg_deltax.mock_now = '{MOCK_NOW}'")
    for table_name in (plain_table, deltax_table):
        conn.execute(
            f"""
            CREATE TABLE {table_name} (
                ts timestamptz NOT NULL,
                id integer NOT NULL,
                device_id integer,
                int_val integer,
                low_text text,
                high_text text,
                active boolean,
                score double precision,
                code text
            )
            """
        )

    conn.execute(f"SELECT deltax_create_table('{deltax_table}', 'ts', '1 day'::interval, 3)")
    conn.execute(
        "SELECT deltax_enable_compression("
        f"'{deltax_table}', segment_by => ARRAY['device_id'], "
        f"order_by => ARRAY[{order_by_sql}], segment_size => %s)",
        (segment_size,),
    )
    conn.commit()

    insert_sql = f"""
        INSERT INTO {{table}} (
            ts, id, device_id, int_val, low_text, high_text, active, score, code
        )
        SELECT
            '{BASE_TS}'::timestamptz + (i * interval '5 minutes') AS ts,
            i AS id,
            CASE WHEN i % 10 = 0 THEN NULL ELSE i % 6 END AS device_id,
            CASE WHEN i % 12 = 0 THEN NULL ELSE (i % 41) - 20 END AS int_val,
            CASE
                WHEN i % 14 = 0 THEN NULL
                WHEN i % 4 = 0 THEN 'red'
                WHEN i % 4 = 1 THEN 'blue'
                WHEN i % 4 = 2 THEN 'green'
                ELSE 'amber'
            END AS low_text,
            CASE
                WHEN i % 15 = 0 THEN NULL
                WHEN i % 5 = 0 THEN 'prefix-' || lpad(i::text, 3, '0') || '-tail'
                WHEN i % 5 = 1 THEN 'middle-' || lpad(i::text, 3, '0') || '-contains'
                ELSE 'token-' || lpad(i::text, 3, '0')
            END AS high_text,
            CASE WHEN i % 9 = 0 THEN NULL ELSE i % 2 = 0 END AS active,
            CASE WHEN i % 16 = 0 THEN NULL ELSE ((i % 37) - 18)::float8 / 3.0 END AS score,
            CASE WHEN i % 13 = 0 THEN NULL ELSE ((i % 50) + 100)::text END AS code
        FROM generate_series(0, 143) AS g(i)
    """
    conn.execute(insert_sql.format(table=plain_table))
    conn.execute(insert_sql.format(table=deltax_table))
    conn.commit()

    _compress_non_default_partitions(conn, deltax_table)
    _analyze_tables(conn, plain_table, deltax_table)

    return plain_table, deltax_table


def create_ordering_edges_pair(
    conn,
    *,
    deltax_table: str = "ordering_edges",
    order_by: tuple[str, ...] = ("ts",),
    segment_size: int = 12,
) -> tuple[str, str]:
    """Create rows with repeated/NULL sort keys for Top-N correctness."""
    plain_table = f"{deltax_table}_plain"
    order_by_sql = ", ".join(f"'{column}'" for column in order_by)

    conn.execute(f"SET pg_deltax.mock_now = '{MOCK_NOW}'")
    for table_name in (plain_table, deltax_table):
        conn.execute(
            f"""
            CREATE TABLE {table_name} (
                ts timestamptz NOT NULL,
                id integer NOT NULL,
                device_id integer,
                sort_val integer,
                text_sort text,
                tie_val integer NOT NULL,
                payload text,
                extra text,
                active boolean,
                metric double precision
            )
            """
        )

    conn.execute(f"SELECT deltax_create_table('{deltax_table}', 'ts', '1 day'::interval, 3)")
    conn.execute(
        "SELECT deltax_enable_compression("
        f"'{deltax_table}', segment_by => ARRAY['device_id'], "
        f"order_by => ARRAY[{order_by_sql}], segment_size => %s)",
        (segment_size,),
    )
    conn.commit()

    insert_sql = f"""
        INSERT INTO {{table}} (
            ts, id, device_id, sort_val, text_sort, tie_val, payload, extra, active, metric
        )
        SELECT
            '{BASE_TS}'::timestamptz + ((i % 48) * interval '2 minutes') AS ts,
            i AS id,
            CASE WHEN i % 17 = 0 THEN NULL ELSE i % 7 END AS device_id,
            CASE WHEN i % 11 = 0 THEN NULL ELSE (i % 19) - 9 END AS sort_val,
            CASE
                WHEN i % 10 = 0 THEN NULL
                WHEN i % 5 = 0 THEN 'echo-' || lpad((i % 23)::text, 2, '0')
                WHEN i % 5 = 1 THEN 'bravo-' || lpad((i % 17)::text, 2, '0')
                WHEN i % 5 = 2 THEN 'delta-' || lpad((i % 19)::text, 2, '0')
                WHEN i % 5 = 3 THEN 'alpha-' || lpad((i % 13)::text, 2, '0')
                ELSE 'charlie-' || lpad((i % 11)::text, 2, '0')
            END AS text_sort,
            i % 5 AS tie_val,
            CASE
                WHEN i % 4 = 0 THEN 'alpha-' || lpad(i::text, 3, '0')
                WHEN i % 4 = 1 THEN 'beta-' || lpad(i::text, 3, '0')
                WHEN i % 4 = 2 THEN 'gamma-' || lpad(i::text, 3, '0')
                ELSE 'delta-' || lpad(i::text, 3, '0')
            END AS payload,
            repeat(chr(65 + (i % 26)), 3) || '-' || (191 - i)::text AS extra,
            i % 3 <> 0 AS active,
            CASE WHEN i % 13 = 0 THEN NULL ELSE ((i % 31) - 15)::float8 / 4.0 END AS metric
        FROM generate_series(0, 191) AS g(i)
    """
    conn.execute(insert_sql.format(table=plain_table))
    conn.execute(insert_sql.format(table=deltax_table))
    conn.commit()

    _compress_non_default_partitions(conn, deltax_table)
    _analyze_tables(conn, plain_table, deltax_table)

    return plain_table, deltax_table


def create_aggregate_matrix_pair(
    conn,
    *,
    deltax_table: str = "aggregate_matrix",
    segment_by: tuple[str, ...] = ("group_key",),
    order_by: tuple[str, ...] = ("ts", "id"),
    segment_size: int = 10,
) -> tuple[str, str]:
    """Create a numeric-heavy aggregate dataset and compress it."""
    plain_table = f"{deltax_table}_plain"
    segment_by_sql = ", ".join(f"'{column}'" for column in segment_by)
    order_by_sql = ", ".join(f"'{column}'" for column in order_by)

    conn.execute(f"SET pg_deltax.mock_now = '{MOCK_NOW}'")
    for table_name in (plain_table, deltax_table):
        conn.execute(
            f"""
            CREATE TABLE {table_name} (
                ts timestamptz NOT NULL,
                id integer NOT NULL,
                group_key integer,
                sub_key integer,
                device_id integer,
                bucket_not_null integer NOT NULL,
                int_not_null integer NOT NULL,
                int_nullable integer,
                all_null_input integer,
                repeat_val integer,
                float_val double precision,
                filter_val integer
            )
            """
        )

    conn.execute(f"SELECT deltax_create_table('{deltax_table}', 'ts', '1 day'::interval, 3)")
    conn.execute(
        "SELECT deltax_enable_compression("
        f"'{deltax_table}', segment_by => ARRAY[{segment_by_sql}], "
        f"order_by => ARRAY[{order_by_sql}], segment_size => %s)",
        (segment_size,),
    )
    conn.commit()

    insert_sql = f"""
        INSERT INTO {{table}} (
            ts, id, group_key, sub_key, device_id, bucket_not_null,
            int_not_null, int_nullable, all_null_input, repeat_val, float_val, filter_val
        )
        SELECT
            '{BASE_TS}'::timestamptz + (i * interval '20 minutes') AS ts,
            i AS id,
            CASE WHEN i % 29 = 0 THEN NULL ELSE i % 6 END AS group_key,
            CASE WHEN i % 13 = 0 THEN NULL ELSE i % 4 END AS sub_key,
            CASE WHEN i % 17 = 0 THEN NULL ELSE i % 8 END AS device_id,
            i % 6 AS bucket_not_null,
            (i % 43) - 21 AS int_not_null,
            CASE WHEN i % 7 = 0 THEN NULL ELSE (i % 37) - 18 END AS int_nullable,
            CASE WHEN i % 6 = 5 THEN NULL ELSE (i % 23) - 11 END AS all_null_input,
            (i % 5) - 2 AS repeat_val,
            CASE WHEN i % 11 = 0 THEN NULL ELSE ((i % 41) - 20)::float8 / 7.0 END AS float_val,
            (i % 19) - 9 AS filter_val
        FROM generate_series(0, 215) AS g(i)
    """
    conn.execute(insert_sql.format(table=plain_table))
    conn.execute(insert_sql.format(table=deltax_table))
    conn.commit()

    _compress_non_default_partitions(conn, deltax_table)
    _analyze_tables(conn, plain_table, deltax_table)

    return plain_table, deltax_table


def create_partition_segment_edges_pair(
    conn,
    *,
    deltax_table: str = "partition_segment_edges",
    segment_size: int = 5,
) -> tuple[str, str]:
    """Create a mixed compressed/uncompressed layout around partition edges."""
    plain_table = f"{deltax_table}_plain"

    conn.execute(f"SET pg_deltax.mock_now = '{MOCK_NOW}'")
    for table_name in (plain_table, deltax_table):
        _create_partition_segment_edges_schema(conn, table_name)

    conn.execute(f"SELECT deltax_create_table('{deltax_table}', 'ts', '1 day'::interval, 3)")
    conn.execute(
        "SELECT deltax_enable_compression("
        f"'{deltax_table}', segment_by => ARRAY['device_id'], "
        "order_by => ARRAY['ts', 'id'], segment_size => %s)",
        (segment_size,),
    )
    conn.commit()

    _insert_partition_segment_edge_rows(conn, plain_table, PARTITION_SEGMENT_EDGE_ROWS)
    _insert_partition_segment_edge_rows(conn, deltax_table, PARTITION_SEGMENT_EDGE_ROWS)
    conn.commit()

    _compress_partitions_by_start_date(
        conn,
        deltax_table,
        ("2025-01-14", "2025-01-15", "2025-01-17"),
    )
    _analyze_tables(conn, plain_table, deltax_table)

    return plain_table, deltax_table


def create_partition_segment_edges_direct_backfill_pair(
    conn,
    *,
    deltax_table: str = "partition_segment_edges_direct",
    segment_size: int = 5,
) -> tuple[str, str]:
    """Create registered partition edge rows via direct compressed COPY."""
    plain_table = f"{deltax_table}_plain"

    conn.execute(f"SET pg_deltax.mock_now = '{MOCK_NOW}'")
    for table_name in (plain_table, deltax_table):
        _create_partition_segment_edges_schema(conn, table_name)

    conn.execute(f"SELECT deltax_create_table('{deltax_table}', 'ts', '1 day'::interval, 3)")
    conn.execute(
        "SELECT deltax_enable_compression("
        f"'{deltax_table}', segment_by => ARRAY[]::text[], "
        "order_by => ARRAY['ts', 'id'], segment_size => %s)",
        (segment_size,),
    )
    conn.commit()

    _insert_partition_segment_edge_rows(
        conn,
        plain_table,
        PARTITION_SEGMENT_EDGE_REGISTERED_ROWS,
    )
    _copy_partition_segment_edge_rows_deltax(
        conn,
        deltax_table,
        PARTITION_SEGMENT_EDGE_REGISTERED_ROWS,
    )
    conn.commit()
    _analyze_tables(conn, plain_table, deltax_table)

    return plain_table, deltax_table
