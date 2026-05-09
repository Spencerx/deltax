"""Deterministic datasets used by the correctness harness."""

from __future__ import annotations


MOCK_NOW = "2025-01-15 12:00:00+00"
BASE_TS = "2025-01-15 00:00:00+00"


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

    partitions = conn.execute(
        "SELECT partition_name FROM deltax_partition_info('events') "
        "WHERE partition_name NOT LIKE '%default%' ORDER BY range_start"
    ).fetchall()
    for (partition_name,) in partitions:
        row_count = conn.execute(f'SELECT count(*) FROM "{partition_name}"').fetchone()[0]
        if row_count > 0:
            conn.execute("SELECT deltax_compress_partition(%s)", (partition_name,))
    conn.commit()

    conn.rollback()
    conn.autocommit = True
    conn.execute("ANALYZE events_plain")
    conn.execute("ANALYZE events")
    conn.autocommit = False

    return "events_plain", "events"
