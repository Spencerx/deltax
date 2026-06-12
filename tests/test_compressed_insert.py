"""Integration tests for P1 transparent INSERT into compressed partitions.

Covers dev/docs/COMPRESSED_DML.md §4 (P1):
  - INSERT into a compressed partition succeeds and lands in the partition
    heap (the "loose row" region).
  - Every read path unions the heap tail with the segment data: plain
    scans, COUNT(*), MIN/MAX/SUM pushdowns, GROUP BY aggregates, point
    lookups (partition bloom sentinels must never hide heap rows), and
    ORDER BY ... LIMIT.
  - UPDATE / DELETE stay rejected.
  - INSERT ... ON CONFLICT stays rejected.
  - deltax_compact_partition() folds loose rows into new segments; results
    are unchanged afterwards and the heap is empty again.

Correctness is asserted by comparing against an identical plain-PostgreSQL
twin table after every step.
"""

import pytest

MOCK_NOW = "2025-01-15 12:00:00+00"
BASE_TS = "2025-01-15 00:00:00+00"
LATE_TS = "2025-01-15 06:30:00+00"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def setup_tables(conn, segment_by=True):
    """Create a deltax table + a plain twin, load identical data, compress."""
    conn.execute(f"SET pg_deltax.mock_now = '{MOCK_NOW}'")
    for name in ("events", "events_plain"):
        conn.execute(f"""
            CREATE TABLE {name} (
                ts TIMESTAMPTZ NOT NULL,
                device_id TEXT NOT NULL,
                val INT,
                temperature DOUBLE PRECISION
            )
        """)
    conn.execute(
        "SELECT deltax.deltax_create_table('events', 'ts', '1 day'::interval)"
    )
    if segment_by:
        conn.execute(
            "SELECT deltax.deltax_enable_compression('events', "
            "segment_by => ARRAY['device_id'], order_by => ARRAY['ts'])"
        )
    else:
        conn.execute(
            "SELECT deltax.deltax_enable_compression('events', "
            "segment_by => ARRAY[]::text[], order_by => ARRAY['ts'])"
        )
    conn.commit()

    rows = []
    for d in range(4):
        for p in range(200):
            rows.append(
                f"('{BASE_TS}'::timestamptz + interval '{p} minutes', "
                f"'device-{d}', {d * 1000 + p}, {20.0 + d}::float8 + {p}::float8 / 4)"
            )
    for name in ("events", "events_plain"):
        conn.execute(
            f"INSERT INTO {name} (ts, device_id, val, temperature) VALUES "
            + ", ".join(rows)
        )
    conn.commit()


def compress_all(conn, table="events"):
    parts = [
        r[0]
        for r in conn.execute(
            f"SELECT partition_name FROM deltax.deltax_partition_info('{table}') "
            "WHERE partition_name NOT LIKE '%default%'"
        ).fetchall()
    ]
    compressed = []
    for p in parts:
        result = conn.execute(
            f"SELECT deltax.deltax_compress_partition('{p}')"
        ).fetchone()[0]
        if "Compressed" in result:
            compressed.append(p)
    conn.commit()
    assert compressed, "expected at least one compressed partition"
    return compressed


def data_partition(conn, table="events"):
    """The compressed partition holding BASE_TS."""
    return conn.execute(
        f"SELECT partition_name FROM deltax.deltax_partition_info('{table}') "
        "WHERE is_compressed ORDER BY partition_name LIMIT 1"
    ).fetchone()[0]


def insert_late_rows(conn, n=25):
    """Insert late-arriving rows into BOTH tables (routed into the
    already-compressed partition for `events`)."""
    rows = []
    for p in range(n):
        rows.append(
            f"('{LATE_TS}'::timestamptz + interval '{p} seconds', "
            f"'device-late', {90000 + p}, 99.5::float8 + {p})"
        )
    for name in ("events", "events_plain"):
        conn.execute(
            f"INSERT INTO {name} (ts, device_id, val, temperature) VALUES "
            + ", ".join(rows)
        )
    conn.commit()


def assert_tables_match(conn):
    """Full result-set equality between the deltax table and the twin."""
    q = "SELECT ts, device_id, val, temperature FROM {} ORDER BY ts, device_id, val"
    got = conn.execute(q.format("events")).fetchall()
    want = conn.execute(q.format("events_plain")).fetchall()
    assert got == want


QUERIES = [
    # (description, query template)
    ("count_star", "SELECT count(*) FROM {}"),
    ("count_where_time",
     f"SELECT count(*) FROM {{}} WHERE ts >= '{BASE_TS}'::timestamptz"),
    ("min_max_ts", "SELECT min(ts), max(ts) FROM {}"),
    ("min_max_sum_val", "SELECT min(val), max(val), sum(val), count(val) FROM {}"),
    ("group_by_device",
     "SELECT device_id, count(*), sum(val) FROM {} GROUP BY device_id ORDER BY device_id"),
    ("point_lookup_new_val", "SELECT count(*) FROM {} WHERE val = 90003"),
    ("point_lookup_old_val", "SELECT count(*) FROM {} WHERE val = 1005"),
    ("point_lookup_absent_val", "SELECT count(*) FROM {} WHERE val = 123456789"),
    ("text_eq_new_value",
     "SELECT count(*), coalesce(sum(val), 0) FROM {} WHERE device_id = 'device-late'"),
    ("topn_ts_desc",
     "SELECT ts, device_id, val FROM {} ORDER BY ts DESC, device_id, val LIMIT 7"),
    ("topn_ts_asc",
     "SELECT ts, device_id, val FROM {} ORDER BY ts ASC, device_id, val LIMIT 7"),
    ("time_range",
     f"SELECT count(*), coalesce(sum(val), 0) FROM {{}} "
     f"WHERE ts BETWEEN '{LATE_TS}'::timestamptz AND "
     f"'{LATE_TS}'::timestamptz + interval '10 seconds'"),
]


def assert_queries_match(conn):
    for desc, q in QUERIES:
        got = conn.execute(q.format("events")).fetchall()
        want = conn.execute(q.format("events_plain")).fetchall()
        assert got == want, f"{desc}: deltax={got} plain={want}"


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestCompressedInsert:
    def test_insert_into_compressed_succeeds(self, db):
        setup_tables(db)
        compress_all(db)
        part = data_partition(db)

        insert_late_rows(db)

        # Loose rows are in the partition heap, not yet in segments. (A
        # direct count goes through the decompress union, so probe the heap
        # physically.)
        loose_bytes = db.execute(
            f"SELECT pg_relation_size('{part}')"
        ).fetchone()[0]
        assert loose_bytes > 0
        total = db.execute(f"SELECT count(*) FROM {part}").fetchone()[0]
        assert total == 800 + 25

        # Direct insert into the partition (not via the parent) also works.
        db.execute(
            f"INSERT INTO {part} (ts, device_id, val, temperature) "
            f"VALUES ('{LATE_TS}'::timestamptz + interval '1 hour', 'device-direct', 95000, 1.5)"
        )
        db.execute(
            "INSERT INTO events_plain (ts, device_id, val, temperature) "
            f"VALUES ('{LATE_TS}'::timestamptz + interval '1 hour', 'device-direct', 95000, 1.5)"
        )
        db.commit()
        assert_tables_match(db)

    def test_read_paths_see_heap_tail(self, db):
        setup_tables(db)
        compress_all(db)
        insert_late_rows(db)

        assert_tables_match(db)
        assert_queries_match(db)

    def test_read_paths_see_heap_tail_no_segment_by(self, db):
        # Without segment_by the decompress path can claim sorted output
        # (pathkeys) — with loose rows it must not, and ORDER BY queries
        # must stay correct via an explicit Sort.
        setup_tables(db, segment_by=False)
        compress_all(db)
        insert_late_rows(db)

        assert_tables_match(db)
        assert_queries_match(db)

    def test_count_pushdown_still_used_with_heap_tail(self, db):
        setup_tables(db)
        compress_all(db)
        insert_late_rows(db)

        plan = "\n".join(
            r[0] for r in db.execute("EXPLAIN SELECT count(*) FROM events").fetchall()
        )
        # DeltaXCount folds the heap tail at exec time, so it stays enabled.
        assert "DeltaXCount" in plan
        got = db.execute("SELECT count(*) FROM events").fetchone()[0]
        want = db.execute("SELECT count(*) FROM events_plain").fetchone()[0]
        assert got == want

    def test_agg_pushdown_bails_with_heap_tail(self, db):
        setup_tables(db)
        compress_all(db)

        q = "SELECT device_id, sum(val) FROM events GROUP BY device_id"
        plan_before = "\n".join(r[0] for r in db.execute(f"EXPLAIN {q}").fetchall())
        assert "DeltaXAgg" in plan_before

        insert_late_rows(db)
        plan_after = "\n".join(r[0] for r in db.execute(f"EXPLAIN {q}").fetchall())
        # The columnar agg cannot ingest loose rows — planner must bail to a
        # plain Agg over scans that union the heap tail.
        assert "DeltaXAgg" not in plan_after

    def test_update_delete_still_rejected(self, db):
        setup_tables(db)
        compress_all(db)
        part = data_partition(db)
        insert_late_rows(db)

        for stmt in (
            f"UPDATE events SET val = 0 WHERE ts >= '{BASE_TS}'::timestamptz",
            f"UPDATE {part} SET val = 0",
            f"DELETE FROM events WHERE ts >= '{BASE_TS}'::timestamptz",
            f"DELETE FROM {part}",
        ):
            with pytest.raises(Exception) as exc:
                db.execute(stmt)
            db.rollback()
            db.execute(f"SET pg_deltax.mock_now = '{MOCK_NOW}'")
            db.commit()
            assert "compressed partition" in str(exc.value)

    def test_insert_on_conflict_rejected(self, db):
        setup_tables(db)
        compress_all(db)

        # ON CONFLICT DO NOTHING without a conflict target needs no unique
        # index, but still goes through the ON CONFLICT machinery — which
        # cannot see rows inside segments, so it must be rejected.
        with pytest.raises(Exception) as exc:
            db.execute(
                "INSERT INTO events (ts, device_id, val, temperature) "
                f"VALUES ('{LATE_TS}', 'x', 1, 1.0) "
                "ON CONFLICT DO NOTHING"
            )
        db.rollback()
        assert "ON CONFLICT" in str(exc.value)

    def test_insert_on_conflict_in_cte_rejected(self, db):
        # A data-modifying CTE hides the INSERT under a top-level SELECT;
        # the ON CONFLICT rejection must still fire (conflict inference is
        # just as blind to segment rows as a top-level INSERT).
        setup_tables(db)
        compress_all(db)

        with pytest.raises(Exception) as exc:
            db.execute(
                "WITH ins AS ("
                "    INSERT INTO events (ts, device_id, val, temperature) "
                f"    VALUES ('{LATE_TS}', 'x', 1, 1.0) "
                "    ON CONFLICT DO NOTHING RETURNING 1"
                ") SELECT count(*) FROM ins"
            )
        db.rollback()
        assert "ON CONFLICT" in str(exc.value)

    def test_compact_partition(self, db):
        setup_tables(db)
        compress_all(db)
        part = data_partition(db)
        insert_late_rows(db)

        segs_before = db.execute(
            f'SELECT count(*) FROM _deltax_compressed."{part.split(".")[-1]}_meta" '
            "WHERE _segment_id > 0"
        ).fetchone()[0]
        rc_before = db.execute(
            "SELECT row_count FROM deltax.deltax_partition WHERE table_name = "
            f"'{part.split('.')[-1]}'"
        ).fetchone()[0]

        result = db.execute(
            f"SELECT deltax.deltax_compact_partition('{part}')"
        ).fetchone()[0]
        db.commit()
        assert "Compacted" in result
        assert "25 loose rows" in result

        # Loose region is empty again (compaction truncates it); rows live
        # in new segments.
        loose_bytes = db.execute(
            f"SELECT pg_relation_size('{part}')"
        ).fetchone()[0]
        assert loose_bytes == 0
        segs_after = db.execute(
            f'SELECT count(*) FROM _deltax_compressed."{part.split(".")[-1]}_meta" '
            "WHERE _segment_id > 0"
        ).fetchone()[0]
        assert segs_after > segs_before
        rc_after = db.execute(
            "SELECT row_count FROM deltax.deltax_partition WHERE table_name = "
            f"'{part.split('.')[-1]}'"
        ).fetchone()[0]
        assert rc_after == rc_before + 25

        # Results unchanged after compaction — including point lookups for
        # values that only ever existed as loose rows (the partition bloom
        # sentinel must have been folded or dropped, never under-covering).
        assert_tables_match(db)
        assert_queries_match(db)

    def test_compact_then_insert_again(self, db):
        setup_tables(db)
        compress_all(db)
        part = data_partition(db)

        insert_late_rows(db, n=10)
        db.execute(f"SELECT deltax.deltax_compact_partition('{part}')")
        db.commit()

        # Second wave of loose rows after a compaction.
        rows = [
            f"('{LATE_TS}'::timestamptz + interval '{p + 100} seconds', "
            f"'device-wave2', {97000 + p}, 7.25)"
            for p in range(5)
        ]
        for name in ("events", "events_plain"):
            db.execute(
                f"INSERT INTO {name} (ts, device_id, val, temperature) VALUES "
                + ", ".join(rows)
            )
        db.commit()

        assert_tables_match(db)
        assert_queries_match(db)

        result = db.execute(
            f"SELECT deltax.deltax_compact_partition('{part}')"
        ).fetchone()[0]
        db.commit()
        assert "Compacted" in result
        assert_tables_match(db)
        assert_queries_match(db)

    def test_compact_noop_without_loose_rows(self, db):
        setup_tables(db)
        compress_all(db)
        part = data_partition(db)
        result = db.execute(
            f"SELECT deltax.deltax_compact_partition('{part}')"
        ).fetchone()[0]
        db.commit()
        assert "no loose rows" in result

    def test_compact_uncompressed_partition_is_noop(self, db):
        setup_tables(db)
        part = db.execute(
            "SELECT partition_name FROM deltax.deltax_partition_info('events') "
            "WHERE partition_name NOT LIKE '%default%' LIMIT 1"
        ).fetchone()[0]
        result = db.execute(
            f"SELECT deltax.deltax_compact_partition('{part}')"
        ).fetchone()[0]
        db.commit()
        assert "not compressed" in result
