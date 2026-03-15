"""Integration tests for time_bucket, first, last, and top-N functions."""

from datetime import datetime, timezone

MOCK_NOW = "2025-01-15 12:00:00+00"
BASE_TS = "2025-01-15 00:00:00+00"


def _setup_topn_table(db):
    """Create a compressed deltax table with multiple groups for top-N tests."""
    db.execute(f"SET pg_deltax.mock_now = '{MOCK_NOW}'")
    db.execute("""
        CREATE TABLE topn_test (
            ts TIMESTAMPTZ NOT NULL,
            category TEXT NOT NULL,
            val INT NOT NULL
        )
    """)
    db.execute("SELECT deltax_create_table('topn_test', 'ts', '1 day'::interval)")
    db.commit()

    # Insert data: 5 categories with different row counts
    # cat-A: 50 rows, cat-B: 40 rows, cat-C: 30 rows, cat-D: 20 rows, cat-E: 10 rows
    values = []
    counts = {"cat-A": 50, "cat-B": 40, "cat-C": 30, "cat-D": 20, "cat-E": 10}
    for cat, n in counts.items():
        for i in range(n):
            values.append(
                f"('{BASE_TS}'::timestamptz + interval '{i} seconds', '{cat}', {i})"
            )
    db.execute(
        f"INSERT INTO topn_test (ts, category, val) VALUES {', '.join(values)}"
    )
    db.commit()

    # Enable compression and compress
    db.execute(
        "SELECT deltax_enable_compression('topn_test', "
        "segment_by => ARRAY['category'], order_by => ARRAY['ts'])"
    )
    db.commit()

    partitions = db.execute(
        "SELECT partition_name FROM deltax_partition_info('topn_test') "
        "WHERE range_start <= '2025-01-15'::timestamptz "
        "AND range_end > '2025-01-15'::timestamptz"
    ).fetchall()
    for row in partitions:
        db.execute(f"SELECT deltax_compress_partition('{row[0]}')")
    db.commit()


def _setup_metrics(db):
    """Helper: create a deltax table and insert test rows."""
    db.execute(
        "CREATE TABLE metrics (ts TIMESTAMPTZ NOT NULL, device TEXT, value FLOAT8)"
    )
    db.commit()

    db.execute("SELECT deltax_create_table('metrics', 'ts')")
    db.commit()

    now = datetime.now(timezone.utc)
    db.execute(
        """
        INSERT INTO metrics (ts, device, value) VALUES
            (%s, 'a', 1.0),
            (%s, 'a', 2.0),
            (%s, 'b', 3.0),
            (%s, 'b', 4.0)
        """,
        (now, now, now, now),
    )
    db.commit()
    return now


def test_time_bucket_5min(db):
    """time_bucket('5 minutes', ts) truncates to 5-min boundary."""
    row = db.execute(
        "SELECT time_bucket('5 minutes'::interval, '2025-06-15 14:23:42+00'::timestamptz)"
    ).fetchone()
    assert row[0] == datetime(2025, 6, 15, 14, 20, 0, tzinfo=timezone.utc)


def test_time_bucket_1hour(db):
    """time_bucket('1 hour', ts) truncates to hour boundary."""
    row = db.execute(
        "SELECT time_bucket('1 hour'::interval, '2025-06-15 14:23:42+00'::timestamptz)"
    ).fetchone()
    assert row[0] == datetime(2025, 6, 15, 14, 0, 0, tzinfo=timezone.utc)


def test_time_bucket_with_offset(db):
    """time_bucket with offset shifts the bucket boundary."""
    row = db.execute(
        "SELECT time_bucket('1 day'::interval, '2025-06-15 14:23:42+00'::timestamptz, '6 hours'::interval)"
    ).fetchone()
    # Bucket starts at 06:00 UTC on 2025-06-15
    assert row[0] == datetime(2025, 6, 15, 6, 0, 0, tzinfo=timezone.utc)


def test_first_last(db):
    """first(value, ts) and last(value, ts) return correct values."""
    db.execute(
        "CREATE TABLE fl (ts TIMESTAMPTZ NOT NULL, value FLOAT8)"
    )
    db.commit()

    db.execute("SELECT deltax_create_table('fl', 'ts')")
    db.commit()

    db.execute(
        """
        INSERT INTO fl (ts, value) VALUES
            ('2025-06-15 10:00:00+00', 100.0),
            ('2025-06-15 11:00:00+00', 200.0),
            ('2025-06-15 12:00:00+00', 300.0)
        """
    )
    db.commit()

    row = db.execute("SELECT first(value, ts), last(value, ts) FROM fl").fetchone()
    assert row[0] == 100.0  # earliest ts
    assert row[1] == 300.0  # latest ts


def test_first_last_with_groups(db):
    """first/last work with GROUP BY."""
    db.execute(
        "CREATE TABLE grouped (ts TIMESTAMPTZ NOT NULL, device TEXT, value FLOAT8)"
    )
    db.commit()

    db.execute("SELECT deltax_create_table('grouped', 'ts')")
    db.commit()

    db.execute(
        """
        INSERT INTO grouped (ts, device, value) VALUES
            ('2025-06-15 10:00:00+00', 'a', 10.0),
            ('2025-06-15 12:00:00+00', 'a', 30.0),
            ('2025-06-15 11:00:00+00', 'b', 20.0),
            ('2025-06-15 13:00:00+00', 'b', 40.0)
        """
    )
    db.commit()

    rows = db.execute(
        "SELECT device, first(value, ts), last(value, ts) "
        "FROM grouped GROUP BY device ORDER BY device"
    ).fetchall()

    assert rows[0] == ("a", 10.0, 30.0)
    assert rows[1] == ("b", 20.0, 40.0)


class TestTopN:
    def test_topn_desc(self, db):
        """Top-3 categories by count, DESC order."""
        _setup_topn_table(db)
        rows = db.execute(
            "SELECT category, COUNT(*) AS cnt FROM topn_test "
            "GROUP BY category ORDER BY COUNT(*) DESC LIMIT 3"
        ).fetchall()
        assert len(rows) == 3
        assert rows[0] == ("cat-A", 50)
        assert rows[1] == ("cat-B", 40)
        assert rows[2] == ("cat-C", 30)

        # Verify EXPLAIN shows DeltaXAgg with TopN info
        explain = db.execute(
            "EXPLAIN ANALYZE SELECT category, COUNT(*) AS cnt FROM topn_test "
            "GROUP BY category ORDER BY COUNT(*) DESC LIMIT 3"
        ).fetchall()
        explain_text = "\n".join(r[0] for r in explain)
        assert "DeltaXAgg" in explain_text, (
            f"Expected DeltaXAgg in plan:\n{explain_text}"
        )
        assert "TopN" in explain_text, (
            f"Expected TopN in EXPLAIN output:\n{explain_text}"
        )

    def test_topn_asc(self, db):
        """Top-3 categories by count, ASC order."""
        _setup_topn_table(db)
        rows = db.execute(
            "SELECT category, COUNT(*) AS cnt FROM topn_test "
            "GROUP BY category ORDER BY COUNT(*) ASC LIMIT 3"
        ).fetchall()
        assert len(rows) == 3
        assert rows[0] == ("cat-E", 10)
        assert rows[1] == ("cat-D", 20)
        assert rows[2] == ("cat-C", 30)

    def test_topn_with_offset(self, db):
        """LIMIT 2 OFFSET 1 skips the top result."""
        _setup_topn_table(db)
        rows = db.execute(
            "SELECT category, COUNT(*) AS cnt FROM topn_test "
            "GROUP BY category ORDER BY COUNT(*) DESC LIMIT 2 OFFSET 1"
        ).fetchall()
        assert len(rows) == 2
        assert rows[0] == ("cat-B", 40)
        assert rows[1] == ("cat-C", 30)

    def test_no_topn_without_limit(self, db):
        """Without LIMIT, no TopN should appear in EXPLAIN."""
        _setup_topn_table(db)
        explain = db.execute(
            "EXPLAIN ANALYZE SELECT category, COUNT(*) AS cnt FROM topn_test "
            "GROUP BY category"
        ).fetchall()
        explain_text = "\n".join(r[0] for r in explain)
        assert "DeltaXAgg" in explain_text, (
            f"Expected DeltaXAgg in plan:\n{explain_text}"
        )
        assert "TopN" not in explain_text, (
            f"TopN should not appear without LIMIT:\n{explain_text}"
        )
