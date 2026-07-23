"""Regression tests for issue #54: compressing a partition whose float
column contains NaN failed with `column "nan" does not exist` — the
per-segment `_sum` was spliced into the colstats INSERT as a bare NaN
token. Also covers ±Infinity and float4 alongside float8.
"""

MOCK_NOW = "2025-01-15 12:00:00+00"

SNAPSHOT_SQL = """
    SELECT n, v8::text, v4::text
    FROM float_specials
    ORDER BY n
"""

AGG_SQL = """
    SELECT sum(v8)::text, sum(v4)::text,
           min(v8)::text, max(v8)::text,
           count(v8), count(*) FILTER (WHERE v8 = 'NaN'::float8)
    FROM float_specials
"""


def setup_table(conn):
    conn.execute(f"SET pg_deltax.mock_now = '{MOCK_NOW}'")
    conn.execute("""
        CREATE TABLE float_specials (
            ts TIMESTAMPTZ NOT NULL,
            n INTEGER NOT NULL,
            v8 DOUBLE PRECISION,
            v4 REAL
        )
    """)
    conn.execute(
        "SELECT deltax.deltax_create_table('float_specials', 'ts', '1 day'::interval)"
    )
    conn.execute(
        "SELECT deltax.deltax_enable_compression('float_specials', "
        "order_by => ARRAY['ts'])"
    )
    conn.commit()


def partition_for(conn, day):
    rows = conn.execute(
        "SELECT partition_name FROM deltax.deltax_partition_info('float_specials') "
        f"WHERE range_start <= '{day}'::timestamptz "
        f"AND range_end > '{day}'::timestamptz"
    ).fetchall()
    assert len(rows) == 1
    return rows[0][0]


def compress(conn, part_name):
    result = conn.execute(
        f"SELECT deltax.deltax_compress_partition('{part_name}')"
    ).fetchone()[0]
    conn.commit()
    assert "Compressed" in result


class TestFloatSpecialValues:
    def test_nan_minimal_repro(self, db):
        """The exact shape from issue #54: one normal row + one NaN row."""
        setup_table(db)
        db.execute("""
            INSERT INTO float_specials (ts, n, v8, v4) VALUES
              ('2025-01-15 00:00:00+00', 1, 1.0, 1.0),
              ('2025-01-15 00:01:00+00', 2, 'NaN'::float8, 'NaN'::float4)
        """)
        db.commit()

        before = db.execute(SNAPSHOT_SQL).fetchall()
        compress(db, partition_for(db, "2025-01-15"))
        assert db.execute(SNAPSHOT_SQL).fetchall() == before

    def test_special_values_roundtrip_and_aggregates(self, db):
        """NaN / Infinity / -Infinity / NULL mixed with normal values; NaN
        first in the segment; aggregates must match plain PG both from the
        colstats fast path and after decompression."""
        setup_table(db)
        values = []
        specials = ["'NaN'", "'Infinity'", "'-Infinity'", "NULL"]
        for i in range(200):
            v8 = specials[i % 4] if i % 5 == 0 else f"{i * 0.25}"
            v4 = specials[(i + 1) % 4] if i % 7 == 0 else f"{i * 0.5}"
            values.append(
                f"('2025-01-15'::timestamptz + interval '{i} minutes', {i}, "
                f"{v8}::float8, {v4}::float4)"
            )
        db.execute(
            "INSERT INTO float_specials (ts, n, v8, v4) VALUES " + ", ".join(values)
        )
        db.commit()

        before = db.execute(SNAPSHOT_SQL).fetchall()
        aggs_before = db.execute(AGG_SQL).fetchall()

        part = partition_for(db, "2025-01-15")
        compress(db, part)

        assert db.execute(SNAPSHOT_SQL).fetchall() == before
        assert db.execute(AGG_SQL).fetchall() == aggs_before

        result = db.execute(
            f"SELECT deltax.deltax_decompress_partition('{part}')"
        ).fetchone()[0]
        db.commit()
        assert "Decompressed" in result
        assert db.execute(SNAPSHOT_SQL).fetchall() == before
        assert db.execute(AGG_SQL).fetchall() == aggs_before

    def test_all_nan_segment(self, db):
        """A partition where every non-null float is NaN (sum, min and max
        are all NaN)."""
        setup_table(db)
        db.execute("""
            INSERT INTO float_specials (ts, n, v8, v4)
            SELECT '2025-01-15'::timestamptz + (i || ' minutes')::interval,
                   i, 'NaN'::float8, 'NaN'::float4
            FROM generate_series(0, 49) i
        """)
        db.commit()

        before = db.execute(SNAPSHOT_SQL).fetchall()
        aggs_before = db.execute(AGG_SQL).fetchall()
        compress(db, partition_for(db, "2025-01-15"))
        assert db.execute(SNAPSHOT_SQL).fetchall() == before
        assert db.execute(AGG_SQL).fetchall() == aggs_before
