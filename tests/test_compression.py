"""Integration tests for Phase 2: compression and decompression."""

import math
import time
import pytest

# The mock_now timestamp used to create partitions — all test data must fall
# within the partitions generated around this time.
MOCK_NOW = "2025-01-15 12:00:00+00"
BASE_TS = "2025-01-15 00:00:00+00"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def setup_metrics_table(conn, table_name="metrics"):
    """Create a partitioned metrics table and insert test data."""
    # Pin "now" so partitions cover our test timestamps
    conn.execute(f"SET pg_cocoon.mock_now = '{MOCK_NOW}'")
    conn.execute(f"""
        CREATE TABLE {table_name} (
            ts TIMESTAMPTZ NOT NULL,
            device_id TEXT NOT NULL,
            temperature DOUBLE PRECISION,
            pressure DOUBLE PRECISION,
            status BOOLEAN
        )
    """)
    conn.execute(f"""
        SELECT cocoon_create_table('{table_name}', 'ts', '1 day'::interval)
    """)
    conn.commit()


def insert_metrics(conn, table_name="metrics", n_devices=10, n_points=100,
                   base_ts=None):
    """Insert n_devices * n_points rows of synthetic metrics data."""
    if base_ts is None:
        base_ts = BASE_TS
    values = []
    for d in range(n_devices):
        for p in range(n_points):
            ts = f"'{base_ts}'::timestamptz + interval '{p} minutes'"
            temp = 20.0 + d * 0.5 + p * 0.01
            pres = 1013.0 + d * 0.1 + p * 0.001
            status = "true" if p % 3 != 0 else "false"
            values.append(
                f"({ts}, 'device-{d:04d}', {temp}, {pres}, {status})"
            )

    # Insert in batches
    batch_size = 500
    for i in range(0, len(values), batch_size):
        batch = values[i:i + batch_size]
        conn.execute(
            f"INSERT INTO {table_name} (ts, device_id, temperature, pressure, status) VALUES "
            + ", ".join(batch)
        )
    conn.commit()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestEnableCompression:
    def test_enable_compression_basic(self, db):
        setup_metrics_table(db)
        result = db.execute(
            "SELECT cocoon_enable_compression('metrics', "
            "segment_by => ARRAY['device_id'], "
            "order_by => ARRAY['ts'])"
        ).fetchone()[0]
        db.commit()
        assert "Compression enabled" in result
        assert "device_id" in result

    def test_enable_compression_no_segment(self, db):
        setup_metrics_table(db)
        result = db.execute(
            "SELECT cocoon_enable_compression('metrics')"
        ).fetchone()[0]
        db.commit()
        assert "Compression enabled" in result

    def test_enable_compression_invalid_column(self, db):
        setup_metrics_table(db)
        with pytest.raises(Exception, match="segment_by column"):
            db.execute(
                "SELECT cocoon_enable_compression('metrics', "
                "segment_by => ARRAY['nonexistent'])"
            )
            db.commit()


class TestCompressDecompress:
    def test_compress_partition(self, db):
        """Compress a partition and verify it's empty + companion exists."""
        setup_metrics_table(db)
        insert_metrics(db, n_devices=5, n_points=50)
        db.execute(
            "SELECT cocoon_enable_compression('metrics', "
            "segment_by => ARRAY['device_id'], "
            "order_by => ARRAY['ts'])"
        )
        db.commit()

        # Find a partition to compress
        partitions = db.execute(
            "SELECT partition_name FROM cocoon_partition_info('metrics') "
            "WHERE range_start <= '2025-01-15'::timestamptz "
            "AND range_end > '2025-01-15'::timestamptz"
        ).fetchall()
        assert len(partitions) > 0
        part_name = partitions[0][0]

        # Count rows before compression
        count_before = db.execute(
            f"SELECT count(*) FROM \"{part_name}\""
        ).fetchone()[0]
        assert count_before > 0

        # Compress
        result = db.execute(
            f"SELECT cocoon_compress_partition('{part_name}')"
        ).fetchone()[0]
        db.commit()
        assert "Compressed" in result

        # Partition should return same rows via transparent decompression
        count_after = db.execute(
            f"SELECT count(*) FROM \"{part_name}\""
        ).fetchone()[0]
        assert count_after == count_before

        # Companion table should exist
        companion_exists = db.execute(
            f"SELECT EXISTS (SELECT 1 FROM pg_tables "
            f"WHERE schemaname = '_cocoon_compressed' AND tablename = '{part_name}')"
        ).fetchone()[0]
        assert companion_exists

        # Catalog should show compressed
        info = db.execute(
            "SELECT is_compressed FROM cocoon_partition_info('metrics') "
            f"WHERE partition_name = '{part_name}'"
        ).fetchone()
        assert info[0] is True

    def test_decompress_partition(self, db):
        """Compress then decompress, verify data matches."""
        setup_metrics_table(db)
        insert_metrics(db, n_devices=3, n_points=20)
        db.execute(
            "SELECT cocoon_enable_compression('metrics', "
            "segment_by => ARRAY['device_id'], "
            "order_by => ARRAY['ts'])"
        )
        db.commit()

        # Get partition and original data
        partitions = db.execute(
            "SELECT partition_name FROM cocoon_partition_info('metrics') "
            "WHERE range_start <= '2025-01-15'::timestamptz "
            "AND range_end > '2025-01-15'::timestamptz"
        ).fetchall()
        part_name = partitions[0][0]

        # Save original data
        original = db.execute(
            f"SELECT ts, device_id, temperature, pressure, status "
            f"FROM \"{part_name}\" ORDER BY device_id, ts"
        ).fetchall()
        original_count = len(original)
        assert original_count > 0

        # Compress
        db.execute(f"SELECT cocoon_compress_partition('{part_name}')")
        db.commit()

        # Decompress
        result = db.execute(
            f"SELECT cocoon_decompress_partition('{part_name}')"
        ).fetchone()[0]
        db.commit()
        assert "Decompressed" in result

        # Verify data matches
        restored = db.execute(
            f"SELECT ts, device_id, temperature, pressure, status "
            f"FROM \"{part_name}\" ORDER BY device_id, ts"
        ).fetchall()
        assert len(restored) == original_count

        for orig, rest in zip(original, restored):
            assert orig[0] == rest[0], f"timestamp mismatch: {orig[0]} vs {rest[0]}"
            assert orig[1] == rest[1], f"device_id mismatch: {orig[1]} vs {rest[1]}"
            assert abs(orig[2] - rest[2]) < 0.001, f"temperature mismatch: {orig[2]} vs {rest[2]}"
            assert abs(orig[3] - rest[3]) < 0.001, f"pressure mismatch: {orig[3]} vs {rest[3]}"
            assert orig[4] == rest[4], f"status mismatch: {orig[4]} vs {rest[4]}"

    def test_compress_empty_partition(self, db):
        """Compressing an empty partition should be a no-op."""
        setup_metrics_table(db)
        db.execute(
            "SELECT cocoon_enable_compression('metrics')"
        )
        db.commit()

        partitions = db.execute(
            "SELECT partition_name FROM cocoon_partition_info('metrics') LIMIT 1"
        ).fetchall()
        part_name = partitions[0][0]

        result = db.execute(
            f"SELECT cocoon_compress_partition('{part_name}')"
        ).fetchone()[0]
        db.commit()
        assert "no rows" in result.lower()

    def test_compress_already_compressed(self, db):
        """Compressing an already-compressed partition should be idempotent."""
        setup_metrics_table(db)
        insert_metrics(db, n_devices=2, n_points=10)
        db.execute(
            "SELECT cocoon_enable_compression('metrics', "
            "segment_by => ARRAY['device_id'])"
        )
        db.commit()

        partitions = db.execute(
            "SELECT partition_name FROM cocoon_partition_info('metrics') "
            "WHERE range_start <= '2025-01-15'::timestamptz "
            "AND range_end > '2025-01-15'::timestamptz"
        ).fetchall()
        part_name = partitions[0][0]

        db.execute(f"SELECT cocoon_compress_partition('{part_name}')")
        db.commit()

        result = db.execute(
            f"SELECT cocoon_compress_partition('{part_name}')"
        ).fetchone()[0]
        db.commit()
        assert "already compressed" in result.lower()


class TestCompressionStats:
    def test_stats_after_compression(self, db):
        setup_metrics_table(db)
        insert_metrics(db, n_devices=5, n_points=50)
        db.execute(
            "SELECT cocoon_enable_compression('metrics', "
            "segment_by => ARRAY['device_id'])"
        )
        db.commit()

        partitions = db.execute(
            "SELECT partition_name FROM cocoon_partition_info('metrics') "
            "WHERE range_start <= '2025-01-15'::timestamptz "
            "AND range_end > '2025-01-15'::timestamptz"
        ).fetchall()
        part_name = partitions[0][0]

        db.execute(f"SELECT cocoon_compress_partition('{part_name}')")
        db.commit()

        stats = db.execute(
            "SELECT * FROM cocoon_compression_stats('metrics') "
            f"WHERE partition_name = '{part_name}'"
        ).fetchone()
        assert stats is not None
        # is_compressed
        assert stats[1] is True
        # raw_size > 0
        assert stats[2] > 0
        # compressed_size > 0
        assert stats[3] > 0
        # compression_ratio > 1
        assert stats[4] > 1.0
        # row_count > 0
        assert stats[5] > 0


class TestCompressionPolicy:
    def test_set_policy(self, db):
        setup_metrics_table(db)
        db.execute(
            "SELECT cocoon_enable_compression('metrics', "
            "segment_by => ARRAY['device_id'])"
        )
        db.commit()
        result = db.execute(
            "SELECT cocoon_set_compression_policy('metrics', '7 days'::interval)"
        ).fetchone()[0]
        db.commit()
        assert "Compression policy set" in result

    def test_policy_without_compression_enabled(self, db):
        setup_metrics_table(db)
        with pytest.raises(Exception, match="enable compression first"):
            db.execute(
                "SELECT cocoon_set_compression_policy('metrics', '7 days'::interval)"
            )
            db.commit()


class TestTransparentQuery:
    """Tests for transparent decompression via the custom scan node."""

    def test_transparent_query_basic(self, db):
        """Query parent table before/after compression — results must match.

        Confirms Bug 1 fix: cache invalidation after compression.
        """
        setup_metrics_table(db)
        insert_metrics(db, n_devices=5, n_points=50)
        db.execute(
            "SELECT cocoon_enable_compression('metrics', "
            "segment_by => ARRAY['device_id'], "
            "order_by => ARRAY['ts'])"
        )
        db.commit()

        # Query BEFORE compression (through parent table)
        before_count = db.execute("SELECT count(*) FROM metrics").fetchone()[0]
        before_sum = db.execute(
            "SELECT sum(temperature) FROM metrics"
        ).fetchone()[0]
        before_distinct = db.execute(
            "SELECT count(DISTINCT device_id) FROM metrics"
        ).fetchone()[0]
        assert before_count > 0

        # Find and compress all non-default partitions
        partitions = db.execute(
            "SELECT partition_name FROM cocoon_partition_info('metrics') "
            "WHERE partition_name NOT LIKE '%default%'"
        ).fetchall()

        compressed_count = 0
        for (part_name,) in partitions:
            row_ct = db.execute(
                f'SELECT count(*) FROM "{part_name}"'
            ).fetchone()[0]
            if row_ct == 0:
                continue
            db.execute(f"SELECT cocoon_compress_partition('{part_name}')")
            db.commit()
            compressed_count += 1

        assert compressed_count > 0, "Should have compressed at least one partition"

        # Query AFTER compression (must go through custom scan node)
        after_count = db.execute("SELECT count(*) FROM metrics").fetchone()[0]
        after_sum = db.execute(
            "SELECT sum(temperature) FROM metrics"
        ).fetchone()[0]

        assert after_count == before_count, (
            f"count mismatch: before={before_count}, after={after_count}"
        )
        assert abs(after_sum - before_sum) < 0.01, (
            f"sum mismatch: before={before_sum}, after={after_sum}"
        )

        # Test pass-by-reference column access (device_id is TEXT)
        after_distinct = db.execute(
            "SELECT count(DISTINCT device_id) FROM metrics"
        ).fetchone()[0]
        assert after_distinct == before_distinct, (
            f"distinct mismatch: before={before_distinct}, after={after_distinct}"
        )

    def test_transparent_query_diverse_types(self, db):
        """Table with SMALLINT, DATE, CHAR(3), BIGINT, TEXT, BOOLEAN, FLOAT8, REAL.

        Confirms Bug 2 fix: correct type mappings for all types.
        """
        db.execute(f"SET pg_cocoon.mock_now = '{MOCK_NOW}'")
        db.execute("""
            CREATE TABLE diverse (
                ts TIMESTAMPTZ NOT NULL,
                device_id TEXT NOT NULL,
                val_small SMALLINT,
                val_date DATE,
                val_char CHAR(3),
                val_bigint BIGINT,
                val_text TEXT,
                val_bool BOOLEAN,
                val_float8 DOUBLE PRECISION,
                val_real REAL
            )
        """)
        db.execute("SELECT cocoon_create_table('diverse', 'ts', '1 day'::interval)")
        db.commit()

        # Insert test data
        for i in range(50):
            db.execute(
                f"INSERT INTO diverse VALUES ("
                f"'{BASE_TS}'::timestamptz + interval '{i} minutes', "
                f"'dev-{i % 3}', "
                f"{i % 100}, "
                f"'2025-01-{(i % 28) + 1:02d}'::date, "
                f"'A{i % 10:02d}', "
                f"{1000000 + i}, "
                f"'text-{i}', "
                f"{'true' if i % 2 == 0 else 'false'}, "
                f"{1.5 + i * 0.1}, "
                f"{2.5 + i * 0.01})"
            )
        db.commit()

        # Query BEFORE compression
        before = {}
        before["count"] = db.execute("SELECT count(*) FROM diverse").fetchone()[0]
        before["sum_small"] = db.execute(
            "SELECT sum(val_small) FROM diverse"
        ).fetchone()[0]
        before["min_date"] = db.execute(
            "SELECT min(val_date) FROM diverse"
        ).fetchone()[0]
        before["distinct_char"] = db.execute(
            "SELECT count(DISTINCT val_char) FROM diverse"
        ).fetchone()[0]
        before["sum_bigint"] = db.execute(
            "SELECT sum(val_bigint) FROM diverse"
        ).fetchone()[0]
        before["bool_count"] = db.execute(
            "SELECT count(*) FROM diverse WHERE val_bool = true"
        ).fetchone()[0]
        before["sum_float8"] = db.execute(
            "SELECT sum(val_float8) FROM diverse"
        ).fetchone()[0]
        before["sum_real"] = db.execute(
            "SELECT sum(val_real) FROM diverse"
        ).fetchone()[0]
        assert before["count"] == 50

        # Enable and compress
        db.execute(
            "SELECT cocoon_enable_compression('diverse', "
            "segment_by => ARRAY['device_id'], "
            "order_by => ARRAY['ts'])"
        )
        db.commit()

        partitions = db.execute(
            "SELECT partition_name FROM cocoon_partition_info('diverse') "
            "WHERE partition_name NOT LIKE '%default%'"
        ).fetchall()

        for (part_name,) in partitions:
            row_ct = db.execute(
                f'SELECT count(*) FROM "{part_name}"'
            ).fetchone()[0]
            if row_ct == 0:
                continue
            db.execute(f"SELECT cocoon_compress_partition('{part_name}')")
            db.commit()

        # Query AFTER compression
        after = {}
        after["count"] = db.execute("SELECT count(*) FROM diverse").fetchone()[0]
        after["sum_small"] = db.execute(
            "SELECT sum(val_small) FROM diverse"
        ).fetchone()[0]
        after["min_date"] = db.execute(
            "SELECT min(val_date) FROM diverse"
        ).fetchone()[0]
        after["distinct_char"] = db.execute(
            "SELECT count(DISTINCT val_char) FROM diverse"
        ).fetchone()[0]
        after["sum_bigint"] = db.execute(
            "SELECT sum(val_bigint) FROM diverse"
        ).fetchone()[0]
        after["bool_count"] = db.execute(
            "SELECT count(*) FROM diverse WHERE val_bool = true"
        ).fetchone()[0]
        after["sum_float8"] = db.execute(
            "SELECT sum(val_float8) FROM diverse"
        ).fetchone()[0]
        after["sum_real"] = db.execute(
            "SELECT sum(val_real) FROM diverse"
        ).fetchone()[0]

        assert after["count"] == before["count"], (
            f"count: {before['count']} vs {after['count']}"
        )
        assert after["sum_small"] == before["sum_small"], (
            f"sum_small: {before['sum_small']} vs {after['sum_small']}"
        )
        assert after["min_date"] == before["min_date"], (
            f"min_date: {before['min_date']} vs {after['min_date']}"
        )
        assert after["distinct_char"] == before["distinct_char"], (
            f"distinct_char: {before['distinct_char']} vs {after['distinct_char']}"
        )
        assert after["sum_bigint"] == before["sum_bigint"], (
            f"sum_bigint: {before['sum_bigint']} vs {after['sum_bigint']}"
        )
        assert after["bool_count"] == before["bool_count"], (
            f"bool_count: {before['bool_count']} vs {after['bool_count']}"
        )
        assert abs(after["sum_float8"] - before["sum_float8"]) < 0.01, (
            f"sum_float8: {before['sum_float8']} vs {after['sum_float8']}"
        )
        assert abs(after["sum_real"] - before["sum_real"]) < 0.1, (
            f"sum_real: {before['sum_real']} vs {after['sum_real']}"
        )

    def test_transparent_query_no_segment_by(self, db):
        """Same validation without segment_by columns."""
        setup_metrics_table(db)
        insert_metrics(db, n_devices=3, n_points=30)
        db.execute(
            "SELECT cocoon_enable_compression('metrics', "
            "order_by => ARRAY['ts'])"
        )
        db.commit()

        # Query BEFORE compression
        before_count = db.execute("SELECT count(*) FROM metrics").fetchone()[0]
        before_sum = db.execute(
            "SELECT sum(temperature) FROM metrics"
        ).fetchone()[0]
        assert before_count > 0

        # Compress all non-default partitions
        partitions = db.execute(
            "SELECT partition_name FROM cocoon_partition_info('metrics') "
            "WHERE partition_name NOT LIKE '%default%'"
        ).fetchall()

        for (part_name,) in partitions:
            row_ct = db.execute(
                f'SELECT count(*) FROM "{part_name}"'
            ).fetchone()[0]
            if row_ct == 0:
                continue
            db.execute(f"SELECT cocoon_compress_partition('{part_name}')")
            db.commit()

        # Query AFTER compression
        after_count = db.execute("SELECT count(*) FROM metrics").fetchone()[0]
        after_sum = db.execute(
            "SELECT sum(temperature) FROM metrics"
        ).fetchone()[0]

        assert after_count == before_count, (
            f"count mismatch: before={before_count}, after={after_count}"
        )
        assert abs(after_sum - before_sum) < 0.01, (
            f"sum mismatch: before={before_sum}, after={after_sum}"
        )

    def test_transparent_query_count_star(self, db):
        """COUNT(*) on compressed partition — no columns decompressed."""
        setup_metrics_table(db)
        insert_metrics(db, n_devices=3, n_points=30)
        db.execute(
            "SELECT cocoon_enable_compression('metrics', "
            "segment_by => ARRAY['device_id'], "
            "order_by => ARRAY['ts'])"
        )
        db.commit()

        before_count = db.execute("SELECT count(*) FROM metrics").fetchone()[0]
        assert before_count > 0

        _compress_all_partitions(db, "metrics")

        after_count = db.execute("SELECT count(*) FROM metrics").fetchone()[0]
        assert after_count == before_count, (
            f"count mismatch: before={before_count}, after={after_count}"
        )

    def test_transparent_query_where_not_in_select(self, db):
        """WHERE filter on column not in SELECT list."""
        setup_metrics_table(db)
        insert_metrics(db, n_devices=5, n_points=50)
        db.execute(
            "SELECT cocoon_enable_compression('metrics', "
            "segment_by => ARRAY['device_id'], "
            "order_by => ARRAY['ts'])"
        )
        db.commit()

        # Count rows for a specific device BEFORE compression
        before_count = db.execute(
            "SELECT count(*) FROM metrics WHERE device_id = 'device-0002'"
        ).fetchone()[0]
        before_ts_vals = db.execute(
            "SELECT ts FROM metrics WHERE device_id = 'device-0002' ORDER BY ts"
        ).fetchall()
        assert before_count > 0

        _compress_all_partitions(db, "metrics")

        # Query with WHERE on device_id but only SELECT ts (device_id not in SELECT)
        after_count = db.execute(
            "SELECT count(*) FROM metrics WHERE device_id = 'device-0002'"
        ).fetchone()[0]
        after_ts_vals = db.execute(
            "SELECT ts FROM metrics WHERE device_id = 'device-0002' ORDER BY ts"
        ).fetchall()

        assert after_count == before_count, (
            f"count mismatch: before={before_count}, after={after_count}"
        )
        assert after_ts_vals == before_ts_vals, "timestamp values mismatch"

    def test_transparent_query_multiple_segments(self, db):
        """Multiple segments with different segment_by values."""
        setup_metrics_table(db)
        insert_metrics(db, n_devices=10, n_points=100)
        db.execute(
            "SELECT cocoon_enable_compression('metrics', "
            "segment_by => ARRAY['device_id'], "
            "order_by => ARRAY['ts'])"
        )
        db.commit()

        # Get per-device aggregates BEFORE compression
        before = db.execute(
            "SELECT device_id, count(*), sum(temperature) "
            "FROM metrics GROUP BY device_id ORDER BY device_id"
        ).fetchall()
        assert len(before) == 10

        _compress_all_partitions(db, "metrics")

        # Get per-device aggregates AFTER compression
        after = db.execute(
            "SELECT device_id, count(*), sum(temperature) "
            "FROM metrics GROUP BY device_id ORDER BY device_id"
        ).fetchall()

        assert len(after) == len(before), (
            f"device count mismatch: {len(before)} vs {len(after)}"
        )
        for b, a in zip(before, after):
            assert b[0] == a[0], f"device_id mismatch: {b[0]} vs {a[0]}"
            assert b[1] == a[1], f"count mismatch for {b[0]}: {b[1]} vs {a[1]}"
            assert abs(b[2] - a[2]) < 0.01, (
                f"sum mismatch for {b[0]}: {b[2]} vs {a[2]}"
            )

    def test_explain_analyze_shows_timing(self, db):
        """EXPLAIN ANALYZE on compressed partition shows Cocoon timing."""
        setup_metrics_table(db)
        insert_metrics(db, n_devices=3, n_points=30)
        db.execute(
            "SELECT cocoon_enable_compression('metrics', "
            "segment_by => ARRAY['device_id'], "
            "order_by => ARRAY['ts'])"
        )
        db.commit()
        _compress_all_partitions(db, "metrics")

        rows = db.execute(
            "EXPLAIN (ANALYZE, COSTS OFF) SELECT * FROM metrics"
        ).fetchall()
        explain_text = "\n".join(r[0] for r in rows)

        assert "Cocoon Timing" in explain_text, (
            f"Expected 'Cocoon Timing' in EXPLAIN ANALYZE output:\n{explain_text}"
        )
        assert "Cocoon Stats" in explain_text, (
            f"Expected 'Cocoon Stats' in EXPLAIN ANALYZE output:\n{explain_text}"
        )
        # Verify timing values are present (e.g., "metadata=")
        assert "metadata=" in explain_text
        assert "decompress=" in explain_text
        assert "segments=" in explain_text


# ---------------------------------------------------------------------------
# Datum conversion edge-case tests
# ---------------------------------------------------------------------------

def _compress_all_partitions(conn, table_name):
    """Enable compression and compress all non-empty, non-default partitions."""
    partitions = conn.execute(
        f"SELECT partition_name FROM cocoon_partition_info('{table_name}') "
        "WHERE partition_name NOT LIKE '%default%'"
    ).fetchall()

    for (part_name,) in partitions:
        row_ct = conn.execute(
            f'SELECT count(*) FROM "{part_name}"'
        ).fetchone()[0]
        if row_ct == 0:
            continue
        conn.execute(f"SELECT cocoon_compress_partition('{part_name}')")
        conn.commit()


class TestDatumConversions:
    """Verify every datum conversion path against PostgreSQL's native handling."""

    def test_timestamp_epoch_conversion(self, db):
        """Timestamps at epoch boundaries must survive compression exactly."""
        db.execute(f"SET pg_cocoon.mock_now = '{MOCK_NOW}'")
        db.execute("""
            CREATE TABLE ts_epoch (
                ts TIMESTAMPTZ NOT NULL,
                label TEXT NOT NULL
            )
        """)
        db.execute("SELECT cocoon_create_table('ts_epoch', 'ts', '1 day'::interval)")
        db.commit()

        # Insert timestamps at known epoch boundaries — all within the
        # partition window around MOCK_NOW (2025-01-15).
        test_timestamps = [
            "2025-01-15 00:00:00+00",
            "2025-01-15 00:00:01+00",
            "2025-01-15 12:00:00+00",
            "2025-01-15 23:59:59+00",
            "2025-01-15 00:00:00.000001+00",
            "2025-01-15 00:00:00.999999+00",
        ]
        for i, ts in enumerate(test_timestamps):
            db.execute(
                f"INSERT INTO ts_epoch VALUES ('{ts}'::timestamptz, 'ts-{i}')"
            )
        db.commit()

        # Query BEFORE compression
        before = db.execute(
            "SELECT ts, EXTRACT(EPOCH FROM ts) FROM ts_epoch ORDER BY ts"
        ).fetchall()
        assert len(before) == len(test_timestamps)

        # Compress
        db.execute(
            "SELECT cocoon_enable_compression('ts_epoch', "
            "segment_by => ARRAY['label'], order_by => ARRAY['ts'])"
        )
        db.commit()
        _compress_all_partitions(db, "ts_epoch")

        # Query AFTER compression (through custom scan)
        after = db.execute(
            "SELECT ts, EXTRACT(EPOCH FROM ts) FROM ts_epoch ORDER BY ts"
        ).fetchall()

        assert len(after) == len(before), (
            f"row count mismatch: {len(before)} vs {len(after)}"
        )
        for b, a in zip(before, after):
            assert b[0] == a[0], f"timestamp mismatch: {b[0]} vs {a[0]}"
            assert b[1] == a[1], f"epoch mismatch: {b[1]} vs {a[1]}"

    def test_date_epoch_conversion(self, db):
        """Dates must survive compression with correct PG-epoch offset."""
        db.execute(f"SET pg_cocoon.mock_now = '{MOCK_NOW}'")
        db.execute("""
            CREATE TABLE date_test (
                ts TIMESTAMPTZ NOT NULL,
                val_date DATE
            )
        """)
        db.execute("SELECT cocoon_create_table('date_test', 'ts', '1 day'::interval)")
        db.commit()

        test_dates = [
            "2025-01-01",
            "2025-01-15",
            "2025-01-28",
            "2000-01-01",
            "1970-01-01",
        ]
        for i, d in enumerate(test_dates):
            db.execute(
                f"INSERT INTO date_test VALUES ("
                f"'{BASE_TS}'::timestamptz + interval '{i} minutes', "
                f"'{d}'::date)"
            )
        db.commit()

        before = db.execute(
            "SELECT val_date, val_date - '2000-01-01'::date "
            "FROM date_test ORDER BY ts"
        ).fetchall()

        db.execute("SELECT cocoon_enable_compression('date_test', order_by => ARRAY['ts'])")
        db.commit()
        _compress_all_partitions(db, "date_test")

        after = db.execute(
            "SELECT val_date, val_date - '2000-01-01'::date "
            "FROM date_test ORDER BY ts"
        ).fetchall()

        assert len(after) == len(before)
        for b, a in zip(before, after):
            assert b[0] == a[0], f"date mismatch: {b[0]} vs {a[0]}"
            assert b[1] == a[1], f"date-diff mismatch: {b[1]} vs {a[1]}"

    def test_integer_types(self, db):
        """SMALLINT, INTEGER, BIGINT edge cases survive compression."""
        db.execute(f"SET pg_cocoon.mock_now = '{MOCK_NOW}'")
        db.execute("""
            CREATE TABLE int_test (
                ts TIMESTAMPTZ NOT NULL,
                val_small SMALLINT,
                val_int INTEGER,
                val_big BIGINT
            )
        """)
        db.execute("SELECT cocoon_create_table('int_test', 'ts', '1 day'::interval)")
        db.commit()

        small_vals = [0, 1, -1, 32767, -32768]
        int_vals = [0, 1, -1, 2147483647, -2147483648]
        big_vals = [0, 1, -1, 9223372036854775807, -9223372036854775808]

        for i in range(len(small_vals)):
            # No explicit casts — column types handle conversion.
            # PG parses `-32768::smallint` as `-(32768::smallint)` which overflows.
            db.execute(
                f"INSERT INTO int_test VALUES ("
                f"'{BASE_TS}'::timestamptz + interval '{i} minutes', "
                f"{small_vals[i]}, "
                f"{int_vals[i]}, "
                f"{big_vals[i]})"
            )
        db.commit()

        before = db.execute(
            "SELECT val_small, val_int, val_big FROM int_test ORDER BY ts"
        ).fetchall()

        db.execute("SELECT cocoon_enable_compression('int_test', order_by => ARRAY['ts'])")
        db.commit()
        _compress_all_partitions(db, "int_test")

        after = db.execute(
            "SELECT val_small, val_int, val_big FROM int_test ORDER BY ts"
        ).fetchall()

        assert len(after) == len(before)
        for b, a in zip(before, after):
            assert b[0] == a[0], f"smallint mismatch: {b[0]} vs {a[0]}"
            assert b[1] == a[1], f"integer mismatch: {b[1]} vs {a[1]}"
            assert b[2] == a[2], f"bigint mismatch: {b[2]} vs {a[2]}"

    def test_float_types(self, db):
        """FLOAT8 and REAL edge cases survive compression."""
        db.execute(f"SET pg_cocoon.mock_now = '{MOCK_NOW}'")
        db.execute("""
            CREATE TABLE float_test (
                ts TIMESTAMPTZ NOT NULL,
                val_f8 DOUBLE PRECISION,
                val_real REAL
            )
        """)
        db.execute("SELECT cocoon_create_table('float_test', 'ts', '1 day'::interval)")
        db.commit()

        f8_vals = [0.0, 1.0, -1.0, 1e308, -1e308, 1e-307, math.pi]
        real_vals = [0.0, 1.0, -1.0, 3.4e38, -3.4e38, 1e-37, 3.14]

        for i in range(len(f8_vals)):
            db.execute(
                f"INSERT INTO float_test VALUES ("
                f"'{BASE_TS}'::timestamptz + interval '{i} minutes', "
                f"{f8_vals[i]}::float8, "
                f"{real_vals[i]}::real)"
            )
        db.commit()

        before = db.execute(
            "SELECT val_f8, val_real FROM float_test ORDER BY ts"
        ).fetchall()

        db.execute("SELECT cocoon_enable_compression('float_test', order_by => ARRAY['ts'])")
        db.commit()
        _compress_all_partitions(db, "float_test")

        after = db.execute(
            "SELECT val_f8, val_real FROM float_test ORDER BY ts"
        ).fetchall()

        assert len(after) == len(before)
        for i, (b, a) in enumerate(zip(before, after)):
            assert b[0] == a[0], f"float8 mismatch at row {i}: {b[0]} vs {a[0]}"
            # REAL (f32) may have representation differences, use tolerance
            assert abs((b[1] or 0) - (a[1] or 0)) < abs(b[1] or 1) * 1e-6, (
                f"real mismatch at row {i}: {b[1]} vs {a[1]}"
            )

    def test_boolean_values(self, db):
        """Boolean true/false patterns survive compression exactly."""
        db.execute(f"SET pg_cocoon.mock_now = '{MOCK_NOW}'")
        db.execute("""
            CREATE TABLE bool_test (
                ts TIMESTAMPTZ NOT NULL,
                val_bool BOOLEAN
            )
        """)
        db.execute("SELECT cocoon_create_table('bool_test', 'ts', '1 day'::interval)")
        db.commit()

        bools = [True, False, True, True, False, False, True, False, True, False]
        for i, b in enumerate(bools):
            db.execute(
                f"INSERT INTO bool_test VALUES ("
                f"'{BASE_TS}'::timestamptz + interval '{i} minutes', "
                f"{'true' if b else 'false'})"
            )
        db.commit()

        before = db.execute(
            "SELECT val_bool FROM bool_test ORDER BY ts"
        ).fetchall()

        db.execute("SELECT cocoon_enable_compression('bool_test', order_by => ARRAY['ts'])")
        db.commit()
        _compress_all_partitions(db, "bool_test")

        after = db.execute(
            "SELECT val_bool FROM bool_test ORDER BY ts"
        ).fetchall()

        assert len(after) == len(before)
        for b, a in zip(before, after):
            assert b[0] == a[0], f"bool mismatch: {b[0]} vs {a[0]}"

    def test_text_and_char_types(self, db):
        """TEXT, VARCHAR, and CHAR types survive compression including edge cases."""
        db.execute(f"SET pg_cocoon.mock_now = '{MOCK_NOW}'")
        db.execute("""
            CREATE TABLE text_test (
                ts TIMESTAMPTZ NOT NULL,
                val_text TEXT,
                val_varchar VARCHAR(255),
                val_char CHAR(5)
            )
        """)
        db.execute("SELECT cocoon_create_table('text_test', 'ts', '1 day'::interval)")
        db.commit()

        texts = ["", "hello", "Hello World!", "multi\nline", "a" * 200]
        varchars = ["short", "medium length string", "x" * 255, "café", "日本語"]
        chars = ["ABC  ", "12345", "X    ", "ab   ", "ZZZZZ"]

        for i in range(len(texts)):
            # Escape single quotes in values
            t = texts[i].replace("'", "''")
            v = varchars[i].replace("'", "''")
            c = chars[i].replace("'", "''")
            db.execute(
                f"INSERT INTO text_test VALUES ("
                f"'{BASE_TS}'::timestamptz + interval '{i} minutes', "
                f"'{t}', '{v}', '{c}')"
            )
        db.commit()

        before = db.execute(
            "SELECT val_text, val_varchar, val_char FROM text_test ORDER BY ts"
        ).fetchall()

        db.execute("SELECT cocoon_enable_compression('text_test', order_by => ARRAY['ts'])")
        db.commit()
        _compress_all_partitions(db, "text_test")

        after = db.execute(
            "SELECT val_text, val_varchar, val_char FROM text_test ORDER BY ts"
        ).fetchall()

        assert len(after) == len(before)
        for i, (b, a) in enumerate(zip(before, after)):
            assert b[0] == a[0], f"text mismatch at row {i}: {b[0]!r} vs {a[0]!r}"
            assert b[1] == a[1], f"varchar mismatch at row {i}: {b[1]!r} vs {a[1]!r}"
            assert b[2] == a[2], f"char mismatch at row {i}: {b[2]!r} vs {a[2]!r}"

    def test_null_handling(self, db):
        """NULL positions must be preserved exactly through compression."""
        db.execute(f"SET pg_cocoon.mock_now = '{MOCK_NOW}'")
        db.execute("""
            CREATE TABLE null_test (
                ts TIMESTAMPTZ NOT NULL,
                val_int INTEGER,
                val_f8 DOUBLE PRECISION,
                val_text TEXT,
                val_bool BOOLEAN
            )
        """)
        db.execute("SELECT cocoon_create_table('null_test', 'ts', '1 day'::interval)")
        db.commit()

        # Various null patterns: first null, last null, consecutive, sparse
        rows = [
            (0, "NULL",  "NULL",   "NULL",    "NULL"),     # all null
            (1, "1",     "1.5",    "'a'",     "true"),     # all non-null
            (2, "NULL",  "2.5",    "'b'",     "false"),    # first col null
            (3, "3",     "NULL",   "'c'",     "true"),     # middle col null
            (4, "4",     "4.5",    "NULL",    "false"),    # text null
            (5, "5",     "5.5",    "'e'",     "NULL"),     # bool null
            (6, "NULL",  "NULL",   "NULL",    "NULL"),     # all null again
            (7, "7",     "7.5",    "'g'",     "true"),     # all non-null
            (8, "8",     "NULL",   "'h'",     "NULL"),     # alternating nulls
            (9, "NULL",  "9.5",    "NULL",    "true"),     # alternating nulls inv
        ]

        for (i, vi, vf, vt, vb) in rows:
            db.execute(
                f"INSERT INTO null_test VALUES ("
                f"'{BASE_TS}'::timestamptz + interval '{i} minutes', "
                f"{vi}, {vf}, {vt}, {vb})"
            )
        db.commit()

        before = db.execute(
            "SELECT val_int, val_f8, val_text, val_bool "
            "FROM null_test ORDER BY ts"
        ).fetchall()

        db.execute("SELECT cocoon_enable_compression('null_test', order_by => ARRAY['ts'])")
        db.commit()
        _compress_all_partitions(db, "null_test")

        after = db.execute(
            "SELECT val_int, val_f8, val_text, val_bool "
            "FROM null_test ORDER BY ts"
        ).fetchall()

        assert len(after) == len(before)
        for i, (b, a) in enumerate(zip(before, after)):
            assert b[0] == a[0], f"int null mismatch at row {i}: {b[0]} vs {a[0]}"
            assert b[1] == a[1], f"f8 null mismatch at row {i}: {b[1]} vs {a[1]}"
            assert b[2] == a[2], f"text null mismatch at row {i}: {b[2]!r} vs {a[2]!r}"
            assert b[3] == a[3], f"bool null mismatch at row {i}: {b[3]} vs {a[3]}"
