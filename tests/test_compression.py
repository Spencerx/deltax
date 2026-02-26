"""Integration tests for Phase 2: compression and decompression."""

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

        # Partition should be empty
        count_after = db.execute(
            f"SELECT count(*) FROM \"{part_name}\""
        ).fetchone()[0]
        assert count_after == 0

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
