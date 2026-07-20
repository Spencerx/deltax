"""Integration tests for non-text column types stored via the Text fallback.

Regression tests for the 2026-07-14 `pingbacks` incident: columns whose type
falls through `classify_column`'s default arm (text[], inet, numeric, uuid, ...)
are compressed as their TEXT rendering (`::text` cast on the compress SELECT).
Reads must reconstruct real typed datums via the type input function — handing
PG a raw text varlena tagged with the column's type oid makes it read text
bytes as e.g. an ArrayType header (garbage dims/pointers → silent NULLs or a
backend crash).

Also covers `deltax_decompress_partition` on jsonb columns, whose dictionary /
LZ4 blobs hold BINARY jsonb payloads and previously panicked the text decoders
with "invalid UTF-8 in dictionary".
"""

MOCK_NOW = "2025-01-15 12:00:00+00"
BASE_TS = "2025-01-15 00:00:00+00"

# Column list + text renderings/typed expressions used for exact-match
# comparison before compression, after compression, and after decompression.
SNAPSHOT_SQL = """
    SELECT n,
           cluster_id,
           default_host,
           hostnames::text,
           array_length(hostnames, 1),
           ip_address::text,
           host(ip_address),
           geo_lat::text,
           round(geo_lat, 3)::text,
           uid::text,
           meta::text
    FROM {table}
    ORDER BY n
"""


def setup_pingbacks_table(conn, table_name="pingbacks"):
    """Create a partitioned table shaped like the incident's pingbacks table."""
    conn.execute(f"SET pg_deltax.mock_now = '{MOCK_NOW}'")
    conn.execute(f"""
        CREATE TABLE {table_name} (
            ts TIMESTAMPTZ NOT NULL,
            n INTEGER NOT NULL,
            cluster_id VARCHAR NOT NULL,
            default_host TEXT,
            hostnames TEXT[],
            ip_address INET,
            geo_lat NUMERIC,
            uid UUID,
            meta JSONB
        )
    """)
    conn.execute(
        f"SELECT deltax.deltax_create_table('{table_name}', 'ts', '1 day'::interval)"
    )
    conn.execute(
        f"SELECT deltax.deltax_enable_compression('{table_name}', "
        "segment_by => ARRAY['cluster_id'], "
        "order_by => ARRAY['ts'])"
    )
    conn.commit()


def insert_low_cardinality(conn, table_name="pingbacks", n_rows=200):
    """Low-cardinality values → Dictionary / DictionaryLz4 codecs.

    Mirrors the incident data: 125-distinct hostnames over 929 rows was
    dictionary-encoded. Every 4th row is NULL for the fallthrough-typed
    columns to exercise null-bitmap reinsertion.
    """
    hostname_arrays = [
        "ARRAY['Be Part of Research','test.dotcms.com']",
        "ARRAY['demo.dotcms.com']",
        "ARRAY['a b c','d,e','f\"g']",  # spaces, comma, quote — array_out quoting
        "ARRAY['x.example.org','y.example.org','z.example.org']",
        "ARRAY[]::text[]",
        "ARRAY['single']",
        "ARRAY['multi word host','another.example']",
        "ARRAY['h1','h2','h3','h4']",
    ]
    ips = ["10.0.1.7", "192.168.44.5", "34.231.41.127", "2001:db8::1", "172.16.0.9"]
    lats = ["39.0469000", "-12.5000", "0.0001", "89.999999", "39.0469"]
    uuids = [
        "0e37df36-f698-11e6-8dd4-cb9ced3df976",
        "6ecd8c99-4036-403d-bf84-cf8400f67836",
        "3f333df6-90a4-4fda-8dd3-9485d27cee36",
    ]
    metas = ['{"k": 1}', '{"k": 2, "tags": ["a", "b"]}', '{"nested": {"x": 1.5}}']

    values = []
    for i in range(n_rows):
        ts = f"'{BASE_TS}'::timestamptz + interval '{i} minutes'"
        cluster = f"'cl-{i % 3}'"
        host = f"'host-{i % 6}.dotcms.com'"
        if i % 4 == 3:
            hostnames = "NULL"
            ip = "NULL"
            lat = "NULL"
            uid = "NULL"
            meta = "NULL"
        else:
            hostnames = hostname_arrays[i % len(hostname_arrays)]
            ip = f"'{ips[i % len(ips)]}'::inet"
            lat = f"'{lats[i % len(lats)]}'::numeric"
            uid = f"'{uuids[i % len(uuids)]}'::uuid"
            meta = f"'{metas[i % len(metas)]}'::jsonb"
        values.append(
            f"({ts}, {i}, {cluster}, {host}, {hostnames}, {ip}, {lat}, {uid}, {meta})"
        )

    conn.execute(
        f"INSERT INTO {table_name} "
        "(ts, n, cluster_id, default_host, hostnames, ip_address, geo_lat, uid, meta) "
        "VALUES " + ", ".join(values)
    )
    conn.commit()


def insert_high_cardinality(conn, table_name="pingbacks", n_rows=60):
    """Unique-per-row values → cardinality > 50% of rows → Lz4Blocked codec.

    Also gives the text[] column > 32 distinct values (past the valbitmap
    distinct cap) with NULLs mixed in.
    """
    values = []
    for i in range(n_rows):
        ts = f"'{BASE_TS}'::timestamptz + interval '{i} minutes'"
        cluster = f"'cl-{i % 2}'"
        host = f"'host-{i}.dotcms.com'"
        if i % 5 == 4:
            hostnames = "NULL"
            ip = "NULL"
            lat = "NULL"
            uid = "NULL"
            meta = "NULL"
        else:
            hostnames = (
                f"ARRAY['u-{i}.example.com','v-{i}.example.com','w space {i}']"
            )
            ip = f"'10.{(i >> 8) & 255}.{i & 255}.{(i * 7) % 250 + 1}'::inet"
            lat = f"'{i}.{i:06d}'::numeric"
            uid = f"'00000000-0000-4000-8000-{i:012d}'::uuid"
            meta = f'\'{{"i": {i}, "s": "row-{i}"}}\'::jsonb'
        values.append(
            f"({ts}, {i}, {cluster}, {host}, {hostnames}, {ip}, {lat}, {uid}, {meta})"
        )

    conn.execute(
        f"INSERT INTO {table_name} "
        "(ts, n, cluster_id, default_host, hostnames, ip_address, geo_lat, uid, meta) "
        "VALUES " + ", ".join(values)
    )
    conn.commit()


def find_partition(conn, table_name="pingbacks"):
    partitions = conn.execute(
        f"SELECT partition_name FROM deltax.deltax_partition_info('{table_name}') "
        "WHERE range_start <= '2025-01-15'::timestamptz "
        "AND range_end > '2025-01-15'::timestamptz"
    ).fetchall()
    assert len(partitions) > 0
    return partitions[0][0]


def compress_partition(conn, part_name, table_name="pingbacks"):
    result = conn.execute(
        f"SELECT deltax.deltax_compress_partition('{part_name}')"
    ).fetchone()[0]
    conn.commit()
    assert "Compressed" in result
    is_compressed = conn.execute(
        f"SELECT is_compressed FROM deltax.deltax_partition_info('{table_name}') "
        f"WHERE partition_name = '{part_name}'"
    ).fetchone()[0]
    assert is_compressed is True


def _roundtrip(db, insert_fn):
    setup_pingbacks_table(db)
    insert_fn(db)

    before = db.execute(SNAPSHOT_SQL.format(table="pingbacks")).fetchall()
    assert len(before) > 0

    part_name = find_partition(db)
    compress_partition(db, part_name)

    # Reads through the compressed custom scan must match pre-compression
    # exactly — typed datums reconstructed from the stored text renderings.
    after_compress = db.execute(SNAPSHOT_SQL.format(table="pingbacks")).fetchall()
    assert after_compress == before, (
        "non-text column values changed after compression — the emit path is "
        "handing back wrong datums"
    )

    # Decompress must restore the identical rows (incident: this errored with
    # 'invalid UTF-8 in dictionary' on binary jsonb dictionaries).
    result = db.execute(
        f"SELECT deltax.deltax_decompress_partition('{part_name}')"
    ).fetchone()[0]
    db.commit()
    assert "Decompressed" in result

    after_decompress = db.execute(SNAPSHOT_SQL.format(table="pingbacks")).fetchall()
    assert after_decompress == before, (
        "non-text column values changed after decompress_partition"
    )


class TestNonTextColumns:
    def test_low_cardinality_roundtrip(self, db):
        """Dictionary/DictionaryLz4 codecs: text[], inet, numeric, uuid, jsonb."""
        _roundtrip(db, insert_low_cardinality)

    def test_high_cardinality_roundtrip(self, db):
        """Lz4Blocked codec: >32-distinct text[] plus unique inet/numeric/uuid."""
        _roundtrip(db, insert_high_cardinality)

    def test_array_length_incident_query(self, db):
        """The exact incident query shape: array_length() over a compressed
        partition, both with and without LIMIT. Previously returned silent
        NULLs (small LIMIT) or crashed the backend (full scan)."""
        setup_pingbacks_table(db)
        insert_low_cardinality(db)

        expected = db.execute(
            "SELECT n, array_length(hostnames, 1) FROM pingbacks ORDER BY n"
        ).fetchall()
        expected_limited = db.execute(
            "SELECT array_length(hostnames, 1) FROM pingbacks "
            "WHERE hostnames IS NOT NULL ORDER BY n LIMIT 5"
        ).fetchall()
        assert any(v is not None for (v,) in expected_limited)

        part_name = find_partition(db)
        compress_partition(db, part_name)

        got = db.execute(
            "SELECT n, array_length(hostnames, 1) FROM pingbacks ORDER BY n"
        ).fetchall()
        assert got == expected

        got_limited = db.execute(
            "SELECT array_length(hostnames, 1) FROM pingbacks "
            "WHERE hostnames IS NOT NULL ORDER BY n LIMIT 5"
        ).fetchall()
        assert got_limited == expected_limited

    def test_typed_functions_on_compressed(self, db):
        """Type-specific functions must work on datums read from compressed
        partitions: unnest(text[]), inet <<= cidr, numeric aggregation."""
        setup_pingbacks_table(db)
        insert_low_cardinality(db)

        pre = {
            "unnest": db.execute(
                "SELECT count(*) FROM (SELECT unnest(hostnames) FROM pingbacks) s"
            ).fetchone()[0],
            "inet": db.execute(
                "SELECT count(*) FROM pingbacks WHERE ip_address <<= '10.0.0.0/8'::cidr"
            ).fetchone()[0],
            "sum": db.execute(
                "SELECT sum(geo_lat)::text FROM pingbacks"
            ).fetchone()[0],
        }

        part_name = find_partition(db)
        compress_partition(db, part_name)

        assert db.execute(
            "SELECT count(*) FROM (SELECT unnest(hostnames) FROM pingbacks) s"
        ).fetchone()[0] == pre["unnest"]
        assert db.execute(
            "SELECT count(*) FROM pingbacks WHERE ip_address <<= '10.0.0.0/8'::cidr"
        ).fetchone()[0] == pre["inet"]
        assert db.execute(
            "SELECT sum(geo_lat)::text FROM pingbacks"
        ).fetchone()[0] == pre["sum"]
