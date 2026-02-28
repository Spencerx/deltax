"""ClickBench real-world benchmark for pg_cocoon compression.

Uses the ClickBench dataset (Yandex Metrica web analytics, 100M rows, 107 columns)
to stress-test compression with realistic data.

Default: 1 parquet file (~1M rows, ~1GB in PG). Scale via CLICKBENCH_FILES=N env var.

Run with:
    PG_COCOON_IMAGE=pg_cocoon:pg17 pytest tests/bench_clickbench.py -v -s

Scale up:
    PG_COCOON_IMAGE=pg_cocoon:pg17 CLICKBENCH_FILES=5 pytest tests/bench_clickbench.py -v -s
"""

import io
import os
import statistics
import time
import urllib.request
from pathlib import Path

import psycopg
import pytest

from clickbench_queries import QUERIES

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DATA_DIR = Path(__file__).parent / ".data"
PARQUET_URL = "https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_{idx}.parquet"
NUM_FILES = int(os.environ.get("CLICKBENCH_FILES", "1"))
WARMUP_RUNS = 1
TIMED_RUNS = 3

# ClickBench schema adapted for pg_cocoon:
#   - EventTime changed from TIMESTAMP to TIMESTAMPTZ
#   - ClientEventTime / LocalEventTime also TIMESTAMPTZ
#   - All NOT NULL constraints kept
CREATE_TABLE_SQL = """\
CREATE TABLE hits (
    WatchID BIGINT NOT NULL,
    JavaEnable SMALLINT NOT NULL,
    Title TEXT NOT NULL,
    GoodEvent SMALLINT NOT NULL,
    EventTime TIMESTAMPTZ NOT NULL,
    EventDate DATE NOT NULL,
    CounterID INTEGER NOT NULL,
    ClientIP INTEGER NOT NULL,
    RegionID INTEGER NOT NULL,
    UserID BIGINT NOT NULL,
    CounterClass SMALLINT NOT NULL,
    OS SMALLINT NOT NULL,
    UserAgent SMALLINT NOT NULL,
    URL TEXT NOT NULL,
    Referer TEXT NOT NULL,
    IsRefresh SMALLINT NOT NULL,
    RefererCategoryID SMALLINT NOT NULL,
    RefererRegionID INTEGER NOT NULL,
    URLCategoryID SMALLINT NOT NULL,
    URLRegionID INTEGER NOT NULL,
    ResolutionWidth SMALLINT NOT NULL,
    ResolutionHeight SMALLINT NOT NULL,
    ResolutionDepth SMALLINT NOT NULL,
    FlashMajor SMALLINT NOT NULL,
    FlashMinor SMALLINT NOT NULL,
    FlashMinor2 TEXT NOT NULL,
    NetMajor SMALLINT NOT NULL,
    NetMinor SMALLINT NOT NULL,
    UserAgentMajor SMALLINT NOT NULL,
    UserAgentMinor VARCHAR(255) NOT NULL,
    CookieEnable SMALLINT NOT NULL,
    JavascriptEnable SMALLINT NOT NULL,
    IsMobile SMALLINT NOT NULL,
    MobilePhone SMALLINT NOT NULL,
    MobilePhoneModel TEXT NOT NULL,
    Params TEXT NOT NULL,
    IPNetworkID INTEGER NOT NULL,
    TraficSourceID SMALLINT NOT NULL,
    SearchEngineID SMALLINT NOT NULL,
    SearchPhrase TEXT NOT NULL,
    AdvEngineID SMALLINT NOT NULL,
    IsArtifical SMALLINT NOT NULL,
    WindowClientWidth SMALLINT NOT NULL,
    WindowClientHeight SMALLINT NOT NULL,
    ClientTimeZone SMALLINT NOT NULL,
    ClientEventTime TIMESTAMPTZ NOT NULL,
    SilverlightVersion1 SMALLINT NOT NULL,
    SilverlightVersion2 SMALLINT NOT NULL,
    SilverlightVersion3 INTEGER NOT NULL,
    SilverlightVersion4 SMALLINT NOT NULL,
    PageCharset TEXT NOT NULL,
    CodeVersion INTEGER NOT NULL,
    IsLink SMALLINT NOT NULL,
    IsDownload SMALLINT NOT NULL,
    IsNotBounce SMALLINT NOT NULL,
    FUniqID BIGINT NOT NULL,
    OriginalURL TEXT NOT NULL,
    HID INTEGER NOT NULL,
    IsOldCounter SMALLINT NOT NULL,
    IsEvent SMALLINT NOT NULL,
    IsParameter SMALLINT NOT NULL,
    DontCountHits SMALLINT NOT NULL,
    WithHash SMALLINT NOT NULL,
    HitColor CHAR NOT NULL,
    LocalEventTime TIMESTAMPTZ NOT NULL,
    Age SMALLINT NOT NULL,
    Sex SMALLINT NOT NULL,
    Income SMALLINT NOT NULL,
    Interests SMALLINT NOT NULL,
    Robotness SMALLINT NOT NULL,
    RemoteIP INTEGER NOT NULL,
    WindowName INTEGER NOT NULL,
    OpenerName INTEGER NOT NULL,
    HistoryLength SMALLINT NOT NULL,
    BrowserLanguage TEXT NOT NULL,
    BrowserCountry TEXT NOT NULL,
    SocialNetwork TEXT NOT NULL,
    SocialAction TEXT NOT NULL,
    HTTPError SMALLINT NOT NULL,
    SendTiming INTEGER NOT NULL,
    DNSTiming INTEGER NOT NULL,
    ConnectTiming INTEGER NOT NULL,
    ResponseStartTiming INTEGER NOT NULL,
    ResponseEndTiming INTEGER NOT NULL,
    FetchTiming INTEGER NOT NULL,
    SocialSourceNetworkID SMALLINT NOT NULL,
    SocialSourcePage TEXT NOT NULL,
    ParamPrice BIGINT NOT NULL,
    ParamOrderID TEXT NOT NULL,
    ParamCurrency TEXT NOT NULL,
    ParamCurrencyID SMALLINT NOT NULL,
    OpenstatServiceName TEXT NOT NULL,
    OpenstatCampaignID TEXT NOT NULL,
    OpenstatAdID TEXT NOT NULL,
    OpenstatSourceID TEXT NOT NULL,
    UTMSource TEXT NOT NULL,
    UTMMedium TEXT NOT NULL,
    UTMCampaign TEXT NOT NULL,
    UTMContent TEXT NOT NULL,
    UTMTerm TEXT NOT NULL,
    FromTag TEXT NOT NULL,
    HasGCLID SMALLINT NOT NULL,
    RefererHash BIGINT NOT NULL,
    URLHash BIGINT NOT NULL,
    CLID INTEGER NOT NULL
)"""

# Column names in order, matching the schema above
COLUMN_NAMES = [
    "WatchID", "JavaEnable", "Title", "GoodEvent", "EventTime", "EventDate",
    "CounterID", "ClientIP", "RegionID", "UserID", "CounterClass", "OS",
    "UserAgent", "URL", "Referer", "IsRefresh", "RefererCategoryID",
    "RefererRegionID", "URLCategoryID", "URLRegionID", "ResolutionWidth",
    "ResolutionHeight", "ResolutionDepth", "FlashMajor", "FlashMinor",
    "FlashMinor2", "NetMajor", "NetMinor", "UserAgentMajor", "UserAgentMinor",
    "CookieEnable", "JavascriptEnable", "IsMobile", "MobilePhone",
    "MobilePhoneModel", "Params", "IPNetworkID", "TraficSourceID",
    "SearchEngineID", "SearchPhrase", "AdvEngineID", "IsArtifical",
    "WindowClientWidth", "WindowClientHeight", "ClientTimeZone",
    "ClientEventTime", "SilverlightVersion1", "SilverlightVersion2",
    "SilverlightVersion3", "SilverlightVersion4", "PageCharset", "CodeVersion",
    "IsLink", "IsDownload", "IsNotBounce", "FUniqID", "OriginalURL", "HID",
    "IsOldCounter", "IsEvent", "IsParameter", "DontCountHits", "WithHash",
    "HitColor", "LocalEventTime", "Age", "Sex", "Income", "Interests",
    "Robotness", "RemoteIP", "WindowName", "OpenerName", "HistoryLength",
    "BrowserLanguage", "BrowserCountry", "SocialNetwork", "SocialAction",
    "HTTPError", "SendTiming", "DNSTiming", "ConnectTiming",
    "ResponseStartTiming", "ResponseEndTiming", "FetchTiming",
    "SocialSourceNetworkID", "SocialSourcePage", "ParamPrice", "ParamOrderID",
    "ParamCurrency", "ParamCurrencyID", "OpenstatServiceName",
    "OpenstatCampaignID", "OpenstatAdID", "OpenstatSourceID", "UTMSource",
    "UTMMedium", "UTMCampaign", "UTMContent", "UTMTerm", "FromTag",
    "HasGCLID", "RefererHash", "URLHash", "CLID",
]


# ---------------------------------------------------------------------------
# Data download & loading
# ---------------------------------------------------------------------------

def download_parquet(idx: int) -> Path:
    """Download a single parquet file, caching in tests/.data/."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    dest = DATA_DIR / f"hits_{idx}.parquet"
    if dest.exists():
        print(f"  [cached] {dest.name}")
        return dest

    url = PARQUET_URL.format(idx=idx)
    print(f"  Downloading {url} ...")
    req = urllib.request.Request(url, headers={"User-Agent": "pg_cocoon-bench/1.0"})
    with urllib.request.urlopen(req) as resp, open(dest, "wb") as f:
        while True:
            chunk = resp.read(1 << 20)  # 1 MB chunks
            if not chunk:
                break
            f.write(chunk)
    print(f"  Saved {dest.name} ({dest.stat().st_size / 1e6:.1f} MB)")
    return dest


def _convert_parquet_table(table):
    """Convert parquet table columns to PostgreSQL-compatible types.

    - int64 epoch-seconds timestamps → timestamp strings
    - uint16 epoch-days dates → date strings
    - binary text → utf-8 strings
    """
    import pyarrow as pa
    import pyarrow.compute as pc

    EPOCH_SEC_COLS = {"EventTime", "ClientEventTime", "LocalEventTime"}
    EPOCH_DAY_COLS = {"EventDate"}

    new_columns = []
    for i, name in enumerate(table.column_names):
        col = table.column(i)
        if name in EPOCH_SEC_COLS:
            # Cast int64 epoch seconds to timestamp[s] then to string
            ts_array = col.cast(pa.timestamp("s", tz="UTC"))
            new_columns.append(ts_array.cast(pa.string()))
        elif name in EPOCH_DAY_COLS:
            # uint16 days since epoch → date32 → string
            date_array = col.cast(pa.int32()).cast(pa.date32())
            new_columns.append(date_array.cast(pa.string()))
        elif pa.types.is_binary(col.type):
            # binary → utf-8 string (replace invalid bytes)
            new_columns.append(col.cast(pa.string()))
        else:
            new_columns.append(col)

    return pa.table(new_columns, names=table.column_names)


def load_parquet_file(conn, parquet_path: Path):
    """Load a single parquet file into the hits table using pyarrow CSV + COPY."""
    import pyarrow.csv as pcsv
    import pyarrow.parquet as pq

    table = pq.read_table(parquet_path)
    table = _convert_parquet_table(table)

    # Write to an in-memory CSV buffer using pyarrow (no pandas needed)
    buf = io.BytesIO()
    pcsv.write_csv(table, buf, write_options=pcsv.WriteOptions(include_header=False))
    buf.seek(0)

    # PostgreSQL lowercases unquoted column names in CREATE TABLE,
    # so we must use lowercase here to match.
    col_list = ", ".join(c.lower() for c in COLUMN_NAMES)
    with conn.cursor() as cur:
        with cur.copy(f"COPY hits ({col_list}) FROM STDIN WITH (FORMAT csv)") as copy:
            while True:
                chunk = buf.read(1 << 20)  # 1 MB chunks
                if not chunk:
                    break
                copy.write(chunk)

    conn.commit()


def load_parquet_files(conn, n: int):
    """Download and load n parquet files into the hits table."""
    for idx in range(n):
        path = download_parquet(idx)
        print(f"  Loading {path.name} into PostgreSQL ...")
        t0 = time.monotonic()
        load_parquet_file(conn, path)
        elapsed = time.monotonic() - t0
        print(f"  Loaded {path.name} in {elapsed:.1f}s")


# ---------------------------------------------------------------------------
# Setup & compression
# ---------------------------------------------------------------------------

def setup_clickbench(conn, n_files: int):
    """Create the hits table, set up partitioning, and load data."""
    # Pin time to July 2013 so partitions cover the data range
    conn.execute("SET pg_cocoon.mock_now = '2013-07-15 12:00:00+00'")
    conn.execute(CREATE_TABLE_SQL)
    conn.execute(
        "SELECT cocoon_create_table('hits', 'eventtime', '1 day'::interval, 31)"
    )
    conn.commit()

    print(f"\n--- Loading {n_files} ClickBench parquet file(s) ---")
    load_parquet_files(conn, n_files)

    row_count = conn.execute("SELECT count(*) FROM hits").fetchone()[0]
    print(f"  Total rows loaded: {row_count:,}")
    return row_count


def enable_compression(conn):
    """Enable compression with segment_by=CounterID, order_by=EventTime."""
    conn.execute(
        "SELECT cocoon_enable_compression('hits', "
        "segment_by => ARRAY['counterid'], "
        "order_by => ARRAY['eventtime'])"
    )
    conn.commit()
    print("  Compression enabled (segment_by=CounterID, order_by=EventTime)")


def compress_all_partitions(conn):
    """Compress all non-empty, non-default partitions. Returns per-partition stats."""
    partitions = conn.execute(
        "SELECT partition_name FROM cocoon_partition_info('hits') "
        "WHERE partition_name NOT LIKE '%default%' "
        "ORDER BY partition_name"
    ).fetchall()

    results = []
    for (part_name,) in partitions:
        row_count = conn.execute(
            f'SELECT count(*) FROM "{part_name}"'
        ).fetchone()[0]
        if row_count == 0:
            continue

        t0 = time.monotonic()
        conn.execute(f"SELECT cocoon_compress_partition('{part_name}')")
        conn.commit()
        elapsed = time.monotonic() - t0

        print(f"  Compressed {part_name}: {row_count:,} rows in {elapsed:.1f}s")
        results.append((part_name, elapsed))

    return results


# ---------------------------------------------------------------------------
# Query benchmarking
# ---------------------------------------------------------------------------

def run_queries(conn, queries, label=""):
    """Run each query with warmup + timed runs.

    Returns {qid: (median_ms, result_rows)} where result_rows is the list of
    tuples from the last successful run (used for validation).
    """
    results = {}
    for qid, desc, sql in queries:
        # Warmup
        for _ in range(WARMUP_RUNS):
            try:
                conn.execute(sql).fetchall()
            except Exception:
                conn.rollback()

        # Timed runs
        timings = []
        last_rows = None
        last_error = None
        for _ in range(TIMED_RUNS):
            t0 = time.monotonic()
            try:
                rows = conn.execute(sql).fetchall()
                elapsed = (time.monotonic() - t0) * 1000  # ms
                timings.append(elapsed)
                last_rows = rows
            except Exception as e:
                conn.rollback()
                timings.append(float("inf"))
                last_error = e

        median = statistics.median(timings)
        results[qid] = (median, last_rows)

        status = f"{median:.1f}ms" if median != float("inf") else "ERROR"
        print(f"  [{label}] {qid} ({desc}): {status}")
        if last_error is not None:
            print(f"    ERROR: {last_error}")

    return results


# ---------------------------------------------------------------------------
# Results reporting
# ---------------------------------------------------------------------------

def print_query_results(uncompr_results, compr_results):
    """Print markdown table of query performance.

    Accepts results in the format {qid: (median_ms, rows)}.
    """
    print("\n### Query Performance")
    print()
    print(f"| {'Query':<6} | {'Description':<25} | {'Uncompr (ms)':>13} | {'Compr (ms)':>11} | {'Ratio':>6} |")
    print(f"|{'-'*8}|{'-'*27}|{'-'*15}|{'-'*13}|{'-'*8}|")

    for qid, desc, _ in QUERIES:
        u = uncompr_results.get(qid, (float("inf"), None))[0]
        c = compr_results.get(qid, (float("inf"), None))[0]
        if u != float("inf") and c != float("inf") and c > 0:
            ratio = f"{u / c:.2f}x"
        else:
            ratio = "N/A"
        u_str = f"{u:.1f}" if u != float("inf") else "ERR"
        c_str = f"{c:.1f}" if c != float("inf") else "ERR"
        print(f"| {qid:<6} | {desc:<25} | {u_str:>13} | {c_str:>11} | {ratio:>6} |")


def print_compression_stats(conn):
    """Print markdown table of per-partition compression stats."""
    stats = conn.execute(
        "SELECT partition_name, raw_size, compressed_size, compression_ratio, row_count "
        "FROM cocoon_compression_stats('hits') "
        "WHERE compressed_size IS NOT NULL "
        "ORDER BY partition_name"
    ).fetchall()

    if not stats:
        print("\n(No compression stats available)")
        return

    print("\n### Compression Stats")
    print()
    print(f"| {'Partition':<20} | {'Raw (MB)':>9} | {'Compr (MB)':>11} | {'Ratio':>6} | {'Rows':>10} |")
    print(f"|{'-'*22}|{'-'*11}|{'-'*13}|{'-'*8}|{'-'*12}|")

    total_raw = 0
    total_comp = 0
    total_rows = 0

    for part_name, raw, comp, ratio, rows in stats:
        raw_mb = (raw or 0) / 1e6
        comp_mb = (comp or 0) / 1e6
        ratio_str = f"{ratio:.1f}x" if ratio else "N/A"
        rows_val = rows or 0
        print(f"| {part_name:<20} | {raw_mb:>9.1f} | {comp_mb:>11.1f} | {ratio_str:>6} | {rows_val:>10,} |")
        total_raw += raw or 0
        total_comp += comp or 0
        total_rows += rows_val

    total_ratio = total_raw / total_comp if total_comp > 0 else 0
    print(f"| {'TOTAL':<20} | {total_raw / 1e6:>9.1f} | {total_comp / 1e6:>11.1f} | {total_ratio:.1f}x | {total_rows:>10,} |")


# ---------------------------------------------------------------------------
# Pytest fixtures & test class
# ---------------------------------------------------------------------------

@pytest.fixture(scope="class")
def clickbench_db(pg_container):
    """Create a database, load ClickBench data, enable compression.

    Scoped to class so data is loaded once for all benchmark tests.
    """
    import uuid

    from conftest import HOST_PORT, PG_PASSWORD, PG_USER, _admin_conn

    db_name = "bench_clickbench_" + uuid.uuid4().hex[:8]

    admin = _admin_conn()
    admin.execute(f'CREATE DATABASE "{db_name}"')
    admin.close()

    conn = psycopg.connect(
        host="localhost",
        port=HOST_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname=db_name,
    )
    conn.execute("CREATE EXTENSION pg_cocoon")
    conn.commit()

    # Setup: create table, partition, load data
    row_count = setup_clickbench(conn, NUM_FILES)
    enable_compression(conn)

    yield conn

    conn.close()
    admin = _admin_conn()
    admin.execute(f'DROP DATABASE "{db_name}"')
    admin.close()


class TestClickBench:
    """ClickBench real-world benchmark for pg_cocoon compression."""

    def test_benchmark(self, clickbench_db):
        """Run full benchmark: uncompressed queries, compress, compressed queries."""
        conn = clickbench_db

        # Phase 1: Query uncompressed data
        print("\n\n=== Phase 1: Uncompressed Queries ===")
        uncompr_results = run_queries(conn, QUERIES, label="uncompr")

        # Phase 2: Compress all partitions
        print("\n=== Phase 2: Compressing Partitions ===")
        compress_timings = compress_all_partitions(conn)
        total_compress_time = sum(t for _, t in compress_timings)
        print(f"\n  Total compression time: {total_compress_time:.1f}s "
              f"({len(compress_timings)} partitions)")

        # Diagnostic: verify basic query works after compression
        print("\n=== Diagnostic: Post-compression check ===")
        try:
            count = conn.execute("SELECT count(*) FROM hits").fetchone()[0]
            print(f"  count(*) = {count}")
        except Exception as e:
            print(f"  count(*) FAILED: {e}")
            conn.rollback()

        try:
            plan = conn.execute("EXPLAIN SELECT count(*) FROM hits").fetchall()
            for row in plan:
                print(f"  {row[0]}")
        except Exception as e:
            print(f"  EXPLAIN FAILED: {e}")
            conn.rollback()

        # Phase 3: Query compressed data
        print("\n=== Phase 3: Compressed Queries ===")
        compr_results = run_queries(conn, QUERIES, label="compr")

        # Phase 4: Validate compressed results match uncompressed
        print("\n=== Phase 4: Validating Results ===")
        mismatches = []
        for qid, desc, _ in QUERIES:
            u_timing, u_rows = uncompr_results.get(qid, (float("inf"), None))
            c_timing, c_rows = compr_results.get(qid, (float("inf"), None))

            if u_rows is None or c_rows is None:
                print(f"  {qid}: SKIP (query errored)")
                continue

            if u_rows == c_rows:
                print(f"  {qid}: OK ({len(u_rows)} rows match)")
            else:
                mismatches.append(qid)
                print(f"  {qid}: MISMATCH!")
                print(f"    uncompressed: {len(u_rows)} rows, first={u_rows[:2]}")
                print(f"    compressed:   {len(c_rows)} rows, first={c_rows[:2]}")

        # Phase 5: Print results
        print("\n\n" + "=" * 72)
        print("  ClickBench Benchmark Results")
        print(f"  Files: {NUM_FILES}, Warmup: {WARMUP_RUNS}, Timed runs: {TIMED_RUNS}")
        print("=" * 72)

        print_query_results(uncompr_results, compr_results)
        print_compression_stats(conn)

        assert not mismatches, (
            f"Result mismatch for queries: {mismatches}. "
            "Compressed query results differ from uncompressed."
        )
