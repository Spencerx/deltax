#!/bin/bash
# Install PG16 + TimescaleDB 2.18.1 alongside the existing PG18/pg_deltax cluster
# on the same EC2 instance, on a separate port (default 5433). Loads the same
# RTABench dataset into a 'test' database so EXPLAIN ANALYZE on the same queries
# can be compared side-by-side with pg_deltax.
#
# Mirrors upstream rtabench TimescaleDB recipe:
#   ~/src/rtabench/timescaledb/{benchmark.sh,compress.sh}
# Differences vs upstream:
#   - PG16/Timescale install runs *alongside* PG18 (not on a clean instance)
#   - Reuses /tmp/rtabench_csv from the pg_deltax setup if already present
#   - Continuous aggregates (caggs.sql) are skipped — pg_deltax doesn't run the
#     1000_* queries either, so they're not part of the comparison
#   - Tunes for query phase to mirror our pg_deltax setup (work_mem 8GB, etc.)
#
# Idempotent: safe to re-run. Drops/recreates the TS database only.

set -euo pipefail

DB=test
PORT=5433
DATA_DIR=/tmp/rtabench_csv
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TS_PG_CONFIG=/usr/lib/postgresql/16/bin/pg_config

export DEBIAN_FRONTEND=noninteractive

# --- Install PG16 + TimescaleDB 2.18.1 -------------------------------------
# pgdg apt repo is already configured by the pg_deltax setup; if not, add it.
if [ ! -f /etc/apt/sources.list.d/pgdg.list ] && [ ! -f /etc/apt/sources.list.d/pgdg.sources ]; then
    sudo apt-get update -y
    sudo apt-get install -y postgresql-common gnupg apt-transport-https lsb-release wget
    sudo /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh -y
fi

# Timescale apt repo (modern signed-by keyring)
if [ ! -f /etc/apt/keyrings/timescaledb.gpg ]; then
    sudo install -d -m 0755 /etc/apt/keyrings
    wget -qO- https://packagecloud.io/timescale/timescaledb/gpgkey \
        | sudo gpg --dearmor -o /etc/apt/keyrings/timescaledb.gpg
fi
sudo bash -c "echo \"deb [signed-by=/etc/apt/keyrings/timescaledb.gpg] https://packagecloud.io/timescale/timescaledb/ubuntu/ $(lsb_release -c -s) main\" > /etc/apt/sources.list.d/timescaledb.list"

sudo apt-get update -y
sudo apt-get install -y postgresql-16 postgresql-client-16 timescaledb-2-postgresql-16 pigz

# --- Configure the PG16 cluster -------------------------------------------
# apt install creates a default 16/main cluster on the next free port (5433
# when 5432 is taken by PG18). Read it back so we don't hardcode wrong.
if ! sudo pg_lsclusters | awk '/^16 +main /{found=1} END{exit !found}'; then
    sudo pg_createcluster 16 main --port=$PORT
fi
ACTUAL_PORT=$(sudo pg_lsclusters | awk '/^16 +main /{print $3}')
echo "PG16 cluster 'main' is on port $ACTUAL_PORT"

# Add timescaledb to shared_preload_libraries (idempotent)
PG16_CONF=/etc/postgresql/16/main/postgresql.conf
if ! sudo grep -qE "^shared_preload_libraries.*timescaledb" "$PG16_CONF"; then
    sudo bash -c "echo \"shared_preload_libraries = 'timescaledb'\" >> $PG16_CONF"
fi

# timescaledb-tune writes recommended settings; --quiet --yes makes it non-interactive.
sudo timescaledb-tune --pg-config "$TS_PG_CONFIG" --conf-path "$PG16_CONF" --quiet --yes || true

sudo systemctl restart postgresql@16-main

# Helper for psql against the PG16/TS cluster
psql_ts() {
    sudo -u postgres /usr/lib/postgresql/16/bin/psql -p "$ACTUAL_PORT" "$@"
}

# --- Database + extension --------------------------------------------------
psql_ts -c "DROP DATABASE IF EXISTS $DB"
psql_ts -c "CREATE DATABASE $DB"
psql_ts -d "$DB" -c "CREATE EXTENSION timescaledb VERSION '2.18.1'"
psql_ts -c "ALTER DATABASE $DB SET timescaledb.enable_chunk_skipping TO true"

# --- Data ------------------------------------------------------------------
sudo mkdir -p "$DATA_DIR"
download_one() {
    local name="$1"
    if [ ! -f "$DATA_DIR/$name.csv" ]; then
        sudo wget -q --continue -O "$DATA_DIR/$name.csv.gz" \
            "https://rtadatasets.timescale.com/$name.csv.gz"
        sudo pigz -d -f "$DATA_DIR/$name.csv.gz"
    fi
}
echo "Ensuring CSVs exist (reusing $DATA_DIR cache if present)..."
for f in customers products orders order_items order_events; do
    download_one "$f" &
done
wait
sudo chmod 644 "$DATA_DIR"/*.csv

# --- Schema + hypertable + compression ------------------------------------
psql_ts -d "$DB" < "$SCRIPT_DIR/create.sql"

psql_ts -d "$DB" -c \
    "SELECT create_hypertable('order_events', 'event_created', chunk_time_interval => interval '3 day', create_default_indexes => false)"
psql_ts -d "$DB" -c \
    "SELECT * FROM enable_chunk_skipping('order_events', 'order_id')"
psql_ts -d "$DB" -c \
    "ALTER TABLE order_events SET (timescaledb.compress, timescaledb.compress_segmentby = '', timescaledb.compress_orderby = 'order_id, event_created')"

# --- Load ------------------------------------------------------------------
LOAD_START=$(date +%s)
for t in customers products orders order_items order_events; do
    echo "Loading $t..."
    psql_ts -d "$DB" -c "COPY $t FROM '$DATA_DIR/$t.csv' WITH (FORMAT csv)"
done
LOAD_END=$(date +%s)
echo "Load time: $((LOAD_END - LOAD_START))s"

# --- Compress chunks in parallel (mirrors upstream compress.sh) -----------
WORKERS=8
CHUNKS=$(psql_ts -d "$DB" -t -X -c \
    "SELECT string_agg('(''' || ch::text || ''')', ',') FROM (SELECT row_number() over (), * from show_chunks('order_events') ch) ch GROUP BY row_number%${WORKERS};")
for chunk in $CHUNKS; do
    psql_ts -d "$DB" -c "set client_min_messages to error; SELECT compress_chunk(c::regclass) FROM (VALUES $chunk) v(c);" &
done
wait

# --- Index + vacuum (match plain-PG/pg_deltax baseline) -------------------
psql_ts -d "$DB" -c "CREATE INDEX ON orders (customer_id)"

echo -n "Vacuum time: "
VACUUM_START=$(date +%s)
psql_ts -d "$DB" -q -t -c "VACUUM FREEZE ANALYZE customers, products, orders, order_items, order_events"
VACUUM_END=$(date +%s)
echo "$((VACUUM_END - VACUUM_START))s"

# --- Tune for query phase, mirror pg_deltax cluster -----------------------
# Note: shared_buffers 8GB on TWO clusters = 16GB on a 32GB host. Fine because
# we never run both clusters at once for benchmarking; the OS pages in/out.
psql_ts -c "ALTER SYSTEM SET shared_buffers = '8GB'"
psql_ts -c "ALTER SYSTEM SET effective_cache_size = '24GB'"
psql_ts -c "ALTER SYSTEM SET max_worker_processes = 16"
psql_ts -c "ALTER SYSTEM SET max_parallel_workers = 16"
psql_ts -c "ALTER SYSTEM SET max_parallel_workers_per_gather = 8"
psql_ts -c "ALTER DATABASE $DB SET work_mem TO '8GB'"
psql_ts -c "ALTER DATABASE $DB SET jit TO off"
sudo systemctl restart postgresql@16-main

# --- Sanity report --------------------------------------------------------
psql_ts -d "$DB" -c "SELECT count(*) AS chunks FROM show_chunks('order_events') AS ch"
psql_ts -d "$DB" -c "SELECT hypertable_size('order_events')"

echo "TimescaleDB cluster ready on port $ACTUAL_PORT, database '$DB'."
echo "Connect: sudo -u postgres /usr/lib/postgresql/16/bin/psql -p $ACTUAL_PORT $DB"
