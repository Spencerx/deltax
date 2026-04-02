# pg_deltax

A PostgreSQL extension for time-series data, built on native declarative partitioning with automatic partition management.

## Features

- **Auto-partitioning**: Convert any table with a timestamp column into a partitioned deltatable
- **Background worker**: Automatically pre-creates future partitions and drains the default partition
- **`time_bucket()`**: Bucket timestamps into uniform intervals for aggregation
- **`first()` / `last()`**: Aggregates that return values associated with the earliest/latest timestamp

## Development

Requires Docker.

```sh
make test                      # run pgrx tests
make build                     # compile the extension
make clippy                    # run clippy
make cargo CMD="fmt --check"   # arbitrary cargo command
```

## Manual testing

```sh
make run    # start postgres with the extension (port 5432)
make psql   # connect to the running instance
```

## Integration tests

```sh
make integration-test                   # runs against PG 17 and 18
make integration-test PG_VERSIONS=17    # single version
```

A Python virtualenv (`.venv/`) is created automatically on first run.

## Build runtime image

```sh
make image  # builds pg_deltax:pg17
```

## Quick start

```sh
make run
# in another terminal:
psql -h localhost -U postgres -c "CREATE EXTENSION pg_deltax;"
```

```sql
CREATE TABLE metrics (ts TIMESTAMPTZ NOT NULL, device TEXT, value FLOAT8);
SELECT deltax_create_table('metrics', 'ts', '1 day');

INSERT INTO metrics VALUES (now(), 'sensor-1', 42.0);

SELECT time_bucket('1 hour', ts), avg(value) FROM metrics GROUP BY 1;
SELECT first(value, ts), last(value, ts) FROM metrics;
SELECT * FROM deltax_partition_info('metrics');

-- Compression
SELECT deltax_enable_compression('metrics', order_by => ARRAY['device', 'ts']);
SELECT deltax_compress_partition('metrics_p20250401');
SELECT * FROM deltax_compression_stats('metrics');

-- Size reporting (accounts for compressed storage)
SELECT pg_size_pretty(deltax_table_size('metrics'));
```

## Function reference

### Partitioning

| Function | Description |
|---|---|
| `deltax_create_table(relation, time_column, partition_interval DEFAULT '1 day', premake DEFAULT 3)` | Convert a table into a partitioned deltatable. Creates initial partitions around "now". |
| `deltax_partition_info(relation)` | List all partitions with their range bounds and compression status. |
| `deltax_deltatable_info(relation)` | Show metadata for a deltatable (time column, interval, partition count). |

### Retention

| Function | Description |
|---|---|
| `deltax_set_retention(relation, drop_after)` | Set a retention policy — partitions older than `drop_after` are automatically dropped by the background worker. |
| `deltax_remove_retention(relation)` | Remove the retention policy. |

### Compression

| Function | Description |
|---|---|
| `deltax_enable_compression(relation, segment_by DEFAULT '{}', order_by DEFAULT '{}', segment_size DEFAULT 30000)` | Enable compression on a deltatable. Configures how data is segmented and ordered within segments. |
| `deltax_set_compression_policy(relation, compress_after)` | Set automatic compression — partitions older than `compress_after` are compressed by the background worker. |
| `deltax_compress_partition(partition)` | Manually compress a single partition. |
| `deltax_decompress_partition(partition)` | Decompress a single partition back to heap storage. |
| `deltax_compression_stats(relation)` | Per-partition compression statistics: raw size, compressed size, ratio, row count. |
| `deltax_table_size(relation)` | Total on-disk size in bytes, accounting for compressed storage. Use with `pg_size_pretty()` for human-readable output. |

### Analytics

| Function | Description |
|---|---|
| `time_bucket(bucket_width, ts)` | Truncate a timestamp to the nearest interval boundary (like `date_trunc` but for arbitrary intervals). |
| `time_bucket(bucket_width, ts, origin)` | Same as above but with an offset (e.g., buckets starting at 06:00 instead of 00:00). |
| `first(value, ts)` | Aggregate: return the value associated with the earliest timestamp. |
| `last(value, ts)` | Aggregate: return the value associated with the latest timestamp. |

## License

Apache-2.0
