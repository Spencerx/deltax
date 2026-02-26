use pgrx::prelude::*;
use pgrx::spi::SpiClient;

use crate::catalog;
use crate::compression::{self, CompressionType, CompressedColumn};

/// Column metadata from information_schema.
#[derive(Debug, Clone)]
struct ColumnMeta {
    name: String,
    data_type: String,
    is_segment_by: bool,
}

// ============================================================================
// SQL-callable functions
// ============================================================================

/// Enable compression on a cocoon hypertable.
///
/// ```sql
/// SELECT cocoon_enable_compression('metrics',
///     segment_by => ARRAY['device_id'],
///     order_by => ARRAY['ts']);
/// ```
#[pg_extern]
fn cocoon_enable_compression(
    relation: &str,
    segment_by: default!(Vec<String>, "ARRAY[]::text[]"),
    order_by: default!(Vec<String>, "ARRAY[]::text[]"),
) -> String {
    Spi::connect_mut(|client| {
        let (schema, table) = crate::partition::resolve_relation(client, relation);
        let ht = catalog::get_hypertable(client, &schema, &table)
            .expect("failed to query hypertable")
            .unwrap_or_else(|| {
                pgrx::error!("pg_cocoon: table {}.{} is not a cocoon table", schema, table)
            });

        // Validate segment_by columns exist
        for col in &segment_by {
            let exists = client
                .select(
                    "SELECT 1 FROM information_schema.columns
                     WHERE table_schema = $1 AND table_name = $2 AND column_name::text = $3",
                    None,
                    &[schema.as_str().into(), table.as_str().into(), col.as_str().into()],
                )
                .expect("failed to check column");
            if exists.is_empty() {
                pgrx::error!("pg_cocoon: segment_by column '{}' not found in {}.{}", col, schema, table);
            }
        }

        // If order_by is empty, default to the time column
        let effective_order_by = if order_by.is_empty() {
            vec![ht.time_column.clone()]
        } else {
            order_by
        };

        catalog::update_hypertable_compression(client, ht.id, &segment_by, &effective_order_by)
            .expect("failed to update compression settings");

        format!(
            "Compression enabled on {}.{} (segment_by: {:?}, order_by: {:?})",
            schema, table, segment_by, effective_order_by
        )
    })
}

/// Set the automatic compression policy for a hypertable.
#[pg_extern]
fn cocoon_set_compression_policy(
    relation: &str,
    compress_after: pgrx::datum::Interval,
) -> String {
    Spi::connect_mut(|client| {
        let (schema, table) = crate::partition::resolve_relation(client, relation);
        let ht = catalog::get_hypertable(client, &schema, &table)
            .expect("failed to query hypertable")
            .unwrap_or_else(|| {
                pgrx::error!("pg_cocoon: table {}.{} is not a cocoon table", schema, table)
            });

        if ht.segment_by.is_empty() && ht.order_by.is_empty() {
            pgrx::error!("pg_cocoon: enable compression first with cocoon_enable_compression()");
        }

        catalog::set_compress_after(client, ht.id, &compress_after)
            .expect("failed to set compression policy");

        format!(
            "Compression policy set on {}.{}: compress_after = {}",
            schema, table, compress_after
        )
    })
}

/// Compress a single partition.
#[pg_extern]
fn cocoon_compress_partition(partition: &str) -> String {
    Spi::connect_mut(|client| {
        compress_partition_impl(client, partition)
    })
}

/// Decompress a single partition.
#[pg_extern]
fn cocoon_decompress_partition(partition: &str) -> String {
    Spi::connect_mut(|client| {
        decompress_partition_impl(client, partition)
    })
}

/// Show compression statistics for a hypertable.
#[pg_extern]
fn cocoon_compression_stats(
    relation: &str,
) -> TableIterator<
    'static,
    (
        name!(partition_name, String),
        name!(is_compressed, bool),
        name!(raw_size, Option<i64>),
        name!(compressed_size, Option<i64>),
        name!(compression_ratio, Option<f64>),
        name!(row_count, Option<i64>),
    ),
> {
    let rows = Spi::connect(|client| {
        let (schema, table) = crate::partition::resolve_relation(client, relation);
        let ht = catalog::get_hypertable(client, &schema, &table)
            .expect("failed to query hypertable")
            .unwrap_or_else(|| {
                pgrx::error!("pg_cocoon: table {}.{} is not a cocoon table", schema, table)
            });

        let result = client
            .select(
                "SELECT table_name, is_compressed, raw_size, compressed_size, row_count
                 FROM cocoon_partition
                 WHERE hypertable_id = $1
                 ORDER BY range_start",
                None,
                &[ht.id.into()],
            )
            .expect("failed to query partitions");

        let mut rows = Vec::new();
        for row in result {
            let name: String = row.get_datum_by_ordinal(1).unwrap().value::<String>().unwrap().unwrap();
            let compressed: bool = row.get_datum_by_ordinal(2).unwrap().value::<bool>().unwrap().unwrap_or(false);
            let raw: Option<i64> = row.get_datum_by_ordinal(3).unwrap().value::<i64>().unwrap();
            let comp: Option<i64> = row.get_datum_by_ordinal(4).unwrap().value::<i64>().unwrap();
            let count: Option<i64> = row.get_datum_by_ordinal(5).unwrap().value::<i64>().unwrap();
            let ratio = match (raw, comp) {
                (Some(r), Some(c)) if c > 0 => Some(r as f64 / c as f64),
                _ => None,
            };
            rows.push((name, compressed, raw, comp, ratio, count));
        }
        rows
    });

    TableIterator::new(rows)
}

// ============================================================================
// Internal implementation
// ============================================================================

fn compress_partition_impl(client: &mut SpiClient, partition: &str) -> String {
    // 1. Look up partition in catalog
    let (schema, part_table) = crate::partition::resolve_relation(client, partition);
    let part_info = catalog::get_partition_by_name(client, &schema, &part_table)
        .expect("failed to query partition")
        .unwrap_or_else(|| {
            pgrx::error!("pg_cocoon: partition {}.{} not found in catalog", schema, part_table)
        });

    if part_info.is_compressed {
        return format!("Partition {}.{} is already compressed", schema, part_table);
    }

    // 2. Get hypertable info (compression settings)
    let ht = catalog::get_hypertable_by_id(client, part_info.hypertable_id)
        .expect("failed to query hypertable")
        .unwrap();

    if ht.order_by.is_empty() && ht.segment_by.is_empty() {
        pgrx::error!("pg_cocoon: compression not enabled on {}.{}. Call cocoon_enable_compression() first.",
            ht.schema_name, ht.table_name);
    }

    // 3. Get column metadata
    let columns = get_column_metadata(client, &schema, &part_table, &ht.segment_by);
    if columns.is_empty() {
        pgrx::error!("pg_cocoon: no columns found for {}.{}", schema, part_table);
    }

    // 4. Count rows
    let part_fqn = crate::partition::fqn(&schema, &part_table);
    let row_count = client
        .select(&format!("SELECT count(*)::int8 FROM {}", part_fqn), None, &[])
        .expect("failed to count rows")
        .first()
        .get_one::<i64>()
        .unwrap()
        .unwrap_or(0);

    if row_count == 0 {
        return format!("Partition {}.{} has no rows to compress", schema, part_table);
    }

    // 5. Build companion table DDL
    let companion_schema = "_cocoon_compressed";
    let companion_fqn = format!("\"{}\".\"{}\"", companion_schema, part_table);

    let mut create_cols = Vec::new();
    // Segment-by columns stay uncompressed
    for col in &columns {
        if col.is_segment_by {
            create_cols.push(format!("\"{}\" {}", col.name, col.data_type));
        }
    }
    // Compressed columns as BYTEA
    for col in &columns {
        if !col.is_segment_by {
            create_cols.push(format!("\"_{}_compressed\" BYTEA", col.name));
        }
    }
    // Min/max metadata for the time column
    create_cols.push(format!("_min_{} TIMESTAMPTZ", ht.time_column));
    create_cols.push(format!("_max_{} TIMESTAMPTZ", ht.time_column));
    create_cols.push("_row_count INT".to_string());

    let create_ddl = format!(
        "CREATE TABLE {} ({})",
        companion_fqn,
        create_cols.join(", ")
    );
    client.update(&create_ddl, None, &[]).expect("failed to create companion table");

    // 6. Build ORDER BY clause
    let order_clause = if !ht.order_by.is_empty() {
        format!(
            "ORDER BY {}",
            ht.order_by
                .iter()
                .map(|c| format!("\"{}\"", c))
                .collect::<Vec<_>>()
                .join(", ")
        )
    } else {
        String::new()
    };

    // 7. Read and compress data per segment
    let mut total_compressed_size: i64 = 0;
    let raw_size = estimate_raw_size(client, &part_fqn);

    if ht.segment_by.is_empty() {
        // No segment_by: entire partition is one segment (split at 100k rows)
        let total = compress_segment(
            client,
            &part_fqn,
            &companion_fqn,
            &columns,
            &ht.time_column,
            "TRUE",
            &order_clause,
            &[],
        );
        total_compressed_size += total;
    } else {
        // Get distinct segment values
        let segment_cols_quoted: Vec<String> = ht.segment_by.iter().map(|c| format!("\"{}\"", c)).collect();
        let segment_cols_str = segment_cols_quoted.join(", ");

        let segments_query = format!(
            "SELECT DISTINCT {} FROM {} ORDER BY {}",
            segment_cols_str, part_fqn, segment_cols_str
        );
        let segment_result = client
            .select(&segments_query, None, &[])
            .expect("failed to get segments");

        // Collect segment values
        let mut segment_filters: Vec<String> = Vec::new();

        for row in segment_result {
            let mut conditions = Vec::new();
            for (i, col_name) in ht.segment_by.iter().enumerate() {
                let val: Option<String> = row
                    .get_datum_by_ordinal(i + 1)
                    .unwrap()
                    .value::<String>()
                    .unwrap();
                match val {
                    Some(v) => {
                        conditions.push(format!("\"{}\" = '{}'", col_name, v.replace('\'', "''")));
                    }
                    None => {
                        conditions.push(format!("\"{}\" IS NULL", col_name));
                    }
                }
            }
            segment_filters.push(conditions.join(" AND "));
        }

        for filter in &segment_filters {
            let total = compress_segment(
                client,
                &part_fqn,
                &companion_fqn,
                &columns,
                &ht.time_column,
                filter,
                &order_clause,
                &ht.segment_by,
            );
            total_compressed_size += total;
        }
    }

    // 8. Truncate original partition (stays attached to parent)
    client
        .update(&format!("TRUNCATE {}", part_fqn), None, &[])
        .expect("failed to truncate partition");

    // 9. Update catalog
    catalog::mark_partition_compressed(
        client,
        part_info.id,
        total_compressed_size,
        raw_size,
        row_count,
    )
    .expect("failed to update catalog");

    format!(
        "Compressed {}.{}: {} rows, ratio {:.1}x",
        schema,
        part_table,
        row_count,
        if total_compressed_size > 0 {
            raw_size as f64 / total_compressed_size as f64
        } else {
            0.0
        }
    )
}

/// Compress a single segment (subset of rows matching the segment filter).
/// Returns the total compressed size in bytes.
fn compress_segment(
    client: &mut SpiClient,
    part_fqn: &str,
    companion_fqn: &str,
    columns: &[ColumnMeta],
    time_column: &str,
    where_clause: &str,
    order_clause: &str,
    segment_by: &[String],
) -> i64 {
    let col_list: String = columns
        .iter()
        .map(|c| format!("\"{}\"::text", c.name))
        .collect::<Vec<_>>()
        .join(", ");

    let query = format!(
        "SELECT {} FROM {} WHERE {} {}",
        col_list, part_fqn, where_clause, order_clause
    );

    let result = client.select(&query, None, &[]).expect("failed to read segment data");

    // Collect all column values
    let mut col_values: Vec<Vec<Option<String>>> = vec![Vec::new(); columns.len()];
    let mut segment_by_values: Vec<Option<String>> = Vec::new();
    let mut row_count = 0u32;

    for row in result {
        for (i, _col) in columns.iter().enumerate() {
            let val: Option<String> = row
                .get_datum_by_ordinal(i + 1)
                .unwrap()
                .value::<String>()
                .unwrap();
            col_values[i].push(val);
        }
        row_count += 1;

        // Capture segment_by values from first row
        if segment_by_values.is_empty() && !segment_by.is_empty() {
            for (i, col) in columns.iter().enumerate() {
                if col.is_segment_by {
                    segment_by_values.push(col_values[i].last().unwrap().clone());
                }
            }
        }
    }

    if row_count == 0 {
        return 0;
    }

    // Compress each non-segment column
    let mut compressed_data: Vec<(String, Vec<u8>)> = Vec::new();
    let mut min_ts: Option<String> = None;
    let mut max_ts: Option<String> = None;
    let mut total_size: i64 = 0;

    for (i, col) in columns.iter().enumerate() {
        if col.is_segment_by {
            continue;
        }

        let values = &col_values[i];
        let compressed = compress_column_values(values, &col.data_type, &col.name);

        // Track min/max for time column
        if col.name == time_column {
            for v in values {
                if let Some(val) = v {
                    match &min_ts {
                        None => min_ts = Some(val.clone()),
                        Some(cur) if val < cur => min_ts = Some(val.clone()),
                        _ => {}
                    }
                    match &max_ts {
                        None => max_ts = Some(val.clone()),
                        Some(cur) if val > cur => max_ts = Some(val.clone()),
                        _ => {}
                    }
                }
            }
        }

        total_size += compressed.len() as i64;
        compressed_data.push((col.name.clone(), compressed));
    }

    // Build INSERT into companion table
    let mut insert_cols = Vec::new();
    let mut insert_vals = Vec::new();

    // Segment-by columns
    for col in columns {
        if col.is_segment_by {
            insert_cols.push(format!("\"{}\"", col.name));
            let val = segment_by_values
                .iter()
                .find(|_| true) // We already captured them
                .cloned()
                .flatten();
            match val {
                Some(v) => insert_vals.push(format!("'{}'", v.replace('\'', "''"))),
                None => insert_vals.push("NULL".to_string()),
            }
        }
    }
    // Capture segment_by values properly
    insert_cols.clear();
    insert_vals.clear();

    let mut seg_idx = 0;
    for col in columns {
        if col.is_segment_by {
            insert_cols.push(format!("\"{}\"", col.name));
            if seg_idx < segment_by_values.len() {
                match &segment_by_values[seg_idx] {
                    Some(v) => insert_vals.push(format!("'{}'", v.replace('\'', "''"))),
                    None => insert_vals.push("NULL".to_string()),
                }
                seg_idx += 1;
            }
        }
    }

    // Compressed columns — use parameterized query to avoid escaping issues with bytea
    for (col_name, _) in &compressed_data {
        insert_cols.push(format!("\"_{}_compressed\"", col_name));
    }

    // Min/max time + row count
    insert_cols.push(format!("_min_{}", time_column));
    insert_cols.push(format!("_max_{}", time_column));
    insert_cols.push("_row_count".to_string());

    // Build the INSERT using hex-encoded bytea literals
    for (_, data) in &compressed_data {
        let hex = hex_encode(data);
        insert_vals.push(format!("'\\x{}'::bytea", hex));
    }

    match &min_ts {
        Some(v) => insert_vals.push(format!("'{}'::timestamptz", v)),
        None => insert_vals.push("NULL".to_string()),
    }
    match &max_ts {
        Some(v) => insert_vals.push(format!("'{}'::timestamptz", v)),
        None => insert_vals.push("NULL".to_string()),
    }
    insert_vals.push(row_count.to_string());

    let insert_sql = format!(
        "INSERT INTO {} ({}) VALUES ({})",
        companion_fqn,
        insert_cols.join(", "),
        insert_vals.join(", ")
    );
    client.update(&insert_sql, None, &[]).expect("failed to insert compressed segment");

    total_size
}

/// Compress a column's values based on the PostgreSQL data type.
fn compress_column_values(values: &[Option<String>], data_type: &str, _col_name: &str) -> Vec<u8> {
    let dt = data_type.to_lowercase();

    if dt.contains("timestamp") {
        // Parse as i64 microseconds and use Gorilla timestamp encoding
        let (non_null, null_bitmap) = compression::extract_nulls(values);
        let timestamps: Vec<i64> = non_null
            .iter()
            .map(|v| parse_timestamp_to_usec(v))
            .collect();
        let data = compression::gorilla::encode_timestamps(&timestamps);
        CompressedColumn {
            type_tag: CompressionType::Gorilla,
            row_count: values.len() as u32,
            null_bitmap,
            data,
        }
        .to_bytes()
    } else if dt == "double precision" || dt == "float8" || dt == "real" || dt == "float4" {
        let (non_null, null_bitmap) = compression::extract_nulls(values);
        if dt == "real" || dt == "float4" {
            let floats: Vec<f32> = non_null.iter().map(|v| v.parse::<f32>().unwrap_or(0.0)).collect();
            let data = compression::gorilla::encode_floats_f32(&floats);
            CompressedColumn {
                type_tag: CompressionType::Gorilla,
                row_count: values.len() as u32,
                null_bitmap,
                data,
            }
            .to_bytes()
        } else {
            let floats: Vec<f64> = non_null.iter().map(|v| v.parse::<f64>().unwrap_or(0.0)).collect();
            let data = compression::gorilla::encode_floats(&floats);
            CompressedColumn {
                type_tag: CompressionType::Gorilla,
                row_count: values.len() as u32,
                null_bitmap,
                data,
            }
            .to_bytes()
        }
    } else if dt == "integer" || dt == "int4" {
        let (non_null, null_bitmap) = compression::extract_nulls(values);
        let ints: Vec<i32> = non_null.iter().map(|v| v.parse::<i32>().unwrap_or(0)).collect();
        let data = compression::integer::encode_i32(&ints);
        CompressedColumn {
            type_tag: CompressionType::DeltaVarint,
            row_count: values.len() as u32,
            null_bitmap,
            data,
        }
        .to_bytes()
    } else if dt == "bigint" || dt == "int8" {
        let (non_null, null_bitmap) = compression::extract_nulls(values);
        let ints: Vec<i64> = non_null.iter().map(|v| v.parse::<i64>().unwrap_or(0)).collect();
        let data = compression::integer::encode_i64(&ints);
        CompressedColumn {
            type_tag: CompressionType::DeltaVarint,
            row_count: values.len() as u32,
            null_bitmap,
            data,
        }
        .to_bytes()
    } else if dt == "boolean" || dt == "bool" {
        let (non_null, null_bitmap) = compression::extract_nulls(values);
        let bools: Vec<bool> = non_null.iter().map(|v| v == "t" || v == "true" || v == "1").collect();
        let data = compression::boolean::encode(&bools);
        CompressedColumn {
            type_tag: CompressionType::BooleanBitmap,
            row_count: values.len() as u32,
            null_bitmap,
            data,
        }
        .to_bytes()
    } else {
        // TEXT and other types
        let (non_null, null_bitmap) = compression::extract_nulls(values);
        let refs: Vec<&str> = non_null.iter().map(|s| s.as_str()).collect();

        let data = if compression::dictionary::should_use_dictionary(&refs) {
            let encoded = compression::dictionary::encode(&refs);
            // Wrap with Dictionary tag
            CompressedColumn {
                type_tag: CompressionType::Dictionary,
                row_count: values.len() as u32,
                null_bitmap,
                data: encoded,
            }
            .to_bytes()
        } else {
            let encoded = compression::lz4::encode(&refs);
            CompressedColumn {
                type_tag: CompressionType::Lz4,
                row_count: values.len() as u32,
                null_bitmap,
                data: encoded,
            }
            .to_bytes()
        };
        data
    }
}

fn parse_timestamp_to_usec(s: &str) -> i64 {
    Spi::get_one_with_args::<i64>(
        "SELECT (EXTRACT(EPOCH FROM $1::timestamptz) * 1000000)::int8",
        &[s.into()],
    )
    .expect("failed to parse timestamp")
    .unwrap()
}

fn hex_encode(data: &[u8]) -> String {
    let mut s = String::with_capacity(data.len() * 2);
    for &b in data {
        s.push_str(&format!("{:02x}", b));
    }
    s
}

/// Get column metadata for a table.
fn get_column_metadata(
    client: &SpiClient,
    schema: &str,
    table: &str,
    segment_by: &[String],
) -> Vec<ColumnMeta> {
    let result = client
        .select(
            "SELECT column_name::text, data_type::text
             FROM information_schema.columns
             WHERE table_schema = $1 AND table_name = $2
             ORDER BY ordinal_position",
            None,
            &[schema.into(), table.into()],
        )
        .expect("failed to get columns");

    let mut columns = Vec::new();
    for row in result {
        let name: String = row.get_datum_by_ordinal(1).unwrap().value::<String>().unwrap().unwrap();
        let data_type: String = row.get_datum_by_ordinal(2).unwrap().value::<String>().unwrap().unwrap();
        let is_segment = segment_by.contains(&name);
        columns.push(ColumnMeta {
            name,
            data_type,
            is_segment_by: is_segment,
        });
    }
    columns
}

/// Estimate raw table size in bytes.
fn estimate_raw_size(client: &SpiClient, table_fqn: &str) -> i64 {
    client
        .select(
            &format!("SELECT pg_total_relation_size('{}'::regclass)::int8", table_fqn),
            None,
            &[],
        )
        .expect("failed to get table size")
        .first()
        .get_one::<i64>()
        .unwrap()
        .unwrap_or(0)
}

// ============================================================================
// Decompression
// ============================================================================

fn decompress_partition_impl(client: &mut SpiClient, partition: &str) -> String {
    // 1. Look up partition
    let (schema, part_table) = crate::partition::resolve_relation(client, partition);
    let part_info = catalog::get_partition_by_name(client, &schema, &part_table)
        .expect("failed to query partition")
        .unwrap_or_else(|| {
            pgrx::error!("pg_cocoon: partition {}.{} not found in catalog", schema, part_table)
        });

    if !part_info.is_compressed {
        return format!("Partition {}.{} is not compressed", schema, part_table);
    }

    let ht = catalog::get_hypertable_by_id(client, part_info.hypertable_id)
        .expect("failed to query hypertable")
        .unwrap();

    // 2. Get column metadata (from the parent table, since partition is truncated)
    let columns = get_column_metadata(client, &ht.schema_name, &ht.table_name, &ht.segment_by);

    let companion_schema = "_cocoon_compressed";
    let companion_fqn = format!("\"{}\".\"{}\"", companion_schema, part_table);
    let part_fqn = crate::partition::fqn(&schema, &part_table);

    // 3. Read compressed segments
    let mut select_cols = Vec::new();
    for col in &columns {
        if col.is_segment_by {
            select_cols.push(format!("\"{}\"", col.name));
        }
    }
    for col in &columns {
        if !col.is_segment_by {
            select_cols.push(format!("\"_{}_compressed\"", col.name));
        }
    }
    select_cols.push("_row_count".to_string());

    let read_query = format!(
        "SELECT {} FROM {}",
        select_cols.join(", "),
        companion_fqn
    );
    let segments = client.select(&read_query, None, &[]).expect("failed to read compressed data");

    let mut total_rows_restored = 0i64;

    for row in segments {
        let mut col_ordinal: usize = 1;
        let mut segment_by_vals: Vec<Option<String>> = Vec::new();
        let mut compressed_blobs: Vec<(String, String, Vec<u8>)> = Vec::new(); // (name, data_type, blob)

        // Read segment_by values
        for col in &columns {
            if col.is_segment_by {
                let val: Option<String> = row
                    .get_datum_by_ordinal(col_ordinal)
                    .unwrap()
                    .value::<String>()
                    .unwrap();
                segment_by_vals.push(val);
                col_ordinal += 1;
            }
        }

        // Read compressed blobs
        for col in &columns {
            if !col.is_segment_by {
                let blob: Option<Vec<u8>> = row
                    .get_datum_by_ordinal(col_ordinal)
                    .unwrap()
                    .value::<Vec<u8>>()
                    .unwrap();
                compressed_blobs.push((
                    col.name.clone(),
                    col.data_type.clone(),
                    blob.unwrap_or_default(),
                ));
                col_ordinal += 1;
            }
        }

        let segment_row_count: i32 = row
            .get_datum_by_ordinal(col_ordinal)
            .unwrap()
            .value::<i32>()
            .unwrap()
            .unwrap_or(0);

        if segment_row_count == 0 {
            continue;
        }

        // Decompress all columns
        let mut decompressed_cols: Vec<(String, Vec<Option<String>>)> = Vec::new();

        // Segment-by columns: repeat the value for every row
        let mut seg_idx = 0;
        for col in &columns {
            if col.is_segment_by {
                let val = &segment_by_vals[seg_idx];
                let repeated: Vec<Option<String>> =
                    (0..segment_row_count).map(|_| val.clone()).collect();
                decompressed_cols.push((col.name.clone(), repeated));
                seg_idx += 1;
            }
        }

        // Compressed columns: decompress
        for (name, data_type, blob) in &compressed_blobs {
            let values = decompress_column_values(blob, data_type);
            decompressed_cols.push((name.clone(), values));
        }

        // Sort columns back to original order
        let mut ordered_cols: Vec<(String, Vec<Option<String>>)> = Vec::new();
        for col in &columns {
            for dc in &decompressed_cols {
                if dc.0 == col.name {
                    ordered_cols.push(dc.clone());
                    break;
                }
            }
        }

        // INSERT rows back into partition
        let col_names: String = ordered_cols
            .iter()
            .map(|(name, _)| format!("\"{}\"", name))
            .collect::<Vec<_>>()
            .join(", ");

        for row_idx in 0..segment_row_count as usize {
            let vals: Vec<String> = ordered_cols
                .iter()
                .enumerate()
                .map(|(col_idx, (_, values))| {
                    let col_meta = &columns[col_idx];
                    match &values[row_idx] {
                        None => "NULL".to_string(),
                        Some(v) => format_value_for_insert(v, &col_meta.data_type),
                    }
                })
                .collect();

            let insert_sql = format!(
                "INSERT INTO {} ({}) VALUES ({})",
                part_fqn,
                col_names,
                vals.join(", ")
            );
            client.update(&insert_sql, None, &[]).expect("failed to insert decompressed row");
        }

        total_rows_restored += segment_row_count as i64;
    }

    // 4. Drop companion table
    client
        .update(&format!("DROP TABLE IF EXISTS {}", companion_fqn), None, &[])
        .expect("failed to drop companion table");

    // 5. Update catalog
    catalog::mark_partition_decompressed(client, part_info.id)
        .expect("failed to update catalog");

    format!(
        "Decompressed {}.{}: {} rows restored",
        schema, part_table, total_rows_restored
    )
}

/// Decompress a column blob back to string representations.
fn decompress_column_values(blob: &[u8], data_type: &str) -> Vec<Option<String>> {
    if blob.is_empty() {
        return Vec::new();
    }

    let cc = CompressedColumn::from_bytes(blob);
    let total_count = cc.row_count as usize;
    let dt = data_type.to_lowercase();

    match cc.type_tag {
        CompressionType::Gorilla => {
            if dt.contains("timestamp") {
                let timestamps = compression::gorilla::decode_timestamps(&cc.data, count_non_null(&cc.null_bitmap, total_count));
                let strings: Vec<String> = timestamps
                    .iter()
                    .map(|&usec| usec_to_timestamp_string(usec))
                    .collect();
                compression::reinsert_nulls(&strings, &cc.null_bitmap, total_count)
            } else if dt == "real" || dt == "float4" {
                let floats = compression::gorilla::decode_floats_f32(&cc.data, count_non_null(&cc.null_bitmap, total_count));
                let strings: Vec<String> = floats.iter().map(|v| v.to_string()).collect();
                compression::reinsert_nulls(&strings, &cc.null_bitmap, total_count)
            } else {
                let floats = compression::gorilla::decode_floats(&cc.data, count_non_null(&cc.null_bitmap, total_count));
                let strings: Vec<String> = floats.iter().map(|v| v.to_string()).collect();
                compression::reinsert_nulls(&strings, &cc.null_bitmap, total_count)
            }
        }
        CompressionType::DeltaVarint => {
            if dt == "integer" || dt == "int4" {
                let ints = compression::integer::decode_i32(&cc.data, count_non_null(&cc.null_bitmap, total_count));
                let strings: Vec<String> = ints.iter().map(|v| v.to_string()).collect();
                compression::reinsert_nulls(&strings, &cc.null_bitmap, total_count)
            } else {
                let ints = compression::integer::decode_i64(&cc.data, count_non_null(&cc.null_bitmap, total_count));
                let strings: Vec<String> = ints.iter().map(|v| v.to_string()).collect();
                compression::reinsert_nulls(&strings, &cc.null_bitmap, total_count)
            }
        }
        CompressionType::Dictionary => {
            let strings = compression::dictionary::decode(&cc.data, count_non_null(&cc.null_bitmap, total_count));
            compression::reinsert_nulls(&strings, &cc.null_bitmap, total_count)
        }
        CompressionType::Lz4 => {
            let strings = compression::lz4::decode(&cc.data, count_non_null(&cc.null_bitmap, total_count));
            compression::reinsert_nulls(&strings, &cc.null_bitmap, total_count)
        }
        CompressionType::BooleanBitmap => {
            let bools = compression::boolean::decode(&cc.data, count_non_null(&cc.null_bitmap, total_count));
            let strings: Vec<String> = bools.iter().map(|&b| if b { "t".to_string() } else { "f".to_string() }).collect();
            compression::reinsert_nulls(&strings, &cc.null_bitmap, total_count)
        }
    }
}

/// Count non-null values given a null bitmap and total count.
fn count_non_null(null_bitmap: &[u8], total_count: usize) -> usize {
    if null_bitmap.is_empty() {
        return total_count;
    }
    let null_count: usize = (0..total_count)
        .filter(|&i| (null_bitmap[i / 8] >> (i % 8)) & 1 == 1)
        .count();
    total_count - null_count
}

fn usec_to_timestamp_string(usec: i64) -> String {
    Spi::get_one_with_args::<String>(
        "SELECT to_timestamp($1)::timestamptz::text",
        &[(usec as f64 / 1_000_000.0).into()],
    )
    .expect("failed to format timestamp")
    .unwrap()
}

fn format_value_for_insert(value: &str, data_type: &str) -> String {
    let dt = data_type.to_lowercase();
    if dt.contains("timestamp") {
        format!("'{}'::timestamptz", value.replace('\'', "''"))
    } else if dt == "boolean" || dt == "bool" {
        if value == "t" || value == "true" || value == "1" {
            "true".to_string()
        } else {
            "false".to_string()
        }
    } else if dt == "integer" || dt == "int4" || dt == "bigint" || dt == "int8"
        || dt == "double precision" || dt == "float8" || dt == "real" || dt == "float4"
    {
        value.to_string()
    } else {
        format!("'{}'", value.replace('\'', "''"))
    }
}

/// Public function used by the background worker for auto-compression.
pub fn auto_compress_partitions(client: &mut SpiClient<'_>, ht: &catalog::HypertableInfo) -> i32 {
    let compress_after = match &ht.compress_after {
        Some(interval) => interval,
        None => return 0,
    };

    if ht.order_by.is_empty() && ht.segment_by.is_empty() {
        return 0;
    }

    // Find partitions eligible for compression:
    // range_end < now() - compress_after AND NOT is_compressed
    let eligible = client
        .select(
            "SELECT table_name FROM cocoon_partition
             WHERE hypertable_id = $1 AND is_compressed = false
               AND range_end < now() - $2::interval",
            None,
            &[ht.id.into(), (*compress_after).into()],
        )
        .expect("failed to query eligible partitions");

    let mut partition_names: Vec<String> = Vec::new();
    for row in eligible {
        let name: String = row
            .get_datum_by_ordinal(1)
            .unwrap()
            .value::<String>()
            .unwrap()
            .unwrap();
        partition_names.push(name);
    }

    let mut compressed = 0;
    for name in &partition_names {
        compress_partition_impl(client, name);
        compressed += 1;
    }

    compressed
}
