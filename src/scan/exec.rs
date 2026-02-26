use pgrx::pg_sys;
use pgrx::prelude::*;
use pgrx::pg_guard;

use crate::compression::{self, CompressionType, CompressedColumn};
use super::SyncStatic;

/// Static CustomExecMethods struct.
pub(crate) static CUSTOM_EXEC_METHODS: SyncStatic<pg_sys::CustomExecMethods> =
    SyncStatic(pg_sys::CustomExecMethods {
        CustomName: super::CUSTOM_NAME.as_ptr(),
        BeginCustomScan: Some(begin_custom_scan),
        ExecCustomScan: Some(exec_custom_scan),
        EndCustomScan: Some(end_custom_scan),
        ReScanCustomScan: Some(rescan_custom_scan),
        MarkPosCustomScan: None,
        RestrPosCustomScan: None,
        EstimateDSMCustomScan: None,
        InitializeDSMCustomScan: None,
        ReInitializeDSMCustomScan: None,
        InitializeWorkerCustomScan: None,
        ShutdownCustomScan: None,
        ExplainCustomScan: Some(super::explain::explain_custom_scan),
    });

/// Decompression state stored as a raw pointer in the CustomScanState.
pub(crate) struct DecompressState {
    /// Column names in the original table (in order).
    col_names: Vec<String>,
    /// Column type OIDs (in order).
    col_types: Vec<pg_sys::Oid>,
    /// Segment-by column names.
    segment_by: Vec<String>,
    /// All decompressed rows for the current segment: outer = column, inner = row values.
    current_segment: Vec<Vec<Option<String>>>,
    /// Current row index within current_segment.
    row_cursor: usize,
    /// Current segment index (0-based).
    segment_index: usize,
    /// Pre-loaded segments data from the companion table.
    segments_data: Vec<SegmentData>,
}

struct SegmentData {
    segment_values: Vec<Option<String>>,
    compressed_blobs: Vec<Vec<u8>>,
    row_count: i32,
}

/// CreateCustomScanState callback.
#[pg_guard]
pub unsafe extern "C-unwind" fn create_custom_scan_state(
    cscan: *mut pg_sys::CustomScan,
) -> *mut pg_sys::Node {
    unsafe {
        let css = pg_sys::palloc0(std::mem::size_of::<pg_sys::CustomScanState>())
            as *mut pg_sys::CustomScanState;

        (*css).ss.ps.type_ = pg_sys::NodeTag::T_CustomScanState;
        (*css).methods = &CUSTOM_EXEC_METHODS.0;

        // Copy custom_private (companion OID list) for use in BeginCustomScan
        (*css).custom_ps = (*cscan).custom_private;

        css as *mut pg_sys::Node
    }
}

/// BeginCustomScan callback: initialize decompression state.
#[pg_guard]
pub unsafe extern "C-unwind" fn begin_custom_scan(
    node: *mut pg_sys::CustomScanState,
    _estate: *mut pg_sys::EState,
    _eflags: i32,
) {
    unsafe {
        // Get companion OID from custom_private (stored as OID list)
        let custom_private = (*node).custom_ps;
        if custom_private.is_null() {
            pgrx::error!("pg_cocoon: missing companion table OID in custom scan state");
        }

        let companion_oid = pg_sys::list_nth_oid(custom_private, 0);

        // Get companion table name
        let companion_name = {
            let name_ptr = pg_sys::get_rel_name(companion_oid);
            if name_ptr.is_null() {
                pgrx::error!(
                    "pg_cocoon: companion table not found for OID {}",
                    u32::from(companion_oid)
                );
            }
            std::ffi::CStr::from_ptr(name_ptr)
                .to_string_lossy()
                .into_owned()
        };

        // Load all data via SPI
        let state = Spi::connect(|client| {
            load_decompress_state(&client, &companion_name)
        });

        // Box and store as raw pointer in custom_ps
        let state_box = Box::new(state);
        let state_ptr = Box::into_raw(state_box);
        (*node).custom_ps = state_ptr as *mut pg_sys::List;
    }
}

/// Load decompression state from the companion table via SPI.
fn load_decompress_state(
    client: &pgrx::spi::SpiClient<'_>,
    companion_name: &str,
) -> DecompressState {
    // Get the partition's hypertable info
    let mut ht_result = client
        .select(
            "SELECT h.segment_by, h.order_by, h.time_column, h.schema_name, h.table_name
             FROM cocoon_partition p
             JOIN cocoon_hypertable h ON h.id = p.hypertable_id
             WHERE p.table_name = $1 AND p.is_compressed = true",
            None,
            &[companion_name.into()],
        )
        .expect("failed to query partition info");

    let ht_row = ht_result.next().unwrap_or_else(|| {
        pgrx::error!(
            "pg_cocoon: no compressed partition info found for {}",
            companion_name
        );
    });

    let segment_by: Vec<String> = ht_row
        .get_datum_by_ordinal(1)
        .unwrap()
        .value::<Vec<String>>()
        .unwrap()
        .unwrap_or_default();
    let time_column: String = ht_row
        .get_datum_by_ordinal(3)
        .unwrap()
        .value::<String>()
        .unwrap()
        .unwrap();
    let parent_schema: String = ht_row
        .get_datum_by_ordinal(4)
        .unwrap()
        .value::<String>()
        .unwrap()
        .unwrap();
    let parent_table: String = ht_row
        .get_datum_by_ordinal(5)
        .unwrap()
        .value::<String>()
        .unwrap()
        .unwrap();

    // Get column info from the parent table
    let col_result = client
        .select(
            "SELECT column_name, udt_name
             FROM information_schema.columns
             WHERE table_schema = $1 AND table_name = $2
             ORDER BY ordinal_position",
            None,
            &[parent_schema.as_str().into(), parent_table.as_str().into()],
        )
        .expect("failed to get column info");

    let mut col_names = Vec::new();
    let mut col_type_names = Vec::new();
    for row in col_result {
        let name: String = row.get_datum_by_ordinal(1).unwrap().value::<String>().unwrap().unwrap();
        let type_name: String = row.get_datum_by_ordinal(2).unwrap().value::<String>().unwrap().unwrap();
        col_names.push(name);
        col_type_names.push(type_name);
    }

    // Build SELECT columns for companion table
    let companion_fqn = format!("\"_cocoon_compressed\".\"{}\"", companion_name);
    let mut select_cols = Vec::new();
    for name in &col_names {
        if segment_by.contains(name) {
            select_cols.push(format!("\"{}\"", name));
        }
    }
    for name in &col_names {
        if !segment_by.contains(name) {
            select_cols.push(format!("\"_{}_compressed\"", name));
        }
    }
    select_cols.push(format!("_min_{}", time_column));
    select_cols.push(format!("_max_{}", time_column));
    select_cols.push("_row_count".to_string());

    let read_query = format!("SELECT {} FROM {}", select_cols.join(", "), companion_fqn);
    let segments_result = client
        .select(&read_query, None, &[])
        .expect("failed to read segments");

    let mut segments_data = Vec::new();
    let non_seg_count = col_names.iter().filter(|n| !segment_by.contains(n)).count();

    for row in segments_result {
        let mut ordinal: usize = 1;
        let mut segment_values = Vec::new();
        let mut compressed_blobs = Vec::new();

        for name in &col_names {
            if segment_by.contains(name) {
                let val: Option<String> = row
                    .get_datum_by_ordinal(ordinal)
                    .unwrap()
                    .value::<String>()
                    .unwrap();
                segment_values.push(val);
                ordinal += 1;
            }
        }
        for name in &col_names {
            if !segment_by.contains(name) {
                let blob: Option<Vec<u8>> = row
                    .get_datum_by_ordinal(ordinal)
                    .unwrap()
                    .value::<Vec<u8>>()
                    .unwrap();
                compressed_blobs.push(blob.unwrap_or_default());
                ordinal += 1;
            }
        }

        // Skip _min_ts, _max_ts columns
        ordinal += 2;

        let row_count: i32 = row
            .get_datum_by_ordinal(ordinal)
            .unwrap()
            .value::<i32>()
            .unwrap()
            .unwrap_or(0);

        segments_data.push(SegmentData {
            segment_values,
            compressed_blobs,
            row_count,
        });
    }

    let col_types: Vec<pg_sys::Oid> = col_type_names.iter().map(|tn| pg_type_oid(tn)).collect();

    DecompressState {
        col_names,
        col_types,
        segment_by,
        current_segment: Vec::new(),
        row_cursor: 0,
        segment_index: 0,
        segments_data,
    }
}

/// ExecCustomScan callback: return the next tuple.
#[pg_guard]
pub unsafe extern "C-unwind" fn exec_custom_scan(
    node: *mut pg_sys::CustomScanState,
) -> *mut pg_sys::TupleTableSlot {
    unsafe {
        let slot = (*node).ss.ss_ScanTupleSlot;
        let state = &mut *((*node).custom_ps as *mut DecompressState);

        loop {
            // If current segment has more rows, return next one
            if !state.current_segment.is_empty() {
                let seg_rows = state.current_segment[0].len();
                if state.row_cursor < seg_rows {
                    fill_slot(slot, state);
                    state.row_cursor += 1;
                    return slot;
                }
            }

            // Move to next segment
            if state.segment_index >= state.segments_data.len() {
                pg_sys::ExecClearTuple(slot);
                return slot;
            }

            let seg = &state.segments_data[state.segment_index];
            state.segment_index += 1;

            if seg.row_count == 0 {
                continue;
            }

            // Decompress all columns
            let mut decompressed = Vec::new();
            let mut blob_idx = 0;
            let mut seg_val_idx = 0;

            for (col_idx, col_name) in state.col_names.iter().enumerate() {
                if state.segment_by.contains(col_name) {
                    let val = &seg.segment_values[seg_val_idx];
                    let repeated: Vec<Option<String>> =
                        (0..seg.row_count).map(|_| val.clone()).collect();
                    decompressed.push(repeated);
                    seg_val_idx += 1;
                } else {
                    let blob = &seg.compressed_blobs[blob_idx];
                    let type_name = pg_type_name(state.col_types[col_idx]);
                    let values = decompress_blob(blob, &type_name);
                    decompressed.push(values);
                    blob_idx += 1;
                }
            }

            state.current_segment = decompressed;
            state.row_cursor = 0;
        }
    }
}

/// EndCustomScan callback: cleanup.
#[pg_guard]
pub unsafe extern "C-unwind" fn end_custom_scan(
    node: *mut pg_sys::CustomScanState,
) {
    unsafe {
        let state_ptr = (*node).custom_ps as *mut DecompressState;
        if !state_ptr.is_null() {
            let _ = Box::from_raw(state_ptr);
            (*node).custom_ps = std::ptr::null_mut();
        }
    }
}

/// ReScanCustomScan callback: reset the scan.
#[pg_guard]
pub unsafe extern "C-unwind" fn rescan_custom_scan(
    node: *mut pg_sys::CustomScanState,
) {
    unsafe {
        let state = &mut *((*node).custom_ps as *mut DecompressState);
        state.segment_index = 0;
        state.row_cursor = 0;
        state.current_segment.clear();
    }
}

// ============================================================================
// Helpers
// ============================================================================

/// Fill a TupleTableSlot from the current segment at the current row cursor.
unsafe fn fill_slot(slot: *mut pg_sys::TupleTableSlot, state: &DecompressState) {
    unsafe {
        pg_sys::ExecClearTuple(slot);

        for col_idx in 0..state.col_names.len() {
            let val = &state.current_segment[col_idx][state.row_cursor];
            let type_oid = state.col_types[col_idx];

            match val {
                None => {
                    (*slot).tts_isnull.add(col_idx).write(true);
                    (*slot).tts_values.add(col_idx).write(pg_sys::Datum::from(0));
                }
                Some(s) => {
                    (*slot).tts_isnull.add(col_idx).write(false);
                    let datum = string_to_datum(s, type_oid);
                    (*slot).tts_values.add(col_idx).write(datum);
                }
            }
        }

        pg_sys::ExecStoreVirtualTuple(slot);
    }
}

/// Convert a string to a PostgreSQL Datum using the type's input function.
fn string_to_datum(s: &str, type_oid: pg_sys::Oid) -> pg_sys::Datum {
    unsafe {
        let cstr = std::ffi::CString::new(s).unwrap();
        let mut typinput: pg_sys::Oid = pg_sys::InvalidOid;
        let mut typioparam: pg_sys::Oid = pg_sys::InvalidOid;
        pg_sys::getTypeInputInfo(type_oid, &mut typinput, &mut typioparam);
        pg_sys::OidInputFunctionCall(typinput, cstr.as_ptr() as *mut _, typioparam, -1)
    }
}

/// Map a PG type name (udt_name) to a type OID.
fn pg_type_oid(type_name: &str) -> pg_sys::Oid {
    match type_name {
        "timestamptz" => pg_sys::TIMESTAMPTZOID,
        "timestamp" => pg_sys::TIMESTAMPOID,
        "float8" => pg_sys::FLOAT8OID,
        "float4" => pg_sys::FLOAT4OID,
        "int4" => pg_sys::INT4OID,
        "int8" => pg_sys::INT8OID,
        "bool" => pg_sys::BOOLOID,
        "text" => pg_sys::TEXTOID,
        "varchar" => pg_sys::VARCHAROID,
        _ => pg_sys::TEXTOID,
    }
}

/// Map a type OID back to a data_type string for codec dispatch.
fn pg_type_name(type_oid: pg_sys::Oid) -> String {
    if type_oid == pg_sys::TIMESTAMPTZOID || type_oid == pg_sys::TIMESTAMPOID {
        "timestamp with time zone".to_string()
    } else if type_oid == pg_sys::FLOAT8OID {
        "double precision".to_string()
    } else if type_oid == pg_sys::FLOAT4OID {
        "real".to_string()
    } else if type_oid == pg_sys::INT4OID {
        "integer".to_string()
    } else if type_oid == pg_sys::INT8OID {
        "bigint".to_string()
    } else if type_oid == pg_sys::BOOLOID {
        "boolean".to_string()
    } else {
        "text".to_string()
    }
}

/// Decompress a column blob, dispatching to the correct codec.
fn decompress_blob(blob: &[u8], data_type: &str) -> Vec<Option<String>> {
    if blob.is_empty() {
        return Vec::new();
    }

    let cc = CompressedColumn::from_bytes(blob);
    let total_count = cc.row_count as usize;
    let non_null_count = count_non_null(&cc.null_bitmap, total_count);
    let dt = data_type.to_lowercase();

    match cc.type_tag {
        CompressionType::Gorilla => {
            if dt.contains("timestamp") {
                let timestamps =
                    compression::gorilla::decode_timestamps(&cc.data, non_null_count);
                let strings: Vec<String> = timestamps
                    .iter()
                    .map(|&usec| {
                        Spi::get_one_with_args::<String>(
                            "SELECT to_timestamp($1)::timestamptz::text",
                            &[(usec as f64 / 1_000_000.0).into()],
                        )
                        .unwrap()
                        .unwrap()
                    })
                    .collect();
                compression::reinsert_nulls(&strings, &cc.null_bitmap, total_count)
            } else if dt == "real" || dt.contains("float4") {
                let floats =
                    compression::gorilla::decode_floats_f32(&cc.data, non_null_count);
                let strings: Vec<String> = floats.iter().map(|v| v.to_string()).collect();
                compression::reinsert_nulls(&strings, &cc.null_bitmap, total_count)
            } else {
                let floats = compression::gorilla::decode_floats(&cc.data, non_null_count);
                let strings: Vec<String> = floats.iter().map(|v| v.to_string()).collect();
                compression::reinsert_nulls(&strings, &cc.null_bitmap, total_count)
            }
        }
        CompressionType::DeltaVarint => {
            if dt == "integer" || dt.contains("int4") {
                let ints = compression::integer::decode_i32(&cc.data, non_null_count);
                let strings: Vec<String> = ints.iter().map(|v| v.to_string()).collect();
                compression::reinsert_nulls(&strings, &cc.null_bitmap, total_count)
            } else {
                let ints = compression::integer::decode_i64(&cc.data, non_null_count);
                let strings: Vec<String> = ints.iter().map(|v| v.to_string()).collect();
                compression::reinsert_nulls(&strings, &cc.null_bitmap, total_count)
            }
        }
        CompressionType::Dictionary => {
            let strings = compression::dictionary::decode(&cc.data, non_null_count);
            compression::reinsert_nulls(&strings, &cc.null_bitmap, total_count)
        }
        CompressionType::Lz4 => {
            let strings = compression::lz4::decode(&cc.data, non_null_count);
            compression::reinsert_nulls(&strings, &cc.null_bitmap, total_count)
        }
        CompressionType::BooleanBitmap => {
            let bools = compression::boolean::decode(&cc.data, non_null_count);
            let strings: Vec<String> = bools
                .iter()
                .map(|&b| if b { "t".to_string() } else { "f".to_string() })
                .collect();
            compression::reinsert_nulls(&strings, &cc.null_bitmap, total_count)
        }
    }
}

fn count_non_null(null_bitmap: &[u8], total_count: usize) -> usize {
    if null_bitmap.is_empty() {
        return total_count;
    }
    let null_count: usize = (0..total_count)
        .filter(|&i| (null_bitmap[i / 8] >> (i % 8)) & 1 == 1)
        .count();
    total_count - null_count
}
