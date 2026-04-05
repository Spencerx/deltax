//! Direct backfill: intercept `COPY ... FROM` with `FORMAT deltax_compress`
//! via a ProcessUtility hook, compress data in-flight, and write directly
//! to companion tables without touching the heap.

use std::ffi::{CStr, CString, c_char};
use std::io::{BufRead, BufReader};
use std::sync::atomic::{AtomicPtr, Ordering};

use pgrx::pg_sys;
use pgrx::pg_sys::ffi::pg_guard_ffi_boundary;
use pgrx::prelude::*;
// SpiClient no longer needed — all SPI calls use short-lived Spi::connect/connect_mut

use crate::catalog;
use crate::compress::{
    ColumnKind, ColumnMeta, TypedColumn,
    PG_EPOCH_OFFSET_USEC,
    build_companion_ddl, classify_column, compress_typed_column,
    compute_segment_blooms, compute_segment_ndistinct,
    compute_typed_minmax, compute_typed_sum,
    format_minmax_for_insert, init_typed_columns, new_typed_column, get_column_metadata,
    sort_typed_columns, supports_minmax, supports_sum,
};
use crate::copyparse::{
    CopyLineReader, CopyTextOptions, HeaderMode, LineResult,
    split_fields, split_field_offsets, parse_and_append, parse_raw_field_and_append,
};

static PREV_PROCESS_UTILITY_HOOK: AtomicPtr<()> = AtomicPtr::new(std::ptr::null_mut());

/// Register the ProcessUtility hook. Must be called from `_PG_init()`.
///
/// # Safety
/// Must be called exactly once during extension initialization.
pub unsafe fn register_process_utility_hook() {
    unsafe {
        let prev = pg_sys::ProcessUtility_hook;
        if let Some(prev_fn) = prev {
            PREV_PROCESS_UTILITY_HOOK.store(prev_fn as *mut (), Ordering::SeqCst);
        }
        pg_sys::ProcessUtility_hook = Some(deltax_process_utility);
    }
}

/// Chain to the previous ProcessUtility hook, or call standard_ProcessUtility.
#[allow(clippy::too_many_arguments)]
unsafe fn chain_to_prev(
    pstmt: *mut pg_sys::PlannedStmt,
    query_string: *const c_char,
    read_only_tree: bool,
    context: pg_sys::ProcessUtilityContext::Type,
    params: pg_sys::ParamListInfo,
    query_env: *mut pg_sys::QueryEnvironment,
    dest: *mut pg_sys::DestReceiver,
    qc: *mut pg_sys::QueryCompletion,
) {
    unsafe {
        let prev_ptr = PREV_PROCESS_UTILITY_HOOK.load(Ordering::SeqCst);
        if !prev_ptr.is_null() {
            let prev_fn: pg_sys::ProcessUtility_hook_type = Some(
                std::mem::transmute::<
                    *mut (),
                    unsafe extern "C-unwind" fn(
                        *mut pg_sys::PlannedStmt,
                        *const c_char,
                        bool,
                        pg_sys::ProcessUtilityContext::Type,
                        pg_sys::ParamListInfo,
                        *mut pg_sys::QueryEnvironment,
                        *mut pg_sys::DestReceiver,
                        *mut pg_sys::QueryCompletion,
                    ),
                >(prev_ptr),
            );
            if let Some(f) = prev_fn {
                pg_guard_ffi_boundary(|| {
                    f(pstmt, query_string, read_only_tree, context, params, query_env, dest, qc)
                });
            }
        } else {
            pg_sys::standard_ProcessUtility(
                pstmt, query_string, read_only_tree, context, params, query_env, dest, qc,
            );
        }
    }
}

#[pg_guard]
#[allow(clippy::too_many_arguments)]
unsafe extern "C-unwind" fn deltax_process_utility(
    pstmt: *mut pg_sys::PlannedStmt,
    query_string: *const c_char,
    read_only_tree: bool,
    context: pg_sys::ProcessUtilityContext::Type,
    params: pg_sys::ParamListInfo,
    query_env: *mut pg_sys::QueryEnvironment,
    dest: *mut pg_sys::DestReceiver,
    qc: *mut pg_sys::QueryCompletion,
) {
    let utility_stmt = unsafe { (*pstmt).utilityStmt };
    if utility_stmt.is_null() || !unsafe { pgrx::is_a(utility_stmt, pg_sys::NodeTag::T_CopyStmt) } {
        unsafe {
            chain_to_prev(pstmt, query_string, read_only_tree, context, params, query_env, dest, qc);
        }
        return;
    }

    let copy_stmt = utility_stmt as *mut pg_sys::CopyStmt;
    let cs = unsafe { &*copy_stmt };

    // Only intercept COPY FROM (not COPY TO)
    if !cs.is_from {
        unsafe {
            chain_to_prev(pstmt, query_string, read_only_tree, context, params, query_env, dest, qc);
        }
        return;
    }

    // Check for FORMAT deltax_compress in options
    let format_idx = find_deltax_format_option(cs.options);
    if format_idx < 0 {
        unsafe {
            chain_to_prev(pstmt, query_string, read_only_tree, context, params, query_env, dest, qc);
        }
        return;
    }

    // This is our COPY — handle it
    handle_copy_from_deltax_compress(copy_stmt, format_idx);

    // Set QueryCompletion to report rows
    if !qc.is_null() {
        unsafe {
            (*qc).commandTag = pg_sys::CommandTag::CMDTAG_COPY;
            (*qc).nprocessed = 0; // updated inside handle_copy_from_deltax_compress via notice
        }
    }
}

/// Walk the options list looking for `FORMAT 'deltax_compress'`.
/// Returns the list index of the matching DefElem, or -1 if not found.
fn find_deltax_format_option(options: *mut pg_sys::List) -> i32 {
    if options.is_null() {
        return -1;
    }
    let list = unsafe { &*options };
    let len = list.length;
    for i in 0..len {
        let cell = unsafe { &*list.elements.add(i as usize) };
        let defelem = unsafe { cell.ptr_value } as *mut pg_sys::DefElem;
        if defelem.is_null() {
            continue;
        }
        let de = unsafe { &*defelem };
        if de.defname.is_null() {
            continue;
        }
        let name = unsafe { CStr::from_ptr(de.defname) };
        if name.to_bytes() != b"format" {
            continue;
        }
        // Get the format value
        if de.arg.is_null() {
            continue;
        }
        let val_str = unsafe { pg_sys::defGetString(defelem) };
        if val_str.is_null() {
            continue;
        }
        let val = unsafe { CStr::from_ptr(val_str) };
        if val.to_bytes().eq_ignore_ascii_case(b"deltax_compress") {
            return i;
        }
    }
    -1
}

/// Build a new options list without the FORMAT defelem (so PG defaults to CSV).
unsafe fn strip_format_option(options: *mut pg_sys::List, format_idx: i32) -> *mut pg_sys::List {
    if options.is_null() {
        return std::ptr::null_mut();
    }
    let list = unsafe { &*options };
    let len = list.length;
    let mut new_list: *mut pg_sys::List = std::ptr::null_mut();
    for i in 0..len {
        if i == format_idx {
            continue;
        }
        let cell = unsafe { &*list.elements.add(i as usize) };
        new_list = unsafe { pg_sys::lappend(new_list, cell.ptr_value) };
    }
    new_list
}

/// Execute SQL via a short-lived SPI connection.
///
/// Each call opens and closes its own `Spi::connect`, so the SPI procedure
/// memory context is freed after every statement. This prevents memory
/// accumulation over thousands of DDL/INSERT calls during a long-running COPY.
fn spi_exec(sql: &str) {
    Spi::connect(|_client| {
        let c_sql = CString::new(sql).expect("SQL string contains null byte");
        let ret = unsafe { pg_sys::SPI_execute(c_sql.as_ptr(), false, 0) };
        if ret < 0 {
            pgrx::error!("SPI_execute failed with code {}", ret);
        }
    });
}

/// Allocate a bytea varlena in palloc'd memory and return (Datum, pointer for pfree).
/// The caller MUST pfree the pointer after SPI_execute_plan to avoid leaking memory.
fn bytea_to_datum(data: &[u8]) -> (pg_sys::Datum, *mut std::ffi::c_void) {
    unsafe {
        let len = data.len() + pg_sys::VARHDRSZ;
        let varlena = pg_sys::palloc(len) as *mut pg_sys::varlena;
        pgrx::set_varsize_4b(varlena, len as i32);
        let dest = pgrx::vardata_any(varlena as *const pg_sys::varlena) as *mut u8;
        std::ptr::copy_nonoverlapping(data.as_ptr(), dest, data.len());
        (pg_sys::Datum::from(varlena as usize), varlena as *mut std::ffi::c_void)
    }
}

/// Resolve a fully-qualified table name to its OID via regclass cast.
fn resolve_relation_oid(fqn: &str) -> pg_sys::Oid {
    Spi::connect(|_client| {
        let sql = format!("SELECT '{}'::regclass::oid", fqn);
        let c_sql = CString::new(sql).expect("SQL contains null byte");
        unsafe {
            let ret = pg_sys::SPI_execute(c_sql.as_ptr(), true, 1);
            if ret < 0 || pg_sys::SPI_processed != 1 {
                pgrx::error!("failed to resolve OID for {}", fqn);
            }
            let tuptable = *pg_sys::SPI_tuptable;
            let tupdesc = tuptable.tupdesc;
            let tuple = *tuptable.vals.add(0);
            let mut isnull = false;
            let datum = pg_sys::SPI_getbinval(tuple, tupdesc, 1, &mut isnull);
            if isnull {
                pgrx::error!("NULL OID for {}", fqn);
            }
            pg_sys::Oid::from_u32(datum.value() as u32)
        }
    })
}

/// Resolve a RangeVar to (schema, table) strings.
unsafe fn rangevar_to_names(rv: *const pg_sys::RangeVar) -> (String, String) {
    let rv = unsafe { &*rv };
    let table = unsafe { CStr::from_ptr(rv.relname) }
        .to_str()
        .unwrap()
        .to_string();
    let schema = if rv.schemaname.is_null() {
        // Resolve from search path
        Spi::get_one_with_args::<String>(
            "SELECT schemaname::text FROM pg_tables WHERE tablename = $1::name LIMIT 1",
            &[table.as_str().into()],
        )
        .expect("failed to look up table schema")
        .unwrap_or_else(|| {
            pgrx::error!("pg_deltax: table '{}' not found", table);
        })
    } else {
        unsafe { CStr::from_ptr(rv.schemaname) }
            .to_str()
            .unwrap()
            .to_string()
    };
    (schema, table)
}

/// Maximum blob buffer size per partition before triggering an early flush.
/// Keeps memory bounded even when multiple partitions are active simultaneously.
const BLOB_BUFFER_THRESHOLD: usize = 256 * 1024 * 1024; // 256 MB

/// Number of meta rows to batch into a single multi-row INSERT.
const META_BATCH_SIZE: usize = 50;

/// Per-partition buffer for accumulating rows during direct backfill.
struct PartitionBuffer {
    partition_id: i32,
    partition_table: String,
    typed_cols: Vec<TypedColumn>,
    row_count: usize,
    next_segment_id: i32,
    blob_buffer: Vec<(u16, i32, Vec<u8>)>,
    blob_buffer_size: usize,
    bloom_buffer: Vec<(i32, Vec<u8>)>,
    total_compressed_size: i64,
    total_rows: i64,
    meta_table_created: bool,
    blobs_table_created: bool,
    blobs_flushed: bool,
    /// Cached meta table FQN and column list for batched INSERTs.
    meta_fqn: Option<String>,
    meta_insert_cols: Option<String>,
    /// Buffered VALUES clauses for batched meta INSERTs.
    meta_insert_rows: Vec<String>,
    /// Cached companion table FQNs and OIDs to avoid repeated SPI lookups.
    blobs_fqn_cached: Option<String>,
    blooms_fqn_cached: Option<String>,
    blobs_oid_cached: Option<pg_sys::Oid>,
    blooms_oid_cached: Option<pg_sys::Oid>,
}

/// State for the entire backfill operation.
struct BackfillState {
    columns: Vec<ColumnMeta>,
    kinds: Vec<ColumnKind>,
    order_col_indices: Vec<usize>,
    segment_size: usize,
    time_col_index: usize,
}

fn handle_copy_from_deltax_compress(copy_stmt: *mut pg_sys::CopyStmt, format_idx: i32) {
    // Bypass DML-on-compressed check for our companion table writes
    crate::scan::set_dml_bypass(true);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        handle_copy_from_inner(copy_stmt, format_idx);
    }));
    crate::scan::set_dml_bypass(false);
    if let Err(e) = result {
        std::panic::resume_unwind(e);
    }
}

/// Extract COPY TEXT options (DELIMITER, NULL, HEADER) from the PG options list.
fn extract_copy_text_options(options: *mut pg_sys::List, format_idx: i32) -> CopyTextOptions {
    let mut opts = CopyTextOptions::default();
    if options.is_null() {
        return opts;
    }
    let list = unsafe { &*options };
    let len = list.length;
    for i in 0..len {
        if i == format_idx {
            continue;
        }
        let cell = unsafe { &*list.elements.add(i as usize) };
        let defelem = unsafe { cell.ptr_value } as *mut pg_sys::DefElem;
        if defelem.is_null() {
            continue;
        }
        let de = unsafe { &*defelem };
        if de.defname.is_null() {
            continue;
        }
        let name = unsafe { CStr::from_ptr(de.defname) };
        let name_bytes = name.to_bytes();
        if name_bytes.eq_ignore_ascii_case(b"delimiter") {
            let val_str = unsafe { pg_sys::defGetString(defelem) };
            if !val_str.is_null() {
                let val = unsafe { CStr::from_ptr(val_str) };
                let bytes = val.to_bytes();
                if !bytes.is_empty() {
                    opts.delimiter = bytes[0];
                }
            }
        } else if name_bytes.eq_ignore_ascii_case(b"null") {
            let val_str = unsafe { pg_sys::defGetString(defelem) };
            if !val_str.is_null() {
                let val = unsafe { CStr::from_ptr(val_str) };
                opts.null_string = val.to_bytes().to_vec();
            }
        } else if name_bytes.eq_ignore_ascii_case(b"header") {
            // HEADER can be boolean (true/false) or 'match'
            let val_str = unsafe { pg_sys::defGetString(defelem) };
            if !val_str.is_null() {
                let val = unsafe { CStr::from_ptr(val_str) };
                let val_bytes = val.to_bytes();
                if val_bytes.eq_ignore_ascii_case(b"match") {
                    opts.header = HeaderMode::Match(Vec::new());
                } else if val_bytes.eq_ignore_ascii_case(b"true")
                    || val_bytes.eq_ignore_ascii_case(b"on")
                    || val_bytes == b"1"
                {
                    opts.header = HeaderMode::Skip;
                }
                // false/off/0 → HeaderMode::None (default)
            }
        }
    }
    opts
}

fn handle_copy_from_inner(copy_stmt: *mut pg_sys::CopyStmt, format_idx: i32) {
    let cs = unsafe { &*copy_stmt };

    // 1. Resolve table
    if cs.relation.is_null() {
        pgrx::error!("pg_deltax: COPY FROM with FORMAT deltax_compress requires a relation");
    }
    let (schema, table) = unsafe { rangevar_to_names(cs.relation) };

    // 2. Validate via SPI — use a short-lived connection so its memory context
    //    is freed before the long-running COPY loop starts.
    let (partitions, columns, kinds, time_col_index, order_col_indices, segment_size) =
        Spi::connect_mut(|client| {
        let ht = catalog::get_deltatable(client, &schema, &table)
            .expect("failed to query deltatable")
            .unwrap_or_else(|| {
                pgrx::error!(
                    "pg_deltax: {}.{} is not a deltax table. Call deltax_create_table() first.",
                    schema, table
                );
            });

        if ht.order_by.is_empty() && ht.segment_by.is_empty() {
            pgrx::error!(
                "pg_deltax: compression not enabled on {}.{}. Call deltax_enable_compression() first.",
                schema, table
            );
        }

        // 3. Load partitions
        let partitions = catalog::get_partitions(client, ht.id)
            .expect("failed to query partitions");

        if partitions.is_empty() {
            pgrx::error!("pg_deltax: no partitions found for {}.{}", schema, table);
        }

        // Get column metadata from the parent table
        let columns = get_column_metadata(client, &schema, &table, &ht.segment_by);
        if columns.is_empty() {
            pgrx::error!("pg_deltax: no columns found for {}.{}", schema, table);
        }

        let kinds: Vec<ColumnKind> = columns
            .iter()
            .map(|c| classify_column(&c.data_type, c.is_segment_by))
            .collect();

        // Find the time column index
        let time_col_index = columns
            .iter()
            .position(|c| c.name == ht.time_column)
            .unwrap_or_else(|| {
                pgrx::error!(
                    "pg_deltax: time column '{}' not found in column metadata",
                    ht.time_column
                );
            });
        // Build order_by column indices
        let order_col_indices: Vec<usize> = ht.order_by
            .iter()
            .filter_map(|name| columns.iter().position(|c| c.name == *name))
            .collect();

        let segment_size = ht.segment_size as usize;

        (partitions, columns, kinds, time_col_index, order_col_indices, segment_size)
    });
    // SPI connection is now closed — its memory context has been freed.

    // Build partition range arrays (in Unix epoch usec) for binary search
    let mut range_starts: Vec<i64> = Vec::with_capacity(partitions.len());
    let mut range_ends: Vec<i64> = Vec::with_capacity(partitions.len());
    let mut part_buffers: Vec<PartitionBuffer> = Vec::with_capacity(partitions.len());

    for p in &partitions {
        let start_usec = p.range_start.into_inner() + PG_EPOCH_OFFSET_USEC;
        let end_usec = p.range_end.into_inner() + PG_EPOCH_OFFSET_USEC;
        range_starts.push(start_usec);
        range_ends.push(end_usec);
        part_buffers.push(PartitionBuffer {
            partition_id: p.id,
            partition_table: p.table_name.clone(),
            typed_cols: init_typed_columns(&columns, &kinds),
            row_count: 0,
            next_segment_id: 1,
            blob_buffer: Vec::new(),
            blob_buffer_size: 0,
            bloom_buffer: Vec::new(),
            total_compressed_size: 0,
            total_rows: 0,
            meta_table_created: false,
            blobs_table_created: false,
            blobs_flushed: false,
            meta_fqn: None,
            meta_insert_cols: None,
            meta_insert_rows: Vec::new(),
            blobs_fqn_cached: None,
            blooms_fqn_cached: None,
            blobs_oid_cached: None,
            blooms_oid_cached: None,
        });
    }

    let state = BackfillState {
        columns,
        kinds,
        order_col_indices,
        segment_size,
        time_col_index,
    };

    // Branch: file-path → pure-Rust parser, stdin → legacy PG parser
    if !cs.filename.is_null() && !cs.is_program {
        let filename = unsafe { CStr::from_ptr(cs.filename) }
            .to_str()
            .unwrap_or_else(|_| pgrx::error!("pg_deltax: filename is not valid UTF-8"));
        let copy_opts = extract_copy_text_options(cs.options, format_idx);
        handle_copy_from_file(
            filename,
            copy_opts,
            &state,
            &mut part_buffers,
            &partitions,
            &range_starts,
            &range_ends,
        );
    } else {
        handle_copy_from_legacy(
            cs,
            format_idx,
            &state,
            &mut part_buffers,
            &partitions,
            &range_starts,
            &range_ends,
        );
    }

    // End-of-COPY flush (shared by both paths)
    for buf in &mut part_buffers {
        if buf.row_count == 0 && buf.total_rows == 0 {
            continue;
        }

        // Flush remaining partial segment
        if buf.row_count > 0 {
            flush_segment(buf, &state);
        }

        // Flush any remaining buffered meta rows
        flush_meta_buffer(buf);

        // Flush any remaining blobs (may already be flushed via partition-change logic)
        if !buf.blob_buffer.is_empty() {
            flush_partition_blobs(buf, &state.columns);
        }

        // ANALYZE companion tables and update catalog
        finalize_partition(buf, &state.columns);
    }

    crate::scan::invalidate_compressed_cache();

    let total_rows: i64 = part_buffers.iter().map(|b| b.total_rows).sum();
    pgrx::notice!(
        "pg_deltax: direct backfill complete, {} rows compressed into {} partitions",
        total_rows,
        part_buffers.iter().filter(|b| b.total_rows > 0).count()
    );
}

// ============================================================================
// Parallel chunk parsing types
// ============================================================================

struct WorkerPartitionResult {
    typed_cols: Vec<TypedColumn>,
    row_count: usize,
}

struct WorkerResult {
    partitions: Vec<Option<WorkerPartitionResult>>,
}

/// Worker function: parse a slice of lines into per-partition TypedColumn buffers.
/// Pure Rust — no pgrx calls (workers can't access the PG backend).
#[allow(clippy::too_many_arguments)]
fn parse_lines_worker(
    buf: &[u8],
    line_ranges: &[(usize, usize)],
    opts_delimiter: u8,
    opts_null_string: &[u8],
    kinds: &[ColumnKind],
    time_col_index: usize,
    range_starts: &[i64],
    range_ends: &[i64],
    is_compressed: &[bool],
    n_partitions: usize,
    base_line_number: u64,
) -> Result<WorkerResult, crate::copyparse::ParseError> {
    let num_columns = kinds.len();
    let mut partitions: Vec<Option<WorkerPartitionResult>> = (0..n_partitions).map(|_| None).collect();
    let mut field_offsets: Vec<(usize, usize)> = Vec::with_capacity(num_columns);

    for (row_idx, &(s, e)) in line_ranges.iter().enumerate() {
        let line_number = base_line_number + row_idx as u64 + 1;
        let line = &buf[s..e];
        split_field_offsets(line, opts_delimiter, &mut field_offsets);

        if field_offsets.len() != num_columns {
            return Err(crate::copyparse::ParseError {
                message: format!(
                    "expected {} fields, got {}",
                    num_columns,
                    field_offsets.len()
                ),
                column: 0,
                line: line_number,
            });
        }

        // Extract time value
        let (ts, te) = field_offsets[time_col_index];
        let time_raw = &line[ts..te];
        if time_raw == opts_null_string {
            return Err(crate::copyparse::ParseError {
                message: "time column value is NULL, cannot route to partition".to_string(),
                column: time_col_index,
                line: line_number,
            });
        }
        let time_str = if memchr::memchr(b'\\', time_raw).is_none() {
            std::str::from_utf8(time_raw).map_err(|_| crate::copyparse::ParseError {
                message: "invalid UTF-8 in time column".to_string(),
                column: time_col_index,
                line: line_number,
            })?
        } else {
            return Err(crate::copyparse::ParseError {
                message: "unexpected escape in time column".to_string(),
                column: time_col_index,
                line: line_number,
            });
        };
        let time_usec = crate::timeparse::parse_timestamp_to_usec(time_str);

        let part_idx = match find_partition(range_starts, range_ends, time_usec) {
            Some(idx) => idx,
            None => {
                return Err(crate::copyparse::ParseError {
                    message: format!(
                        "timestamp {} does not fit any partition",
                        time_usec
                    ),
                    column: time_col_index,
                    line: line_number,
                });
            }
        };

        if is_compressed[part_idx] {
            return Err(crate::copyparse::ParseError {
                message: "partition is already compressed. Decompress it first to load new data.".to_string(),
                column: 0,
                line: line_number,
            });
        }

        // Lazily initialize partition buffers
        let wp = partitions[part_idx].get_or_insert_with(|| {
            let typed_cols: Vec<TypedColumn> = kinds.iter().map(|k| new_typed_column(*k)).collect();
            WorkerPartitionResult {
                typed_cols,
                row_count: 0,
            }
        });

        // Parse each field into the partition's typed columns
        for (i, kind) in kinds.iter().enumerate() {
            let (fs, fe) = field_offsets[i];
            let raw_field = &line[fs..fe];
            parse_raw_field_and_append(
                raw_field,
                opts_null_string,
                *kind,
                &mut wp.typed_cols[i],
                i,
                line_number,
            )?;
        }
        wp.row_count += 1;
    }

    Ok(WorkerResult { partitions })
}

/// Pure-Rust file-path COPY: read the file directly, parse TEXT format,
/// convert types, and route to partition buffers.
fn handle_copy_from_file(
    filename: &str,
    opts: CopyTextOptions,
    state: &BackfillState,
    part_buffers: &mut [PartitionBuffer],
    partitions: &[crate::catalog::PartitionInfo],
    range_starts: &[i64],
    range_ends: &[i64],
) {
    let n_workers = crate::get_parallel_workers();
    if n_workers <= 1 {
        return handle_copy_from_file_sequential(
            filename, opts, state, part_buffers, partitions, range_starts, range_ends,
        );
    }

    let file = std::fs::File::open(filename).unwrap_or_else(|e| {
        pgrx::error!("pg_deltax: cannot open file '{}': {}", filename, e);
    });
    let mut reader = BufReader::with_capacity(128 * 1024 * 1024, file);
    let mut line_reader = CopyLineReader::new();
    let mut buf: Vec<u8> = Vec::with_capacity(160 * 1024 * 1024);

    let mut total_rows: i64 = 0;
    let mut last_part_idx: Option<usize> = None;
    let mut parse_time_us: u64 = 0;
    let copy_start = std::time::Instant::now();

    // Initial fill
    {
        let data = reader.fill_buf().unwrap_or_else(|e| {
            pgrx::error!("pg_deltax: read error: {}", e);
        });
        buf.extend_from_slice(data);
        let n = data.len();
        reader.consume(n);
    }

    // Handle HEADER
    if matches!(opts.header, HeaderMode::Skip | HeaderMode::Match(_)) {
        match line_reader.next_line(&buf, 0) {
            LineResult::Row(_, e) => {
                let eol_len = match line_reader.eol {
                    Some(crate::copyparse::Eol::CrLf) => 2,
                    _ => 1,
                };
                buf.drain(..e + eol_len);
            }
            LineResult::EndOfCopy => {
                return;
            }
            LineResult::Incomplete => {
                pgrx::error!("pg_deltax: file has no complete header line");
            }
        }
    }

    let num_columns = state.columns.len();
    let is_compressed: Vec<bool> = partitions.iter().map(|p| p.is_compressed).collect();
    let n_partitions = part_buffers.len();

    pgrx::notice!(
        "pg_deltax: parallel COPY with {} workers",
        n_workers
    );

    loop {
        // Phase 1: Find all line boundaries (sequential, memchr-fast)
        let t_parse = std::time::Instant::now();
        let mut line_ranges: Vec<(usize, usize)> = Vec::new();
        let mut pos: usize = 0;
        let mut end_of_copy = false;

        loop {
            match line_reader.next_line(&buf, pos) {
                LineResult::Row(s, e) => {
                    line_ranges.push((s, e));
                    let eol_len = match line_reader.eol {
                        Some(crate::copyparse::Eol::CrLf) => 2,
                        _ => 1,
                    };
                    pos = e + eol_len;
                }
                LineResult::EndOfCopy => {
                    end_of_copy = true;
                    break;
                }
                LineResult::Incomplete => break,
            }
        }

        if line_ranges.is_empty() {
            if end_of_copy {
                break;
            }
            // Need more data (line spans batch boundary)
            let data = reader.fill_buf().unwrap_or_else(|e| {
                pgrx::error!("pg_deltax: read error: {}", e);
            });
            if data.is_empty() {
                // EOF — handle trailing line without terminator
                if !buf.is_empty() {
                    let line = &buf[..];
                    let raw_fields = split_fields(line, opts.delimiter);
                    if raw_fields.len() == num_columns {
                        line_reader.line_number += 1;
                        handle_trailing_line(
                            &raw_fields, &opts, state, part_buffers, partitions,
                            range_starts, range_ends, &mut last_part_idx,
                            &mut total_rows, line_reader.line_number,
                        );
                    }
                }
                break;
            }
            buf.extend_from_slice(data);
            let n = data.len();
            reader.consume(n);
            continue;
        }

        let scan_time = t_parse.elapsed().as_micros() as u64;

        // Phase 2: Parallel parse
        let t_parallel = std::time::Instant::now();
        let chunk_size = line_ranges.len().div_ceil(n_workers);
        let base_line = line_reader.line_number - line_ranges.len() as u64;

        let buf_ref = &buf;
        let null_string_ref = &opts.null_string;
        let kinds_ref = &state.kinds;
        let is_compressed_ref = &is_compressed;
        let delimiter = opts.delimiter;
        let time_col_index = state.time_col_index;

        let worker_results: Vec<Result<WorkerResult, crate::copyparse::ParseError>> =
            std::thread::scope(|s| {
                line_ranges
                    .chunks(chunk_size)
                    .enumerate()
                    .map(|(chunk_idx, chunk)| {
                        let chunk_base_line = base_line + (chunk_idx * chunk_size) as u64;
                        s.spawn(move || {
                            parse_lines_worker(
                                buf_ref,
                                chunk,
                                delimiter,
                                null_string_ref,
                                kinds_ref,
                                time_col_index,
                                range_starts,
                                range_ends,
                                is_compressed_ref,
                                n_partitions,
                                chunk_base_line,
                            )
                        })
                    })
                    .collect::<Vec<_>>()
                    .into_iter()
                    .map(|h| h.join().unwrap())
                    .collect()
            });

        parse_time_us += scan_time + t_parallel.elapsed().as_micros() as u64;

        // Phase 3: Merge + flush (sequential, on PG backend thread)
        merge_and_flush_results(worker_results, part_buffers, state, &mut last_part_idx, &mut total_rows);

        // Drain consumed bytes
        if pos > 0 {
            buf.drain(..pos);
        }

        if end_of_copy {
            break;
        }

        // Read more data
        let data = reader.fill_buf().unwrap_or_else(|e| {
            pgrx::error!("pg_deltax: read error: {}", e);
        });
        if data.is_empty() {
            // EOF — handle trailing partial line
            if !buf.is_empty() {
                let line = &buf[..];
                let raw_fields = split_fields(line, opts.delimiter);
                if raw_fields.len() == num_columns {
                    line_reader.line_number += 1;
                    handle_trailing_line(
                        &raw_fields, &opts, state, part_buffers, partitions,
                        range_starts, range_ends, &mut last_part_idx,
                        &mut total_rows, line_reader.line_number,
                    );
                }
            }
            break;
        }
        buf.extend_from_slice(data);
        let n = data.len();
        reader.consume(n);
    }

    let copy_elapsed = copy_start.elapsed();
    pgrx::notice!(
        "pg_deltax: COPY (Rust parser, {} workers) done: {} rows in {:.1}s, parse={:.1}s ({:.0}%)",
        n_workers,
        total_rows,
        copy_elapsed.as_secs_f64(),
        parse_time_us as f64 / 1e6,
        if copy_elapsed.as_secs_f64() > 0.0 {
            (parse_time_us as f64 / 1e6) / copy_elapsed.as_secs_f64() * 100.0
        } else {
            0.0
        }
    );
}

/// Handle a trailing line at EOF (no terminator). Used by both parallel and sequential paths.
#[allow(clippy::too_many_arguments)]
fn handle_trailing_line(
    raw_fields: &[&[u8]],
    opts: &CopyTextOptions,
    state: &BackfillState,
    part_buffers: &mut [PartitionBuffer],
    partitions: &[crate::catalog::PartitionInfo],
    range_starts: &[i64],
    range_ends: &[i64],
    last_part_idx: &mut Option<usize>,
    total_rows: &mut i64,
    line_number: u64,
) {
    let time_raw = raw_fields[state.time_col_index];
    if time_raw == opts.null_string.as_slice() {
        pgrx::error!(
            "pg_deltax: time column value is NULL at line {}",
            line_number
        );
    }
    let time_str = std::str::from_utf8(time_raw).unwrap_or_else(|_| {
        pgrx::error!("pg_deltax: invalid UTF-8 in time column");
    });
    let time_usec = crate::timeparse::parse_timestamp_to_usec(time_str);

    let part_idx = match find_partition(range_starts, range_ends, time_usec) {
        Some(idx) => idx,
        None => {
            pgrx::error!(
                "pg_deltax: row at line {} with timestamp {} does not fit any partition",
                line_number,
                time_usec
            );
        }
    };

    if partitions[part_idx].is_compressed {
        pgrx::error!(
            "pg_deltax: partition '{}' is already compressed.",
            partitions[part_idx].table_name
        );
    }

    if let Some(prev_idx) = last_part_idx
        .filter(|&idx| idx != part_idx && !part_buffers[idx].blob_buffer.is_empty())
    {
        flush_partition_blobs(&mut part_buffers[prev_idx], &state.columns);
    }

    let pbuf = &mut part_buffers[part_idx];
    for (i, (raw_field, kind)) in raw_fields.iter().zip(state.kinds.iter()).enumerate() {
        if let Err(e) = parse_raw_field_and_append(
            raw_field,
            &opts.null_string,
            *kind,
            &mut pbuf.typed_cols[i],
            i,
            line_number,
        ) {
            pgrx::error!(
                "pg_deltax: parse error at line {}, column {}: {}",
                e.line, e.column, e.message
            );
        }
    }
    pbuf.row_count += 1;
    *total_rows += 1;

    if pbuf.row_count >= state.segment_size {
        flush_segment(pbuf, state);
    }
}

/// Merge worker results into partition buffers, flushing segments as they fill.
fn merge_and_flush_results(
    worker_results: Vec<Result<WorkerResult, crate::copyparse::ParseError>>,
    part_buffers: &mut [PartitionBuffer],
    state: &BackfillState,
    _last_part_idx: &mut Option<usize>,
    total_rows: &mut i64,
) {
    for result in worker_results {
        let result = match result {
            Ok(r) => r,
            Err(e) => {
                pgrx::error!(
                    "pg_deltax: parse error at line {}, column {}: {}",
                    e.line,
                    e.column,
                    e.message
                );
            }
        };

        for (part_idx, worker_part) in result.partitions.into_iter().enumerate() {
            if let Some(wp) = worker_part {
                let pbuf = &mut part_buffers[part_idx];
                for (i, worker_col) in wp.typed_cols.into_iter().enumerate() {
                    pbuf.typed_cols[i].extend(worker_col);
                }
                pbuf.row_count += wp.row_count;
                *total_rows += wp.row_count as i64;

                if pbuf.row_count >= state.segment_size {
                    flush_segment(pbuf, state);
                }
            }
        }
    }
}

/// Sequential file-path COPY (fallback for single-worker mode).
fn handle_copy_from_file_sequential(
    filename: &str,
    opts: CopyTextOptions,
    state: &BackfillState,
    part_buffers: &mut [PartitionBuffer],
    partitions: &[crate::catalog::PartitionInfo],
    range_starts: &[i64],
    range_ends: &[i64],
) {
    let file = std::fs::File::open(filename).unwrap_or_else(|e| {
        pgrx::error!("pg_deltax: cannot open file '{}': {}", filename, e);
    });
    let mut reader = BufReader::with_capacity(8 * 1024 * 1024, file);
    let mut line_reader = CopyLineReader::new();
    let mut buf: Vec<u8> = Vec::with_capacity(16 * 1024 * 1024);

    let mut total_rows: i64 = 0;
    let mut last_part_idx: Option<usize> = None;
    let mut parse_time_us: u64 = 0;
    let copy_start = std::time::Instant::now();

    // Initial fill
    {
        let data = reader.fill_buf().unwrap_or_else(|e| {
            pgrx::error!("pg_deltax: read error: {}", e);
        });
        buf.extend_from_slice(data);
        let n = data.len();
        reader.consume(n);
    }

    // Handle HEADER
    if matches!(opts.header, HeaderMode::Skip | HeaderMode::Match(_)) {
        match line_reader.next_line(&buf, 0) {
            LineResult::Row(_, e) => {
                let eol_len = match line_reader.eol {
                    Some(crate::copyparse::Eol::CrLf) => 2,
                    _ => 1,
                };
                buf.drain(..e + eol_len);
            }
            LineResult::EndOfCopy => {
                return;
            }
            LineResult::Incomplete => {
                pgrx::error!("pg_deltax: file has no complete header line");
            }
        }
    }

    let num_columns = state.columns.len();
    let mut pos: usize = 0;
    let mut field_offsets: Vec<(usize, usize)> = Vec::with_capacity(num_columns);

    loop {
        let t_parse = std::time::Instant::now();
        match line_reader.next_line(&buf, pos) {
            LineResult::Row(s, e) => {
                parse_time_us += t_parse.elapsed().as_micros() as u64;

                let line_start = s;
                let line_end = e;
                split_field_offsets(&buf[line_start..line_end], opts.delimiter, &mut field_offsets);

                if field_offsets.len() != num_columns {
                    pgrx::error!(
                        "pg_deltax: line {}: expected {} fields, got {}",
                        line_reader.line_number,
                        num_columns,
                        field_offsets.len()
                    );
                }

                let (ts, te) = field_offsets[state.time_col_index];
                let time_raw = &buf[line_start + ts..line_start + te];
                if time_raw == opts.null_string.as_slice() {
                    pgrx::error!(
                        "pg_deltax: time column value is NULL at line {}, cannot route to partition",
                        line_reader.line_number
                    );
                }
                let time_str = if memchr::memchr(b'\\', time_raw).is_none() {
                    std::str::from_utf8(time_raw).unwrap_or_else(|_| {
                        pgrx::error!("pg_deltax: invalid UTF-8 in time column at line {}", line_reader.line_number);
                    })
                } else {
                    pgrx::error!("pg_deltax: unexpected escape in time column at line {}", line_reader.line_number);
                };
                let time_usec = crate::timeparse::parse_timestamp_to_usec(time_str);

                let part_idx = match find_partition(range_starts, range_ends, time_usec) {
                    Some(idx) => idx,
                    None => {
                        pgrx::error!(
                            "pg_deltax: row at line {} with timestamp {} does not fit any partition",
                            line_reader.line_number,
                            time_usec
                        );
                    }
                };

                if partitions[part_idx].is_compressed {
                    pgrx::error!(
                        "pg_deltax: partition '{}' is already compressed. Decompress it first to load new data.",
                        partitions[part_idx].table_name
                    );
                }

                if let Some(prev_idx) = last_part_idx.filter(|&idx| idx != part_idx && !part_buffers[idx].blob_buffer.is_empty()) {
                    flush_partition_blobs(&mut part_buffers[prev_idx], &state.columns);
                }
                last_part_idx = Some(part_idx);

                let pbuf = &mut part_buffers[part_idx];
                for (i, kind) in state.kinds.iter().enumerate() {
                    let (fs, fe) = field_offsets[i];
                    let raw_field = &buf[line_start + fs..line_start + fe];
                    if let Err(e) = parse_raw_field_and_append(
                        raw_field,
                        &opts.null_string,
                        *kind,
                        &mut pbuf.typed_cols[i],
                        i,
                        line_reader.line_number,
                    ) {
                        pgrx::error!(
                            "pg_deltax: parse error at line {}, column {} ('{}'): {}",
                            e.line,
                            e.column,
                            state.columns[i].name,
                            e.message
                        );
                    }
                }
                pbuf.row_count += 1;
                total_rows += 1;

                if pbuf.row_count >= state.segment_size {
                    flush_segment(pbuf, state);
                }

                let eol_len = match line_reader.eol {
                    Some(crate::copyparse::Eol::CrLf) => 2,
                    _ => 1,
                };
                pos = e + eol_len;
            }
            LineResult::EndOfCopy => {
                break;
            }
            LineResult::Incomplete => {
                parse_time_us += t_parse.elapsed().as_micros() as u64;

                if pos > 0 {
                    buf.drain(..pos);
                    pos = 0;
                }

                let data = reader.fill_buf().unwrap_or_else(|e| {
                    pgrx::error!("pg_deltax: read error: {}", e);
                });
                if data.is_empty() {
                    if !buf.is_empty() {
                        let line = &buf[..];
                        let raw_fields = split_fields(line, opts.delimiter);
                        if raw_fields.len() == num_columns {
                            line_reader.line_number += 1;
                            handle_trailing_line(
                                &raw_fields, &opts, state, part_buffers, partitions,
                                range_starts, range_ends, &mut last_part_idx,
                                &mut total_rows, line_reader.line_number,
                            );
                        }
                    }
                    break;
                }
                buf.extend_from_slice(data);
                let n = data.len();
                reader.consume(n);
            }
        }
    }

    let copy_elapsed = copy_start.elapsed();
    pgrx::notice!(
        "pg_deltax: COPY (Rust parser) done: {} rows in {:.1}s, parse={:.1}s ({:.0}%)",
        total_rows,
        copy_elapsed.as_secs_f64(),
        parse_time_us as f64 / 1e6,
        if copy_elapsed.as_secs_f64() > 0.0 {
            (parse_time_us as f64 / 1e6) / copy_elapsed.as_secs_f64() * 100.0
        } else {
            0.0
        }
    );
}

/// Stdin/program COPY path: use PG's BeginCopyFrom for protocol handling,
/// but NextCopyFromRawFields for line/field parsing (skipping PG's InputFunctionCall),
/// then Rust type conversion via `parse_and_append`.
fn handle_copy_from_legacy(
    cs: &pg_sys::CopyStmt,
    format_idx: i32,
    state: &BackfillState,
    part_buffers: &mut [PartitionBuffer],
    partitions: &[crate::catalog::PartitionInfo],
    range_starts: &[i64],
    range_ends: &[i64],
) {
    // Strip FORMAT deltax_compress; PG defaults to TEXT format (tab-separated).
    let final_options = unsafe { strip_format_option(cs.options, format_idx) };

    let rel_oid = unsafe {
        pg_sys::RangeVarGetRelidExtended(
            cs.relation,
            pg_sys::AccessShareLock as pg_sys::LOCKMODE,
            0,
            None,
            std::ptr::null_mut(),
        )
    };

    let rel = unsafe { pg_sys::table_open(rel_oid, pg_sys::AccessShareLock as pg_sys::LOCKMODE) };
    let pstate = unsafe { pg_sys::make_parsestate(std::ptr::null_mut()) };

    let cstate = unsafe {
        pg_sys::BeginCopyFrom(
            pstate,
            rel,
            cs.whereClause,
            cs.filename,
            cs.is_program,
            None, // data_source_cb
            cs.attlist,
            final_options,
        )
    };

    let num_columns = state.columns.len();

    let mut total_rows: i64 = 0;
    let mut last_part_idx: Option<usize> = None;
    let mut parse_time_us: u64 = 0;
    let copy_start = std::time::Instant::now();

    // NextCopyFromRawFields returns raw char** fields — PG handles the COPY
    // protocol and line/field splitting, but skips InputFunctionCall.
    // We do type conversion in Rust via parse_and_append.
    let mut raw_fields: *mut *mut std::ffi::c_char = std::ptr::null_mut();
    let mut nfields: std::ffi::c_int = 0;
    let mut line_number: u64 = 0;

    loop {
        let t_parse = std::time::Instant::now();
        let has_row = unsafe {
            pg_sys::NextCopyFromRawFields(cstate, &mut raw_fields, &mut nfields)
        };
        parse_time_us += t_parse.elapsed().as_micros() as u64;

        if !has_row {
            break;
        }

        line_number += 1;

        if nfields as usize != num_columns {
            pgrx::error!(
                "pg_deltax: line {}: expected {} fields, got {}",
                line_number,
                num_columns,
                nfields
            );
        }

        // Convert raw C strings to Option<&str> (NULL fields have null pointer)
        // and extract the time column value for partition routing.
        let time_str = unsafe {
            let ptr = *raw_fields.add(state.time_col_index);
            if ptr.is_null() {
                pgrx::error!(
                    "pg_deltax: time column value is NULL at line {}, cannot route to partition",
                    line_number
                );
            }
            CStr::from_ptr(ptr).to_str().unwrap_or_else(|_| {
                pgrx::error!("pg_deltax: invalid UTF-8 in time column at line {}", line_number);
            })
        };
        let time_usec = crate::timeparse::parse_timestamp_to_usec(time_str);

        let part_idx = match find_partition(range_starts, range_ends, time_usec) {
            Some(idx) => idx,
            None => {
                pgrx::error!(
                    "pg_deltax: row at line {} with timestamp {} does not fit any partition",
                    line_number,
                    time_usec
                );
            }
        };

        if partitions[part_idx].is_compressed {
            pgrx::error!(
                "pg_deltax: partition '{}' is already compressed. Decompress it first to load new data.",
                partitions[part_idx].table_name
            );
        }

        if let Some(prev_idx) = last_part_idx.filter(|&idx| idx != part_idx && !part_buffers[idx].blob_buffer.is_empty()) {
            flush_partition_blobs(&mut part_buffers[prev_idx], &state.columns);
        }
        last_part_idx = Some(part_idx);

        // Append each field using Rust type conversion
        let pbuf = &mut part_buffers[part_idx];
        for i in 0..num_columns {
            let field_str: Option<&str> = unsafe {
                let ptr = *raw_fields.add(i);
                if ptr.is_null() {
                    None
                } else {
                    Some(CStr::from_ptr(ptr).to_str().unwrap_or_else(|_| {
                        pgrx::error!(
                            "pg_deltax: invalid UTF-8 in column {} at line {}",
                            i, line_number
                        );
                    }))
                }
            };

            if let Err(e) = parse_and_append(
                field_str,
                state.kinds[i],
                &mut pbuf.typed_cols[i],
                i,
                line_number,
            ) {
                pgrx::error!(
                    "pg_deltax: parse error at line {}, column {} ('{}'): {}",
                    e.line,
                    e.column,
                    state.columns[i].name,
                    e.message
                );
            }
        }
        pbuf.row_count += 1;
        total_rows += 1;

        if pbuf.row_count >= state.segment_size {
            flush_segment(pbuf, state);
        }
    }

    let copy_elapsed = copy_start.elapsed();
    pgrx::notice!(
        "pg_deltax: COPY (Rust types, PG protocol) done: {} rows in {:.1}s, parse={:.1}s ({:.0}%)",
        total_rows,
        copy_elapsed.as_secs_f64(),
        parse_time_us as f64 / 1e6,
        if copy_elapsed.as_secs_f64() > 0.0 {
            (parse_time_us as f64 / 1e6) / copy_elapsed.as_secs_f64() * 100.0
        } else {
            0.0
        }
    );

    unsafe { pg_sys::EndCopyFrom(cstate) };
    unsafe { pg_sys::free_parsestate(pstate) };
    unsafe { pg_sys::table_close(rel, pg_sys::AccessShareLock as pg_sys::LOCKMODE) };
}

/// Per-column compression result produced by worker threads.
struct ColResult {
    col_idx: u16,
    col_i: usize,
    compressed: Vec<u8>,
    min_val: Option<String>,
    max_val: Option<String>,
    sum_val: Option<String>,
    nonnull_count: i64,
}

/// Flush a full segment from a partition buffer, using parallel compression.
fn flush_segment(
    buf: &mut PartitionBuffer,
    state: &BackfillState,
) {
    let t_start = std::time::Instant::now();

    // Ensure companion tables exist (created together on first segment).
    // Blobs and blooms tables are created WITHOUT primary keys for fast heap_insert.
    // PKs are added in finalize_partition after all data is loaded.
    if !buf.meta_table_created {
        let (_, blobs_fqn, blooms_fqn, meta_ddl, _, _) =
            build_companion_ddl(&buf.partition_table, &state.columns);
        spi_exec(&meta_ddl);
        // STORAGE EXTERNAL: skip TOAST pglz compression — blobs are already zstd-compressed.
        spi_exec(&format!(
            "CREATE TABLE {} (_col_idx SMALLINT NOT NULL, _segment_id INT NOT NULL, _data BYTEA COMPRESSION lz4)",
            blobs_fqn
        ));
        if crate::BLOOM_FILTERS.get() {
            spi_exec(&format!(
                "CREATE TABLE {} (_segment_id INT NOT NULL, _data BYTEA COMPRESSION lz4 NOT NULL)",
                blooms_fqn
            ));
        }
        buf.meta_table_created = true;
        buf.blobs_table_created = true;
    }

    // Sort by order_by columns
    let t_sort_start = std::time::Instant::now();
    sort_typed_columns(&mut buf.typed_cols, &state.order_col_indices, buf.row_count);
    let sort_ms = t_sort_start.elapsed().as_millis();

    let (meta_fqn, _, _, _, _, _) =
        build_companion_ddl(&buf.partition_table, &state.columns);

    // Compute ndistinct
    let ndistinct = compute_segment_ndistinct(&buf.typed_cols, &state.columns);

    // Segment_by values from the buffered data (extract from first row if present)
    let seg_values: Vec<Option<String>> = state.columns
        .iter()
        .enumerate()
        .filter(|(_, c)| c.is_segment_by)
        .map(|(i, _)| {
            if let TypedColumn::Text(v) = &buf.typed_cols[i] {
                if v.is_empty() { None } else { v[0].clone() }
            } else {
                None
            }
        })
        .collect();

    let seg_id = buf.next_segment_id;
    buf.next_segment_id += 1;

    // Build list of non-segment-by column indices for parallel compression
    let non_segby: Vec<(u16, usize)> = {
        let mut col_idx: u16 = 0;
        let mut result = Vec::new();
        for (i, col) in state.columns.iter().enumerate() {
            if col.is_segment_by {
                continue;
            }
            result.push((col_idx, i));
            col_idx += 1;
        }
        result
    };

    // Parallel compression: distribute columns across workers
    let t_compress_start = std::time::Instant::now();
    let n_workers = crate::get_parallel_workers();
    let typed_cols_ref = &buf.typed_cols;
    let columns_ref = &state.columns;

    let col_results: Vec<ColResult> = if n_workers > 1 && non_segby.len() > 1 {
        let chunk_size = non_segby.len().div_ceil(n_workers);
        std::thread::scope(|s| {
            non_segby
                .chunks(chunk_size)
                .map(|chunk| {
                    s.spawn(move || {
                        chunk.iter().map(|&(col_idx, col_i)| {
                            let col = &columns_ref[col_i];
                            let compressed = compress_typed_column(&typed_cols_ref[col_i], &col.data_type);
                            let (min_val, max_val) = if supports_minmax(&col.data_type) {
                                compute_typed_minmax(&typed_cols_ref[col_i], &col.data_type)
                            } else {
                                (None, None)
                            };
                            let (sum_val, nonnull_count) = if supports_sum(&col.data_type) {
                                compute_typed_sum(&typed_cols_ref[col_i])
                            } else {
                                (None, 0)
                            };
                            ColResult { col_idx, col_i, compressed, min_val, max_val, sum_val, nonnull_count }
                        }).collect::<Vec<_>>()
                    })
                })
                .collect::<Vec<_>>()
                .into_iter()
                .flat_map(|h| h.join().unwrap())
                .collect()
        })
    } else {
        // Single-threaded fallback
        non_segby.iter().map(|&(col_idx, col_i)| {
            let col = &columns_ref[col_i];
            let compressed = compress_typed_column(&typed_cols_ref[col_i], &col.data_type);
            let (min_val, max_val) = if supports_minmax(&col.data_type) {
                compute_typed_minmax(&typed_cols_ref[col_i], &col.data_type)
            } else {
                (None, None)
            };
            let (sum_val, nonnull_count) = if supports_sum(&col.data_type) {
                compute_typed_sum(&typed_cols_ref[col_i])
            } else {
                (None, 0)
            };
            ColResult { col_idx, col_i, compressed, min_val, max_val, sum_val, nonnull_count }
        }).collect()
    };

    let compress_ms = t_compress_start.elapsed().as_millis();

    // Build meta INSERT SQL on main thread
    let t_meta_start = std::time::Instant::now();
    let mut total_size: i64 = 0;
    let mut blobs: Vec<(u16, Vec<u8>)> = Vec::new();

    // Index col_results by col_i for lookup
    let mut col_minmax: std::collections::HashMap<usize, (Option<String>, Option<String>)> =
        std::collections::HashMap::new();
    let mut col_sums: std::collections::HashMap<usize, (Option<String>, i64)> =
        std::collections::HashMap::new();

    for cr in &col_results {
        total_size += cr.compressed.len() as i64;
        if supports_minmax(&state.columns[cr.col_i].data_type) {
            col_minmax.insert(cr.col_i, (cr.min_val.clone(), cr.max_val.clone()));
        }
        if supports_sum(&state.columns[cr.col_i].data_type) {
            col_sums.insert(cr.col_i, (cr.sum_val.clone(), cr.nonnull_count));
        }
    }
    for cr in col_results {
        blobs.push((cr.col_idx, cr.compressed));
    }

    // Build VALUES clause for this segment's meta row
    let mut insert_vals = Vec::new();

    insert_vals.push(seg_id.to_string());

    // Segment-by columns
    let mut seg_idx = 0;
    for col in &state.columns {
        if col.is_segment_by && seg_idx < seg_values.len() {
            match &seg_values[seg_idx] {
                Some(v) => insert_vals.push(format!("'{}'", v.replace('\'', "''"))),
                None => insert_vals.push("NULL".to_string()),
            }
            seg_idx += 1;
        }
    }

    // Min/max columns
    for (i, col) in state.columns.iter().enumerate() {
        if !col.is_segment_by && supports_minmax(&col.data_type) {
            match col_minmax.get(&i) {
                Some((Some(min_val), Some(max_val))) => {
                    insert_vals.push(format_minmax_for_insert(min_val, &col.data_type));
                    insert_vals.push(format_minmax_for_insert(max_val, &col.data_type));
                }
                _ => {
                    insert_vals.push("NULL".to_string());
                    insert_vals.push("NULL".to_string());
                }
            }
        }
    }

    // Sum and non-null count
    for (i, col) in state.columns.iter().enumerate() {
        if !col.is_segment_by && supports_sum(&col.data_type) {
            match col_sums.get(&i) {
                Some((Some(sum_val), nonnull_count)) => {
                    insert_vals.push(sum_val.clone());
                    insert_vals.push(nonnull_count.to_string());
                }
                _ => {
                    insert_vals.push("NULL".to_string());
                    insert_vals.push("0".to_string());
                }
            }
        }
    }

    // Ndistinct
    let mut nd_idx = 0;
    for col in &state.columns {
        if !col.is_segment_by {
            if nd_idx < ndistinct.len() {
                insert_vals.push(ndistinct[nd_idx].to_string());
            } else {
                insert_vals.push("0".to_string());
            }
            nd_idx += 1;
        }
    }

    insert_vals.push((buf.row_count as u32).to_string());

    // Bloom filters
    let bloom_data = if crate::BLOOM_FILTERS.get() {
        compute_segment_blooms(&buf.typed_cols, &state.columns, &ndistinct)
    } else {
        Vec::new()
    };

    // Cache the meta table name and column list (same for every segment of this partition)
    if buf.meta_fqn.is_none() {
        buf.meta_fqn = Some(meta_fqn.clone());

        let mut cols = Vec::new();
        cols.push("_segment_id".to_string());
        for col in &state.columns {
            if col.is_segment_by {
                cols.push(format!("\"{}\"", col.name));
            }
        }
        for col in &state.columns {
            if !col.is_segment_by && supports_minmax(&col.data_type) {
                cols.push(format!("\"_min_{}\"", col.name));
                cols.push(format!("\"_max_{}\"", col.name));
            }
        }
        for col in &state.columns {
            if !col.is_segment_by && supports_sum(&col.data_type) {
                cols.push(format!("\"_sum_{}\"", col.name));
                cols.push(format!("\"_nonnull_count_{}\"", col.name));
            }
        }
        for col in &state.columns {
            if !col.is_segment_by {
                cols.push(format!("\"_ndistinct_{}\"", col.name));
            }
        }
        cols.push("_row_count".to_string());
        buf.meta_insert_cols = Some(cols.join(", "));
    }

    // Buffer the VALUES row
    buf.meta_insert_rows.push(format!("({})", insert_vals.join(", ")));

    // Flush meta batch if full
    if buf.meta_insert_rows.len() >= META_BATCH_SIZE {
        flush_meta_buffer(buf);
    }
    let meta_ms = t_meta_start.elapsed().as_millis();

    buf.total_compressed_size += total_size;
    buf.total_rows += buf.row_count as i64;

    // Buffer blobs for column-major flush
    for (col_idx, blob) in blobs {
        buf.blob_buffer_size += blob.len();
        buf.blob_buffer.push((col_idx, seg_id, blob));
    }
    if !bloom_data.is_empty() {
        buf.bloom_buffer.push((seg_id, bloom_data));
    }

    // Flush blobs immediately if buffer exceeds threshold to bound memory usage.
    let t_blob_flush_start = std::time::Instant::now();
    let did_flush_blobs = buf.blob_buffer_size >= BLOB_BUFFER_THRESHOLD;
    if did_flush_blobs {
        flush_partition_blobs(buf, &state.columns);
    }
    let blob_flush_ms = t_blob_flush_start.elapsed().as_millis();

    // Log timing every 100 segments for profiling
    let total_ms = t_start.elapsed().as_millis();
    if buf.next_segment_id % 100 == 0 || did_flush_blobs {
        pgrx::notice!(
            "pg_deltax: segment timing: sort={}ms compress={}ms meta={}ms blob_flush={}ms total={}ms (workers={}, {} rows)",
            sort_ms, compress_ms, meta_ms, blob_flush_ms, total_ms,
            n_workers, buf.row_count
        );
    }

    // Reset for next segment
    buf.typed_cols = init_typed_columns(&state.columns, &state.kinds);
    buf.row_count = 0;
}

/// Flush buffered meta rows as a single multi-row INSERT.
fn flush_meta_buffer(buf: &mut PartitionBuffer) {
    if buf.meta_insert_rows.is_empty() {
        return;
    }
    let meta_fqn = buf.meta_fqn.as_ref().expect("meta_fqn not set");
    let cols = buf.meta_insert_cols.as_ref().expect("meta_insert_cols not set");
    let insert_sql = format!(
        "INSERT INTO {} ({}) VALUES {}",
        meta_fqn,
        cols,
        buf.meta_insert_rows.join(", ")
    );
    spi_exec(&insert_sql);
    buf.meta_insert_rows.clear();
}

/// Flush a partition's blob and bloom buffers to the companion tables.
/// Called when the COPY loop moves to a different partition (for time-sorted data),
/// or at end-of-COPY for remaining buffers. This keeps peak memory bounded to
/// one partition's worth of compressed blobs at a time.
fn flush_partition_blobs(
    buf: &mut PartitionBuffer,
    columns: &[ColumnMeta],
) {
    if buf.blob_buffer.is_empty() && buf.bloom_buffer.is_empty() {
        return;
    }

    // Cache companion table FQNs on first call
    if buf.blobs_fqn_cached.is_none() {
        let (_, blobs_fqn, blooms_fqn, _, _, _) =
            build_companion_ddl(&buf.partition_table, columns);
        buf.blobs_fqn_cached = Some(blobs_fqn);
        buf.blooms_fqn_cached = Some(blooms_fqn);
    }
    let blobs_fqn = buf.blobs_fqn_cached.as_ref().unwrap();
    let blooms_fqn = buf.blooms_fqn_cached.as_ref().unwrap();

    // Create tables without PK for fast heap_insert (PK added in finalize_partition)
    if !buf.blobs_table_created {
        spi_exec(&format!(
            "CREATE TABLE {} (_col_idx SMALLINT NOT NULL, _segment_id INT NOT NULL, _data BYTEA COMPRESSION lz4)",
            blobs_fqn
        ));
        if crate::BLOOM_FILTERS.get() {
            spi_exec(&format!(
                "CREATE TABLE {} (_segment_id INT NOT NULL, _data BYTEA COMPRESSION lz4 NOT NULL)",
                blooms_fqn
            ));
        }
        buf.blobs_table_created = true;
    }

    // Sort blobs column-major (col_idx, segment_id) for sequential TOAST I/O on read
    buf.blob_buffer.sort_by_key(|&(col_idx, seg_id, _)| (col_idx, seg_id));

    // Use direct heap_insert bypassing SPI entirely.
    // This avoids per-INSERT executor overhead, plan caching, and catalog cache bloat.
    // BulkInsertState uses a ring buffer to avoid polluting shared_buffers.
    if !buf.blob_buffer.is_empty() {
        let blobs_oid = *buf.blobs_oid_cached.get_or_insert_with(|| resolve_relation_oid(blobs_fqn));
        unsafe {
            // Create a temporary memory context for TOAST processing.
            // heap_insert internally calls toast_insert_or_update which palloc's
            // compressed copies and intermediate buffers in CurrentMemoryContext.
            // Without resetting, these accumulate for the entire transaction (~30GB for ClickBench).
            let insert_ctx = pg_sys::AllocSetContextCreateInternal(
                pg_sys::CurrentMemoryContext,
                c"direct_backfill_insert".as_ptr(),
                pg_sys::ALLOCSET_DEFAULT_MINSIZE as usize,
                pg_sys::ALLOCSET_DEFAULT_INITSIZE as usize,
                pg_sys::ALLOCSET_DEFAULT_MAXSIZE as usize,
            );

            let rel = pg_sys::table_open(blobs_oid, pg_sys::RowExclusiveLock as pg_sys::LOCKMODE);
            let tupdesc = (*rel).rd_att;
            let bistate = pg_sys::GetBulkInsertState();
            let cid = pg_sys::GetCurrentCommandId(true);

            for (col_idx, seg_id, blob) in buf.blob_buffer.drain(..) {
                let old_ctx = pg_sys::MemoryContextSwitchTo(insert_ctx);

                let (bytea_datum, _) = bytea_to_datum(&blob);
                drop(blob);

                let mut values: [pg_sys::Datum; 3] = [
                    pg_sys::Datum::from(col_idx as i16),
                    pg_sys::Datum::from(seg_id),
                    bytea_datum,
                ];
                let mut nulls: [bool; 3] = [false, false, false];

                let tuple = pg_sys::heap_form_tuple(
                    tupdesc,
                    values.as_mut_ptr(),
                    nulls.as_mut_ptr(),
                );
                pg_sys::heap_insert(rel, tuple, cid, 0, bistate);
                pg_sys::heap_freetuple(tuple);

                // Switch back and reset: frees all TOAST temp allocations + our bytea copy
                pg_sys::MemoryContextSwitchTo(old_ctx);
                pg_sys::MemoryContextReset(insert_ctx);
            }

            pg_sys::FreeBulkInsertState(bistate);
            pg_sys::table_close(rel, pg_sys::RowExclusiveLock as pg_sys::LOCKMODE);
            pg_sys::MemoryContextDelete(insert_ctx);
        }
    }

    // Blooms: same approach with per-insert context reset
    if !buf.bloom_buffer.is_empty() {
        let blooms_oid = *buf.blooms_oid_cached.get_or_insert_with(|| resolve_relation_oid(blooms_fqn));
        unsafe {
            let insert_ctx = pg_sys::AllocSetContextCreateInternal(
                pg_sys::CurrentMemoryContext,
                c"direct_backfill_bloom_insert".as_ptr(),
                pg_sys::ALLOCSET_DEFAULT_MINSIZE as usize,
                pg_sys::ALLOCSET_DEFAULT_INITSIZE as usize,
                pg_sys::ALLOCSET_DEFAULT_MAXSIZE as usize,
            );

            let rel = pg_sys::table_open(blooms_oid, pg_sys::RowExclusiveLock as pg_sys::LOCKMODE);
            let tupdesc = (*rel).rd_att;
            let bistate = pg_sys::GetBulkInsertState();
            let cid = pg_sys::GetCurrentCommandId(true);

            for (seg_id, bloom_data) in buf.bloom_buffer.drain(..) {
                let old_ctx = pg_sys::MemoryContextSwitchTo(insert_ctx);

                let (bytea_datum, _) = bytea_to_datum(&bloom_data);
                drop(bloom_data);

                let mut values: [pg_sys::Datum; 2] = [
                    pg_sys::Datum::from(seg_id),
                    bytea_datum,
                ];
                let mut nulls: [bool; 2] = [false, false];

                let tuple = pg_sys::heap_form_tuple(
                    tupdesc,
                    values.as_mut_ptr(),
                    nulls.as_mut_ptr(),
                );
                pg_sys::heap_insert(rel, tuple, cid, 0, bistate);
                pg_sys::heap_freetuple(tuple);

                pg_sys::MemoryContextSwitchTo(old_ctx);
                pg_sys::MemoryContextReset(insert_ctx);
            }

            pg_sys::FreeBulkInsertState(bistate);
            pg_sys::table_close(rel, pg_sys::RowExclusiveLock as pg_sys::LOCKMODE);
            pg_sys::MemoryContextDelete(insert_ctx);
        }
    }

    pgrx::notice!(
        "pg_deltax: flushed {} MB of blobs for partition '{}' ({} rows total)",
        buf.blob_buffer_size / (1024 * 1024),
        buf.partition_table,
        buf.total_rows
    );
    buf.blob_buffer_size = 0;
    buf.blobs_flushed = true;
}

/// ANALYZE companion tables and mark partition as compressed in catalog.
fn finalize_partition(
    buf: &mut PartitionBuffer,
    columns: &[ColumnMeta],
) {
    if buf.total_rows == 0 {
        return;
    }

    let (meta_fqn, blobs_fqn, blooms_fqn, _, _, _) =
        build_companion_ddl(&buf.partition_table, columns);

    // Add primary keys now that all data is loaded (much faster than maintaining
    // indexes during insert — PostgreSQL builds the B-tree in a single sort pass).
    spi_exec(&format!(
        "ALTER TABLE {} ADD PRIMARY KEY (_col_idx, _segment_id)",
        blobs_fqn
    ));
    if buf.blobs_table_created && crate::BLOOM_FILTERS.get() {
        spi_exec(&format!(
            "ALTER TABLE {} ADD PRIMARY KEY (_segment_id)",
            blooms_fqn
        ));
    }

    spi_exec(&format!("ANALYZE {}", meta_fqn));
    spi_exec(&format!("ANALYZE {}", blobs_fqn));

    if buf.blobs_table_created && crate::BLOOM_FILTERS.get() {
        spi_exec(&format!("ANALYZE {}", blooms_fqn));
    }

    // Use a short-lived SPI connection for catalog update
    let partition_id = buf.partition_id;
    let total_compressed_size = buf.total_compressed_size;
    let total_rows = buf.total_rows;
    Spi::connect_mut(|client| {
        catalog::mark_partition_compressed(
            client,
            partition_id,
            total_compressed_size,
            0, // raw_size not meaningful for direct backfill
            total_rows,
        )
        .expect("failed to update partition catalog");
    });
}

/// Binary search for partition by time value.
fn find_partition(range_starts: &[i64], range_ends: &[i64], time_usec: i64) -> Option<usize> {
    // Partitions are sorted by range_start. Find the last partition where range_start <= time_usec
    let pos = range_starts.partition_point(|&start| start <= time_usec);
    if pos == 0 {
        return None;
    }
    let idx = pos - 1;
    if time_usec < range_ends[idx] {
        Some(idx)
    } else {
        None
    }
}

