//! Direct backfill: intercept `COPY ... FROM` with `FORMAT deltax_compress`
//! via a ProcessUtility hook, compress data in-flight, and write directly
//! to companion tables without touching the heap.

use std::ffi::{CStr, CString, c_char};
use std::sync::atomic::{AtomicPtr, Ordering};

use pgrx::pg_sys;
use pgrx::pg_sys::ffi::pg_guard_ffi_boundary;
use pgrx::prelude::*;
// SpiClient no longer needed — all SPI calls use short-lived Spi::connect/connect_mut

use crate::catalog;
use crate::compress::{
    ColumnKind, ColumnMeta, TypedColumn,
    PG_EPOCH_OFFSET_USEC, PG_EPOCH_OFFSET_DAYS,
    build_companion_ddl, classify_column, compress_typed_column,
    compute_segment_blooms, compute_segment_ndistinct,
    compute_typed_minmax, compute_typed_sum,
    format_minmax_for_insert, init_typed_columns, get_column_metadata,
    sort_typed_columns, supports_minmax, supports_sum,
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
}

/// State for the entire backfill operation.
struct BackfillState {
    columns: Vec<ColumnMeta>,
    kinds: Vec<ColumnKind>,
    order_col_indices: Vec<usize>,
    segment_size: usize,
    time_col_index: usize,
    time_col_kind: ColumnKind,
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

fn handle_copy_from_inner(copy_stmt: *mut pg_sys::CopyStmt, format_idx: i32) {
    let cs = unsafe { &*copy_stmt };

    // 1. Resolve table
    if cs.relation.is_null() {
        pgrx::error!("pg_deltax: COPY FROM with FORMAT deltax_compress requires a relation");
    }
    let (schema, table) = unsafe { rangevar_to_names(cs.relation) };

    // 2. Validate via SPI — use a short-lived connection so its memory context
    //    is freed before the long-running COPY loop starts.
    let (partitions, columns, kinds, time_col_index, time_col_kind, order_col_indices, segment_size) =
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
        let time_col_kind = kinds[time_col_index];

        // Build order_by column indices
        let order_col_indices: Vec<usize> = ht.order_by
            .iter()
            .filter_map(|name| columns.iter().position(|c| c.name == *name))
            .collect();

        let segment_size = ht.segment_size as usize;

        (partitions, columns, kinds, time_col_index, time_col_kind, order_col_indices, segment_size)
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
        });
    }

    let state = BackfillState {
        columns,
        kinds,
        order_col_indices,
        segment_size,
        time_col_index,
        time_col_kind,
    };

    // 4. Open the relation and start COPY parsing
    // Strip FORMAT deltax_compress; PG defaults to TEXT format (tab-separated).
    // Users can override with DELIMITER ',' for CSV data.
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

    // Create an ExprContext for NextCopyFrom
    let estate = unsafe { pg_sys::CreateExecutorState() };
    let econtext = unsafe { pg_sys::CreateExprContext(estate) };

    // Get number of attributes from the relation
    let tupdesc = unsafe { (*rel).rd_att };
    let natts = unsafe { (*tupdesc).natts as usize };

    let mut values: Vec<pg_sys::Datum> = vec![pg_sys::Datum::from(0); natts];
    let mut nulls: Vec<bool> = vec![false; natts];

    let mut total_rows: i64 = 0;
    let mut last_part_idx: Option<usize> = None;
    let mut parse_time_us: u64 = 0;
    let copy_start = std::time::Instant::now();

    // 5. Core COPY loop — runs outside SPI so no SPI memory context accumulates.
    //
    // CRITICAL: NextCopyFrom allocates result datums (especially TEXT varlenas)
    // in CurrentMemoryContext. PostgreSQL's own CopyFrom switches to the
    // per-tuple memory context before calling NextCopyFrom, then resets it
    // after each row. We must do the same, otherwise every text datum leaks
    // into the long-lived transaction context (~1.5KB/row × 100M rows = 150GB).
    let per_tuple_ctx = unsafe { (*econtext).ecxt_per_tuple_memory };

    while {
        // Switch to per-tuple context before NextCopyFrom so all palloc'd
        // datums (text input conversions, etc.) land in the resettable context.
        let t_parse = std::time::Instant::now();
        let old_ctx = unsafe { pg_sys::MemoryContextSwitchTo(per_tuple_ctx) };
        let has_row = unsafe {
            pg_sys::NextCopyFrom(
                cstate,
                econtext,
                values.as_mut_ptr(),
                nulls.as_mut_ptr(),
            )
        };
        unsafe { pg_sys::MemoryContextSwitchTo(old_ctx) };
        parse_time_us += t_parse.elapsed().as_micros() as u64;
        has_row
    } {
        // Extract time column value (reads from Datum, no palloc)
        let time_usec = extract_time_usec(
            values[state.time_col_index],
            nulls[state.time_col_index],
            state.time_col_kind,
        );

        // Binary search for partition
        let part_idx = match find_partition(&range_starts, &range_ends, time_usec) {
            Some(idx) => idx,
            None => {
                pgrx::error!(
                    "pg_deltax: row with timestamp {} does not fit any partition",
                    time_usec
                );
            }
        };

        // Check if partition is already compressed
        if partitions[part_idx].is_compressed {
            pgrx::error!(
                "pg_deltax: partition '{}' is already compressed. Decompress it first to load new data.",
                partitions[part_idx].table_name
            );
        }

        // When partition changes, flush the previous partition's blobs to free memory.
        // For time-sorted data this means only one partition's blobs are in memory at a time.
        if let Some(prev_idx) = last_part_idx.filter(|&idx| idx != part_idx && !part_buffers[idx].blob_buffer.is_empty()) {
            flush_partition_blobs(&mut part_buffers[prev_idx], &state.columns);
        }
        last_part_idx = Some(part_idx);

        // Append row to partition buffer — copies datum values into Rust-owned
        // typed columns. After this, the palloc'd datums in per_tuple_ctx are
        // no longer needed and will be freed by ResetPerTupleExprContext below.
        append_datums_to_columns(
            &values,
            &nulls,
            &state.columns,
            &state.kinds,
            &mut part_buffers[part_idx].typed_cols,
        );
        part_buffers[part_idx].row_count += 1;
        total_rows += 1;

        // Flush if buffer full
        if part_buffers[part_idx].row_count >= state.segment_size {
            flush_segment(&mut part_buffers[part_idx], &state);
        }

        // Reset per-tuple memory context — frees all palloc'd datums from
        // NextCopyFrom (text varlenas, input conversion results, etc.)
        // This is equivalent to PostgreSQL's ResetPerTupleExprContext(estate) macro.
        unsafe {
            pg_sys::MemoryContextReset(per_tuple_ctx);
        }
    }

    let copy_elapsed = copy_start.elapsed();
    pgrx::notice!(
        "pg_deltax: COPY loop done: {} rows in {:.1}s, parse={:.1}s ({:.0}%)",
        total_rows,
        copy_elapsed.as_secs_f64(),
        parse_time_us as f64 / 1e6,
        (parse_time_us as f64 / 1e6) / copy_elapsed.as_secs_f64() * 100.0
    );

    unsafe { pg_sys::EndCopyFrom(cstate) };
    unsafe { pg_sys::free_parsestate(pstate) };
    unsafe { pg_sys::table_close(rel, pg_sys::AccessShareLock as pg_sys::LOCKMODE) };
    unsafe { pg_sys::FreeExecutorState(estate) };

    // 6. End-of-COPY flush
    for buf in &mut part_buffers {
        if buf.row_count == 0 && buf.total_rows == 0 {
            continue;
        }

        // Flush remaining partial segment
        if buf.row_count > 0 {
            flush_segment(buf, &state);
        }

        // Flush any remaining blobs (may already be flushed via partition-change logic)
        if !buf.blob_buffer.is_empty() {
            flush_partition_blobs(buf, &state.columns);
        }

        // ANALYZE companion tables and update catalog
        finalize_partition(buf, &state.columns);
    }

    crate::scan::invalidate_compressed_cache();

    pgrx::notice!(
        "pg_deltax: direct backfill complete, {} rows compressed into {} partitions",
        total_rows,
        part_buffers.iter().filter(|b| b.total_rows > 0).count()
    );
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

    // Build INSERT statement
    let mut insert_cols = Vec::new();
    let mut insert_vals = Vec::new();

    insert_cols.push("_segment_id".to_string());
    insert_vals.push(seg_id.to_string());

    // Segment-by columns
    let mut seg_idx = 0;
    for col in &state.columns {
        if col.is_segment_by {
            insert_cols.push(format!("\"{}\"", col.name));
            if seg_idx < seg_values.len() {
                match &seg_values[seg_idx] {
                    Some(v) => insert_vals.push(format!("'{}'", v.replace('\'', "''"))),
                    None => insert_vals.push("NULL".to_string()),
                }
                seg_idx += 1;
            }
        }
    }

    // Min/max columns
    for (i, col) in state.columns.iter().enumerate() {
        if !col.is_segment_by && supports_minmax(&col.data_type) {
            insert_cols.push(format!("\"_min_{}\"", col.name));
            insert_cols.push(format!("\"_max_{}\"", col.name));
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
            insert_cols.push(format!("\"_sum_{}\"", col.name));
            insert_cols.push(format!("\"_nonnull_count_{}\"", col.name));
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
            insert_cols.push(format!("\"_ndistinct_{}\"", col.name));
            if nd_idx < ndistinct.len() {
                insert_vals.push(ndistinct[nd_idx].to_string());
            } else {
                insert_vals.push("0".to_string());
            }
            nd_idx += 1;
        }
    }

    insert_cols.push("_row_count".to_string());
    insert_vals.push((buf.row_count as u32).to_string());

    // Bloom filters
    let bloom_data = if crate::BLOOM_FILTERS.get() {
        compute_segment_blooms(&buf.typed_cols, &state.columns, &ndistinct)
    } else {
        Vec::new()
    };

    let insert_sql = format!(
        "INSERT INTO {} ({}) VALUES ({})",
        meta_fqn,
        insert_cols.join(", "),
        insert_vals.join(", ")
    );
    spi_exec(&insert_sql);
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

    let (_, blobs_fqn, blooms_fqn, _, _, _) =
        build_companion_ddl(&buf.partition_table, columns);

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
        let blobs_oid = resolve_relation_oid(&blobs_fqn);
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
        let blooms_oid = resolve_relation_oid(&blooms_fqn);
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

/// Extract time column value as Unix epoch microseconds.
fn extract_time_usec(datum: pg_sys::Datum, is_null: bool, kind: ColumnKind) -> i64 {
    if is_null {
        pgrx::error!("pg_deltax: time column value is NULL, cannot route to partition");
    }
    match kind {
        ColumnKind::TimestampTz | ColumnKind::Timestamp => {
            // TimestampTz/Timestamp is i64 PG-epoch usec
            let pg_usec = datum.value() as i64;
            pg_usec + PG_EPOCH_OFFSET_USEC
        }
        ColumnKind::Date => {
            let pg_days = datum.value() as i32 as i64;
            (pg_days + PG_EPOCH_OFFSET_DAYS) * 86_400_000_000
        }
        _ => {
            pgrx::error!("pg_deltax: time column has unsupported type for partition routing");
        }
    }
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

/// Append raw Datum/null arrays into typed column accumulators.
fn append_datums_to_columns(
    values: &[pg_sys::Datum],
    nulls: &[bool],
    columns: &[ColumnMeta],
    kinds: &[ColumnKind],
    typed_cols: &mut [TypedColumn],
) {
    for (i, (col, kind)) in columns.iter().zip(kinds.iter()).enumerate() {
        if col.is_segment_by {
            // For segment_by columns, read as text
            if let TypedColumn::Text(vec) = &mut typed_cols[i] {
                if nulls[i] {
                    vec.push(None);
                } else {
                    let s = unsafe {
                        String::from_datum(values[i], false)
                    };
                    vec.push(s);
                }
            }
            continue;
        }

        if nulls[i] {
            match &mut typed_cols[i] {
                TypedColumn::Int16(v) => v.push(None),
                TypedColumn::Int32(v) => v.push(None),
                TypedColumn::Int64(v) => v.push(None),
                TypedColumn::Float32(v) => v.push(None),
                TypedColumn::Float64(v) => v.push(None),
                TypedColumn::Bool(v) => v.push(None),
                TypedColumn::Text(v) => v.push(None),
            }
            continue;
        }

        let d = values[i];
        match kind {
            ColumnKind::Int16 => {
                if let TypedColumn::Int16(vec) = &mut typed_cols[i] {
                    vec.push(Some(d.value() as i16));
                }
            }
            ColumnKind::Int32 => {
                if let TypedColumn::Int32(vec) = &mut typed_cols[i] {
                    vec.push(Some(d.value() as i32));
                }
            }
            ColumnKind::Int64 => {
                if let TypedColumn::Int64(vec) = &mut typed_cols[i] {
                    vec.push(Some(d.value() as i64));
                }
            }
            ColumnKind::Float32 => {
                if let TypedColumn::Float32(vec) = &mut typed_cols[i] {
                    vec.push(Some(f32::from_bits(d.value() as u32)));
                }
            }
            ColumnKind::Float64 => {
                if let TypedColumn::Float64(vec) = &mut typed_cols[i] {
                    vec.push(Some(f64::from_bits(d.value() as u64)));
                }
            }
            ColumnKind::Bool => {
                if let TypedColumn::Bool(vec) = &mut typed_cols[i] {
                    vec.push(Some(d.value() != 0));
                }
            }
            ColumnKind::Timestamp | ColumnKind::TimestampTz => {
                if let TypedColumn::Int64(vec) = &mut typed_cols[i] {
                    let pg_usec = d.value() as i64;
                    vec.push(Some(pg_usec + PG_EPOCH_OFFSET_USEC));
                }
            }
            ColumnKind::Date => {
                if let TypedColumn::Int64(vec) = &mut typed_cols[i] {
                    let pg_days = d.value() as i32 as i64;
                    vec.push(Some((pg_days + PG_EPOCH_OFFSET_DAYS) * 86_400_000_000));
                }
            }
            ColumnKind::Text => {
                if let TypedColumn::Text(vec) = &mut typed_cols[i] {
                    let s = unsafe {
                        String::from_datum(d, false)
                    };
                    vec.push(s);
                }
            }
        }
    }
}
