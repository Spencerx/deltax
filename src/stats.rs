//! Populate `pg_class.reltuples` and `pg_statistic` for compressed
//! partitions so PG's built-in selectivity functions stop falling back
//! to `DEFAULT_EQ_SEL` (0.005 for numeric equality, ~2.5e-5 for text
//! equality). This is the ingredient that lets the planner pick the
//! right join side on queries like Q17 (`event_type='Delivered'`) and
//! keeps point lookups (Q07 `order_id = N`) off the parallel path.
//!
//! Source of truth:
//! - `deltax.deltax_partition.row_count` — authoritative total rows
//! - `deltax.deltax_partition.column_ndistinct` — per-column merged-HLL
//!   estimate written by `compress.rs` at compress time (or SQL
//!   fallback for the standalone analyze UDF)
//! - `_<partition>_colstats._nonnull_count` — summed for nullfrac

use std::collections::HashMap;

use cardinality_estimator::CardinalityEstimator;
use pgrx::pg_sys;
use pgrx::spi::{self, SpiClient};

use crate::compress::ColumnMeta;

/// Write `pg_class.reltuples` + one `pg_statistic` row per column for
/// the compressed child partition.
pub fn write_partition_stats(
    client: &mut SpiClient,
    part_rel_oid: pg_sys::Oid,
    col_ndistinct: &HashMap<String, i64>,
    row_count: i64,
    colstats_fqn: &str,
    columns: &[ColumnMeta],
) -> spi::SpiResult<()> {
    if row_count <= 0 {
        return Ok(());
    }

    // Single SPI pass over the colstats table to get per-column
    // SUM(nonnull). One row per non-segment-by column, in _col_idx order.
    let nonnull_by_col_idx = load_nonnull_counts(client, colstats_fqn)?;

    // Fetch `(attname, attnum, attlen, atttypid)` for every non-dropped
    // column of the partition so we can map our `ColumnMeta` back to PG's
    // attnum, pick stawidth from attlen, and choose a histogram encoding
    // from atttypid.
    let attrs = load_pg_attribute(client, part_rel_oid)?;

    // Per-column `[min, max]` (order-preserving i64 encoding), persisted at
    // compression time. Feeds the per-column histogram so range predicates
    // on the order-by / time column stop collapsing to a default
    // selectivity. Empty if the partition predates `column_minmax`.
    let col_minmax = load_column_minmax(client, part_rel_oid)?;

    // Estimate average tuple width — feeds `pg_class.relpages`.
    let mut sum_widths: i64 = 0;
    for a in &attrs {
        sum_widths += stawidth_for_attlen(a.attlen) as i64;
    }
    let avg_tuple_width = sum_widths.max(32);

    // Walk our pg_deltax columns, match to the partition's pg_attribute
    // entry by name, emit one pg_statistic row each. Segment-by columns
    // have null estimates from our HLL map (they're stored as the
    // partition's segment_values, not in the blob), so we skip them
    // here and let PG default them — there are usually only a handful.
    let mut nonseg_idx: i32 = 0;
    for col in columns {
        if col.is_segment_by {
            continue;
        }
        let attr = match attrs.iter().find(|a| a.attname == col.name) {
            Some(a) => a,
            None => {
                nonseg_idx += 1;
                continue; // column was dropped post-compression
            }
        };
        let stawidth = stawidth_for_attlen(attr.attlen);

        // NOT NULL columns have no nulls by definition. For nullable
        // columns, derive nullfrac from the colstats nonnull sum — but a
        // missing or 0 entry means "no per-column info" (e.g. the order-by
        // time column isn't indexed the same way in `_colstats`), NOT that
        // the column is all-null. Treating 0 as all-null wrote
        // `stanullfrac = 1.0` for `event_created`, which zeroed its range
        // selectivity `(1 - nullfrac)` and neutralised the histogram.
        let stanullfrac = if attr.attnotnull {
            0.0
        } else {
            let nonnull = nonnull_by_col_idx
                .get(&nonseg_idx)
                .copied()
                .filter(|&n| n > 0)
                .unwrap_or(row_count);
            ((row_count - nonnull) as f32 / row_count as f32).clamp(0.0, 1.0)
        };

        let ndistinct = col_ndistinct.get(&col.name).copied().unwrap_or(0);
        let stadistinct = stadistinct_value(ndistinct, row_count);

        // Histogram slot from the persisted [min, max], when the type is
        // one we can encode and the column isn't constant.
        let histogram = col_minmax.get(&col.name).and_then(|&(lo, hi)| {
            histogram_eligible(attr.atttypid, lo, hi).then(|| (attr.atttypid, vec![lo, hi]))
        });

        upsert_pg_statistic_row(
            client,
            part_rel_oid,
            attr.attnum,
            stadistinct,
            stanullfrac,
            stawidth,
            histogram,
            false,
        )?;

        nonseg_idx += 1;
    }

    update_reltuples(client, part_rel_oid, row_count, avg_tuple_width as i32)?;

    // Make the new stats visible to other backends at commit time.
    invalidate_relcache(part_rel_oid);

    Ok(())
}

/// Load `SUM(_nonnull_count)` per `_col_idx` in a single pass.
fn load_nonnull_counts(
    client: &mut SpiClient,
    colstats_fqn: &str,
) -> spi::SpiResult<HashMap<i32, i64>> {
    let query = format!(
        "SELECT _col_idx::int4, SUM(_nonnull_count)::int8 \
         FROM {} GROUP BY _col_idx",
        colstats_fqn
    );
    let mut out = HashMap::new();
    for row in client.select(&query, None, &[])? {
        let idx: i32 = row
            .get_datum_by_ordinal(1)
            .ok()
            .and_then(|d| d.value::<i32>().ok().flatten())
            .unwrap_or(-1);
        let nonnull: i64 = row
            .get_datum_by_ordinal(2)
            .ok()
            .and_then(|d| d.value::<i64>().ok().flatten())
            .unwrap_or(0);
        if idx >= 0 {
            out.insert(idx, nonnull);
        }
    }
    Ok(out)
}

struct AttrInfo {
    attname: String,
    attnum: i16,
    attlen: i16,
    atttypid: pg_sys::Oid,
    attnotnull: bool,
}

fn load_pg_attribute(
    client: &mut SpiClient,
    rel_oid: pg_sys::Oid,
) -> spi::SpiResult<Vec<AttrInfo>> {
    let rel_oid_int = u32::from(rel_oid) as i64;
    let query = "SELECT attname::text, attnum::int2, attlen::int2, atttypid::int8, attnotnull \
                 FROM pg_attribute \
                 WHERE attrelid = $1::oid AND attnum > 0 AND NOT attisdropped \
                 ORDER BY attnum";
    let mut out = Vec::new();
    for row in client.select(query, None, &[rel_oid_int.into()])? {
        let attname: String = row
            .get_datum_by_ordinal(1)
            .ok()
            .and_then(|d| d.value::<String>().ok().flatten())
            .unwrap_or_default();
        let attnum: i16 = row
            .get_datum_by_ordinal(2)
            .ok()
            .and_then(|d| d.value::<i16>().ok().flatten())
            .unwrap_or(0);
        let attlen: i16 = row
            .get_datum_by_ordinal(3)
            .ok()
            .and_then(|d| d.value::<i16>().ok().flatten())
            .unwrap_or(-1);
        let atttypid: pg_sys::Oid = row
            .get_datum_by_ordinal(4)
            .ok()
            .and_then(|d| d.value::<i64>().ok().flatten())
            .map(|v| pg_sys::Oid::from(v as u32))
            .unwrap_or(pg_sys::InvalidOid);
        let attnotnull: bool = row
            .get_datum_by_ordinal(5)
            .ok()
            .and_then(|d| d.value::<bool>().ok().flatten())
            .unwrap_or(false);
        if !attname.is_empty() {
            out.push(AttrInfo {
                attname,
                attnum,
                attlen,
                atttypid,
                attnotnull,
            });
        }
    }
    Ok(out)
}

/// Whether we emit a `[min, max]` histogram for a column of this type, given
/// the order-preserving i64 bounds. Skips types we can't decode and constant
/// columns (`lo >= hi`), where a degenerate 1-bucket histogram would only
/// confuse range selectivity. Date bounds are compared at day granularity
/// (colstats stores dates as Unix-epoch microseconds; `decode_encoded_to_datum`
/// truncates to days), so a range inside a single day is treated as constant.
fn histogram_eligible(type_oid: pg_sys::Oid, lo: i64, hi: i64) -> bool {
    if !histogram_type_eligible(type_oid) {
        return false;
    }
    match type_oid {
        pg_sys::DATEOID => (lo / 86_400_000_000) < (hi / 86_400_000_000),
        _ => lo < hi,
    }
}

/// Types whose order-preserving i64 colstats encoding we can decode back to
/// a native Datum for a histogram. FLOAT/TEXT/NUMERIC fall through.
fn histogram_type_eligible(type_oid: pg_sys::Oid) -> bool {
    matches!(
        type_oid,
        pg_sys::INT2OID
            | pg_sys::INT4OID
            | pg_sys::INT8OID
            | pg_sys::TIMESTAMPOID
            | pg_sys::TIMESTAMPTZOID
            | pg_sys::DATEOID
    )
}

/// Construct the 2-element `[min, max]` bounds array Datum for a histogram
/// slot, decoding the order-preserving i64 colstats encoding back to the
/// column's native type. Caller must have checked `histogram_eligible`.
///
/// `pg_statistic.stavaluesN` is an `anyarray` pseudo-type column, so it can't
/// be populated through a SQL `INSERT` of a concrete array (PG rejects the
/// row-type mismatch). PG's own ANALYZE forms the tuple in C with a real
/// array Datum; we do the same.
unsafe fn build_histogram_array(type_oid: pg_sys::Oid, bounds: &[i64]) -> pg_sys::Datum {
    let mut elems: Vec<pg_sys::Datum> = bounds
        .iter()
        .map(|&v| crate::scan::exec::count_minmax::decode_encoded_to_datum(v, type_oid))
        .collect();
    let mut typlen: i16 = 0;
    let mut typbyval: bool = false;
    let mut typalign: std::os::raw::c_char = 0;
    let arr = unsafe {
        pg_sys::get_typlenbyvalalign(type_oid, &mut typlen, &mut typbyval, &mut typalign);
        pg_sys::construct_array(
            elems.as_mut_ptr(),
            elems.len() as i32,
            type_oid,
            typlen as i32,
            typbyval,
            typalign,
        )
    };
    pg_sys::Datum::from(arr)
}

/// Look up the btree `<` operator for a type (strategy 1), used as `staop1`
/// for the histogram slot. Returns `InvalidOid` if the type has no btree
/// ordering (caller then skips the histogram).
fn lookup_lt_operator(client: &mut SpiClient, type_oid: pg_sys::Oid) -> pg_sys::Oid {
    let t = u32::from(type_oid) as i64;
    client
        .select(
            "SELECT amopopr::int8 FROM pg_amop \
             WHERE amoplefttype = $1::oid AND amoprighttype = $1::oid \
               AND amopstrategy = 1 AND amopmethod = 403 LIMIT 1",
            None,
            &[t.into()],
        )
        .ok()
        .and_then(|t| t.into_iter().next())
        .and_then(|row| row.get_datum_by_ordinal(1).ok().and_then(|d| d.value::<i64>().ok().flatten()))
        .map(|v| pg_sys::Oid::from(v as u32))
        .unwrap_or(pg_sys::InvalidOid)
}

/// Load the persisted per-column `[min, max]` (order-preserving i64) map for
/// a compressed partition from `deltax.deltax_partition.column_minmax`.
/// Empty map if the partition predates the column or has no eligible cols.
fn load_column_minmax(
    client: &mut SpiClient,
    part_rel_oid: pg_sys::Oid,
) -> spi::SpiResult<HashMap<String, (i64, i64)>> {
    let part_oid_int = u32::from(part_rel_oid) as i64;
    let json_text: Option<String> = client
        .select(
            "SELECT column_minmax::text FROM deltax.deltax_partition \
             WHERE table_name = (SELECT relname FROM pg_class WHERE oid = $1::oid) \
               AND is_compressed = true",
            None,
            &[part_oid_int.into()],
        )?
        .next()
        .and_then(|row| {
            row.get_datum_by_ordinal(1)
                .ok()
                .and_then(|d| d.value::<String>().ok().flatten())
        });
    Ok(json_text
        .and_then(|t| crate::scan::cost::parse_minmax_json(&t))
        .unwrap_or_default())
}

/// Translate pg_attribute.attlen into a `stawidth`. Fixed-width types
/// use attlen directly; varlena types (`attlen < 0`) get a conservative
/// 32-byte default — pg_statistic's `stawidth` only feeds I/O and
/// width-dependent cost paths, not the equality selectivity we care
/// about here, so a rough estimate is fine.
fn stawidth_for_attlen(attlen: i16) -> i32 {
    if attlen > 0 { attlen as i32 } else { 32 }
}

/// Encode ndistinct per PG's sign convention: positive = absolute count
/// of distinct values; negative = fraction of `row_count`. PG's ANALYZE
/// flips to the fraction form when ndistinct / row_count > 0.1, which
/// lets the estimator handle tables that grow without a re-ANALYZE.
fn stadistinct_value(ndistinct: i64, row_count: i64) -> f32 {
    if ndistinct <= 0 || row_count <= 0 {
        return 0.0;
    }
    let nd = ndistinct as f64;
    let rc = row_count as f64;
    if nd < 0.1 * rc {
        nd as f32
    } else {
        (-nd / rc) as f32
    }
}

/// `UPDATE pg_class SET reltuples = $1, relpages = ... WHERE oid = $2`.
/// Keep `relpages >= 1` so PG doesn't mistake us for "never analyzed"
/// in its cost paths.
fn update_reltuples(
    client: &mut SpiClient,
    rel_oid: pg_sys::Oid,
    row_count: i64,
    avg_tuple_width: i32,
) -> spi::SpiResult<()> {
    let rel_oid_int = u32::from(rel_oid) as i64;
    let rel_pages: i32 = {
        let tuples_per_page = (8192 / avg_tuple_width.max(1)).max(1) as i64;
        ((row_count + tuples_per_page - 1) / tuples_per_page).max(1) as i32
    };
    client.update(
        "UPDATE pg_class SET reltuples = $1::real, relpages = $2::int \
         WHERE oid = $3::oid",
        None,
        &[
            (row_count as f32).into(),
            rel_pages.into(),
            rel_oid_int.into(),
        ],
    )?;
    Ok(())
}

/// DELETE-then-INSERT on pg_statistic for a single (rel, attnum, inherit=false)
/// row. pg_statistic has no convenient upsert (the unique index is on
/// `(starelid, staattnum, stainherit)` but it's a system index not
/// advertised for `ON CONFLICT`), so two-step is the conventional
/// pattern — same thing `update_attstats` does in the backend.
#[allow(clippy::too_many_arguments)]
fn upsert_pg_statistic_row(
    client: &mut SpiClient,
    attrelid: pg_sys::Oid,
    attnum: i16,
    stadistinct: f32,
    stanullfrac: f32,
    stawidth: i32,
    histogram: Option<(pg_sys::Oid, Vec<i64>)>,
    stainherit: bool,
) -> spi::SpiResult<()> {
    let attrelid_int = u32::from(attrelid) as i64;
    client.update(
        "DELETE FROM pg_statistic \
         WHERE starelid = $1::oid AND staattnum = $2::int2 AND stainherit = $3",
        None,
        &[attrelid_int.into(), attnum.into(), stainherit.into()],
    )?;

    // Resolve the histogram slot (btree `<` operator + bounds array Datum)
    // before forming the tuple; skip it if the type has no btree ordering
    // or there aren't at least two distinct bounds.
    let hist_slot: Option<(pg_sys::Oid, pg_sys::Datum)> = match histogram {
        Some((type_oid, bounds)) if bounds.len() >= 2 => {
            let ltopr = lookup_lt_operator(client, type_oid);
            if ltopr == pg_sys::InvalidOid {
                None
            } else {
                Some((ltopr, unsafe { build_histogram_array(type_oid, &bounds) }))
            }
        }
        _ => None,
    };

    // The catalog DELETE above must be visible to the heap insert's unique
    // index check.
    unsafe { pg_sys::CommandCounterIncrement() };
    unsafe {
        form_and_insert_pg_statistic(
            attrelid, attnum, stadistinct, stanullfrac, stawidth, hist_slot, stainherit,
        )
    };
    Ok(())
}

/// Form a `pg_statistic` tuple in C and insert it. Slot 1 carries a
/// `STATISTIC_KIND_HISTOGRAM` (2) when `hist_slot` is `Some((ltopr, array))`:
/// `staop1` is the type's btree `<`, `stacoll1` stays 0 (eligible types are
/// non-collatable), `stavalues1` is the 2-element bounds array. The unused
/// slots are zeroed (stakind/staop/stacoll) or NULL (stanumbers/stavalues).
/// `stanumbers*` stay NULL — we claim no MCV/correlation, only the histogram.
unsafe fn form_and_insert_pg_statistic(
    attrelid: pg_sys::Oid,
    attnum: i16,
    stadistinct: f32,
    stanullfrac: f32,
    stawidth: i32,
    hist_slot: Option<(pg_sys::Oid, pg_sys::Datum)>,
    stainherit: bool,
) {
    use pgrx::IntoDatum;

    let natts = pg_sys::Natts_pg_statistic as usize;
    let mut values: Vec<pg_sys::Datum> = vec![pg_sys::Datum::from(0usize); natts];
    let mut nulls: Vec<bool> = vec![true; natts];

    let put = |values: &mut [pg_sys::Datum], nulls: &mut [bool], anum: u32, d: Option<pg_sys::Datum>| {
        let i = (anum - 1) as usize;
        match d {
            Some(v) => {
                values[i] = v;
                nulls[i] = false;
            }
            None => nulls[i] = true,
        }
    };
    // `staopN` / `stacollN` are NOT NULL columns whose unused value is 0.
    // pgrx's `Oid::into_datum()` maps `InvalidOid` (0) to SQL NULL, which
    // would leave a NULL `stacoll1` — and PG then ignores the histogram
    // slot entirely. Build a non-null zero Datum directly instead.
    let oid_d = |o: pg_sys::Oid| Some(pg_sys::Datum::from(u32::from(o) as usize));
    // Same NOT-NULL reasoning for `stakindN` (int2) and the float4 columns:
    // build non-null Datums directly so a 0/0.0 value doesn't become NULL.
    let i16_d = |v: i16| Some(pg_sys::Datum::from(v as u16 as usize));
    let f32_d = |v: f32| Some(pg_sys::Datum::from(v.to_bits() as usize));
    let i32_d = |v: i32| Some(pg_sys::Datum::from(v as u32 as usize));

    put(&mut values, &mut nulls, pg_sys::Anum_pg_statistic_starelid, oid_d(attrelid));
    put(&mut values, &mut nulls, pg_sys::Anum_pg_statistic_staattnum, attnum.into_datum());
    put(&mut values, &mut nulls, pg_sys::Anum_pg_statistic_stainherit, stainherit.into_datum());
    put(&mut values, &mut nulls, pg_sys::Anum_pg_statistic_stanullfrac, f32_d(stanullfrac));
    put(&mut values, &mut nulls, pg_sys::Anum_pg_statistic_stawidth, i32_d(stawidth));
    put(&mut values, &mut nulls, pg_sys::Anum_pg_statistic_stadistinct, f32_d(stadistinct));

    // Five (kind, op, coll, numbers, values) slots. Only slot 1 may carry a
    // histogram; the rest are empty.
    for slot in 0u32..5 {
        let (kind, op, vals): (i16, pg_sys::Oid, Option<pg_sys::Datum>) = if slot == 0 {
            match hist_slot {
                Some((ltopr, arr)) => (
                    pg_sys::STATISTIC_KIND_HISTOGRAM as i16,
                    ltopr,
                    Some(arr),
                ),
                None => (0, pg_sys::InvalidOid, None),
            }
        } else {
            (0, pg_sys::InvalidOid, None)
        };
        put(&mut values, &mut nulls, pg_sys::Anum_pg_statistic_stakind1 + slot, i16_d(kind));
        put(&mut values, &mut nulls, pg_sys::Anum_pg_statistic_staop1 + slot, oid_d(op));
        put(
            &mut values,
            &mut nulls,
            pg_sys::Anum_pg_statistic_stacoll1 + slot,
            oid_d(pg_sys::InvalidOid),
        );
        put(&mut values, &mut nulls, pg_sys::Anum_pg_statistic_stanumbers1 + slot, None);
        put(&mut values, &mut nulls, pg_sys::Anum_pg_statistic_stavalues1 + slot, vals);
    }

    unsafe {
        let rel = pg_sys::table_open(
            pg_sys::StatisticRelationId,
            pg_sys::RowExclusiveLock as pg_sys::LOCKMODE,
        );
        let tuple =
            pg_sys::heap_form_tuple((*rel).rd_att, values.as_mut_ptr(), nulls.as_mut_ptr());
        pg_sys::CatalogTupleInsert(rel, tuple);
        pg_sys::heap_freetuple(tuple);
        pg_sys::table_close(rel, pg_sys::RowExclusiveLock as pg_sys::LOCKMODE);
    }
}

/// Propagate a relcache invalidation so other backends pick up the
/// fresh pg_statistic/pg_class rows on next planning. Compression
/// already holds AccessExclusiveLock on the partition, so this is
/// the only catalog-cache invalidation needed.
fn invalidate_relcache(rel_oid: pg_sys::Oid) {
    unsafe {
        pg_sys::CacheInvalidateRelcacheByRelid(rel_oid);
    }
}

/// Entry point for the standalone `deltax_analyze_partition` UDF. The
/// authoritative per-column distinct counts are the merged-HLL estimates
/// written to `deltax.deltax_partition.column_ndistinct` at compression
/// time, so read those rather than re-deriving from `_colstats`.
///
/// The old fallback — `SUM(per-segment _ndistinct)` from `_colstats` —
/// badly overcounts low-cardinality columns (it double-counts every
/// value that appears in more than one segment: `event_type` with 9
/// distinct values summed to 264 across ~30 segments), which made the
/// planner treat the column as near-unique and pick worse join orders.
/// Columns absent from the persisted map (e.g. a partition compressed by
/// a build that predates HLL persistence) are simply left for PG to
/// default, which is neutral rather than actively wrong.
pub fn analyze_partition_from_catalog(
    client: &mut SpiClient,
    part_rel_oid: pg_sys::Oid,
    colstats_fqn: &str,
    columns: &[ColumnMeta],
    row_count: i64,
) -> spi::SpiResult<()> {
    // Read the persisted merged-HLL map (column name -> ndistinct) for
    // this partition. column_ndistinct is keyed by column name, the same
    // key write_partition_stats expects.
    let part_oid_int = u32::from(part_rel_oid) as i64;
    let json_text: Option<String> = client
        .select(
            "SELECT column_ndistinct::text FROM deltax.deltax_partition \
             WHERE table_name = (SELECT relname FROM pg_class WHERE oid = $1::oid) \
               AND is_compressed = true",
            None,
            &[part_oid_int.into()],
        )?
        .next()
        .and_then(|row| {
            row.get_datum_by_ordinal(1)
                .ok()
                .and_then(|d| d.value::<String>().ok().flatten())
        });

    let mut col_ndistinct: HashMap<String, i64> = HashMap::new();
    if let Some(text) = json_text {
        crate::scan::cost::parse_ndistinct_json(&text, &mut col_ndistinct);
    }
    // Cap at row_count to keep stadistinct_value's sign convention sane.
    for v in col_ndistinct.values_mut() {
        *v = (*v).min(row_count);
    }

    write_partition_stats(
        client,
        part_rel_oid,
        &col_ndistinct,
        row_count,
        colstats_fqn,
        columns,
    )
}

/// Deserialize a partition's `column_hll` JSON (`{col_name: <sketch>}`) and
/// union each column's sketch into the table-wide accumulator. Sketches that
/// fail to parse are skipped (the caller falls back to the heuristic).
fn merge_partition_hll_json(text: &str, acc: &mut HashMap<String, CardinalityEstimator<u64>>) {
    let Ok(serde_json::Value::Object(obj)) = serde_json::from_str::<serde_json::Value>(text) else {
        return;
    };
    for (name, val) in obj {
        if let Ok(sketch) = serde_json::from_value::<CardinalityEstimator<u64>>(val) {
            match acc.get_mut(&name) {
                Some(existing) => existing.merge(&sketch),
                None => {
                    acc.insert(name, sketch);
                }
            }
        }
    }
}

/// Merge per-partition distinct counts into a table-wide estimate. Columns
/// whose per-partition value ranges are mostly disjoint (e.g. the time column,
/// or order_id correlated with time) have additive distinct counts → SUM;
/// columns with overlapping ranges (the same low-cardinality values in every
/// partition, e.g. `event_type`) repeat → MAX. Capped at the table row count.
fn merge_ndistinct(nds: &[i64], ranges: &[(i64, i64)], total_rows: i64) -> i64 {
    if nds.is_empty() {
        return 0;
    }
    let sum: i64 = nds.iter().sum();
    let max: i64 = *nds.iter().max().unwrap();
    let disjoint = if ranges.len() == nds.len() && ranges.len() > 1 {
        let mut r = ranges.to_vec();
        r.sort_unstable();
        let overlaps = (1..r.len()).filter(|&i| r[i].0 <= r[i - 1].1).count();
        (overlaps as f64) < 0.5 * ((r.len() - 1) as f64)
    } else {
        false
    };
    let nd = if disjoint { sum } else { max };
    nd.clamp(1, total_rows.max(1))
}

/// Build a table-wide histogram's bound list from per-partition `[min, max]`:
/// the sorted per-partition minimums plus the global maximum. For roughly
/// equal-sized partitions this is an equi-depth histogram over the order-by
/// column; for columns with a constant range it collapses to `[min, max]`.
fn parent_histogram_bounds(type_oid: pg_sys::Oid, ranges: &[(i64, i64)]) -> Option<Vec<i64>> {
    if !histogram_type_eligible(type_oid) || ranges.is_empty() {
        return None;
    }
    let mut bounds: Vec<i64> = ranges.iter().map(|r| r.0).collect();
    bounds.sort_unstable();
    bounds.push(ranges.iter().map(|r| r.1).max().unwrap());
    // Strictly ascending: drop adjacent duplicates (for DATE, at day
    // granularity, since the element Datum truncates to days).
    if type_oid == pg_sys::DATEOID {
        bounds.dedup_by_key(|b| *b / 86_400_000_000);
    } else {
        bounds.dedup();
    }
    (bounds.len() >= 2).then_some(bounds)
}

/// Populate the parent relation's `pg_class.reltuples` + `pg_statistic`
/// (`stainherit = true`, the inheritance-tree stats PG reads for a
/// partitioned parent) by merging the per-partition stats persisted in
/// `deltax.deltax_partition`. DeltaXAppend scans the parent as one baserel,
/// so the planner reads JOIN selectivity (e.g. `oe.order_id = oi.order_id`)
/// from the parent's stats — without them, join cardinality is badly
/// mis-estimated and the planner picks hash joins where nested loops win.
pub fn write_table_stats(
    client: &mut SpiClient,
    schema: &str,
    table: &str,
) -> spi::SpiResult<()> {
    let parent_oid: pg_sys::Oid = {
        let fqn = format!("{}.{}", quote_ident(schema), quote_ident(table));
        let mut oid = pg_sys::InvalidOid;
        for row in client.select(&format!("SELECT '{}'::regclass::oid", fqn), None, &[])? {
            oid = row
                .get_datum_by_ordinal(1)
                .ok()
                .and_then(|d| d.value::<pg_sys::Oid>().ok().flatten())
                .unwrap_or(pg_sys::InvalidOid);
        }
        oid
    };
    if parent_oid == pg_sys::InvalidOid {
        return Ok(());
    }

    // Gather per-partition (ndistinct, minmax) lists per column + total rows,
    // plus a merged table-wide HLL per column (accurate global distinct count,
    // unlike the SUM/MAX heuristic over per-partition estimates).
    let mut total_rows: i64 = 0;
    let mut nd_by_col: HashMap<String, Vec<i64>> = HashMap::new();
    let mut mm_by_col: HashMap<String, Vec<(i64, i64)>> = HashMap::new();
    let mut hll_by_col: HashMap<String, CardinalityEstimator<u64>> = HashMap::new();
    let query = "SELECT row_count, column_ndistinct::text, column_minmax::text, column_hll::text \
                 FROM deltax.deltax_partition \
                 WHERE is_compressed = true AND deltatable_id = (\
                     SELECT id FROM deltax.deltax_deltatable \
                     WHERE schema_name = $1 AND table_name = $2)";
    for row in client.select(query, None, &[schema.into(), table.into()])? {
        let rc: i64 = row
            .get_datum_by_ordinal(1)
            .ok()
            .and_then(|d| d.value::<i64>().ok().flatten())
            .unwrap_or(0);
        total_rows += rc;
        if let Some(text) = row
            .get_datum_by_ordinal(2)
            .ok()
            .and_then(|d| d.value::<String>().ok().flatten())
        {
            let mut m = HashMap::new();
            crate::scan::cost::parse_ndistinct_json(&text, &mut m);
            for (name, nd) in m {
                nd_by_col.entry(name).or_default().push(nd);
            }
        }
        if let Some(text) = row
            .get_datum_by_ordinal(3)
            .ok()
            .and_then(|d| d.value::<String>().ok().flatten())
            && let Some(m) = crate::scan::cost::parse_minmax_json(&text)
        {
            for (name, mm) in m {
                mm_by_col.entry(name).or_default().push(mm);
            }
        }
        if let Some(text) = row
            .get_datum_by_ordinal(4)
            .ok()
            .and_then(|d| d.value::<String>().ok().flatten())
        {
            merge_partition_hll_json(&text, &mut hll_by_col);
        }
    }
    if total_rows <= 0 {
        return Ok(());
    }

    let attrs = load_pg_attribute(client, parent_oid)?;
    let mut sum_widths: i64 = 0;
    for a in &attrs {
        sum_widths += stawidth_for_attlen(a.attlen) as i64;
    }
    let avg_tuple_width = sum_widths.max(32);

    for attr in &attrs {
        let empty_nd: Vec<i64> = Vec::new();
        let empty_mm: Vec<(i64, i64)> = Vec::new();
        let nds = nd_by_col.get(&attr.attname).unwrap_or(&empty_nd);
        let mms = mm_by_col.get(&attr.attname).unwrap_or(&empty_mm);

        // Prefer the merged table-wide HLL estimate (accurate global distinct);
        // fall back to the SUM/MAX heuristic for partitions that predate
        // `column_hll`.
        let merged_nd = match hll_by_col.get(&attr.attname) {
            Some(hll) => (hll.estimate() as i64).clamp(1, total_rows.max(1)),
            None => merge_ndistinct(nds, mms, total_rows),
        };
        let stadistinct = stadistinct_value(merged_nd, total_rows);
        // We don't aggregate per-partition nonnull counts at the table level;
        // NOT NULL columns are 0, the rest default to 0 (neutral — avoids the
        // all-null trap). Per-column nullfrac stays on the child stats.
        let stanullfrac = 0.0f32;
        let stawidth = stawidth_for_attlen(attr.attlen);
        let histogram =
            parent_histogram_bounds(attr.atttypid, mms).map(|b| (attr.atttypid, b));

        upsert_pg_statistic_row(
            client,
            parent_oid,
            attr.attnum,
            stadistinct,
            stanullfrac,
            stawidth,
            histogram,
            true,
        )?;
    }

    update_reltuples(client, parent_oid, total_rows, avg_tuple_width as i32)?;
    invalidate_relcache(parent_oid);
    Ok(())
}

/// Minimal SQL identifier quoting for building a `schema.table` regclass
/// literal. Doubles embedded quotes; always wraps in double quotes.
fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stawidth_for_attlen_uses_fixed_width_directly() {
        // Positive attlen → bytes-per-row for a fixed-width type. Negative
        // (varlena, cstring) → conservative 32-byte default per the comment.
        assert_eq!(stawidth_for_attlen(1), 1);
        assert_eq!(stawidth_for_attlen(8), 8);
        assert_eq!(stawidth_for_attlen(16), 16);
        assert_eq!(stawidth_for_attlen(-1), 32);
        assert_eq!(stawidth_for_attlen(-2), 32);
        assert_eq!(stawidth_for_attlen(0), 32);
    }

    #[test]
    fn partition_hll_serialize_merge_roundtrip_unions_distinct() {
        // Two partitions with disjoint value sets; the merged estimate should
        // approximate the union cardinality (~2000), not either half.
        let mut a = CardinalityEstimator::<u64>::new();
        for v in 0u64..1000 {
            a.insert(&v);
        }
        let mut b = CardinalityEstimator::<u64>::new();
        for v in 1000u64..2000 {
            b.insert(&v);
        }
        let ja = crate::compress::serialize_partition_hll(&["k"], std::slice::from_ref(&a)).unwrap();
        let jb = crate::compress::serialize_partition_hll(&["k"], std::slice::from_ref(&b)).unwrap();

        let mut acc: HashMap<String, CardinalityEstimator<u64>> = HashMap::new();
        merge_partition_hll_json(&ja, &mut acc);
        merge_partition_hll_json(&jb, &mut acc);

        let est = acc.get("k").unwrap().estimate() as i64;
        assert!((1800..=2200).contains(&est), "union estimate off: {est}");
    }

    #[test]
    fn merge_partition_hll_json_ignores_garbage() {
        let mut acc: HashMap<String, CardinalityEstimator<u64>> = HashMap::new();
        merge_partition_hll_json("not json", &mut acc);
        merge_partition_hll_json("[1,2,3]", &mut acc); // not an object
        assert!(acc.is_empty());
    }

    #[test]
    fn merge_ndistinct_sums_disjoint_ranges() {
        // Three partitions with non-overlapping value ranges → additive.
        let nds = [100, 100, 100];
        let ranges = [(0, 99), (100, 199), (200, 299)];
        assert_eq!(merge_ndistinct(&nds, &ranges, 10_000), 300);
    }

    #[test]
    fn merge_ndistinct_maxes_overlapping_ranges() {
        // Same low-cardinality values in every partition (identical range) →
        // the distinct count doesn't grow; take the max, not the sum.
        let nds = [9, 8, 9];
        let ranges = [(0, 8), (0, 8), (0, 8)];
        assert_eq!(merge_ndistinct(&nds, &ranges, 10_000), 9);
    }

    #[test]
    fn merge_ndistinct_caps_at_row_count() {
        let nds = [800, 800];
        let ranges = [(0, 799), (800, 1599)];
        assert_eq!(merge_ndistinct(&nds, &ranges, 1000), 1000);
    }

    #[test]
    fn parent_histogram_bounds_builds_sorted_multibucket() {
        // Disjoint, out-of-order partition ranges → sorted mins + global max.
        let ranges = [(200, 299), (0, 99), (100, 199)];
        let b = parent_histogram_bounds(pg_sys::INT4OID, &ranges).unwrap();
        assert_eq!(b, vec![0, 100, 200, 299]);
    }

    #[test]
    fn parent_histogram_bounds_collapses_constant_range_to_two_points() {
        let ranges = [(0, 27), (0, 27), (0, 27)];
        let b = parent_histogram_bounds(pg_sys::INT4OID, &ranges).unwrap();
        assert_eq!(b, vec![0, 27]);
    }

    #[test]
    fn parent_histogram_bounds_none_for_ineligible_or_empty() {
        assert!(parent_histogram_bounds(pg_sys::TEXTOID, &[(0, 9)]).is_none());
        assert!(parent_histogram_bounds(pg_sys::INT4OID, &[]).is_none());
        // All identical single value → fewer than 2 distinct bounds.
        assert!(parent_histogram_bounds(pg_sys::INT4OID, &[(5, 5), (5, 5)]).is_none());
    }

    #[test]
    fn histogram_eligible_accepts_ordered_int_and_time_types() {
        assert!(histogram_eligible(pg_sys::INT2OID, 1, 2));
        assert!(histogram_eligible(pg_sys::INT4OID, 100, 9000));
        assert!(histogram_eligible(pg_sys::INT8OID, -5, 5));
        assert!(histogram_eligible(pg_sys::TIMESTAMPOID, 1_000_000, 2_000_000));
        assert!(histogram_eligible(pg_sys::TIMESTAMPTZOID, 1_000_000, 2_000_000));
    }

    #[test]
    fn histogram_eligible_date_compares_at_day_granularity() {
        let day = 86_400_000_000i64;
        // Spans two distinct days → eligible.
        assert!(histogram_eligible(pg_sys::DATEOID, day, 3 * day));
        // Same day (µs apart) → not eligible (would be a constant histogram).
        assert!(!histogram_eligible(pg_sys::DATEOID, 1, 100));
    }

    #[test]
    fn histogram_eligible_rejects_constant_and_unsupported() {
        // Constant column (lo == hi) and inverted bounds → skip.
        assert!(!histogram_eligible(pg_sys::INT4OID, 7, 7));
        assert!(!histogram_eligible(pg_sys::INT4OID, 9, 1));
        // Types we don't emit histograms for (text, float for now) → skip.
        assert!(!histogram_eligible(pg_sys::TEXTOID, 0, 10));
        assert!(!histogram_eligible(pg_sys::FLOAT4OID, 0, 10));
    }

    #[test]
    fn stadistinct_value_returns_zero_for_unknown_inputs() {
        assert_eq!(stadistinct_value(0, 100), 0.0);
        assert_eq!(stadistinct_value(-1, 100), 0.0);
        assert_eq!(stadistinct_value(50, 0), 0.0);
        assert_eq!(stadistinct_value(50, -10), 0.0);
    }

    #[test]
    fn stadistinct_value_emits_absolute_count_when_ndistinct_is_small() {
        // PG convention: positive stadistinct is an absolute count of
        // distinct values, used when ndistinct/row_count ≤ 0.1 — the table
        // is "wide enough" that the count is meaningful as the table grows.
        assert_eq!(stadistinct_value(10, 1000), 10.0);
        assert_eq!(stadistinct_value(99, 1000), 99.0);
    }

    #[test]
    fn stadistinct_value_flips_to_fraction_at_density_threshold() {
        // PG convention: when ndistinct/row_count > 0.1, store the
        // *negated fraction* so the estimator scales correctly as the
        // partition gains/loses rows without a re-ANALYZE.
        let v = stadistinct_value(500, 1000);
        assert!((v - (-0.5)).abs() < 1e-6, "got {}", v);

        let v2 = stadistinct_value(900, 1000);
        assert!((v2 - (-0.9)).abs() < 1e-6, "got {}", v2);

        // Just past the 0.1 boundary → still negative fraction form.
        let v3 = stadistinct_value(101, 1000);
        assert!(
            v3 < 0.0,
            "boundary should flip to fraction form, got {}",
            v3
        );
    }
}
