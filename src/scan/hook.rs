use pgrx::pg_sys;
use pgrx::pg_guard;
use std::collections::HashMap;
use std::ffi::c_int;
use std::sync::atomic::Ordering;

use super::PREV_HOOK;
use super::PREV_UPPER_HOOK;
use super::PREV_EXECUTOR_START_HOOK;
use super::path;
use super::cost;

thread_local! {
    /// Cache of partition OID → companion table OID (or InvalidOid if not compressed).
    static COMPRESSED_CACHE: std::cell::RefCell<HashMap<pg_sys::Oid, pg_sys::Oid>> =
        std::cell::RefCell::new(HashMap::new());

    /// When true, the ExecutorStart hook skips the DML-on-compressed check.
    /// Used by internal operations like cocoon_decompress_partition.
    static DML_BYPASS: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

pub fn invalidate_compressed_cache() {
    COMPRESSED_CACHE.with(|cache| cache.borrow_mut().clear());
}

/// Set or clear the DML bypass flag for internal operations.
pub(crate) fn set_dml_bypass(bypass: bool) {
    DML_BYPASS.with(|flag| flag.set(bypass));
}

/// The planner hook. Called for each relation during path generation.
#[pg_guard]
pub unsafe extern "C-unwind" fn cocoon_set_rel_pathlist(
    root: *mut pg_sys::PlannerInfo,
    rel: *mut pg_sys::RelOptInfo,
    rti: pg_sys::Index,
    rte: *mut pg_sys::RangeTblEntry,
) {
    unsafe {
        // Chain the previous hook first
        let prev = PREV_HOOK.load(Ordering::SeqCst);
        if !prev.is_null() {
            let prev_fn: pg_sys::set_rel_pathlist_hook_type = Some(std::mem::transmute::<*mut (), unsafe extern "C-unwind" fn(*mut pg_sys::PlannerInfo, *mut pg_sys::RelOptInfo, u32, *mut pg_sys::RangeTblEntry)>(prev));
            if let Some(f) = prev_fn {
                f(root, rel, rti, rte);
            }
        }

        // Only handle regular tables
        if (*rte).rtekind != pg_sys::RTEKind::RTE_RELATION {
            return;
        }

        // Check if this is the parent of a partitioned table (for CocoonAppend)
        if (*rel).reloptkind == pg_sys::RelOptKind::RELOPT_BASEREL && (*rte).inh {
            if let Some(companion_oids) = collect_compressed_children(root, rti) {
                path::add_cocoon_append_path(root, rel, &companion_oids);
                return;
            }
        }

        // Only process base relations and child member relations (partitions)
        if (*rel).reloptkind != pg_sys::RelOptKind::RELOPT_BASEREL
            && (*rel).reloptkind != pg_sys::RelOptKind::RELOPT_OTHER_MEMBER_REL
        {
            return;
        }

        let rel_oid = (*rte).relid;

        // Check if this relation is a compressed partition
        let companion_oid = COMPRESSED_CACHE.with(|cache| {
            let mut cache = cache.borrow_mut();
            if let Some(&oid) = cache.get(&rel_oid) {
                return oid;
            }

            let oid = check_compressed_partition(rel_oid);
            cache.insert(rel_oid, oid);
            oid
        });

        if companion_oid == pg_sys::InvalidOid {
            return;
        }

        // Add the custom decompress path
        path::add_decompress_path(root, rel, companion_oid);
    }
}

/// The create_upper_paths hook. Detects simple aggregate patterns over cocoon
/// scans and injects optimized custom paths:
/// - COUNT(*) → CocoonCount (sum of segment row_counts)
/// - MIN/MAX(time_col) → CocoonMinMax (global min/max from segment metadata)
#[pg_guard]
pub unsafe extern "C-unwind" fn cocoon_create_upper_paths(
    root: *mut pg_sys::PlannerInfo,
    stage: pg_sys::UpperRelationKind::Type,
    input_rel: *mut pg_sys::RelOptInfo,
    output_rel: *mut pg_sys::RelOptInfo,
    extra: *mut std::ffi::c_void,
) {
    unsafe {
        // Chain the previous hook first
        let prev = PREV_UPPER_HOOK.load(Ordering::SeqCst);
        if !prev.is_null() {
            type UpperHookFn = unsafe extern "C-unwind" fn(
                *mut pg_sys::PlannerInfo,
                pg_sys::UpperRelationKind::Type,
                *mut pg_sys::RelOptInfo,
                *mut pg_sys::RelOptInfo,
                *mut std::ffi::c_void,
            );
            let prev_fn: UpperHookFn = std::mem::transmute(prev);
            prev_fn(root, stage, input_rel, output_rel, extra);
        }

        // Only handle GROUP_AGG stage
        if stage != pg_sys::UpperRelationKind::UPPERREL_GROUP_AGG {
            return;
        }

        let parse = (*root).parse;

        // No GROUP BY
        if !(*parse).groupClause.is_null() {
            return;
        }

        // No HAVING
        if !(*parse).havingQual.is_null() {
            return;
        }

        // No WHERE clause (check parse tree jointree quals)
        let jointree = (*parse).jointree;
        if !jointree.is_null() && !(*jointree).quals.is_null() {
            return;
        }

        // Check target list: all non-junk entries must be aggregates
        let tlist = (*parse).targetList;
        if tlist.is_null() {
            return;
        }

        let nentries = (*tlist).length;
        let mut aggrefs: Vec<*const pg_sys::Aggref> = Vec::new();

        for i in 0..nentries {
            let te = pg_sys::list_nth(tlist, i) as *const pg_sys::TargetEntry;
            if te.is_null() {
                continue;
            }
            if (*te).resjunk {
                continue;
            }

            let expr = (*te).expr as *const pg_sys::Node;
            if expr.is_null() {
                return;
            }
            if (*expr).type_ != pg_sys::NodeTag::T_Aggref {
                return;
            }

            aggrefs.push(expr as *const pg_sys::Aggref);
        }

        if aggrefs.is_empty() {
            return;
        }

        // Extract companion OIDs from the cheapest input path.
        // Handles CocoonDecompress/CocoonAppend CustomPaths directly,
        // and also AppendPaths whose subpaths are CocoonDecompress.
        let cheapest = (*input_rel).cheapest_total_path;
        if cheapest.is_null() {
            return;
        }

        let companion_oids = match extract_companion_oids(root, cheapest) {
            Some(oids) if !oids.is_empty() => oids,
            _ => return,
        };

        // Single aggstar (COUNT(*)) pushdown
        if aggrefs.len() == 1 && (*aggrefs[0]).aggstar {
            path::add_count_star_path(root, output_rel, &companion_oids);
            return;
        }

        // Collect MIN/MAX aggregate specifications
        let mut agg_specs: Vec<path::MinMaxAggSpec> = Vec::new();

        for &aggref in &aggrefs {
            // No COUNT(*) mixed with MIN/MAX
            if (*aggref).aggstar {
                return;
            }

            // FILTER clause not supported
            if !(*aggref).aggfilter.is_null() {
                return;
            }

            // Get function name (min or max)
            let func_name_ptr = pg_sys::get_func_name((*aggref).aggfnoid);
            if func_name_ptr.is_null() {
                return;
            }
            let func_name = std::ffi::CStr::from_ptr(func_name_ptr)
                .to_str()
                .unwrap_or("");
            let is_min = match func_name {
                "min" => true,
                "max" => false,
                _ => return,
            };

            // Must have exactly one argument
            let args = (*aggref).args;
            if args.is_null() || (*args).length != 1 {
                return;
            }

            // Extract the Var from the single argument's TargetEntry
            let arg_te = pg_sys::list_nth(args, 0) as *const pg_sys::TargetEntry;
            if arg_te.is_null() {
                return;
            }
            let arg_expr = (*arg_te).expr as *const pg_sys::Node;
            if arg_expr.is_null() || (*arg_expr).type_ != pg_sys::NodeTag::T_Var {
                return; // Only push down for plain column references
            }
            let var_node = arg_expr as *const pg_sys::Var;
            let varno = (*var_node).varno as usize;
            let varattno = (*var_node).varattno;

            // Get column name from the range table entry
            if varno == 0 || varno >= (*root).simple_rel_array_size as usize {
                return;
            }
            let rte = *(*root).simple_rte_array.add(varno);
            if rte.is_null() {
                return;
            }
            let relid = (*rte).relid;
            let col_name_ptr = pg_sys::get_attname(relid, varattno, true);
            if col_name_ptr.is_null() {
                return;
            }

            // Verify the companion table has _min_{colname}
            let col_name = std::ffi::CStr::from_ptr(col_name_ptr)
                .to_string_lossy()
                .into_owned();
            let min_col_cname = std::ffi::CString::new(format!("_min_{}", col_name)).unwrap();
            let attnum = pg_sys::get_attnum(companion_oids[0], min_col_cname.as_ptr());
            if attnum == pg_sys::InvalidAttrNumber as i16 {
                return; // Column doesn't have segment min/max metadata
            }

            // Get type info for the result
            let result_type_oid = (*aggref).aggtype;
            let mut typlen: i16 = 0;
            let mut typbyval: bool = false;
            pg_sys::get_typlenbyval(result_type_oid, &mut typlen, &mut typbyval);

            agg_specs.push(path::MinMaxAggSpec {
                is_min,
                varattno,
                result_type_oid,
                typlen,
                typbyval,
            });
        }

        if agg_specs.is_empty() {
            return;
        }

        path::add_minmax_path(
            root,
            output_rel,
            &companion_oids,
            &agg_specs,
        );
    }
}

/// Extract companion OIDs from a planner path for COUNT(*) pushdown.
///
/// Handles:
/// - CocoonDecompress/CocoonAppend CustomPath: extract OIDs from custom_private
/// - AppendPath: walk subpaths, extract OIDs from CocoonDecompress CustomPaths
///
/// Returns None if the path doesn't contain cocoon scan nodes, or if there
/// are non-cocoon subpaths with actual data (uncompressed partitions).
unsafe fn extract_companion_oids(
    root: *mut pg_sys::PlannerInfo,
    path: *const pg_sys::Path,
) -> Option<Vec<pg_sys::Oid>> {
    unsafe {
        if (*path).type_ == pg_sys::NodeTag::T_CustomPath {
            extract_oids_from_custom_path(path as *const pg_sys::CustomPath)
        } else if (*path).type_ == pg_sys::NodeTag::T_AppendPath {
            let append_path = path as *const pg_sys::AppendPath;
            let subpaths = (*append_path).subpaths;
            if subpaths.is_null() {
                return None;
            }
            let num_subpaths = (*subpaths).length;
            let mut oids = Vec::new();
            for i in 0..num_subpaths {
                let subpath = pg_sys::list_nth(subpaths, i) as *const pg_sys::Path;
                if subpath.is_null() {
                    continue;
                }
                if (*subpath).type_ == pg_sys::NodeTag::T_CustomPath {
                    let cpath = subpath as *const pg_sys::CustomPath;
                    if let Some(sub_oids) = extract_oids_from_custom_path(cpath) {
                        oids.extend(sub_oids);
                    } else if subpath_has_data(root, subpath) {
                        return None;
                    }
                } else if subpath_has_data(root, subpath) {
                    // Non-cocoon subpath with actual data — can't push down
                    return None;
                }
                // Empty partition (relpages=0) — safe to skip
            }
            if oids.is_empty() { None } else { Some(oids) }
        } else {
            None
        }
    }
}

/// Check if a subpath's underlying table has actual data on disk.
///
/// Looks up the raw `relpages` from `pg_class` via syscache, bypassing PG's
/// inflated estimates in `RelOptInfo.pages` (which PG sets to 10 for
/// never-analyzed tables even when physically empty).
unsafe fn subpath_has_data(
    root: *mut pg_sys::PlannerInfo,
    subpath: *const pg_sys::Path,
) -> bool {
    unsafe {
        let parent = (*subpath).parent;
        if parent.is_null() {
            return false;
        }
        // RelOptInfo.relid is the range table index (RTI)
        let rti = (*parent).relid;
        if rti == 0 {
            return false;
        }
        let rte = *(*root).simple_rte_array.add(rti as usize);
        if rte.is_null() {
            return false;
        }
        let rel_oid = (*rte).relid;
        // Check raw relpages from pg_class (0 for truly empty/truncated tables)
        cost::get_relpages(rel_oid) > 0
    }
}

/// Extract companion OIDs from a CocoonDecompress or CocoonAppend CustomPath.
unsafe fn extract_oids_from_custom_path(
    cpath: *const pg_sys::CustomPath,
) -> Option<Vec<pg_sys::Oid>> {
    unsafe {
        let methods = (*cpath).methods;
        if methods.is_null() {
            return None;
        }
        let name = std::ffi::CStr::from_ptr((*methods).CustomName);
        if name != super::COCOON_APPEND_NAME && name != super::CUSTOM_NAME {
            return None;
        }
        let private_list = (*cpath).custom_private;
        if private_list.is_null() {
            return None;
        }
        let num_oids = (*private_list).length;
        let mut oids = Vec::new();
        for i in 0..num_oids {
            oids.push(pg_sys::list_nth_oid(private_list, i));
        }
        if oids.is_empty() { None } else { Some(oids) }
    }
}

/// Collect companion OIDs for all compressed children of a partitioned parent.
///
/// Iterates `root->append_rel_list` for children of `parent_rti`.
/// - If a child has a compressed companion, adds its OID to the list.
/// - If a child has no companion AND has uncompressed rows (reltuples > 0),
///   returns None (cannot use CocoonAppend).
/// - Empty partitions (reltuples <= 0) are safely skipped.
///
/// Returns `Some(companion_oids)` if we found at least one compressed child
/// and no uncompressed data; `None` otherwise.
unsafe fn collect_compressed_children(
    root: *mut pg_sys::PlannerInfo,
    parent_rti: pg_sys::Index,
) -> Option<Vec<pg_sys::Oid>> {
    unsafe {
        let list = (*root).append_rel_list;
        if list.is_null() {
            return None;
        }

        let len = (*list).length;
        let mut companion_oids: Vec<pg_sys::Oid> = Vec::new();

        for i in 0..len {
            let node = pg_sys::list_nth(list, i) as *const pg_sys::AppendRelInfo;
            if node.is_null() {
                continue;
            }

            if (*node).parent_relid != parent_rti {
                continue;
            }

            let child_rti = (*node).child_relid;
            let child_rte = *(*root).simple_rte_array.add(child_rti as usize);
            let child_oid = (*child_rte).relid;

            // Check if this child has a compressed companion
            let companion_oid = COMPRESSED_CACHE.with(|cache| {
                let mut cache = cache.borrow_mut();
                if let Some(&oid) = cache.get(&child_oid) {
                    return oid;
                }
                let oid = check_compressed_partition(child_oid);
                cache.insert(child_oid, oid);
                oid
            });

            if companion_oid != pg_sys::InvalidOid {
                companion_oids.push(companion_oid);
            } else {
                // Not compressed — check if partition has data
                let reltuples = cost::get_reltuples(child_oid);
                if reltuples > 0.0 {
                    // Uncompressed partition with data — cannot use CocoonAppend
                    return None;
                }
                // Empty partition, safe to skip
            }
        }

        if companion_oids.is_empty() {
            None
        } else {
            Some(companion_oids)
        }
    }
}

/// Check if a relation OID corresponds to a compressed partition
/// by looking for a companion table in _cocoon_compressed schema.
pub(crate) unsafe fn check_compressed_partition(rel_oid: pg_sys::Oid) -> pg_sys::Oid {
    unsafe {
        // Get the relation name
        let name_ptr = pg_sys::get_rel_name(rel_oid);
        if name_ptr.is_null() {
            return pg_sys::InvalidOid;
        }
        let rel_name = std::ffi::CStr::from_ptr(name_ptr)
            .to_string_lossy()
            .into_owned();

        // Look up _cocoon_compressed schema OID
        let schema_cstr = c"_cocoon_compressed";
        let compressed_ns_oid = pg_sys::get_namespace_oid(schema_cstr.as_ptr(), true);
        if compressed_ns_oid == pg_sys::InvalidOid {
            return pg_sys::InvalidOid;
        }

        // Skip tables already in the _cocoon_compressed schema to avoid recursion
        let rel_ns_oid = pg_sys::get_rel_namespace(rel_oid);
        if rel_ns_oid == compressed_ns_oid {
            return pg_sys::InvalidOid;
        }

        // Check if _cocoon_compressed.<rel_name> exists
        let companion_cname = std::ffi::CString::new(rel_name).unwrap();
        pg_sys::get_relname_relid(companion_cname.as_ptr(), compressed_ns_oid)
    }
}

/// ExecutorStart hook: block DML on compressed partitions.
///
/// INSERT/UPDATE/DELETE on a compressed partition would silently produce
/// incorrect results (writes go to the truncated heap, reads come from the
/// companion table). This hook raises an error before execution begins.
#[pg_guard]
pub unsafe extern "C-unwind" fn cocoon_executor_start(
    query_desc: *mut pg_sys::QueryDesc,
    eflags: c_int,
) {
    unsafe {
        let operation = (*query_desc).operation;

        // Only check DML commands
        if operation != pg_sys::CmdType::CMD_INSERT
            && operation != pg_sys::CmdType::CMD_UPDATE
            && operation != pg_sys::CmdType::CMD_DELETE
        {
            call_prev_executor_start(query_desc, eflags);
            return;
        }

        // Skip check when internal operations (e.g. decompress) set the bypass flag
        if DML_BYPASS.with(|flag| flag.get()) {
            call_prev_executor_start(query_desc, eflags);
            return;
        }

        let planned_stmt = (*query_desc).plannedstmt;
        if planned_stmt.is_null() {
            call_prev_executor_start(query_desc, eflags);
            return;
        }

        let result_relations = (*planned_stmt).resultRelations;
        if !result_relations.is_null() {
            let rtable = (*planned_stmt).rtable;
            let n = (*result_relations).length;

            for i in 0..n {
                // resultRelations is an IntList of 1-based RTE indices
                let rti = (*(*result_relations).elements.add(i as usize)).int_value;
                if rti <= 0 || rtable.is_null() {
                    continue;
                }

                // Get the RTE at this index (0-based in the list)
                let rte = pg_sys::list_nth(rtable, rti - 1) as *const pg_sys::RangeTblEntry;
                if rte.is_null() {
                    continue;
                }
                let relid = (*rte).relid;

                let companion_oid = COMPRESSED_CACHE.with(|cache| {
                    let mut cache = cache.borrow_mut();
                    if let Some(&oid) = cache.get(&relid) {
                        return oid;
                    }
                    let oid = check_compressed_partition(relid);
                    cache.insert(relid, oid);
                    oid
                });

                if companion_oid != pg_sys::InvalidOid {
                    let op_name = match operation {
                        pg_sys::CmdType::CMD_INSERT => "INSERT into",
                        pg_sys::CmdType::CMD_UPDATE => "UPDATE",
                        pg_sys::CmdType::CMD_DELETE => "DELETE from",
                        _ => "modify",
                    };
                    let rel_name_ptr = pg_sys::get_rel_name(relid);
                    let rel_name = if rel_name_ptr.is_null() {
                        format!("OID {}", relid)
                    } else {
                        std::ffi::CStr::from_ptr(rel_name_ptr)
                            .to_string_lossy()
                            .into_owned()
                    };
                    pgrx::error!(
                        "cannot {} compressed partition \"{}\", decompress it first",
                        op_name,
                        rel_name,
                    );
                }
            }
        }

        call_prev_executor_start(query_desc, eflags);
    }
}

/// Chain to the previous ExecutorStart hook or call standard_ExecutorStart.
unsafe fn call_prev_executor_start(query_desc: *mut pg_sys::QueryDesc, eflags: c_int) {
    unsafe {
        let prev = PREV_EXECUTOR_START_HOOK.load(Ordering::SeqCst);
        if !prev.is_null() {
            let prev_fn: unsafe extern "C-unwind" fn(*mut pg_sys::QueryDesc, c_int) =
                std::mem::transmute(prev);
            prev_fn(query_desc, eflags);
        } else {
            pg_sys::standard_ExecutorStart(query_desc, eflags);
        }
    }
}
