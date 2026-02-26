use pgrx::pg_sys;
use pgrx::pg_guard;

/// ExplainCustomScan callback: output info for EXPLAIN.
#[pg_guard]
pub unsafe extern "C-unwind" fn explain_custom_scan(
    _node: *mut pg_sys::CustomScanState,
    _ancestors: *mut pg_sys::List,
    es: *mut pg_sys::ExplainState,
) {
    unsafe {
        let label = c"Storage";
        let value = c"Compressed (CocoonDecompress)";
        pg_sys::ExplainPropertyText(label.as_ptr(), value.as_ptr(), es);
    }
}
