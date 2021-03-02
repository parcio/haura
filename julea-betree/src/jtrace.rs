#![allow(unused_imports)]

use julea_sys::{j_trace_file_begin, j_trace_file_end, JTraceFileOperation};

#[cfg(feature = "jtrace")]
pub unsafe fn with<T>(
    op: JTraceFileOperation,
    path: *const i8,
    mut f: impl FnMut() -> (T, (u64, u64)),
) -> (T, (u64, u64)) {
    j_trace_file_begin(path, op);
    let (val, (length, offset)) = f();
    j_trace_file_end(path, op, length, offset);
    (val, (length, offset))
}

#[cfg(not(feature = "jtrace"))]
#[inline(always)]
pub unsafe fn with<T>(
    _op: JTraceFileOperation,
    _path: *const i8,
    mut f: impl FnMut() -> (T, (u64, u64)),
) -> (T, (u64, u64)) {
    f()
}

#[cfg(feature = "jtrace")]
pub unsafe fn with_once<T>(
    op: JTraceFileOperation,
    path: &[u8],
    f: impl FnOnce() -> (T, (u64, u64)),
) -> (T, (u64, u64)) {
    j_trace_file_begin(path.as_ptr().cast::<i8>(), op);
    let (val, (length, offset)) = f();
    j_trace_file_end(path.as_ptr().cast::<i8>(), op, length, offset);
    (val, (length, offset))
}

#[cfg(not(feature = "jtrace"))]
#[inline(always)]
pub unsafe fn with_once<T>(
    _op: JTraceFileOperation,
    _path: &[u8],
    f: impl FnOnce() -> (T, (u64, u64)),
) -> (T, (u64, u64)) {
    f()
}
