#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

unsafe impl Sync for JBackend {}

pub const FALSE: gboolean = 0;
pub const TRUE: gboolean = !FALSE;
