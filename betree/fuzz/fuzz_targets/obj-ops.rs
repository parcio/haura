#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|ops: Vec<betree_fuzz::ObjOp>| {
    betree_fuzz::run_obj_ops(&ops);
});
