#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|ops: Vec<betree_fuzz::ObjOp>| {
    // eprintln!("{:?}", ops);
    betree_fuzz::run_obj_ops(&ops);
});
