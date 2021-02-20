#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|ops: Vec<betree_fuzz::KvOp>| {
    // eprintln!("{:?}", ops);
    betree_fuzz::run_kv_ops(&ops);
});
