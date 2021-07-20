#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|ops: Vec<betree_fuzz::KvOp>| {
    betree_fuzz::run_kv_ops(&ops);
});
