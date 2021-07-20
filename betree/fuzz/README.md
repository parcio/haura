You'll need:

* a nightly Rust for fuzzing, to pass custom LLVM flags.
* cargo-fuzz, `cargo install cargo-fuzz`

Then you can run:

```
cargo fuzz run kv-ops -- -detect_leaks=0 -max_len=16384 -jobs=6 -workers=6
cargo fuzz run obj-ops -- -detect_leaks=0 -max_len=16384
```

After 4.5h with 8 workers, a thread limit of >4million threads was exceeded, the last inputs
were wrongly marked as crashes. Our storage thread pool is getting dropped though, and htop
(as well as grep Thread /proc/self/status) show a limited number of threads.
Apparently, asan remembers every thread ever created, and eventually gives up on that:
https://github.com/google/sanitizers/issues/273

To mitigate this for a while, the thread pool size is set to 1, but that only buys so much time.

Alternatively, we can disable asan by passing `-s none`, but... then we have no asan anymore.
