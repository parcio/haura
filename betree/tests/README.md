# betree tests

This directory contains integration tests for the `betree` crate.
Execution of all test cases might take up extensive resources on your system, it is advised to have swap at hand to avoid out of memory errors.

The advised number of threads is relative to the available amount of memory with the following relation:

``` 
amount_of_memory_in_gb / 2 = number_of_threads
```

To run the tests with these restrictions (default is number of cpus):

``` sh
$ cargo test -- --test-threads=$threads
```

## Failing tests

To investigate failing tests logs can be used with `--nocapture` and `env_logger`

``` sh
RUST_LOG=trace RUST_BACKTRACE=1 cargo test $failing_test -- --nocapture
```

Or to investigate with `rust-lldb`, `lldb`, or `gdb`

``` sh
rust-lldb target/debug/deps/betree_tests-$hash
```

