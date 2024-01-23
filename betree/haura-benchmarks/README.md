# betree-perf

This repository contains tooling and benchmarks for the betree-storage-stack, which are not universally useful enough
to add into the main repository in their current state. They are focused only on object-/tiered storage, and consist of

- `src/bin/{json-flatten,json-merge,sysinfo-log}.rs`: Tooling to aggregate multiple newline-delimited JSON streams into one final file
- `src/lib.rs`: Shared setup between benchmarks
- `src/main.rs`: CLI to select and configure a benchmark, also spawns the sysinfo-log binary
- `src/{ingest, rewrite, switchover, tiered1, zip}.rs`: Individual benchmarks
- `run.sh`: Example usage, runs benchmarks with different configurations

The relative path to the betree-storage-stack library in `Cargo.toml` may need adjustment, if this repository is not cloned into the `/betree`
directory of the main repository.
