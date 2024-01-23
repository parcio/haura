# betree-perf

This directory contains some additional tools and benchmarks which can be helpful when assessing the performance 

- `src/bin/{json-flatten,json-merge,sysinfo-log}.rs`: Tooling to aggregate multiple newline-delimited JSON streams into one final file
- `src/lib.rs`: Shared setup between benchmarks
- `src/main.rs`: CLI to select and configure a benchmark, also spawns the sysinfo-log binary
- `src/{ingest, rewrite, switchover, tiered1, zip, scientific_evaluation, filesystem, filesystem_zip, checkpoints}.rs`: Individual benchmarks
- `run.sh`: Example usage, runs benchmarks with different configurations
