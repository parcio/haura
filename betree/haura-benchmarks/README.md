# betree-perf

This directory contains some additional tools and benchmarks which can be helpful when assessing the performance 

- `src/bin/{json-flatten,json-merge,sysinfo-log}.rs`: Tooling to aggregate multiple newline-delimited JSON streams into one final file
- `src/lib.rs`: Shared setup between benchmarks
- `src/main.rs`: CLI to select and configure a benchmark, also spawns the sysinfo-log binary
- `src/{ingest, rewrite, switchover, tiered1, zip, scientific_evaluation, filesystem, filesystem_zip, checkpoints}.rs`: Individual benchmarks
- `run.sh`: Example usage, runs benchmarks with different configurations

## Configuration

All benchmark invocations can be seen in the `run.sh` script, which can be used
to create a custom benchmark run. Benchmarks are represented by their own
function you can uncomment at the bottom of the script.

If you have followed the general scripts to setup `bectl` and `haura` in the
[documentation](https://parcio.github.io/haura/) you are good to go. Otherwise,
provide a configuration for the benchmarks either by pointing to a valid
configuration in the `BETREE_CONFIG` environment variable or by creating a
`perf-config.json` in the `haura-benchmarks` directory. A collection of example
configurations can be found in the `example_config` directory.

``` sh
$ # EITHER
$ export BETREE_CONFIG=<path_to_config>
$ # OR
$ cp example_config/example-config.json perf-config.json
```

Be sure to modify the example config, if chosen, to your desired specification.


## Running the benchmark

If you have configured your benchmarks *and* chosen a configuration for Haura,
you can start the benchmark. If required for identification of multiple runs a
name can be given with each invocation which will be used in the stored results:

``` sh
$ ./run.sh my-benchmark-run
```

After each individual benchmark an idle period of 1 minute is done by default.
