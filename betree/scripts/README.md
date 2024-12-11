# Allocation Log Visualization

This script visualizes the allocation and deallocation of blocks within the key-value database. It helps to understand how storage space is being used and identify potential optimization opportunities.

The allocation log visualization script is tested with Python 3.12.7 and the packages listed in `requirements.txt`.

The main dependencies are matplotlib, tqdm and sortedcontainers.

## Setup

Run the following to create a working environment for the script:

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r scripts/requirements.txt
```

## Generating the Allocation Log

To generate the `allocation_log.bin` file, you need to enable the allocation_log feature flag when compiling the `betree` crate. For instance by running
```bash
cargo build --features allocation_log
```
or by enabling it in the `Cargo.toml`.

The path where the log is saved can be set with the runtime configuration parameter `allocation_log_file_path`. The default is `$PWD/allocation_log.bin`

## Using the Allocation Log

Once a log file has been obtained simply run the following to visualize the (de-)allocations recorded.
```bash
./scripts/visualize_allocation_log allocation_log.bin
```

To get help and see the options available run the script with the `-h` flag.

