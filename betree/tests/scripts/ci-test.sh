#! /bin/env bash

# num_thread=$(echo "$(cat /proc/meminfo | head -n 1 | xargs | cut -d ' ' -f 2) / 1024 / 1024 / 2" | bc)
failed=$(cargo test -- --test-threads 1 -Z unstable-options --format json | tee /dev/stderr | grep suite | grep failed | jq '.failed')
exit $failed
