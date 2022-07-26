#!/bin/env bash

./scripts/prepare-test.sh

num_thread=$(echo "$(head -n 1 /proc/meminfo | xargs | cut -d ' ' -f 2) / 1024 / 1024 / 2" | bc)

if [ "$num_thread" -gt "$(nproc)" ]
then
    num_thread=$(nproc)
fi

cargo test -- --test-threads "$num_thread"

./scripts/cleanup-test.sh
