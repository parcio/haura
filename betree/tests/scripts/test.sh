#!/bin/env bash

./scripts/prepare-test.sh

num_thread=$(echo "$(grep 'MemFree' /proc/meminfo | xargs | cut -d ' ' -f 2) / 1024 / 1024 / 5" | bc)

if [ "$num_thread" -gt "$(nproc)" ]
then
    num_thread=$(nproc)
fi

echo "Using ${num_thread} threads."

cargo test -- --test-threads "$num_thread"

./scripts/cleanup-test.sh
