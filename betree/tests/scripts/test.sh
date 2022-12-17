#!/bin/env bash

./scripts/prepare-test.sh

num_thread=$(echo "$(grep 'MemAvailable' /proc/meminfo | xargs | cut -d ' ' -f 2) / 1024 / 1024 / 4" | bc)

if [ "$num_thread" -gt "$(nproc)" ]
then
    num_thread=$(nproc)
fi

if [ "$num_thread" = 0 ]
then
    echo "Could not determine enough memory to run tests."
    echo "Trying with single thread..."
    num_thread=1
fi

echo "Using ${num_thread} threads."

cargo test -- --test-threads "$num_thread"

./scripts/cleanup-test.sh
