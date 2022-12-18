#! /bin/env bash

set -e

if [ -e scripts/prepare-test.sh ]
then
    ./scripts/prepare-test.sh
fi

cargo test -- --test-threads "${HAURA_NUM_THREAD}"

if [ -e scripts/cleanup-test.sh ]
then
    ./scripts/cleanup-test.sh
fi
