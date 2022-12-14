#! /bin/env bash

set -e

# execute separately to avoid swallowing return code
cargo build --tests

if [ -e scripts/prepare-test.sh ]
then
    ./scripts/prepare-test.sh
fi

failed=$(cargo test -- --test-threads "${HAURA_NUM_THREAD}" -Z unstable-options --format json \
    | grep name \
    | grep failed \
    | jq '"Test: " + .name + "\n-----LOG-----\n" + .stdout + "---END LOG---\n" ' \
      )
echo "FAILED TESTS" > fail.log
echo "############" > fail.log
for tst in "${failed[@]}"
do
    printf '%b' "$(echo "$tst" | sed -e 's/\"//g')" > fail.log
done

if [ -e scripts/cleanup-test.sh ]
then
    ./scripts/cleanup-test.sh
fi

if [ -z "$failed" ]
then
    exit 0
else
    exit 1
fi
