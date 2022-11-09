#! /bin/env bash

set -e

./scripts/prepare-test.sh

num_thread=$(echo "$(grep 'MemFree' /proc/meminfo | xargs | cut -d ' ' -f 2) / 1024 / 1024 / 5" | bc)
failed=$(cargo test -- --test-threads "${num_thread}" -Z unstable-options --format json \
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
if [ -z "$failed" ]
then
        exit 0
else
        exit 1
fi

./scripts/cleanup-test.sh
