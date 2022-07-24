#! /bin/env bash

set -e

./prepare-test.sh

# num_thread=$(echo "$(cat /proc/meminfo | head -n 1 | xargs | cut -d ' ' -f 2) / 1024 / 1024 / 2" | bc)
failed=$(cargo test -- --test-threads 1 -Z unstable-options --format json \
    | grep name \
    | grep failed \
    | jq '"Test: " + .name + "\n-----LOG-----\n" + .stdout + "---END LOG---\n" ' \
      )
echo "FAILED TESTS"
echo "############"
for tst in "${failed[@]}"
do
    printf '%b' "$(echo "$tst" | sed -e 's/\"//g')"
done
if [ -z "$failed" ]
then
        exit 0
else
        exit 1
fi

./cleanup-test.sh
