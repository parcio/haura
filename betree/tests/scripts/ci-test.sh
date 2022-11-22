#! /bin/env bash

set -e

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
if [ -z "$failed" ]
then
        exit 0
else
        exit 1
fi

./scripts/cleanup-test.sh
