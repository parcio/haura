#!/bin/env bash

set -e

tiers=("fastest" "fast")

for name in "${tiers[@]}"
do
    truncate -s 2G "test_disk_tier_${name}"
done
