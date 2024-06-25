#!/bin/env bash

set -e

# This script contains a structured approach to run multiple fio runs with
# multiple parameters. It is intended to be modified to customize your benchmark
# runs.
export_options=(--group_reporting --output-format=json --output=output.json --write_bw_log=bench --write_lat_log=bench --write_hist_log=bench --write_iops_log=bench --log_hist_msec=100 --log_avg_msec=100 --directory=./.bench-fio-tmp-data)
root=$PWD

# Below are possible configuration options. Add elements to run multiple
# benchmarks.
modes=(write read randread)
ioengines=("external:${root}/src/fio-engine-haura.o")
blocksizes=(4k 4m)
jobs=(1 2 3 4 5 6 7 8)
size_gb=4
runtime=30s
extra_options=(--disrespect-fio-options)
id="results_ID"

mkdir "$id"
pushd "$id" || exit

for ioengine in "${ioengines[@]}"
do
    for blocksize in "${blocksizes[@]}"
    do
        for job in "${jobs[@]}"
        do
            for mode in "${modes[@]}"
            do
                name="${mode}_$(echo "$ioengine" | awk -F'/' '{print $NF}')_${blocksize}_${job}"
                mkdir "${name}"
                pushd "${name}" || exit
                size=$((size_gb * 1024 / job))
                mkdir .bench-fio-tmp-data
                "${root}/fio-fio-3.33/fio" "--name=${name}" "--readwrite=${mode}" "--ioengine=${ioengine}" "--blocksize=${blocksize}" "--numjobs=${job}" "--runtime=${runtime}" "--size=${size}M" "${export_options[@]}" "${extra_options[@]}"
                rm -rf .bench-fio-tmp-data
                popd || exit
            done
        done
    done
done

popd || exit
