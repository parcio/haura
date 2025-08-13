#!/usr/bin/env bash
# shellcheck disable=SC2030,SC2031 # we exploit this characteristic to start several test scenarios - merging them would lead to pollution

function ensure_zip {
    local url
    url="https://cdn.kernel.org/pub/linux/kernel/v5.x/linux-5.15.58.tar.xz"

    if [ ! -e "$ZIP_ARCHIVE" ]; then
        mkdir data
        pushd data || exit

        curl "$url" -o linux.tar.xz
        tar xf linux.tar.xz
        rm linux.tar.xz
        zip -0 -r linux.zip linux-*
        rm -r linux-*

        popd || exit
    fi
}

function ensure_bectl {
    pushd ../../bectl || exit
    cargo build --release --features betree_storage_stack/nvm
    popd || return
}

function ensure_config {
    if [ ! -e "$BETREE_CONFIG" ]; then
        echo "No Haura configuration found at: ${BETREE_CONFIG}"
        exit 1
    fi
}

total_runs=0

function run {
    local vdev_type="$1"
    local name="$2"
    local mode="$3"
    shift 3

    if [ "$total_runs" -gt 0 ]; then
        sleep 15
    fi
    total_runs=$((total_runs + 1))

    local out_path
    out_path="results/$(date -I)_${vdev_type}/${name}_$(date +%s)"
    mkdir -p "$out_path"

    pushd "$out_path" || return

    echo "running $mode with these settings:"
    env | grep BETREE__
    env >"env"
    "$ROOT/../../target/release/bectl" config print-active >"config"
    numactl --cpunodebind=0 --membind=0 "$ROOT/target/release/betree-perf" "$mode" "$@"

    # Generate flamegraph while running the benchmark
    #flamegraph --output="flamegraph.svg" -- numactl --cpunodebind=0 --membind=0 "$ROOT/target/release/betree-perf" "$mode" "$@"
#--freq 70  
    #rm -f perf.data


    echo "merging results into $out_path/out.jsonl"
    "$ROOT/target/release/json-merge" \
        --timestamp-key epoch_ms \
        ./betree-metrics.jsonl \
        ./proc.jsonl \
        ./sysinfo.jsonl |
        "$ROOT/target/release/json-flatten" >"out.jsonl"

    popd || return
}

function tiered() {
    (
        export BETREE__ALLOC_STRATEGY='[[0],[0],[],[]]'
        run "$RUN_IDENT" tiered1_all0_alloc tiered1
    )

    (
        export BETREE__ALLOC_STRATEGY='[[0],[1],[],[]]'
        run "$RUN_IDENT" tiered1_id_alloc tiered1
    )

    (
        export BETREE__ALLOC_STRATEGY='[[1],[1],[],[]]'
        run "$RUN_IDENT" tiered1_all1_alloc tiered1
    )
}

function scientific_evaluation() {
    # Invocation: <Evaluation Length s> <Object Size in bytes> <Samples> <Min. Sample Size> <Max. Sample Size>
    run "$RUN_IDENT" random_evaluation_read evaluation-read 30 $((25 * 1024 * 1024 * 1024)) $((8192)) $((1 * 1024)) $((12 * 1024 * 1024))
}

function evaluation_rw() {
    # Invocation: <Evaluation Length s> <Object Size in bytes> <Samples> <Min. Sample Size> <Max. Sample Size>
    run "$RUN_IDENT" random_evaluation_rw evaluation-rw 30 $((25 * 1024 * 1024 * 1024)) $((8192)) $((1 * 1024)) $((12 * 1024 * 1024))
}

function filesystem_zip() {
    export BETREE__ALLOC_STRATEGY='[[0],[1],[2],[]]'
    run "$RUN_IDENT" file_system_three "$ZIP_ARCHIVE"
}

function checkpoints() {
    export BETREE__ALLOC_STRATEGY='[[0, 1],[1],[],[]]'
    run "$RUN_IDENT" checkpoints_fastest checkpoints
}

function filesystem() {
    export BETREE__ALLOC_STRATEGY='[[0],[1],[2],[]]'
    run "$RUN_IDENT" file_system_three filesystem
}

function zip_cache() {
    local F_CD_START=1040032667

    for cache_mib in 32 128 512 2048; do
        (
            export BETREE__CACHE_SIZE=$((cache_mib * 1024 * 1024))
            run "$RUN_IDENT" "zip_cache_$cache_mib" zip 4 100 10 "$ZIP_ARCHIVE" "$F_CD_START"
        )
    done
}

function zip_mt() {
    local F="$PWD/data/linux.zip"
    local F_CD_START=1

    for cache_mib in 256 512 1024 2048; do
        echo "using $cache_mib MiB of cache"
        (
            export BETREE__CACHE_SIZE=$((cache_mib * 1024 * 1024))

            local total=10000

            for num_workers in 1 2 3 4 5 6 7 8 9 10; do
                echo "running with $num_workers workers"
                local per_worker=$((total / num_workers))
                local per_run=$((per_worker / 10))

                run "$RUN_IDENT" "zip_mt_${cache_mib}_${num_workers}_${per_run}_10" zip "$num_workers" "$per_run" 10 "$F" "$F_CD_START"
            done
        )
    done
}

function zip_tiered() {
    local F_CD_START=1 #242415017 #1040032667
    # for cache_mib in 256 512 1024; do
    for cache_mib in 32 64; do
        echo "using $cache_mib MiB of cache"
        (
            export BETREE__CACHE_SIZE=$((cache_mib * 1024 * 1024))

            local total=10000

            for num_workers in 1 2 3 4 5 6 7 8; do
                echo "running with $num_workers workers"
                local per_worker=$((total / num_workers))
                local per_run=$((per_worker / 10))

                (
                    export BETREE__ALLOC_STRATEGY='[[0],[0],[],[]]'
                    run "$RUN_IDENT" "zip_tiered_all0_${cache_mib}_${num_workers}_${per_run}_10" zip "$num_workers" "$per_run" 10 "$ZIP_ARCHIVE" "$F_CD_START"
                )

                (
                    export BETREE__ALLOC_STRATEGY='[[0],[1],[],[]]'
                    run "$RUN_IDENT" "zip_tiered_id_${cache_mib}_${num_workers}_${per_run}_10" zip "$num_workers" "$per_run" 10 "$ZIP_ARCHIVE" "$F_CD_START"
                )

                (
                    export BETREE__ALLOC_STRATEGY='[[1],[1],[],[]]'
                    run "$RUN_IDENT" "zip_tiered_all1_${cache_mib}_${num_workers}_${per_run}_10" zip "$num_workers" "$per_run" 10 "$ZIP_ARCHIVE" "$F_CD_START"
                )

            done
        )
    done
}

function ingest() {
    (
        (
            export BETREE__COMPRESSION="None"
            run "$RUN_IDENT" ingest_hdd_none ingest "$ZIP_ARCHIVE"
        )

        for level in $(seq 1 16); do
            (
                export BETREE__COMPRESSION="{ Zstd = { level = $level } }"
                run "$RUN_IDENT" "ingest_hdd_zstd_$level" ingest "$ZIP_ARCHIVE"
            )
        done
    )
}

function switchover() {
    run "$RUN_IDENT" switchover_tiny switchover 32 "$((32 * 1024 * 1024))"
    run "$RUN_IDENT" switchover_small switchover 8 "$((128 * 1024 * 1024))"
    run "$RUN_IDENT" switchover_medium switchover 4 "$((2 * 1024 * 1024 * 1024))"
    run "$RUN_IDENT" switchover_large switchover 4 "$((8 * 1024 * 1024 * 1024))"
}

function ci() {
    run "$RUN_IDENT" switchover_small switchover 4 "$((128 * 1024 * 1024))"
}

function ycsb_a() {
    # Default parameters: use generated data with integers
    local data_source="${YCSB_DATA_SOURCE:-file}"
    local data_type="${YCSB_DATA_TYPE:-int}"
    local data_path="${YCSB_DATA_PATH:-/home/skarim/Code/smash/haura/betree/haura-benchmarks/silesia_corpus}"
    
    #run "$RUN_IDENT" "ycsb_a_${YCSB_SUFFIX:-unnamed}" ycsb-a "$((2 * 1024 * 1024 * 1024))" 0 2 --data-source "$data_source" --data-type "$data_type" --data-path "$data_path" --entry-size "$ENTRY_SIZE"
    run "$RUN_IDENT" "ycsb_a_${YCSB_SUFFIX:-unnamed}" ycsb-a "$((1 * 1024 * 1024))" 0 1 --data-source "$data_source" --data-type "$data_type" --data-path "$data_path" --entry-size "$ENTRY_SIZE"
    #run "$RUN_IDENT" "ycsb_a_${YCSB_SUFFIX:-unnamed}" ycsb-a "$((1 * 1024 * 1024))" 0 2 --data-source "$data_source" --data-type "$data_type" --data-path "$data_path" --entry-size "$ENTRY_SIZE"
    run "$RUN_IDENT" "ycsb_a_${YCSB_SUFFIX:-unnamed}" ycsb-a "$((1 * 1024 * 1024))" 0 3 --data-source "$data_source" --data-type "$data_type" --data-path "$data_path" --entry-size "$ENTRY_SIZE"
    #run "$RUN_IDENT" "ycsb_a_${YCSB_SUFFIX:-unnamed}" ycsb-a "$((1 * 1024 * 1024))" 0 4 --data-source "$data_source" --data-type "$data_type" --data-path "$data_path" --entry-size "$ENTRY_SIZE"
    run "$RUN_IDENT" "ycsb_a_${YCSB_SUFFIX:-unnamed}" ycsb-a "$((1 * 1024 * 1024))" 0 6 --data-source "$data_source" --data-type "$data_type" --data-path "$data_path" --entry-size "$ENTRY_SIZE"
    run "$RUN_IDENT" "ycsb_a_${YCSB_SUFFIX:-unnamed}" ycsb-a "$((1 * 1024 * 1024))" 0 10 --data-source "$data_source" --data-type "$data_type" --data-path "$data_path" --entry-size "$ENTRY_SIZE"
    run "$RUN_IDENT" "ycsb_a_${YCSB_SUFFIX:-unnamed}" ycsb-a "$((1 * 1024 * 1024))" 0 15 --data-source "$data_source" --data-type "$data_type" --data-path "$data_path" --entry-size "$ENTRY_SIZE"
    run "$RUN_IDENT" "ycsb_a_${YCSB_SUFFIX:-unnamed}" ycsb-a "$((1 * 1024 * 1024))" 0 20 --data-source "$data_source" --data-type "$data_type" --data-path "$data_path" --entry-size "$ENTRY_SIZE"
    run "$RUN_IDENT" "ycsb_a_${YCSB_SUFFIX:-unnamed}" ycsb-a "$((1 * 1024 * 1024))" 0 25 --data-source "$data_source" --data-type "$data_type" --data-path "$data_path" --entry-size "$ENTRY_SIZE"
}

function ycsb_b() {
    # Default parameters: use generated data with integers
    local data_source="${YCSB_DATA_SOURCE:-file}"
    local data_type="${YCSB_DATA_TYPE:-int}"
    local data_path="${YCSB_DATA_PATH:-/home/skarim/Code/smash/haura/betree/haura-benchmarks/silesia_corpus}"
    
    #run "$RUN_IDENT" "ycsb_b_${YCSB_SUFFIX:-unnamed}" ycsb-b "$((2 * 1024 * 1024 * 1024))" 0 2 --data-source "$data_source" --data-type "$data_type" --data-path "$data_path" --entry-size "$ENTRY_SIZE"
    run "$RUN_IDENT" "ycsb_b_${YCSB_SUFFIX:-unnamed}" ycsb-b "$((1 * 1024 * 1024))" 0 1 --data-source "$data_source" --data-type "$data_type" --data-path "$data_path" --entry-size "$ENTRY_SIZE"
    #run "$RUN_IDENT" "ycsb_b_${YCSB_SUFFIX:-unnamed}" ycsb-b "$((1 * 1024 * 1024))" 0 2 --data-source "$data_source" --data-type "$data_type" --data-path "$data_path" --entry-size "$ENTRY_SIZE"
    run "$RUN_IDENT" "ycsb_b_${YCSB_SUFFIX:-unnamed}" ycsb-b "$((1 * 1024 * 1024))" 0 3 --data-source "$data_source" --data-type "$data_type" --data-path "$data_path" --entry-size "$ENTRY_SIZE"
    #run "$RUN_IDENT" "ycsb_b_${YCSB_SUFFIX:-unnamed}" ycsb-b "$((1 * 1024 * 1024))" 0 4 --data-source "$data_source" --data-type "$data_type" --data-path "$data_path" --entry-size "$ENTRY_SIZE"
    run "$RUN_IDENT" "ycsb_b_${YCSB_SUFFIX:-unnamed}" ycsb-b "$((1 * 1024 * 1024))" 0 6 --data-source "$data_source" --data-type "$data_type" --data-path "$data_path" --entry-size "$ENTRY_SIZE"
    run "$RUN_IDENT" "ycsb_b_${YCSB_SUFFIX:-unnamed}" ycsb-b "$((1 * 1024 * 1024))" 0 10 --data-source "$data_source" --data-type "$data_type" --data-path "$data_path" --entry-size "$ENTRY_SIZE"
    run "$RUN_IDENT" "ycsb_b_${YCSB_SUFFIX:-unnamed}" ycsb-b "$((1 * 1024 * 1024))" 0 15 --data-source "$data_source" --data-type "$data_type" --data-path "$data_path" --entry-size "$ENTRY_SIZE"
    run "$RUN_IDENT" "ycsb_b_${YCSB_SUFFIX:-unnamed}" ycsb-b "$((1 * 1024 * 1024))" 0 20 --data-source "$data_source" --data-type "$data_type" --data-path "$data_path" --entry-size "$ENTRY_SIZE"
    run "$RUN_IDENT" "ycsb_b_${YCSB_SUFFIX:-unnamed}" ycsb-b "$((1 * 1024 * 1024))" 0 25 --data-source "$data_source" --data-type "$data_type" --data-path "$data_path" --entry-size "$ENTRY_SIZE"
}

function ycsb_c() {
    # Default parameters: use generated data with integers
    local data_source="${YCSB_DATA_SOURCE:-file}"
    local data_type="${YCSB_DATA_TYPE:-int}"
    local data_path="${YCSB_DATA_PATH:-/home/skarim/Code/smash/haura/betree/haura-benchmarks/silesia_corpus}"
    
    #run "$RUN_IDENT" "ycsb_c_${YCSB_SUFFIX:-unnamed}" ycsb-c "$((2 * 1024 * 1024 * 1024))" 0 2 --data-source "$data_source" --data-type "$data_type" --data-path "$data_path" --entry-size "$ENTRY_SIZE"
    run "$RUN_IDENT" "ycsb_c_${YCSB_SUFFIX:-unnamed}" ycsb-c "$((1 * 1024 * 1024))" 0 1 --data-source "$data_source" --data-type "$data_type" --data-path "$data_path" --entry-size "$ENTRY_SIZE"
    #run "$RUN_IDENT" "ycsb_c_${YCSB_SUFFIX:-unnamed}" ycsb-c "$((1 * 1024 * 1024))" 0 2 --data-source "$data_source" --data-type "$data_type" --data-path "$data_path" --entry-size "$ENTRY_SIZE"
    #run "$RUN_IDENT" "ycsb_c_${YCSB_SUFFIX:-unnamed}" ycsb-c "$((1 * 1024 * 1024))" 0 3 --data-source "$data_source" --data-type "$data_type" --data-path "$data_path" --entry-size "$ENTRY_SIZE"
    #run "$RUN_IDENT" "ycsb_c_${YCSB_SUFFIX:-unnamed}" ycsb-c "$((1 * 1024 * 1024))" 0 4 --data-source "$data_source" --data-type "$data_type" --data-path "$data_path" --entry-size "$ENTRY_SIZE"
    #run "$RUN_IDENT" "ycsb_c_${YCSB_SUFFIX:-unnamed}" ycsb-c "$((1 * 1024 * 1024))" 0 5 --data-source "$data_source" --data-type "$data_type" --data-path "$data_path" --entry-size "$ENTRY_SIZE"
    #run "$RUN_IDENT" "ycsb_c_${YCSB_SUFFIX:-unnamed}" ycsb-c "$((1 * 1024 * 1024))" 0 8 --data-source "$data_source" --data-type "$data_type" --data-path "$data_path" --entry-size "$ENTRY_SIZE"
    #run "$RUN_IDENT" "ycsb_c_${YCSB_SUFFIX:-unnamed}" ycsb-c "$((1 * 1024 * 1024))" 0 10 --data-source "$data_source" --data-type "$data_type" --data-path "$data_path" --entry-size "$ENTRY_SIZE"
    #run "$RUN_IDENT" "ycsb_c_${YCSB_SUFFIX:-unnamed}" ycsb-c "$((1 * 1024 * 1024))" 0 15 --data-source "$data_source" --data-type "$data_type" --data-path "$data_path" --entry-size "$ENTRY_SIZE"
    #run "$RUN_IDENT" "ycsb_c_${YCSB_SUFFIX:-unnamed}" ycsb-c "$((1 * 1024 * 1024))" 0 20 --data-source "$data_source" --data-type "$data_type" --data-path "$data_path" --entry-size "$ENTRY_SIZE"
    #run "$RUN_IDENT" "ycsb_c_${YCSB_SUFFIX:-unnamed}" ycsb-c "$((1 * 1024 * 1024))" 0 25 --data-source "$data_source" --data-type "$data_type" --data-path "$data_path" --entry-size "$ENTRY_SIZE"
}

function ycsb_d() {
    # Default parameters: use generated data with integers
    local data_source="${YCSB_DATA_SOURCE:-file}"
    local data_type="${YCSB_DATA_TYPE:-int}"
    local data_path="${YCSB_DATA_PATH:-/home/skarim/Code/smash/haura/betree/haura-benchmarks/silesia_corpus}"
    
    run "$RUN_IDENT" "ycsb_d_${YCSB_SUFFIX:-unnamed}" ycsb-d "$((2 * 1024 * 1024 * 1024))" 0 2 --data-source "$data_source" --data-type "$data_type" --data-path "$data_path" --entry-size "$ENTRY_SIZE"
#    run "$RUN_IDENT" ycsb_d_memory ycsb-d "$((4 * 1024 * 1024 * 1024))" 1 6 --data-source "$data_source" --data-type "$data_type" --data-path "$data_path" --entry-size "$ENTRY_SIZE"
}

function ycsb_e() {
    # Default parameters: use generated data with integers
    local data_source="${YCSB_DATA_SOURCE:-file}"
    local data_type="${YCSB_DATA_TYPE:-int}"
    local data_path="${YCSB_DATA_PATH:-/home/skarim/Code/smash/haura/betree/haura-benchmarks/silesia_corpus}"
    
    #run "$RUN_IDENT" "ycsb_e_${YCSB_SUFFIX:-unnamed}" ycsb-e "$((2 * 1024 * 1024 * 1024))" 0 2 --data-source "$data_source" --data-type "$data_type" --data-path "$data_path" --entry-size "$ENTRY_SIZE"
    run "$RUN_IDENT" "ycsb_e_${YCSB_SUFFIX:-unnamed}" ycsb-e "$((1 * 1024 * 1024))" 0 1 --data-source "$data_source" --data-type "$data_type" --data-path "$data_path" --entry-size "$ENTRY_SIZE"
    #run "$RUN_IDENT" "ycsb_e_${YCSB_SUFFIX:-unnamed}" ycsb-e "$((1 * 1024 * 1024))" 0 2 --data-source "$data_source" --data-type "$data_type" --data-path "$data_path" --entry-size "$ENTRY_SIZE"
    run "$RUN_IDENT" "ycsb_e_${YCSB_SUFFIX:-unnamed}" ycsb-e "$((1 * 1024 * 1024))" 0 3 --data-source "$data_source" --data-type "$data_type" --data-path "$data_path" --entry-size "$ENTRY_SIZE"
    #run "$RUN_IDENT" "ycsb_e_${YCSB_SUFFIX:-unnamed}" ycsb-e "$((1 * 1024 * 1024))" 0 4 --data-source "$data_source" --data-type "$data_type" --data-path "$data_path" --entry-size "$ENTRY_SIZE"
    run "$RUN_IDENT" "ycsb_e_${YCSB_SUFFIX:-unnamed}" ycsb-e "$((1 * 1024 * 1024))" 0 6 --data-source "$data_source" --data-type "$data_type" --data-path "$data_path" --entry-size "$ENTRY_SIZE"
    run "$RUN_IDENT" "ycsb_e_${YCSB_SUFFIX:-unnamed}" ycsb-e "$((1 * 1024 * 1024))" 0 10 --data-source "$data_source" --data-type "$data_type" --data-path "$data_path" --entry-size "$ENTRY_SIZE"
    run "$RUN_IDENT" "ycsb_e_${YCSB_SUFFIX:-unnamed}" ycsb-e "$((1 * 1024 * 1024))" 0 15 --data-source "$data_source" --data-type "$data_type" --data-path "$data_path" --entry-size "$ENTRY_SIZE"
    run "$RUN_IDENT" "ycsb_e_${YCSB_SUFFIX:-unnamed}" ycsb-e "$((1 * 1024 * 1024))" 0 20 --data-source "$data_source" --data-type "$data_type" --data-path "$data_path" --entry-size "$ENTRY_SIZE"
    run "$RUN_IDENT" "ycsb_e_${YCSB_SUFFIX:-unnamed}" ycsb-e "$((1 * 1024 * 1024))" 0 25 --data-source "$data_source" --data-type "$data_type" --data-path "$data_path" --entry-size "$ENTRY_SIZE"
}

function ycsb_f() {
    # Default parameters: use generated data with integers
    local data_source="${YCSB_DATA_SOURCE:-file}"
    local data_type="${YCSB_DATA_TYPE:-int}"
    local data_path="${YCSB_DATA_PATH:-/home/skarim/Code/smash/haura/betree/haura-benchmarks/silesia_corpus}"
    
    run "$RUN_IDENT" "ycsb_f_${YCSB_SUFFIX:-unnamed}" ycsb-f "$((2 * 1024 * 1024 * 1024))" 0 2 --data-source "$data_source" --data-type "$data_type" --data-path "$data_path" --entry-size "$ENTRY_SIZE"
#    run "$RUN_IDENT" ycsb_f_memory ycsb-f "$((4 * 1024 * 1024 * 1024))" 1 6 --data-source "$data_source" --data-type "$data_type" --data-path "$data_path" --entry-size "$ENTRY_SIZE"
}

function ycsb_g() {
    # Default parameters: use generated data with integers
    local data_source="${YCSB_DATA_SOURCE:-file}"
    local data_type="${YCSB_DATA_TYPE:-int}"
    local data_path="${YCSB_DATA_PATH:-/home/skarim/Code/smash/haura/betree/haura-benchmarks/silesia_corpus}"
    
    run "$RUN_IDENT" "ycsb_g_${YCSB_SUFFIX:-unnamed}" ycsb-g "$((2 * 1024 * 1024))" 0 1 --data-source "$data_source" --data-type "$data_type" --data-path "$data_path" --entry-size "$ENTRY_SIZE"
    run "$RUN_IDENT" "ycsb_g_${YCSB_SUFFIX:-unnamed}" ycsb-g "$((2 * 1024 * 1024))" 0 4 --data-source "$data_source" --data-type "$data_type" --data-path "$data_path" --entry-size "$ENTRY_SIZE"
    run "$RUN_IDENT" "ycsb_g_${YCSB_SUFFIX:-unnamed}" ycsb-g "$((2 * 1024 * 1024))" 0 8 --data-source "$data_source" --data-type "$data_type" --data-path "$data_path" --entry-size "$ENTRY_SIZE"
    run "$RUN_IDENT" "ycsb_g_${YCSB_SUFFIX:-unnamed}" ycsb-g "$((2 * 1024 * 1024))" 0 12 --data-source "$data_source" --data-type "$data_type" --data-path "$data_path" --entry-size "$ENTRY_SIZE"
    run "$RUN_IDENT" "ycsb_g_${YCSB_SUFFIX:-unnamed}" ycsb-g "$((2 * 1024 * 1024))" 0 16 --data-source "$data_source" --data-type "$data_type" --data-path "$data_path" --entry-size "$ENTRY_SIZE"
    run "$RUN_IDENT" "ycsb_g_${YCSB_SUFFIX:-unnamed}" ycsb-g "$((2 * 1024 * 1024))" 0 20 --data-source "$data_source" --data-type "$data_type" --data-path "$data_path" --entry-size "$ENTRY_SIZE"
    run "$RUN_IDENT" "ycsb_g_${YCSB_SUFFIX:-unnamed}" ycsb-g "$((2 * 1024 * 1024))" 0 24 --data-source "$data_source" --data-type "$data_type" --data-path "$data_path" --entry-size "$ENTRY_SIZE"
    run "$RUN_IDENT" "ycsb_g_${YCSB_SUFFIX:-unnamed}" ycsb-g "$((2 * 1024 * 1024))" 0 28 --data-source "$data_source" --data-type "$data_type" --data-path "$data_path" --entry-size "$ENTRY_SIZE"
    run "$RUN_IDENT" "ycsb_g_${YCSB_SUFFIX:-unnamed}" ycsb-g "$((2 * 1024 * 1024))" 0 32 --data-source "$data_source" --data-type "$data_type" --data-path "$data_path" --entry-size "$ENTRY_SIZE"
    run "$RUN_IDENT" "ycsb_g_${YCSB_SUFFIX:-unnamed}" ycsb-g "$((2 * 1024 * 1024))" 0 36 --data-source "$data_source" --data-type "$data_type" --data-path "$data_path" --entry-size "$ENTRY_SIZE"
    run "$RUN_IDENT" "ycsb_g_${YCSB_SUFFIX:-unnamed}" ycsb-g "$((2 * 1024 * 1024))" 0 40 --data-source "$data_source" --data-type "$data_type" --data-path "$data_path" --entry-size "$ENTRY_SIZE"
}

function ycsb_h() {
    # Default parameters: use generated data with integers
    local data_source="${YCSB_DATA_SOURCE:-file}"
    local data_type="${YCSB_DATA_TYPE:-int}"
    local data_path="${YCSB_DATA_PATH:-/home/skarim/Code/smash/haura/betree/haura-benchmarks/silesia_corpus}"
    
    run "$RUN_IDENT" "ycsb_h_${YCSB_SUFFIX:-unnamed}" ycsb-h "$((1 * 1024 * 1024))" 0 2 --data-source "$data_source" --data-type "$data_type" --data-path "$data_path" --entry-size "$ENTRY_SIZE"
#    run "$RUN_IDENT" ycsb_h_memory ycsb-h "$((768 * 1024 * 1024))" 1 8 --data-source "$data_source" --data-type "$data_type" --data-path "$data_path" --entry-size "$ENTRY_SIZE"
}

function ycsb_i() {
    # Default parameters: use generated data with integers
    local data_source="${YCSB_DATA_SOURCE:-file}"
    local data_type="${YCSB_DATA_TYPE:-int}"
    local data_path="${YCSB_DATA_PATH:-/home/skarim/Code/smash/haura/betree/haura-benchmarks/silesia_corpus}"
    
    run "$RUN_IDENT" "ycsb_i_${YCSB_SUFFIX:-unnamed}" ycsb-i "$((1 * 1024 * 1024))" 0 2 --data-source "$data_source" --data-type "$data_type" --data-path "$data_path" --entry-size "$ENTRY_SIZE"
#    run "$RUN_IDENT" ycsb_i_memory ycsb-i "$((768 * 1024 * 1024))" 1 8 --data-source "$data_source" --data-type "$data_type" --data-path "$data_path" --entry-size "$ENTRY_SIZE"
}

function set_compression() {
    local compression="$1"
    
    jq '.compression' perf-config.json
    
    # Parse the compression string as JSON and set it
    echo "$compression" | jq '.' > /dev/null 2>&1
    if [ $? -eq 0 ]; then
        # Valid JSON, use it directly
        echo "DEBUG: Valid JSON detected, setting as object"
        jq ".compression = $compression" perf-config.json > temp-config.json && mv temp-config.json perf-config.json
    else
        # Not valid JSON, treat as string
        echo "DEBUG: Invalid JSON, treating as string"
        jq ".compression = \"$compression\"" perf-config.json > temp-config.json && mv temp-config.json perf-config.json
    fi
    
    jq '.compression' perf-config.json
}

function run_all_ycsb_compressions() {
    # Define entry size configurations
    local entry_sizes=(
        '512|entry512'
        '4096|entry4k'
        '16384|entry16k'
        '30000|entry32k'
    )
    
    # Define compression configurations
    local compressions=(
        'None|none'
        '{"Zstd": {"level": 1}}|zstd1'
        '{"Zstd": {"level": 5}}|zstd5'
        '{"Zstd": {"level": 10}}|zstd10'
        '{"Lz4": {"level": 1}}|lz4_1'
        '{"Lz4": {"level": 5}}|lz4_5'
        '{"Lz4": {"level": 10}}|lz4_10'
        '{"Snappy": {}}|snappy'
        #'{"Dictionary": {"max_dict_size": 128, "min_frequency": 1}}|dict'
        #'{"Toast": {"min_compress_size" : 32, "max_ratio_percent": 90}}|toast'
        #'{"Delta": {"value_size" : 1, "signed": false}}|delta'
        #'{"Rle": {"min_run_length" : 2, "value_size": 1}}|rle'
        #'{"Gorilla": {"use_f64" : false}}|gorilla'        
        # Add any other compression types...
    )
    
    # Loop through entry sizes
    for entry_config in "${entry_sizes[@]}"; do
        IFS='|' read -r entry_size entry_suffix <<< "$entry_config"
        
        echo "========================================="
        echo "Testing with ENTRY_SIZE=$entry_size"
        echo "========================================="
        
        export ENTRY_SIZE="$entry_size"
        
        # Loop through compressions for this entry size
        for compression_config in "${compressions[@]}"; do
            IFS='|' read -r compression compression_suffix <<< "$compression_config"
            
            echo "Running YCSB tests with entry_size=$entry_size, compression: $compression"
            
            # Set compression in config file
            set_compression "$compression"
            
            # Set combined suffix
            export YCSB_SUFFIX="${entry_suffix}_${compression_suffix}"
            
            # Run all YCSB tests
            #ycsb_a
            #ycsb_b
            #ycsb_c
            #ycsb_d
            #ycsb_e
            #ycsb_f
            ycsb_g
            #ycsb_h
            #ycsb_i
            
            echo "Completed YCSB tests for entry_size=$entry_size, compression: $compression"
            echo ""
        done
        
        echo "Completed all compression tests for ENTRY_SIZE=$entry_size"
        echo ""
    done
}

#cargo build --release --features betree_storage_stack/memory_metrics
cargo build --release --features betree_storage_stack/compression_metrics

if [ -z "$BETREE_CONFIG" ]; then
    export BETREE_CONFIG="$PWD/perf-config.json"
fi

export ROOT="$PWD"
export ZIP_ARCHIVE="$PWD/data/linux.zip"
# Default entry size for YCSB tests (can be overridden)
export ENTRY_SIZE="${ENTRY_SIZE:-30000}"
# Category under which the default runs should be made, a function may modify
# this if multiple categories are needed.
export RUN_IDENT="default"

if [ "$1" == "-h" ] || [ "$1" == "--help" ] || [ "$1" = "help" ]; then
    echo "Usage:"
    echo "  $0 [identifier]"
    exit 0
fi

if [ -n "$*" ]; then
    export RUN_IDENT=$*
fi

ensure_bectl
ensure_zip
ensure_config

# Uncomment the scenarios which you want to run.  Assure that the used
# configuration is valid for the scenario as some of them require a minimum
# amount of tiers.

#zip_cache
#zip_tiered
#zip_mt
#tiered
#scientific_evaluation
#evaluation_rw
#filesystem
#filesystem_zip
#checkpoints
#switchover
#ingest
#ycsb_a
#ycsb_b
#ycsb_c
#ycsb_d
#ycsb_e
#ycsb_f
#ycsb_g
#ycsb_h
#ycsb_i

# Run all YCSB tests with different entry sizes and compression configurations
run_all_ycsb_compressions