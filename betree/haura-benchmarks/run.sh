#!/usr/bin/env bash
# shellcheck disable=SC2030,SC2031 # we exploit this characteristic to start several test scenarios - merging them would lead to pollution

function ensure_zip {
  local url
  url="https://cdn.kernel.org/pub/linux/kernel/v5.x/linux-5.15.58.tar.xz"

  if [ ! -e "$ZIP_ARCHIVE" ]
  then
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
  cargo build --release
  popd || return
}

function ensure_config {
  if [ ! -e "$BETREE_CONFIG" ]
  then
    echo "No Haura configuration found at: ${BETREE_CONFIG}"
    exit 1
  fi
}

function run {
  local vdev_type="$1"
  local name="$2"
  local mode="$3"
  shift 3

  local out_path
  out_path="results/$(date -I)_${vdev_type}/${name}_$(date +%s)"
  mkdir -p "$out_path"

  pushd "$out_path" || return

  echo "running $mode with these settings:"
  env | grep BETREE__
  env > "env"
  "$ROOT/../../target/release/bectl" config print-active > "config"
  "$ROOT/target/release/betree-perf" "$mode" "$@"

  echo "merging results into $out_path/out.jsonl"
  "$ROOT/target/release/json-merge" \
    --timestamp-key epoch_ms \
    ./betree-metrics.jsonl \
    ./proc.jsonl \
    ./sysinfo.jsonl \
    | "$ROOT/target/release/json-flatten" > "out.jsonl"

  popd || return

  sleep 60
}

function tiered() {
  (
    export BETREE__ALLOC_STRATEGY='[[0],[0],[],[]]'
    run "$VDEV_TYPE" tiered1_all0_alloc tiered1
  )

  (
    export BETREE__ALLOC_STRATEGY='[[0],[1],[],[]]'
    run "$VDEV_TYPE" tiered1_id_alloc tiered1
  )

  (
    export BETREE__ALLOC_STRATEGY='[[1],[1],[],[]]'
    run "$VDEV_TYPE" tiered1_all1_alloc tiered1
  )
}

function scientific_evaluation() {
  export BETREE__ALLOC_STRATEGY='[[0],[1],[],[]]'
  run "$VDEV_TYPE" scientific_evaluation_id_alloc evaluation-read 30
}

function evaluation_rw() {
  export BETREE__ALLOC_STRATEGY='[[0],[1],[],[]]'
  run "$VDEV_TYPE" file_system_three evaluation-rw
}

function filesystem_zip() {
  export BETREE__ALLOC_STRATEGY='[[0],[1],[2],[]]'
  run "$VDEV_TYPE" file_system_three "$ZIP_ARCHIVE"
}

function checkpoints() {
  export BETREE__ALLOC_STRATEGY='[[0, 1],[1],[],[]]'
  run "$VDEV_TYPE" checkpoints_fastest checkpoints
}

function filesystem() {
  export BETREE__ALLOC_STRATEGY='[[0],[1],[2],[]]'
  run "$VDEV_TYPE" file_system_three filesystem
}

function zip_cache() {
  local F_CD_START=1040032667

  for cache_mib in 32 64 128 256 512 1024 2048 4096 8192; do
    (
      export BETREE__CACHE_SIZE=$((cache_mib * 1024 * 1024))
      run "$VDEV_TYPE" "zip_cache_$cache_mib" zip 4 100 10 "$ZIP_ARCHIVE" "$F_CD_START"
    )
  done
}

function zip_mt() {
  local F="$PWD/data/linux.zip"
  local F_CD_START=1040032667

  for cache_mib in 256 512 1024 2048; do
    echo "using $cache_mib MiB of cache"
    (
      export BETREE__CACHE_SIZE=$((cache_mib * 1024 * 1024))

      local total=10000

      for num_workers in 1 2 3 4 5 6 7 8 9 10; do
        echo "running with $num_workers workers"
        local per_worker=$((total / num_workers))
        local per_run=$((per_worker / 10))

        run "$VDEV_TYPE" "zip_mt_${cache_mib}_${num_workers}_${per_run}_10" zip "$num_workers" "$per_run" 10 "$F" "$F_CD_START"
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

      export BETREE__STORAGE__TIERS="[ [ { mem = $((1 * 1024 * 1024 * 1024)) } ], [ { mem = $((1 * 1024 * 1024 * 1024)) } ] ]"

      for num_workers in 1 2 3 4 5 6 7 8 9 10; do
        echo "running with $num_workers workers"
        local per_worker=$((total / num_workers))
        local per_run=$((per_worker / 10))

        (
          export BETREE__ALLOC_STRATEGY='[[0],[0],[],[]]'
          run "$VDEV_TYPE" "zip_tiered_all0_${cache_mib}_${num_workers}_${per_run}_10" zip "$num_workers" "$per_run" 10 "$ZIP_ARCHIVE" "$F_CD_START"
        )

        (
          export BETREE__ALLOC_STRATEGY='[[0],[1],[],[]]'
          run "$VDEV_TYPE" "zip_tiered_id_${cache_mib}_${num_workers}_${per_run}_10" zip "$num_workers" "$per_run" 10 "$ZIP_ARCHIVE" "$F_CD_START"
        )

        (
          export BETREE__ALLOC_STRATEGY='[[1],[1],[],[]]'
          run "$VDEV_TYPE" "zip_tiered_all1_${cache_mib}_${num_workers}_${per_run}_10" zip "$num_workers" "$per_run" 10 "$ZIP_ARCHIVE" "$F_CD_START"
        )

      done
    )
  done
}

function ingest() {
  (
    (
      export BETREE__COMPRESSION="None"
      run "default" ingest_hdd_none ingest "$ZIP_ARCHIVE"
    )

    for level in $(seq 1 16); do
      (
        export BETREE__COMPRESSION="{ Zstd = { level = $level } }"
        run "$VDEV_TYPE" "ingest_hdd_zstd_$level" ingest "$ZIP_ARCHIVE"
      )
    done
  )
}

function switchover() {
  run "$VDEV_TYPE" switchover_tiny switchover 32 "$((32 * 1024 * 1024))"
  run "$VDEV_TYPE" switchover_small switchover 8 "$((128 * 1024 * 1024))"
  run "$VDEV_TYPE" switchover_medium switchover 4 "$((2 * 1024 * 1024 * 1024))"
  run "$VDEV_TYPE" switchover_large switchover 4 "$((8 * 1024 * 1024 * 1024))"
}

cargo build --release

if [ -z "$BETREE_CONFIG" ]
then
  export BETREE_CONFIG="$PWD/perf-config.json"
fi

export ROOT="$PWD"
export ZIP_ARCHIVE="$PWD/data/linux.zip"
# Category under which the default runs should be made, a function may modify
# this if multiple categories are needed.
export VDEV_TYPE="default"

if [ "$1" == "-h" ] || [ "$1" == "--help" ] || [ "$1" = "help" ]
then
    echo "Usage:"
    echo "  $0 [identifier]"
    exit 0
fi

if [ -n "$*" ]
then
    export VDEV_TYPE=$*
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
