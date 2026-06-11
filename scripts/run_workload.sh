#!/bin/bash
###############################################################################
# Run a db_bench write-stall workload against KVACCEL.
#
# Mirrors the paper's evaluation: 4 B keys, 4 KB values, a single write thread,
# slowdown disabled (level0_*_trigger set very high) so write stalls — and
# KVACCEL's redirection to Dev-LSM — are actually exercised.
#
# The KVACCEL-specific knob is -num-vlsm:
#   -num-vlsm=2   Main-LSM + Dev-LSM coordinated by DB_MASTER (KVACCEL, default)
#   -num-vlsm=1   single instance, ~ stock RocksDB behaviour (baseline)
#
# Env: DB_BENCH (path to binary), DB (data dir), BENCH (benchmarks), NUM, NUM_VLSM
###############################################################################
set -euo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DB_BENCH="${DB_BENCH:-$REPO/rocksdb/db_bench}"
DB="${DB:-/mnt/cosmos/db_bench}"
BENCH="${BENCH:-fillrandom,stats}"          # workload A: write-only fillrandom
NUM="${NUM:-100000}"
NUM_VLSM="${NUM_VLSM:-2}"

sudo rm -rf "$DB"
sudo mkdir -p "$DB"

sudo "$DB_BENCH" \
  -num="$NUM" \
  -benchmarks="$BENCH" \
  --db="$DB" \
  -num-vlsm="$NUM_VLSM" \
  -key_size=4 -value_size=4096 -threads=1 \
  -write_buffer_size=134217728 -max_write_buffer_number=8 \
  -level0_slowdown_writes_trigger=1000 -level0_stop_writes_trigger=1000 \
  -max_background_jobs=2 -max_background_compactions=1 -max_background_flushes=1 \
  -subcompactions=0 -statistics -report_bg_io_stats=true -stats_interval_seconds=1 \
  -histogram -compression_ratio=1 -disable_wal=false \
  -seed=1699101730035899 -universal_compression_size_percent=0
