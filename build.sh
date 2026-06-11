#!/bin/bash
###############################################################################
# KVACCEL build script.
#
# KVACCEL is implemented as a set of additions and small modifications on top
# of RocksDB 8.3.2. This script integrates the KVACCEL sources into a clean
# RocksDB v8.3.2 checkout and builds the db_bench benchmark tool.
#
#   kvaccel/     -- new KVACCEL source files (the contribution), laid out in
#                   RocksDB-relative paths so they drop straight into the tree.
#   integration/ -- the five stock RocksDB 8.3.2 files KVACCEL modifies, shipped
#                   as full drop-in replacements (options.h, options/db_options.*,
#                   src.mk, tools/db_bench_tool.cc).
#
# Env:
#   ROCKSDB_SRC   path to a RocksDB source checkout (default: ./rocksdb; cloned if absent)
#   ROCKSDB_TAG   RocksDB tag to build against (default: v8.3.2)
#   JOBS          parallel build jobs (default: nproc)
#   DEBUG_LEVEL   RocksDB build level (default: 0 = release, required for perf runs)
#
# NOTE: KVACCEL's Dev-LSM (the device-side key-value write buffer) runs on a
# dual-interface SSD built on the Cosmos+ OpenSSD platform and is reached via
# NVMe passthrough to /dev/nvme1n1 (see kvaccel/db/db_impl/iLSM.cc). db_bench
# links and runs without the device, but the write-redirection path is only
# exercised with the KVACCEL-programmed OpenSSD attached.
###############################################################################
set -euo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROCKSDB_SRC="${ROCKSDB_SRC:-$REPO/rocksdb}"
ROCKSDB_TAG="${ROCKSDB_TAG:-v8.3.2}"
JOBS="${JOBS:-$(nproc)}"
DEBUG_LEVEL="${DEBUG_LEVEL:-0}"

# 1) Obtain RocksDB 8.3.2
if [ ! -d "$ROCKSDB_SRC" ]; then
  echo ">> Cloning RocksDB ($ROCKSDB_TAG) into $ROCKSDB_SRC"
  git clone --depth 1 --branch "$ROCKSDB_TAG" https://github.com/facebook/rocksdb "$ROCKSDB_SRC"
fi

# 2) Overlay KVACCEL: new files first, then the modified-stock drop-in replacements.
echo ">> Installing KVACCEL sources into $ROCKSDB_SRC"
cp -rv "$REPO/kvaccel/."     "$ROCKSDB_SRC/"
cp -rv "$REPO/integration/." "$ROCKSDB_SRC/"

# 3) Build db_bench (release). DEBUG_LEVEL=0 is required for benchmarking.
echo ">> Building db_bench (DEBUG_LEVEL=$DEBUG_LEVEL)"
make -C "$ROCKSDB_SRC" -j"$JOBS" DEBUG_LEVEL="$DEBUG_LEVEL" db_bench

echo ""
echo "Done. Benchmark binary: $ROCKSDB_SRC/db_bench"
echo "Run a write-stall workload with:  ./scripts/run_workload.sh"
