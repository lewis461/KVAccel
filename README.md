# KVACCEL — A Write Accelerator for LSM-Tree KV Stores with Host–SSD Collaboration

**Paper:** Kihwan Kim, Hyunsun Chung, Seonghoon Ahn, Junhyeok Park, Safdar Jamil,
Hongsu Byun, Myungcheol Lee, Jinchun Choi, Youngjae Kim.
*KVACCEL: A Novel Write Accelerator for LSM-Tree-Based KV Stores with Host-SSD
Collaboration.* IEEE IPDPS 2025, pp. 666–677. DOI: 10.1109/IPDPS64566.2025.00065.

KVACCEL eliminates **write stalls** in LSM-tree key-value stores (RocksDB) by
exploiting the SSD bandwidth that sits idle *during* a stall. The key insight:
while the host blocks writes to run a compaction, the device's PCIe/NAND
bandwidth is largely unused. KVACCEL turns the SSD into a **dual-interface
device** — a normal block interface and a key-value interface on the same NAND
flash — and, the moment a write stall is detected, **redirects incoming writes
to an in-device key-value write buffer** instead of slowing them down. When the
stall clears, the buffered pairs are **rolled back** (merged) into the main
LSM-tree. No write slowdown, no extra hardware (PM/FPGA/GPU/DPU), and read
performance is preserved.

Against ADOC (the prior state of the art) KVACCEL improves write-intensive
throughput and performance-per-CPU-utilization by up to **17%**, with
comparable performance on mixed read/write workloads.

> This repository packages the host-side implementation as an **overlay on
> RocksDB 8.3.2**: the new KVACCEL sources plus the handful of stock RocksDB
> files they modify, integrated by `build.sh`. It is the `kvfusion_buffering`
> variant — the final version, with the rollback path enabled.

## How it works

Two LSM-trees live on one physical SSD:

- **Main-LSM** — the normal RocksDB instance on the SSD's **block** interface.
  Serves all writes when there is no stall.
- **Dev-LSM** — a key-value store running **inside** the SSD on the device's
  **key-value** interface, used as a temporary write buffer during a stall.

Four software modules (all inside `DB_MASTER`) decide, per operation, which
interface to use:

| Module | Role | Where in the code |
|---|---|---|
| **Detector** | Polls Main-LSM's stall signals — L0 file count, MemTable size, pending-compaction bytes — and flags a write stall. | `DB_MASTER::Monitor_Consumer()` |
| **Controller** | Routes each write: no stall → Main-LSM; stall → Dev-LSM. Routes each read to wherever the key currently lives. | `DB_MASTER::Put()` / `DB_MASTER::Get()` |
| **Metadata Manager** | In-memory hash table tracking which keys are currently in Dev-LSM (membership test for reads). | `HashMap<> keyTracker` + `double_hash()` (`util/hash_map.h`) |
| **Rollback Manager** | When the stall clears, bulk-scans Dev-LSM and merges its key-value pairs back into Main-LSM, then resets Dev-LSM. | `DB_MASTER::Rollback_Consumer()` |

**Write path:** Detector reports stall? → Controller writes the pair to Dev-LSM
and records it in the Metadata Manager. No stall? → write goes to Main-LSM (and
if a stale copy sits in Dev-LSM, the metadata is updated to point at Main-LSM).

**Read path:** Metadata Manager locates the key → read from Dev-LSM if it is
buffered there, otherwise from Main-LSM.

**Rollback** uses an **iterator-based bulk range scan**: an in-device iterator
walks the whole Dev-LSM key range, serializes the pairs, and DMAs them to the
host in 512 KB chunks (the platform's max DMA transfer unit) to be merged into
Main-LSM. Because a stall is short-lived, Dev-LSM never grows large, so each
rollback finishes within the gap between stalls. Rollback can be scheduled
*eagerly* (better for read-heavy workloads, since Dev-LSM reads hit the slower
device) or *lazily* (better for write-heavy workloads).

**Range queries** run two iterators — one over Main-LSM, one over Dev-LSM — and
merge them through an iterator comparator so the scan is correct even while
writes are in flight.

## Repository layout

```
kvaccel/                              new KVACCEL sources (the contribution)
  include/rocksdb/db_master.h           DB_MASTER: Detector + Controller + Metadata + Rollback
  include/rocksdb/iLSM.h                Dev-LSM client: NVMe key-value passthrough API
  include/rocksdb/clue_entry_set.h     routing-cache helper
  util/hash_map.h                      Metadata Manager hash table (keyTracker)
  db/db_impl/db_impl_master.cc         DB_MASTER impl: write/read paths + Monitor & Rollback threads
  db/db_impl/iLSM.cc                   Dev-LSM impl: KV PUT/GET, iterator SEEK/NEXT bulk scan, reset
  db/compaction/clue_entry_set.cc      routing-cache helper impl

integration/                          stock RocksDB 8.3.2 files KVACCEL modifies (drop-in replacements)
  include/rocksdb/options.h            forward-declares DB_MASTER; adds db_master_ptr / rollback_mutex
  options/db_options.{h,cc}            threads the two new option fields through DBOptions
  src.mk                               adds the new .cc files to the RocksDB build
  tools/db_bench_tool.cc               db_bench wiring: -num-vlsm flag, routes I/O via DB_MASTER, stats

build.sh                              clone RocksDB v8.3.2, overlay the above, build db_bench
scripts/
  format_device.sh                    mkfs + mount the SSD block region (Main-LSM)
  run_workload.sh                      run a write-stall db_bench workload
```

The host code is a thin, well-isolated layer: KVACCEL adds 7 source files and
touches only 5 stock RocksDB files, so it rebases onto upstream RocksDB easily.
`db_impl_master.cc` is self-contained and is the only master translation unit
compiled (it carries the write path, read path, and both background threads).

## Build

```bash
# Clones RocksDB v8.3.2, overlays the KVACCEL sources, builds db_bench (release).
./build.sh
# Binary: ./rocksdb/db_bench
```

`DEBUG_LEVEL=0` (release) is the default and is required for performance runs.
Override `ROCKSDB_SRC` to point at an existing checkout, or `JOBS` for parallelism.

## Run

```bash
# 1) Format + mount the SSD's block region (Main-LSM lives here).
./scripts/format_device.sh                 # DEV=/dev/nvme1n1 MNT=/mnt/cosmos

# 2) Run a write-only stall workload (4 B key, 4 KB value, slowdown disabled).
./scripts/run_workload.sh                   # NUM_VLSM=2 -> KVACCEL (Main + Dev-LSM)
NUM_VLSM=1 ./scripts/run_workload.sh        # baseline ~ stock RocksDB
BENCH="readwhilewriting,stats" ./scripts/run_workload.sh   # mixed read/write
```

The KVACCEL knob is **`-num-vlsm`**: `2` runs Main-LSM + Dev-LSM coordinated by
`DB_MASTER`; `1` collapses to a single instance (baseline). `db_bench` reports
the KVACCEL counters `totalvlsmL0` (aggregate L0 files) and `totalCE`.

## Hardware requirement (Dev-LSM)

Dev-LSM is **not** a host emulation — it runs on a real **dual-interface SSD**
built on the **Cosmos+ OpenSSD** platform (Xilinx Zynx-7000 / ARM Cortex-A9,
1 TB NAND, 4 ch × 8 way, PCIe Gen2 ×8). The device firmware splits the logical
NAND address space into a block region (Main-LSM) and a key-value region
(Dev-LSM); `kvaccel/db/db_impl/iLSM.cc` drives the KV region over NVMe
passthrough to `/dev/nvme1n1` using vendor KV opcodes (`KV_PUT`, `KV_GET`,
`KV_ITER_*`). `db_bench` links and runs without the programmed device, but the
write-redirection path is only exercised with the KVACCEL OpenSSD attached.

Host system used in the paper: Intel Xeon Gold 6226R (CPU usage capped at
8 cores), 384 GB DDR4, Ubuntu 22.04 / Linux 6.6.

## Tunables

Thresholds live in `kvaccel/include/rocksdb/db_master.h`:

- `monitor_interval` — Detector polling period (µs).
- `rollback_threshold` — Dev-LSM size/count at which the Rollback Manager fires.
- `BLOOM_FILTER_SIZE`, `HASH_FUNCTION` — membership-test sizing.

Dev-LSM buffer sizing (`BUFFERING_MODE`, value-buffer entry/size limits, DMA
chunk geometry) lives in `kvaccel/include/rocksdb/iLSM.h`.

## License

Inherits RocksDB's dual GPLv2 / Apache-2.0 license. See `LICENSE`.
