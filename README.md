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
to an in-device key-value write buffer** instead of slowing them down. 

> RocksDB 8.3.2**: the new KVACCEL sources plus the handful of stock RocksDB
> files they modify, integrated by `build.sh`.

## How it works

Two LSM-trees live on one physical SSD:

- **Main-LSM** — the normal RocksDB instance on the SSD's **block** interface.
  Serves all writes when there is no stall.
- **Dev-LSM** — a key-value store running **inside** the SSD on the device's
  **key-value** interface, used as a temporary write buffer during a stall.

## Build

```bash
# Clones RocksDB v8.3.2, overlays the KVACCEL sources, builds db_bench (release).
./build.sh
# Binary: ./rocksdb/db_bench
```

`DEBUG_LEVEL=0` (release) is the default and is required for performance runs.
Override `ROCKSDB_SRC` to point at an existing checkout, or `JOBS` for parallelism.

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
## License

Inherits RocksDB's dual GPLv2 / Apache-2.0 license. See `LICENSE`.
