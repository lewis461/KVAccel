#!/bin/bash
###############################################################################
# Format and mount the block interface of the dual-interface SSD.
#
# KVACCEL's Main-LSM lives on the block region of the OpenSSD, exposed here as
# /dev/nvme1n1 and mounted at /mnt/cosmos with ext4. The key-value region of
# the same device (Dev-LSM) is reached separately via NVMe passthrough from
# inside db_bench and needs no file system.
#
# Requires the custom e2fsprogs build with assume_storage_prezeroed support.
###############################################################################
set -euo pipefail
DEV="${DEV:-/dev/nvme1n1}"
MNT="${MNT:-/mnt/cosmos}"
MKFS="${MKFS:-mkfs.ext4}"   # point at the custom e2fsprogs build if needed

sudo "$MKFS" -E assume_storage_prezeroed=1 "$DEV"
sudo mkdir -p "$MNT"
sudo mount "$DEV" "$MNT"
echo "Mounted $DEV at $MNT"
