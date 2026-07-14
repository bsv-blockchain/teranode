# Disk IBD Benchmark

Measure how long Teranode takes to ingest a chain directly from a stopped
SV Node datadir, with the P2P network removed from the picture.

## Prerequisites

- A stopped SV Node (`bitcoind`) datadir containing `blocks/index` and
  `blocks/blk*.dat`. The node process must not be running. A crashed/dirty
  datadir works — the sync stops at the last sealed block-index entry.
- A clean Teranode data volume (so the benchmark starts from genesis).

## Run

Set the disk-sync settings (env or settings.conf):

```
legacy_diskSyncDir=/path/to/svnode/datadir
legacy_diskSyncStopAtHeight=100000   # 0 = sync to the disk tip
```

Start Teranode normally. The legacy service detects `diskSyncDir`, disables
P2P, and feeds blocks from disk through the full validation pipeline. Progress
and a final summary (blocks, txs, GB, wall-time, blocks/s) are logged with the
`[DiskSync]` prefix.

## Notes

- Below the highest checkpoint, validation uses the same quick path as a real
  legacy IBD, so the timing reflects real-world sync time.
- To benchmark "time to reach block N", set `legacy_diskSyncStopAtHeight=N`.
- The datadir is read-only and is opened without replaying the LevelDB
  write-ahead log, so a dirty/crashed node's files are safe to read; the sync
  simply stops at the last sealed block.

## Smoke verification (manual, needs a real datadir)

On a machine with a stopped SV Node Testnet datadir, run a daemon with
`legacy_diskSyncDir` set and `legacy_diskSyncStopAtHeight=1000`. Confirm:

- `[DiskSync] feeding N blocks ...` is logged,
- the run completes with a `[DiskSync] done:` summary,
- `getblockchaininfo` reports best height 1000.

If the run fails immediately with a `block magic ... does not match` error at the
first block, the `nDataPos` framing offset is wrong for this datadir's format.
That is a bug — file an issue with the datadir's SV Node version so the
`blk*.dat` framing handling in `services/legacy/diskblocks/reader.go` can be
corrected against a fixture, rather than patching the offset locally.
