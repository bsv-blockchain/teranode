# tstn UTXO seed

This directory contains a pre-built UTXO snapshot for the `tstn` settings context.
Importing it (seeding) populates a fresh Teranode's stores with the chain state up
to a known block, so the node can start assembling and validating blocks from that
point instead of syncing from genesis.

## What's in this directory

The seed is keyed by a block hash. For this seed the tip is:

```
000000000c130c0bdbb5177ff69b7cc160a74401ec30f874ea7321a708de387a
```

| File | Contents |
| --- | --- |
| `<hash>.utxo-headers` | Block headers (and coinbase txs) up to and including the tip block. |
| `<hash>.utxo-set` | The full UTXO set as of the tip block. |
| `<hash>.*.sha256` | Checksum sidecars used to verify the files before import. |

## What importing does

The `seeder` command performs three writes, in this order:

1. **Headers → blockchain store.** Every block header from the `.utxo-headers`
   file is written to the blockchain store, marked as mined and persisted.
2. **UTXOs → UTXO store.** Every UTXO from the `.utxo-set` file is written to the
   UTXO store, then a `lastProcessed.dat` marker is written to the block store
   recording the tip height.
3. **BlockAssembler checkpoint → blockchain store.** Once the UTXO set has been
   imported successfully, the BlockAssembler state is set to the UTXO-set tip, so
   block assembly resumes on top of the seeded chain rather than from genesis.

The import is safe to re-run: if `lastProcessed.dat` already exists the UTXO pass
is skipped, and if a BlockAssembler checkpoint already exists the seeder refuses to
overwrite a store that is already owned by a running/seeded assembler (use `-force`
to override — see below).

## Prerequisites

**Teranode must NOT be running.** Seeding writes directly into the stores; doing
this while services are live will corrupt state.

The stores referenced by the `tstn` context must be up and reachable *before* you
seed. For `tstn` these are:

| Store | Backend (tstn) | Notes |
| --- | --- | --- |
| Blockchain store | PostgreSQL — `postgres://teranode:teranode@localhost:5432/teranode` | Must be running and migrated. Receives headers and the BlockAssembler checkpoint. |
| UTXO store | Aerospike — `aerospike://localhost:3000/utxo-store?set=utxo` | Must be running. Receives the UTXO set. |
| Block store | File — `file://${DATADIR}/blockstore` (`DATADIR` defaults to `./data`) | Directory must be writable. Receives the `lastProcessed.dat` marker. |

Confirm the exact URLs for your environment:

```bash
grep -E '\.tstn|^blockstore|^utxostore|^blockchain_store' settings.conf
```

## Importing the seed

Build the CLI once from the repository root:

```bash
make build-teranode-cli
```

Copy the seed files to `/tmp` (the directory the seeder reads from), then run the
seeder pointing `-inputDir` at `/tmp` and `-hash` at the seed's tip hash:

```bash
cp seeds/tstn/000000000c130c0bdbb5177ff69b7cc160a74401ec30f874ea7321a708de387a.* /tmp/
SETTINGS_CONTEXT=tstn ./teranode-cli seeder \
  -inputDir /tmp \
  -hash 000000000c130c0bdbb5177ff69b7cc160a74401ec30f874ea7321a708de387a
```

`-inputDir` can be any directory that holds the `<hash>.utxo-headers` and
`<hash>.utxo-set` files; `/tmp` is used here as the working location. Point it at
a different path if your files live elsewhere.

Progress is logged to stdout (headers processed, transactions/UTXOs processed, and
finally the BlockAssembler state that was set). The command exits when both passes
complete.

## Flags

| Flag | Default | Purpose |
| --- | --- | --- |
| `-inputDir` | *(required)* | Directory containing the `<hash>.utxo-headers` and `<hash>.utxo-set` files. |
| `-hash` | *(required)* | Tip block hash — used to locate the input files. |
| `-skipHeaders` | `false` | Skip the header import pass. |
| `-skipUTXOs` | `false` | Skip the UTXO import pass (also skips the BlockAssembler checkpoint). |
| `-force` | `false` | Import even when `lastProcessed.dat` or a BlockAssembler checkpoint already exists. **Only use on a store you intend to overwrite** — forcing over live/seeded state will corrupt it. |

## Validating file integrity

Each data file ships with a `.sha256` sidecar. The checksum covers the **entire
binary file**, including the 8-byte magic header (`U-S-1.0 ` for the UTXO set,
`U-H-2.0 ` for the headers), so a standard `shasum -c` validates both files.
Validate them before importing to rule out truncation or corruption in transit:

```bash
cd /tmp
shasum -a 256 -c 000000000c130c0bdbb5177ff69b7cc160a74401ec30f874ea7321a708de387a.utxo-set.sha256
shasum -a 256 -c 000000000c130c0bdbb5177ff69b7cc160a74401ec30f874ea7321a708de387a.utxo-headers.sha256
```

Expected output:

```
000000000c130c0bdbb5177ff69b7cc160a74401ec30f874ea7321a708de387a.utxo-set: OK
000000000c130c0bdbb5177ff69b7cc160a74401ec30f874ea7321a708de387a.utxo-headers: OK
```

If either check does not print `OK`, the file is damaged — re-fetch it and do not
import.

## Inspecting the files

Use the CLI's `filereader` to decode a seed file's header and metadata without
importing anything. This confirms the file type and shows which block the seed
targets:

```bash
SETTINGS_CONTEXT=tstn ./teranode-cli filereader \
  /tmp/000000000c130c0bdbb5177ff69b7cc160a74401ec30f874ea7321a708de387a.utxo-set
```

For this seed the UTXO-set file reports:

```
file type:                 utxo-set

block hash:                000000000c130c0bdbb5177ff69b7cc160a74401ec30f874ea7321a708de387a
block height:              300
previous block hash:       0000000014fbdef445c999082474c542d3cd721bcef283d0624fbf3b0143d6d7
```

And the headers file:

```bash
SETTINGS_CONTEXT=tstn ./teranode-cli filereader \
  /tmp/000000000c130c0bdbb5177ff69b7cc160a74401ec30f874ea7321a708de387a.utxo-headers
```

```
file type:                 utxo-headers

Reading V2 utxo-headers (with coinbase transactions)
```

Useful flags:

| Flag | Purpose |
| --- | --- |
| `-verbose` | Print every record (each UTXO / header entry), not just the summary. |
| `-checkHeights` | Verify height consistency across the UTXO-headers records. |
| `-useStore` | Resolve the argument as a blob-store key instead of a local file path. |

`filereader` also accepts a bare hash (resolved against the store with `-useStore`)
in place of a path.

## After importing

Start Teranode with the same settings context:

```bash
SETTINGS_CONTEXT=tstn <your normal start command>
```

The node comes up with the blockchain, UTXO set, and BlockAssembler checkpoint all
positioned at the seed's tip block, and continues from there.
