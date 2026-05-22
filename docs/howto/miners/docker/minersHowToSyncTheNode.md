# Sync Docker Teranode

Last modified: 28-April-2026

Docker deployments sync through the
[teranode-quickstart](https://github.com/bsv-blockchain/teranode-quickstart)
workflow. You can either let Teranode sync from the network or seed it from a
compatible UTXO snapshot before startup.

## Choose a Sync Method

| Method | Best for | Command path |
| --- | --- | --- |
| Network sync | Fresh installs without a seed source | `./start.sh` |
| Snapshot seeding | Faster initial sync when a compatible snapshot is available | `./seed.sh ... && ./start.sh` |
| Existing Teranode data | Recovery from your own backup | Restore volumes, then `./start.sh` |

Snapshots are usually pruned. They speed up UTXO initialization, but they do
not provide full historical transaction data unless the source explicitly
contains it. Enable `blockpersister` before syncing if you need raw historical
block data for an explorer, indexer, or archive.

## Network Sync

Network sync needs no seed data:

```bash
./setup.sh
./start.sh
```

Monitor progress:

```bash
./status.sh
./logs.sh blockchain
./rpc.sh getblockchaininfo
```

Mainnet network sync can take days and depends on hardware, bandwidth, peer
quality, and current chain activity.

## Seed from a Snapshot

Seeding writes UTXO state before the full stack starts. Start from a clean data
volume:

```bash
./stop.sh
./clean.sh --data-only
```

### Teratestnet

Teratestnet has a canonical snapshot. Quickstart derives the URL from the block
hash:

```bash
./seed.sh 000000002ea94a515ad9fd40d710fd249fe8610acef7b74f459446812d565187
./start.sh
```

### Mainnet and Testnet

Mainnet and standard testnet do not have a canonical public snapshot URL. Bring
your own compatible seed directory or URL:

```bash
./seed.sh <block-hash> /path/to/seed-dir
./start.sh
```

or:

```bash
./seed.sh <block-hash> https://example.com/path/to/snapshot.zip
./start.sh
```

The block hash must match the snapshot. The snapshot network must match the
configured `.env` network.

### Legacy SV Node Export

If your seed source is an existing SV Node data directory, export it to
quickstart-compatible seed files with the Teranode image. Stop SV Node
gracefully before reading its `blocks` and `chainstate` directories:

```bash
bitcoin-cli stop
```

From the quickstart repository root, load the pinned Teranode version and write
the export to a local seed directory:

```bash
set -a
. ./.env
set +a

mkdir -p /path/to/teranode-seed

docker run --rm \
  --entrypoint /app/teranode-cli \
  -v /path/to/svnode-data:/svnode:ro \
  -v /path/to/teranode-seed:/seed \
  ghcr.io/bsv-blockchain/teranode:"$TERANODE_VERSION" \
  bitcointoutxoset --bitcoinDir=/svnode --outputDir=/seed
```

Use the block hash from the generated seed filenames when loading quickstart:

```bash
./seed.sh <block-hash> /path/to/teranode-seed
./start.sh
```

## Seed from `.env`

You can also configure seed values in `.env`:

```env
SEED_HASH=<block-hash>
SEED_DIR=/path/to/seed-dir
```

Then run:

```bash
./seed.sh
./start.sh
```

Use `SEED_URL` instead of `SEED_DIR` when the seed is a downloadable ZIP file.

## Restore Existing Data

If you maintain backups of the quickstart Docker volumes, restore them while the
stack is stopped, keep `.env` aligned with the restored network and version,
then start normally:

```bash
./start.sh
./status.sh
```

Do not restore data from one network into a configuration for another network.

## FSM State

`./start.sh` performs the normal FSM transition. If a startup race leaves the
node in `INIT`, set the state manually:

```bash
./cli.sh setfsmstate --fsmstate RUNNING
./cli.sh getfsmstate
```

## Troubleshooting Sync

- Check container health with `./status.sh`.
- Check service logs with `./logs.sh blockchain`, `./logs.sh legacy`, or
  `./logs.sh p2p`.
- Confirm the configured network in `.env`.
- Confirm that seed data matches the configured network and block hash.
- For full mode, confirm that public asset and P2P endpoints pass the
  reachability check from `./start.sh`.
- If local state is inconsistent, reset with `./clean.sh --data-only` and seed
  or sync again.

## Related Documentation

- [Install Teranode with Docker](./minersHowToInstallation.md)
- [Reset Docker Teranode](./minersHowToResetTeranode.md)
- Quickstart network notes: <https://github.com/bsv-blockchain/teranode-quickstart/blob/main/docs/NETWORKS.md>
