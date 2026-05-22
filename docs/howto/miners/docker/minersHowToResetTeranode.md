# Reset Docker Teranode

Last modified: 28-April-2026

Use quickstart's `clean.sh` script to remove Docker deployment data. Resetting
is destructive and should only be done when you need to resync, reseed, switch
networks, or recover from bad local state.

## Before You Reset

- Make sure you are in the quickstart repository root.
- Back up anything you need from `.env`, external reverse-proxy configuration,
  and any archival data.
- Stop the stack first when possible.

```bash
./stop.sh
```

## Remove Data Only

This removes Docker Compose volumes and preserves `.env`:

```bash
./clean.sh --data-only
```

This is the normal reset path before reseeding or syncing again with the same
configuration.

## Switch Networks

Different networks cannot share UTXO, PostgreSQL, Kafka, or block data. To
switch networks:

```bash
./stop.sh
./clean.sh --data-only
./setup.sh
./start.sh
```

`./setup.sh` rewrites `.env` for the new network.

## Remove Configuration Only

This removes `.env` and keeps Docker volumes:

```bash
./clean.sh --config-only
```

Use this only when you want to rerun setup without deleting existing data.
Changing the configured network without deleting data is not supported.

## Remove Everything

This removes both `.env` and Docker volumes:

```bash
./clean.sh --all
```

For non-interactive automation, add `--force`:

```bash
./clean.sh --all --force
```

## Reseed After Reset

For teratestnet:

```bash
./clean.sh --data-only
./seed.sh 000000002ea94a515ad9fd40d710fd249fe8610acef7b74f459446812d565187
./start.sh
```

For mainnet or standard testnet, provide your own compatible seed source:

```bash
./clean.sh --data-only
./seed.sh <block-hash> /path/to/seed-dir
./start.sh
```

## Verify

After reset and restart:

```bash
./status.sh
./logs.sh blockchain
```

## Related Documentation

- [Syncing the Blockchain](./minersHowToSyncTheNode.md)
- [Start and Stop Docker Teranode](./minersHowToStopStartDockerTeranode.md)
- Quickstart clean script: <https://github.com/bsv-blockchain/teranode-quickstart/blob/main/clean.sh>
