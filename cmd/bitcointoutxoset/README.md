# bitcointoutxoset

Extracts and converts the UTXO set from an SV Node (bitcoind) data directory into Teranode's native format. Reads LevelDB (`chainstate` + `blocks`) and writes `{blockhash}.utxo-headers` and `{blockhash}.utxo-set` files consumed by the Teranode `seeder` command.

## Usage

Invoked via `teranode-cli`:

```bash
teranode-cli bitcointoutxoset --bitcoinDir=<bitcoin-data-path> --outputDir=<output-dir-path> [options]
```

The SV Node must be gracefully shut down (`bitcoin-cli stop`) **and the LevelDB WAL flushed** before export — see [Prerequisite](#prerequisite-bitcoind-must-be-cleanly-shut-down-with-wal-flushed) below. For the full export and seeding workflow, see the [Syncing the Blockchain Guide](../../docs/howto/miners/docker/minersHowToSyncTheNode.md#legacy-sv-node-export).

## Prerequisite: bitcoind must be cleanly shut down with WAL flushed

LevelDB writes go first to a write-ahead log (`*.log` file in the db directory) and an in-memory memtable. The memtable is later flushed to a sealed sstable (`*.ldb`) and the WAL discarded. On a clean shutdown LevelDB seals the memtable into an sstable so the WAL is empty; on a crash the WAL is replayed at the next writable open to recover unsealed writes.

`bitcointoutxoset` opens `<datadir>/blocks/index/` and `<datadir>/chainstate/` LevelDBs **read-only**, which does **not** replay the WAL — it only reads sealed sstables and the manifest. Unflushed WAL entries are invisible and can produce:

```text
ERROR | bitcoin_to_utxo_set.go:204 | teranode-cli| Could not write headers: PROCESSING (4): chainstate tip block not found in index: <hash>
```

A single `bitcoin-cli stop` does not always seal the WAL — bitcoind may log `Shutdown: done` while leaving multi-megabyte `*.log` files behind.

### Pre-flight

1. `bitcoin-cli -datadir=<datadir> stop`, wait for `Shutdown: done`.
2. Check WAL sizes:

    ```bash
    ls -lh <datadir>/blocks/index/*.log <datadir>/chainstate/*.log
    ```

    Each should be sub-kilobyte. That's the normal empty-WAL state.
3. If either is megabytes, bounce offline to force a seal:

    ```bash
    bitcoind -datadir=<datadir> -listen=0 -connect=0 -daemon
    # wait for "init message: Done loading" in debug.log
    bitcoin-cli -datadir=<datadir> stop
    # wait for "Shutdown: done"
    ```

    `-listen=0 -connect=0` keep the chain tip stable during the bounce.
4. Re-check `.log` sizes, then run the seeder.

## Flags

- `--bitcoinDir` — SV Node data directory (required, must contain `blocks` and `chainstate`)
- `--outputDir` — output directory for generated files (required)
- `--skipHeaders` — skip processing block headers
- `--skipUTXOs` — skip processing UTXOs
- `--blockHash` — block hash to start from
- `--previousBlockHash` — previous block hash
- `--blockHeight` — block height to start from
- `--dumpRecords` — dump N records from index for inspection

## Development

- Main logic: `bitcoin_to_utxo_set.go`
- Run tests: `go test -race -tags testtxmetacache ./...` in this directory, or `make test` from project root.

---

For more information, see the main project documentation.
