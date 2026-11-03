# seeder

The `seeder` package is a command-line tool designed to process blockchain headers and UTXO sets. It initializes the seeder service, handles headers and UTXOs, and manages related operations such as profiling and signal handling.

## Usage

This package is typically used to process blockchain data from specified input files and store the results in appropriate stores.

### Features

- Process UTXO headers and sets
- Store processed data in configurable storage backends (PostgreSQL, SQLite, or Aerospike based on settings)
- Handle system signals for graceful termination
- Start a profiler server for debugging

### Checksum verification

Before consuming a headers or UTXO-set file, the seeder looks for a
`<file>.sha256` checksum sidecar next to it (the format written by the blob
file store and by `bitcointoutxoset`: a hex-encoded SHA-256 digest, optionally
followed by the filename). If the sidecar is present and doesn't match the
file's actual content, the seeder refuses to import it rather than silently
seeding from a corrupted snapshot. If no sidecar is present at all, the import
proceeds with a warning — older snapshots, or ones from a source that never
produced a sidecar, are not blocked. This check is unauthenticated (it catches
corruption/transfer errors, not tampering). Pass `-skipChecksum` only when
the caller has already verified the same files (as a fetch script does after
download); verification reads each file in full.

### Tuning (UTXO import)

The UTXO set is imported in two concurrent passes over the same file: one for transactions that fit a single UTXO store record, one for transactions spanning multiple records (highest unspent output index >= `utxostore_utxoBatchSize`), which take a much slower create path. The first error in either pass stops the import; re-run the seeder to resume. Settings:

| Key | Default | Meaning |
| --- | --- | --- |
| `workerCount` | 16384 | Concurrent creates in the single-record pass (1 to 1,048,576). Must comfortably exceed `utxostore_storeBatcherSize` so store batches fill. |
| `multiRecordWorkerCount` | `seeder_externalStoreConcurrency` | Concurrent creates in the multi-record pass (1 to 1,048,576). |
| `channelSize` | 1000 | Buffer between each pass's reader and its workers. |
| `seeder_externalStoreConcurrency` | 256 | Replaces `utxostore_externalStoreConcurrency` for the seeder run, even when that is set explicitly. `0` or less means unlimited, as for `utxostore_externalStoreConcurrency`; `multiRecordWorkerCount` then defaults to 1024. |
| `seeder_externalStoreFsyncMode` | `data` | fsync mode for a `file://` external store whose URL does not set `fsyncMode`. `data` fsyncs each blob but not its directory entry, so a re-run after a host crash rewrites any blob whose name was lost. When weaker than `full`, the seeder runs syncfs on the external store before writing `lastProcessed.dat`. `none` is faster but not safe to resume after a host crash (not a process crash): wipe and re-seed instead; the seeder logs a warning at start when the effective mode is `none`. An unsupported platform or unusable store path fails before the import starts. |

The per-1M progress log lines report, per pass, how many transactions were read and sent to workers; the multi-record pass count is the number of multi-record transactions.

## Development

- See `seeder.go` for the main logic and entry points.
- Run tests with `go test -race -tags testtxmetacache ./...` in this directory, or use `make test` from the project root.

---

For more information, see the main project documentation.
