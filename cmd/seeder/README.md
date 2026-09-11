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
corruption/transfer errors, not tampering) and cannot be disabled.

## Development

- See `seeder.go` for the main logic and entry points.
- Run tests with `go test -race -tags testtxmetacache ./...` in this directory, or use `make test` from the project root.

---

For more information, see the main project documentation.
