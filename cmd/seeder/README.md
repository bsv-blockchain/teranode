# seeder

The `seeder` package is a command-line tool designed to process blockchain headers and UTXO sets. It initializes the seeder service, handles headers and UTXOs, and manages related operations such as profiling and signal handling.

## Usage

This package is typically used to process blockchain data from specified input files and store the results in appropriate stores.

### Features
- Process UTXO headers and sets
- Store processed data in Aerospike
- Handle system signals for graceful termination
- Start a profiler server for debugging

## Development

- See `seeder.go` for the main logic and entry points.
- Run tests with `go test ./...` in this directory.

---

For more information, see the main project documentation.
