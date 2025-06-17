# utxopersister

The `utxopersister` package provides the implementation for persisting UTXO (Unspent Transaction Output) data.

## Usage

This package is typically used as a service to persist UTXO data from a blockchain source into a storage backend, such as a blob store or a direct blockchain store. It supports both direct blockchain store interactions and blockchain client-based operations, depending on the configuration.

## Features
- Persist UTXO data efficiently
- Support for blob stores and direct blockchain stores
- Includes tracing and optional HTTP profiling servers

## Development

- See `utxo_persister.go` for the main logic and entry points.
- Run tests with `go test ./...` in this directory.

---

For more information, see the main project documentation.
