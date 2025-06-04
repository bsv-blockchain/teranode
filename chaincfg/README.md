# chaincfg

The `chaincfg` package provides network parameters and genesis block definitions for Bitcoin SV and related networks. It is essential for initializing and configuring the blockchain state and consensus behavior in Bitcoin SV node implementations.

## Usage

This package is typically imported by other packages or binaries that require access to network parameters, genesis blocks, checkpoints, and consensus rules for supported Bitcoin SV networks (mainnet, testnet, regtest, STN, etc.).

## Features
- Genesis block and coinbase transaction definitions
- Network-specific checkpoints and proof-of-work limits
- Activation heights for protocol upgrades
- Utility functions for network parameter lookup and registration
- Support for custom network registration

## Development

- See `params.go` and `genesis.go` for the main logic and definitions.
- Run tests with `go test ./...` in this directory.

---

For more information, see the main project documentation.