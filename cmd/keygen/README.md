# keygen

The `keygen` package provides utilities for generating cryptographic keys. It is designed to facilitate the creation of secure private keys and their integration into libp2p hosts for peer-to-peer networking.

## Usage

This package is typically used as a command-line tool to generate Ed25519 private keys, encode them as hex strings, and decode them back for use in libp2p applications.

## Features
- Generate Ed25519 private keys
- Create libp2p hosts using the generated keys

## Development

- See `key_generator.go` for the main logic and entry points.
- Run tests with `go test ./...` in this directory.

---

For more information, see the main project documentation.
