# peercli

The `peercli` package provides a command-line interface for interacting with Bitcoin peers. It allows users to connect to peers, send various Bitcoin protocol messages, and interact with the peer in an interactive mode.

## Usage

This package is typically used as a CLI tool to establish connections with Bitcoin peers and send protocol messages such as "inv", "getdata", "ping", etc.

## Features
- Connect to Bitcoin peers
- Send protocol messages (e.g., "inv", "getdata", "ping")
- Interactive CLI mode for user commands

## Development

- See `main.go` for the main logic and entry points.
- Run tests with `go test ./...` in this directory.

## Example

To connect to a Bitcoin peer and interact with it, use the following commands:

```bash
# Connect to a Bitcoin peer
$ go run main.go connect --address=95.216.243.249:8333

# Once connected, you can send commands interactively
peer-cli> send ping

# Exit the interactive mode
peer-cli> exit
```

---

For more information, see the main project documentation.
