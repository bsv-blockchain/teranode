# Chain Integrity Checker

The `Chain Integrity Checker` tool verifies the integrity of a local blockchain generated through testing. It is not intended for use on mainnet.

## Usage

This tool is typically used to ensure the correctness of blockchain data after generating and mining blocks locally. Follow these steps:

1. Remove old data:
   ```shell
   rm -rf data
   ```

2. Run the node for at least 110 blocks:
   ```shell
   docker compose --profile chainintegrity -f compose/docker-compose-host.yml up -d
   ```

3. Stop the node after mining 110+ blocks:
   ```shell
   docker compose -f compose/docker-compose-host.yml down teranode-1 teranode-2 teranode-3
   ```

4. Run the chain integrity checker:
   ```shell
   go run compose/cmd/chainintegrity/main.go --logfile=chainintegrity --debug
   ```

5. Cleanup:
   ```shell
   docker compose -f compose/docker-compose-host.yml down
   ```

## Features
- Verify the integrity of a local blockchain.
- Debugging and inspection of blockchain data.
- Logs detailed information for analysis.

## Development

- See `main.go` in the `compose/cmd/chainintegrity` directory for the main logic.
- Run tests with `go test ./...` in this directory.

---

For more information, see the main project documentation.
