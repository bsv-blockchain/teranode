# ▶️ Developer Guides - Running the Services Locally

This section will walk you through the commands and configurations needed to run your services locally for development purposes.

## 🚀 Quickstart: Run All Services

### Prerequisites

Start PostgreSQL before running Teranode:

```shell
# Start PostgreSQL in Docker
./scripts/postgres.sh
```

> **Note**: Development mode uses in-memory Kafka by default (no Docker setup required). For advanced testing with Docker-based Kafka, run `./scripts/kafka.sh` (requires adding `127.0.0.1 kafka-shared` to `/etc/hosts` first). If you configure your settings to use Aerospike for UTXO storage, you'll also need to run:
>
> ```bash
> # Start Aerospike in Docker
> ./scripts/aerospike.sh
> ```

### Start Teranode

Execute all services in a single terminal window with the command below. Replace `[YOUR_CONTEXT]` with your specific development context identifier.

```shell
SETTINGS_CONTEXT=dev.[YOUR_CONTEXT] go run .
```

> **📝 Note:** Confirm that settings for your context are correctly established as outlined in the [Installation Guide](./developerSetup.md).
>
> **⚠️ Warning:** When restarting services, it's recommended to clean the data directory first:
>
> ```shell
> rm -rf data
> ```

## 📝 Advanced Configuration

### Database Backend Configuration

Teranode supports multiple database backends for UTXO storage, configured via settings rather than build tags:

1. **PostgreSQL** (Default for development):

    ```shell
    # Make sure PostgreSQL is running
    ./scripts/postgres.sh

    # Your settings_local.conf should have a PostgreSQL connection string
    utxostore.dev.[YOUR_CONTEXT] = postgres://teranode:teranode@localhost:5432/teranode?blockHeightRetention=5

    # Run with the PostgreSQL backend
    SETTINGS_CONTEXT=dev.[YOUR_CONTEXT] go run .
    ```

2. **SQLite** (Lightweight option):

    ```shell
    # Your settings_local.conf should have an SQLite connection string
    utxostore.dev.[YOUR_CONTEXT] = sqlite:///utxostore?blockHeightRetention=5

    # Run with SQLite backend
    SETTINGS_CONTEXT=dev.[YOUR_CONTEXT] go run .
    ```

3. **Aerospike** (High-performance option):

    > **Warning: Aerospike Requirements**
    >
    > - Requires both the appropriate settings AND the 'aerospike' build tag
    > - See the Aerospike Integration section below
    > - **Important**: Unlike PostgreSQL and SQLite, Aerospike requires the build tag because the Aerospike driver code won't be compiled into the binary without it. If you configure Aerospike in settings but don't use the tag, the application will fail at runtime with an 'unknown database driver' error.

> **Note:** The database backend is determined by the connection string prefix in your settings:
>
> - PostgreSQL: `postgres://`
> - SQLite: `sqlite:///`
> - Aerospike: `aerospike://`

## 🏷️ Build Tags

Teranode supports various build tags that enable specific features or configurations. These tags are specified using the `-tags` flag with the `go run` or `go build` commands.

### Aerospike Integration

To use Aerospike as the UTXO storage backend:

1. First, start the Aerospike Docker container:

    ```shell
    ./scripts/aerospike.sh
    ```

2. Run Teranode with the aerospike tag:

    ```shell
    rm -rf data && SETTINGS_CONTEXT=dev.[YOUR_CONTEXT] go run -tags aerospike .
    ```

### Transaction Metadata Cache Configurations

Teranode supports different transaction metadata cache sizes through build tags:

- **Large Cache (Default)**: Used when no specific tx metadata cache tag is specified

  ```shell
  SETTINGS_CONTEXT=dev.[YOUR_CONTEXT] go run -tags aerospike .
  ```

  > **Note**: The large cache is the default — it is selected by the *absence* of `smalltxmetacache` or `testtxmetacache`. There is no `largetxmetacache` build tag; passing it has no effect.

- **Small Cache**: Reduces memory usage with a smaller transaction metadata cache

  ```shell
  SETTINGS_CONTEXT=dev.[YOUR_CONTEXT] go run -tags aerospike,smalltxmetacache .
  ```

- **Test Cache**: Configured specifically for testing scenarios

  ```shell
  SETTINGS_CONTEXT=dev.[YOUR_CONTEXT] go run -tags aerospike,testtxmetacache .
  ```

### Multiple Tags

You can combine multiple tags by separating them with commas:

```shell
SETTINGS_CONTEXT=dev.[YOUR_CONTEXT] go run -tags aerospike,smalltxmetacache .
```

### Network Configuration

Teranode supports different Bitcoin networks (mainnet, testnet, etc.). This is primarily controlled through settings but can be overridden using the `network` environment variable:

```shell
# Run on testnet
network=testnet SETTINGS_CONTEXT=dev.[YOUR_CONTEXT] go run .

# Run on testnet with Aerospike
network=testnet SETTINGS_CONTEXT=dev.[YOUR_CONTEXT] go run -tags aerospike .
```

> **Note:** The network setting defaults to what's specified in your settings_local.conf under `network.dev.[YOUR_CONTEXT]`. The environment variable overrides this setting.

### Testing Tags

For running various test suites (not typically needed for development):

- `test_tna` - TNA integration test suite (`test/tna/`)
- `test_tec` - TEC integration test suite (`test/tec/`)
- `test_tnd` - TND integration test suite (`test/tnd/`)
- `test_tnf` - TNF integration test suite (`test/tnf/`)
- `test_smoke` - Smoke tests (docker-based, see `test/e2e/docker/`)
- `test_functional` - Functional tests (subset of smoke tests)

### Component Options

Launch the node with specific components using command-line options. This allows you to enable only the components you need for your development tasks.

```shell
rm -rf data && SETTINGS_CONTEXT=dev.[YOUR_CONTEXT] go run -tags aerospike . [OPTIONS]
```

Enable or disable components by setting the corresponding option to `1` or `0`. **Note: Options are case-sensitive and must be lowercase.**

| Component | Option | Description |
| --- | --- | --- |
| Alert | `-alert=1` | Alert system for network notifications |
| Asset | `-asset=1` | Asset handling service |
| Block Assembly | `-blockassembly=1` | Block assembly service |
| Block Persister | `-blockpersister=1` | Block persistence service |
| Block Validation | `-blockvalidation=1` | Block validation service |
| Blockchain | `-blockchain=1` | Blockchain processing service |
| Legacy | `-legacy=1` | Legacy API support |
| P2P | `-p2p=1` | Peer-to-peer networking service |
| Propagation | `-propagation=1` | Data propagation service |
| Pruner | `-pruner=1` | UTXO data pruning service |
| RPC | `-rpc=1` | RPC interface service |
| Subtree Validation | `-subtreevalidation=1` | Subtree validation service |
| UTXO Persister | `-utxopersister=1` | UTXO persistence service |
| Validator | `-validator=1` | Transaction validation service |

#### Additional Options

| Option | Description |
| --- | --- |
| `-all=<1\|0>` | Enable/disable all services unless explicitly overridden by other flags. By default, when no flags are specified, the system behaves as if `-all=1` was set. |
| `-help=1` | Display command-line help information |
| `-wait_for_postgres=1` | Wait for PostgreSQL to be available before starting |
| `-localTestStartFromState=X` | Start blockchain FSM from a specific state for testing |

#### Examples

To start the node with only validation and UTXO storage:

```shell
SETTINGS_CONTEXT=dev.[YOUR_CONTEXT] go run -tags aerospike . -validator=1 -utxopersister=1
```

### Wait For PostgreSQL

If you want Teranode to wait for PostgreSQL to be available before starting:

```shell
SETTINGS_CONTEXT=dev.[YOUR_CONTEXT] go run . -wait_for_postgres=1
```

This is useful in containerized environments or when PostgreSQL might not be immediately ready.

### Health Checks

Teranode exposes health check endpoints on port 8000 (configurable in settings):

- `/health/readiness` - Indicates if the system is ready to accept requests
- `/health/liveness` - Indicates if the system is running properly

### Logging Configuration

Teranode respects the `NO_COLOR` environment variable to disable colored output in logs.

```shell
NO_COLOR=1 SETTINGS_CONTEXT=dev.[YOUR_CONTEXT] go run .
```

#### Component Selection Examples

**Running specific components only:**

To initiate the node with only specific components, such as `Validator`:

```shell
SETTINGS_CONTEXT=dev.[YOUR_CONTEXT] go run -tags aerospike . -validator=1
```

**Disabling all services by default and enabling only specific ones:**

This is particularly useful for development:

```shell
SETTINGS_CONTEXT=dev.[YOUR_CONTEXT] go run -tags aerospike . -all=0 -validator=1 -rpc=1
```

## 🔧 Running Individual Services

You can also run each service on its own:

1. Navigate to a service's directory:

    ```shell
    cd services/validator
    ```

2. Run the service:

    ```shell
    SETTINGS_CONTEXT=dev.[YOUR_CONTEXT] go run .
    ```

## 📜 Running Specific Commands

For executing particular tasks, use commands found under the `cmd/` directory.

## 🖥 Running UI Dashboard

For UI Dashboard:

```shell
make dev-dashboard
```

Remember to replace `[YOUR_CONTEXT]` with your actual username throughout all commands.

This guide aims to provide a streamlined process for running services and nodes during development.

## 🌐 Running a Local Testnet Network

For a complete Docker-based node deployment with an interactive setup wizard,
use **[teranode-quickstart](https://github.com/bsv-blockchain/teranode-quickstart)**.
It supports mainnet, testnet, teratestnet, and regtest.

Quickstart provides:

- `./setup.sh` for first-time network and mode selection
- `./start.sh`, `./stop.sh`, `./status.sh`, and `./logs.sh` for operations
- `./rpc.sh` and `./cli.sh` wrappers for JSON-RPC and `teranode-cli`
- `./seed.sh` for compatible UTXO snapshots
- `./update.sh` for Teranode image version updates

Use this repository for core development, individual service work, and
contributing to Teranode. Use quickstart when you want to run the complete
Docker Compose stack as an operator or integration test environment.

```bash
git clone https://github.com/bsv-blockchain/teranode-quickstart.git
cd teranode-quickstart
./setup.sh
./start.sh
./status.sh
```

## Additional Resources

If you encounter any issues, consult the detailed documentation or reach out to the development team for assistance.
