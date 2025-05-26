# ▶️ Developer Guides - Running the Services Locally

This section will walk you through the commands and configurations needed to run your services locally for development purposes.

## 🚀 Quickstart: Run All Services

### Prerequisites

Before running Teranode, ensure the required infrastructure services are started:

```shell
# Start Kafka in Docker
./scripts/kafka.sh

# Start PostgreSQL in Docker
./scripts/postgres.sh
```

> **Note:** If you configure your settings to use Aerospike for UTXO storage, you'll also need to run:
> ```bash
> # Start Aerospike in Docker
> ./scripts/aerospike.sh
> ```

### Start Teranode

Execute all services in a single terminal window with the command below. Replace `[YOUR_USERNAME]` with your specific username.

```shell
SETTINGS_CONTEXT=dev.[YOUR_USERNAME] go run .
```

> **📝 Note:** Confirm that settings for your username are correctly established as outlined in the Installation Guide. If not yet done, please review it [here](../tutorials/developers/developerSetup.md).
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
   ```
   # Make sure PostgreSQL is running
   ./scripts/postgres.sh

   # Your settings_local.conf should have a PostgreSQL connection string
   utxostore.dev.[YOUR_USERNAME] = postgres://teranode:teranode@localhost:5432/teranode?blockHeightRetention=5

   # Run with the PostgreSQL backend
   SETTINGS_CONTEXT=dev.[YOUR_USERNAME] go run .
   ```

2. **SQLite** (Lightweight option):
   ```
   # Your settings_local.conf should have an SQLite connection string
   utxostore.dev.[YOUR_USERNAME] = sqlite:///utxostore?blockHeightRetention=5

   # Run with SQLite backend
   SETTINGS_CONTEXT=dev.[YOUR_USERNAME] go run .
   ```

3. **Aerospike** (High-performance option):
   - Requires both the appropriate settings AND the 'aerospike' build tag
   - See the Aerospike Integration section below
   - **Important**: Unlike PostgreSQL and SQLite, Aerospike requires the build tag because the Aerospike driver code won't be compiled into the binary without it. If you configure Aerospike in settings but don't use the tag, the application will fail at runtime with an 'unknown database driver' error.

> **Note:** The database backend is determined by the connection string prefix in your settings:
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
   rm -rf data && SETTINGS_CONTEXT=dev.[YOUR_USERNAME] go run -tags aerospike .
   ```

### Transaction Metadata Cache Configurations

Teranode supports different transaction metadata cache sizes through build tags:

- **Large Cache (Default)**: Used when no specific tx metadata cache tag is specified
  ```shell
  SETTINGS_CONTEXT=dev.[YOUR_USERNAME] go run -tags aerospike .
  ```

- **Small Cache**: Reduces memory usage with a smaller transaction metadata cache
  ```shell
  SETTINGS_CONTEXT=dev.[YOUR_USERNAME] go run -tags aerospike,smalltxmetacache .
  ```

- **Test Cache**: Configured specifically for testing scenarios
  ```shell
  SETTINGS_CONTEXT=dev.[YOUR_USERNAME] go run -tags aerospike,testtxmetacache .
  ```

### Multiple Tags

You can combine multiple tags by separating them with commas:

```shell
SETTINGS_CONTEXT=dev.[YOUR_USERNAME] go run -tags aerospike,smalltxmetacache .
```

### Network Configuration

Teranode supports different Bitcoin networks (mainnet, testnet, etc.). This is primarily controlled through settings but can be overridden using the `network` environment variable:

```shell
# Run on testnet
network=testnet SETTINGS_CONTEXT=dev.[YOUR_USERNAME] go run .

# Run on testnet with Aerospike
network=testnet SETTINGS_CONTEXT=dev.[YOUR_USERNAME] go run -tags aerospike .
```

> **Note:** The network setting defaults to what's specified in your settings_local.conf under `network.dev.[YOUR_USERNAME]`. The environment variable overrides this setting.

### Testing Tags

For running various test suites (not typically needed for development):

- `test_all`: Runs all tests
- `test_smoke_rpc`: Runs smoke tests for RPC functionality
- `test_services`: Tests specific to services
- `test_longlong`: For extended duration tests


### Component Options

Launch the node with specific components using command-line options. This allows you to enable only the components you need for your development tasks.

```shell
rm -rf data && SETTINGS_CONTEXT=dev.[YOUR_USERNAME] go run -tags aerospike . [OPTIONS]
```

Enable or disable components by setting the corresponding option to `1` or `0`. Options are not case-sensitive.

| Component          | Option                      | Description                           |
|--------------------|---------------------------|---------------------------------------|
| Alert              | `-Alert=1`                  | Alert system for network notifications|
| Asset              | `-Asset=1`                  | Asset handling service                |
| Block Assembly     | `-BlockAssembly=1`          | Block assembly service                |
| Block Persister    | `-BlockPersister=1`         | Block persistence service             |
| Block Validation   | `-BlockValidation=1`        | Block validation service              |
| Blockchain         | `-Blockchain=1`             | Blockchain processing service         |
| Legacy             | `-Legacy=1`                 | Legacy API support                    |
| P2P                | `-P2P=1`                    | Peer-to-peer networking service       |
| Propagation        | `-Propagation=1`            | Data propagation service              |
| RPC                | `-RPC=1`                    | RPC interface service                 |
| Subtree Validation | `-SubtreeValidation=1`      | Subtree validation service            |
| UTXO Persister     | `-UTXOPersister=1`          | UTXO persistence service              |
| Validator          | `-Validator=1`              | Transaction validation service        |

#### Additional Options

| Option                        | Description                                     |
|-------------------------------|-------------------------------------------------|
| `-all=<1|0>`                  | Enable/disable all services unless explicitly overridden by other flags. By default (when no flags are specified), the system behaves as if `-all=1` was set. |
| `-help=1`                     | Display command-line help information            |
| `-wait_for_postgres=1`        | Wait for PostgreSQL to be available before starting |
| `-localTestStartFromState=X`  | Start blockchain FSM from a specific state (for testing) |

#### Example Usage:

To start the node with only validation and UTXO storage:

```shell
SETTINGS_CONTEXT=dev.[YOUR_USERNAME] go run -tags aerospike . -Validator=1 -UTXOPersister=1
```

### Wait For PostgreSQL

If you want Teranode to wait for PostgreSQL to be available before starting:

```shell
SETTINGS_CONTEXT=dev.[YOUR_USERNAME] go run . -wait_for_postgres=1
```

This is useful in containerized environments or when PostgreSQL might not be immediately ready.

### Health Checks

Teranode exposes health check endpoints on port 8000 (configurable in settings):

- `/health/readiness` - Indicates if the system is ready to accept requests
- `/health/liveness` - Indicates if the system is running properly

### Logging Configuration

Teranode respects the `NO_COLOR` environment variable to disable colored output in logs.

```shell
NO_COLOR=1 SETTINGS_CONTEXT=dev.[YOUR_USERNAME] go run .
```




#### Example Usage:

**Running specific components only:**

To initiate the node with only specific components, such as `Validator`:

```shell
SETTINGS_CONTEXT=dev.[YOUR_USERNAME] go run -tags aerospike . -Validator=1
```

**Disabling all services by default and enabling only specific ones:**

This is particularly useful for development:

```shell
SETTINGS_CONTEXT=dev.[YOUR_USERNAME] go run -tags aerospike . -all=0 -Validator=1 -RPC=1
```

## 🔧 Running Individual Services

You can also run each service on its own:

1. Navigate to a service's directory:
   ```shell
   cd services/validator
   ```
2. Run the service:
   ```shell
   SETTINGS_CONTEXT=dev.[YOUR_USERNAME] go run .
   ```

## 📜 Running Specific Commands

For executing particular tasks, use commands found under the _cmd/_ directory:

1. Change directory to the command's location:
   ```shell
   cd cmd/txblaster
   ```
2. Execute the command:
   ```shell
   SETTINGS_CONTEXT=dev.[YOUR_USERNAME] go run .
   ```


## 🖥 Running UI Dashboard

For UI Dashboard:

```shell
make dev-dashboard
```

Remember to replace `[YOUR_USERNAME]` with your actual username throughout all commands.

This guide aims to provide a streamlined process for running services and nodes during development.

If you encounter any issues, consult the detailed documentation or reach out to the development team for assistance.
