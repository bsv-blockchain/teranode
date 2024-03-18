# ▶️ Developer Guides - Running the Services Locally

This section will walk you through the commands and configurations needed to run your services locally for development purposes.

## 🚀 Quickstart: Run All Services

Execute all services in a single terminal window with the command below. Replace `[YOUR_USERNAME]` with your specific username.

```shell
SETTINGS_CONTEXT=dev.[YOUR_USERNAME] go run .
```

> **📝 Note:** Confirm that settings for your username are correctly established as outlined in the Installation Guide. If not yet done, please review it [here](developerSetup.md).
>
> **⚠️ Warning:** When using BadgerDB or SQLite, the data directory must be deleted before rerunning the services:
>
> ```shell
> rm -rf data
> ```

## 🛠 Advanced Configuration

### Native Mode

For a performance boost during development, enable native mode to use the C secp256k1 library. (This step is optional for development.)

```shell
rm -rf data && SETTINGS_CONTEXT=dev.[YOUR_USERNAME] go run -tags native .
```

### Aerospike Integration

Add Aerospike support by including the "aerospike" tag.

```shell
rm -rf data && SETTINGS_CONTEXT=dev.[YOUR_USERNAME] go run -tags native,aerospike .
```

### Custom Settings

Launch the node with specific components using the `[OPTIONS]` parameter.

```shell
rm -rf data && SETTINGS_CONTEXT=dev.[YOUR_USERNAME] go run -tags native,aerospike . [OPTIONS]
```

Enable or disable components by setting the corresponding option to `1` or `0`. Options are not case-sensitive.


| Component          | Option                 | Description                           |
|--------------------|------------------------|---------------------------------------|
| Blockchain         | `-Blockchain=1`        | Start the Blockchain component.       |
| Block Assembly     | `-BlockAssembly=1`     | Start the Block Assembly process.     |
| Block Validation   | `-BlockValidation=1`   | Begin the Block Validation process.   |
| Subtree Validation | `-SubtreeValidation=1` | Begin the Subtree Validation process. |
| Validator          | `-Validator=1`         | Activate the Validator.               |
| Utxo Store         | `-UtxoStore=1`         | Initiate the UTXO Store.              |
| Tx Meta Store      | `-TxMetaStore=1`       | Start the Transaction Meta Store.     |
| Propagation        | `-Propagation=1`       | Begin the Propagation process.        |
| Seeder             | `-Seeder=1`            | Activate the Seeder component.        |
| Miner              | `-Miner=1`             | Start the Miner component.            |
| Asset Service      | `-Asset=1`             | Initiate the Asset Service.           |
| Coinbase           | `-Coinbase=1`          | Activate the Coinbase component.      |
| BlockPersister     | `-BlockPersister=1`    | Start the Block Persister process.    |
| P2P                | `-P2P=1`               | Begin the P2P communication process.  |
| Help               | `-help=1`              | Display the help information.         |



#### Example Usage:

To initiate the node with only specific components, such as `Validator` and `UtxoStore`:

  ```shell
  SETTINGS_CONTEXT=dev.[YOUR_USERNAME] go run -tags native,aerospike . -Validator=1 -UtxoStore=1
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

Each service has dedicated documentation with more detailed instructions. [See service documentation (TODO)](#service-documentation-link).

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

More information on commands is available in their respective documentation. [See command documentation](#command-documentation-link).

## 🖥 Running UI Dashboard

For UI Dashboard:

```shell
make dev-dashboard
```

Remember to replace `[YOUR_USERNAME]` with your actual username throughout all commands. This guide aims to provide a streamlined process for running services and nodes during development. If you encounter any issues, consult the detailed documentation or reach out to the development team for assistance.
