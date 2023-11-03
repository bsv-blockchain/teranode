# ▶️ Developer Guides - Running the Services Locally


## Running the node locally in development

You can run all services in 1 terminal window, using the following command:

```shell
SETTINGS_CONTEXT=dev.[YOUR_USERNAME] go run .
```

_Note - Please make sure you have created the relevant settings for your username ([YOUR_USERNAME]) as part of the installation steps. If you have not done so, please review the Installation Guide link above.

_Note2 - If you use badger or sqlite as the datastore, you need to delete the data directory before running._

```shell
rm -rf data && SETTINGS_CONTEXT=dev.[YOUR_USERNAME] go run .
```

### Running the node in native mode

In standard mode, the node will use the Go secp256k1 capabilities. However, you can enable the "native" mode, which uses the significantly faster native C secp256k1 library. Notice that this is not required or has any advantage in development mode.

```shell
rm -rf data && SETTINGS_CONTEXT=dev.[YOUR_USERNAME] go run -tags native .
```

### Running the node with Aerospike support

If you need support for Aerospike, you need to add "aerospike" to the tags:

```shell
rm -rf data && SETTINGS_CONTEXT=dev.[YOUR_USERNAME] go run -tags native,aerospike .
```

### Running the node with custom settings


You can start the node with custom settings by specifying which components of the node to run.

```shell
rm -rf data && SETTINGS_CONTEXT=dev.[YOUR_USERNAME] go run -tags native,aerospike . [OPTIONS]
```

Where `[OPTIONS]` are the desired components you want to start.

#### Components Options

Each component can be activated by setting its value to `1`, or disabled with `0`. Here's a table summarizing the available components:

| Component       | Option          | Description                                         |
|-----------------|-----------------|-----------------------------------------------------|
| Blockchain      | `-Blockchain=1`     | Start the Blockchain component.                      |
| Block Assembly  | `-BlockAssembly=1`  | Start the Block Assembly process.                    |
| Block Validation| `-BlockValidation=1`| Begin the Block Validation process.                  |
| Validator       | `-Validator=1`      | Activate the Validator.                              |
| Utxo Store      | `-UtxoStore=1`      | Initiate the UTXO Store.                             |
| Tx Meta Store   | `-TxMetaStore=1`    | Start the Transaction Meta Store.                    |
| Propagation     | `-Propagation=1`    | Begin the Propagation process.                       |
| Seeder          | `-Seeder=1`         | Activate the Seeder component.                       |
| Miner           | `-Miner=1`          | Start the Miner component.                           |
| Blob Server     | `-BlobServer=1`     | Initiate the Blob Server.                            |
| Coinbase        | `-Coinbase=1`       | Activate the Coinbase component.                     |
| Bootstrap       | `-Bootstrap=1`      | Start the Bootstrap process.                         |
| P2P             | `-P2P=1`            | Begin the P2P communication process.                 |
| Help            | `-help=1`           | Display the help information.                        |


#### Example:

To start the node with only `Validator`, `UtxoStore`, `Propagation`, and `Seeder` components:

```shell
rm -rf data && SETTINGS_CONTEXT=dev.[YOUR_USERNAME] go run -tags native,aerospike . -Validator=1 -UtxoStore=1 -Propagation=1 -Seeder=1
```

Note - the variable names are not case-sensitive, and can be inputted in any case. For example, `-validator=1` is the same as `-Validator=1`.


## Running specific services locally in development

Although you can run specific services using the command above, you can also run each service individually.

To do this, go to a specific service location (any directory under _service/_). Example:

```shell
cd services/validator
```

and run the service using the following command:

```shell
SETTINGS_CONTEXT=dev.[YOUR_USERNAME] go run .
```

Details on each specific service can be found in their relevant documentation (see sections below  -- **LINK TO RELEVANT SECTIONS ONCE THEY EXIST** --).

## Running specific commands locally in development

Besides services, there are a number of commands that can be executed to perform specific tasks. These commands are located under _cmd/_. Example:

```shell
cd cmd/txblaster
SETTINGS_CONTEXT=dev.[YOUR_USERNAME] go run .
```

Details on each specific command can be found in their relevant documentation (see sections below  -- **LINK TO RELEVANT SECTIONS ONCE THEY EXIST** --).

## Running UI Dashboard locally:

To run the UI Dashboard locally, run the following command:

```shell
make dev-dashboard
```
