# Teranode CLI Documentation

Last Modified: 4-May-2025

## Overview

The teranode-cli is a command-line interface tool for interacting with Teranode services. It provides various commands for maintenance, debugging, and operational tasks.

## Basic Usage

To access the CLI in a Docker container:

```bash
docker exec -it blockchain teranode-cli


Usage: teranode-cli <command> [options]

    Available Commands:
    aerospikereader      Aerospike Reader
    bitcointoutxoset     Bitcoin to Utxoset
    checkblocktemplate   Check block template
    export-blocks        Export blockchain to CSV
    filereader           File Reader
    getfsmstate          Get the current FSM State
    import-blocks        Import blockchain from CSV
    seeder               Seeder
    setfsmstate          Set the FSM State
    settings             Settings
    utxopersister        Utxo Persister

    Use 'teranode-cli <command> --help' for more information about a command

```

## Available Commands

### Configuration

| Command     | Description                  | Key Options |
|-------------|------------------------------|-------------|
| `settings`  | View system configuration    | None        |

### Data Management

| Command            | Description                          | Key Options                                   |
|--------------------|--------------------------------------|-----------------------------------------------|
| `aerospikereader`  | Read transaction data from Aerospike | `<txid>` - Transaction ID to lookup           |
| `bitcointoutxoset` | Convert Bitcoin data to UTXO set     | `--bitcoinDir` - Location of bitcoin data     |
|                    |                                      | `--outputDir` - Output directory for UTXO set |
|                    |                                      | `--skipHeaders` - Skip processing headers     |
|                    |                                      | `--skipUTXOs` - Skip processing UTXOs         |
|                    |                                      | `--blockHash` - Block hash to start from      |
|                    |                                      | `--previousBlockHash` - Previous block hash    |
|                    |                                      | `--blockHeight` - Block height to start from  |
|                    |                                      | `--dumpRecords` - Dump records from index     |
| `export-blocks`    | Export blockchain data to CSV        | `--file` - CSV file path to export            |
| `import-blocks`    | Import blockchain data from CSV      | `--file` - CSV file path to import            |
| `utxopersister`    | Manage UTXO persistence              | None                                          |

### System Tools

| Command              | Description                   | Key Options                                                      |
|----------------------|-------------------------------|------------------------------------------------------------------|
| `checkblocktemplate` | Check block template validity | None                                                             |
| `seeder`             | Seed initial blockchain data  | `--inputDir` - Input directory for data                          |
|                      |                               | `--hash` - Hash of the data to process                           |
|                      |                               | `--skipHeaders` - Skip processing headers                        |
|                      |                               | `--skipUTXOs` - Skip processing UTXOs                            |
| `filereader`         | Read and process files        | `--verbose` - Enable verbose output                              |
|                      |                               | `--checkHeights` - Check heights in UTXO headers                 |
|                      |                               | `--useStore` - Use store                                         |
| `getfsmstate`        | Get the current FSM state     | None                                                             |
| `setfsmstate`        | Set the FSM state             | `--fsmstate` - Target FSM state                                  |
|                      |                               | &nbsp;&nbsp;Values: running, idle, catchingblocks, legacysyncing |

## Detailed Command Reference

### Aerospike Reader

```bash
teranode-cli aerospikereader <txid>
```

Retrieves transaction data from an Aerospike database using the provided transaction ID.

### Bitcoin to UTXO Set

```bash
teranode-cli bitcointoutxoset --bitcoinDir=<bitcoin-data-path> --outputDir=<output-dir-path> [options]
```

Options:

- `--bitcoinDir`: Location of Bitcoin data (required)
- `--outputDir`: Output directory for UTXO set (required)
- `--skipHeaders`: Skip processing headers
- `--skipUTXOs`: Skip processing UTXOs
- `--blockHash`: Block hash to start from
- `--previousBlockHash`: Previous block hash
- `--blockHeight`: Block height to start from
- `--dumpRecords`: Dump records from index

### File Reader

```bash
teranode-cli filereader [path] [options]
```

Options:

- `--verbose`: Enable verbose output
- `--checkHeights`: Check heights in UTXO headers
- `--useStore`: Use store

### FSM State Management

```bash
teranode-cli getfsmstate
```

Gets the current FSM state of the system.

```bash
teranode-cli setfsmstate --fsmstate=<state>
```

Options:

- `--fsmstate`: Target FSM state (required)
    - Valid values: running, idle, catchingblocks, legacysyncing

### Export Blocks

```bash
teranode-cli export-blocks --file=<path>
```

Exports blockchain data to a CSV file.

Options:

- `--file`: CSV file path to export (required)

### Import Blocks

```bash
teranode-cli import-blocks --file=<path>
```

Import blockchain data from a CSV file.

Options:

- `--file`: CSV file path to import (required)

### Check Block Template

```bash
teranode-cli checkblocktemplate
```

Validates the current block template. Useful for miners to ensure block templates are correctly formed.

### Seeder

```bash
teranode-cli seeder --inputDir=<input-dir> --hash=<hash> [options]
```

Options:

- `--inputDir`: Input directory for UTXO set and headers (required)
- `--hash`: Hash of the UTXO set / headers to process (required)
- `--skipHeaders`: Skip processing headers
- `--skipUTXOs`: Skip processing UTXOs

## Error Handling

The CLI will exit with status code 1 when:

- Invalid commands are provided
- Required arguments are missing
- Command execution fails

## Environment

The CLI is available in all Teranode containers and automatically configured to work with the local Teranode instance.
