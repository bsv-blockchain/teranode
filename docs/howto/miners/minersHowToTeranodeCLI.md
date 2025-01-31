# Teranode CLI Documentation

Last Modified: 31-January-2024

## Overview

The teranode-cli is a command-line interface tool for interacting with Teranode services. It provides various commands for maintenance, debugging, and operational tasks.

## Basic Usage


To access the CLI in a Docker container:
```
docker exec -it blockchain teranode-cli


Usage: teranode-cli <command> [options]

    Available Commands:
    aerospikereader      Aerospike Reader
    assemblyblaster      Assembly Blaster
    bitcoin2utxoset      Bitcoin 2 Utxoset
    blockchainstatus     Blockchain Status
    chainintegrity       Chain Integrity
    filereader           File Reader
    propagation_blaster  Propagation Blaster
    recover_tx           Recover one or more transactions by TXID and block height
    s3_blaster           Blaster for S3
    s3inventoryintegrity S3 Inventory Integrity
    seeder               Seeder
    settings             Settings
    txblockidcheck       Transaction Block ID Check
    unspend              Unspend a transaction by its TXID
    utxopersister        Utxo Persister

    Use 'teranode-cli <command> --help' for more information about a command

```

## Available Commands

### Core Operations

| Command            | Description               | Key Options                                         |
|--------------------|---------------------------|-----------------------------------------------------|
| `settings`         | View settings             | None                                                |
| `blockchainstatus` | Monitor blockchain status | `--miners`, `--refresh`                             |
| `chainintegrity`   | Verify chain integrity    | `--interval`, `--threshold`, `--debug`, `--logfile` |

### Transaction Management

| Command          | Description                          | Key Options                                    |
|------------------|--------------------------------------|------------------------------------------------|
| `unspend`        | Unspend a transaction (⚠️ Dangerous) | `--txid`                                       |
| `recover_tx`     | Recover transactions (⚠️ Dangerous)  | `<txID> <blockHeight> [spending_txids]`        |
| `txblockidcheck` | Verify transaction block IDs         | `--txhash`, `--utxostore`, `--blockchainstore` |

### Data Storage & Integrity

| Command                | Description                  | Key Options                        |
|------------------------|------------------------------|------------------------------------|
| `aerospikereader`      | Read from Aerospike database | `<txid>`                           |
| `s3_blaster`           | Test S3 operations           | `--workers`, `--usePrefix`         |
| `s3inventoryintegrity` | Verify S3 inventory          | `--verbose`, `--quick`, `-d`, `-f` |

### UTXO Management

| Command           | Description                      | Key Options                   |
|-------------------|----------------------------------|-------------------------------|
| `utxopersister`   | Manage UTXO persistence          | None                          |
| `bitcoin2utxoset` | Convert Bitcoin data to UTXO set | `--bitcoinDir`, `--outputDir` |

### System Tools

| Command       | Description                  | Key Options                   |
|---------------|------------------------------|-------------------------------|
| `seeder`      | Seed initial blockchain data | `--inputDir`, `--hash`        |
| `filereader`  | Read and process files       | `--verbose`, `--checkHeights` |
| `getfsmstate` | Get FSM state                | None                          |
| `setfsmstate` | Set FSM state                | `--fsmstate`                  |

## Detailed Command Reference

### Dangerous Commands ⚠️

Some commands are marked as dangerous and require explicit confirmation:
- `unspend`
- `recover_tx`

These commands will prompt for confirmation by requiring you to type the command name.

### Blockchain Status
```bash
teranode-cli blockchainstatus --miners=<miner-list> --refresh=<seconds>
```
- `--miners`: Comma-separated list of blockchain miners to monitor
- `--refresh`: Refresh rate in seconds (default: 5)

### Chain Integrity
```bash
teranode-cli chainintegrity [options]
```
Options:
- `--interval`: Check interval in seconds (default: 10)
- `--threshold`: Alert threshold (default: 5)
- `--debug`: Enable debug logging
- `--logfile`: Log file path (default: chainintegrity.log)

### Transaction Recovery
```bash
teranode-cli recover_tx <txID> <blockHeight> [spending_txids] --simulate
```
- `--simulate`: Run in simulation mode without making changes

## Error Handling

The CLI will exit with status code 1 when:
- Invalid commands are provided
- Required arguments are missing
- Command execution fails

## Environment

The CLI is available in all Teranode containers and automatically configured to work with the local Teranode instance.
