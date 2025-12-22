# Developer's Guide to Teranode-CLI

Last Modified: 21-May-2025

## Overview

The Teranode-CLI is a command-line interface tool designed for developers to interact with Teranode services during development and testing. Unlike the production environment where you might access it through Docker containers, as a developer you'll build and run it directly on your machine.

This guide provides a comprehensive walkthrough of using the Teranode-CLI in a development environment.

## Building the CLI

Before using Teranode-CLI, you need to build it from source:

```bash
go build -o teranode-cli ./cmd/teranodecli
```

This will create a `teranode-cli` executable in your current directory.

## Basic Usage

```bash
# General format
SETTINGS_CONTEXT=dev.[YOUR_CONTEXT] ./teranode-cli <command> [options]

# Getting help
./teranode-cli
```

### Important Settings Context

All commands require your `SETTINGS_CONTEXT` environment variable to be set correctly. This ensures the CLI uses your development settings:

```bash
# Either set it for the session
export SETTINGS_CONTEXT=dev.[YOUR_CONTEXT]

# Or prefix each command
SETTINGS_CONTEXT=dev.[YOUR_CONTEXT] ./teranode-cli <command>
```

Replace `[YOUR_CONTEXT]` with your specific development context (e.g., `dev.johndoe` or simply `dev`).

## Available Commands

### FSM State Management

One of the most common uses of Teranode-CLI during development is managing the Finite State Machine (FSM) state of your Teranode instance.

#### Getting the Current State

```bash
SETTINGS_CONTEXT=dev.[YOUR_CONTEXT] ./teranode-cli getfsmstate
```

Typical output:

```text
Current FSM State: IDLE
```

#### Setting a New State

```bash
SETTINGS_CONTEXT=dev.[YOUR_CONTEXT] ./teranode-cli setfsmstate --fsmstate <state>
```

Valid states you can issue are:

- `running` - Normal operation mode (processes transactions and creates blocks)
- `idle` - Idle mode (default startup state)

Example to switch to RUNNING state:

```bash
SETTINGS_CONTEXT=dev.[YOUR_CONTEXT] ./teranode-cli setfsmstate --fsmstate running
```

Expected output:

```text
Setting FSM state to: running
FSM state successfully set to: RUNNING
```

### View System Configuration

To inspect your current system settings:

```bash
SETTINGS_CONTEXT=dev.[YOUR_CONTEXT] ./teranode-cli settings
```

This will display a comprehensive list of all settings currently in effect, including which specific settings are overridden by your `[YOUR_CONTEXT]` configuration.

### Data Management Commands

#### Aerospike Reader

Retrieve transaction data from Aerospike using a transaction ID:

```bash
SETTINGS_CONTEXT=dev.[YOUR_CONTEXT] ./teranode-cli aerospikereader <txid>
```

The `<txid>` must be a valid 64-character transaction ID.

#### File Reader

Inspect data files:

```bash
SETTINGS_CONTEXT=dev.[YOUR_CONTEXT] ./teranode-cli filereader [path] [options]
```

Options:

- `--verbose` - Enable verbose output
- `--checkHeights` - Check heights in UTXO headers
- `--useStore` - Use store

#### Bitcoin to UTXO Set Conversion

Convert Bitcoin blockchain data to UTXO set format:

```bash
SETTINGS_CONTEXT=dev.[YOUR_CONTEXT] ./teranode-cli bitcointoutxoset --bitcoinDir=<bitcoin-data-path> --outputDir=<output-dir-path> [options]
```

Options:

- `--bitcoinDir` - Location of Bitcoin data (required)
- `--outputDir` - Output directory for UTXO set (required)
- `--skipHeaders` - Skip processing headers
- `--skipUTXOs` - Skip processing UTXOs
- `--blockHash` - Block hash to start from
- `--previousBlockHash` - Previous block hash
- `--blockHeight` - Block height to start from
- `--dumpRecords` - Dump records from index

#### UTXO Persister Management

Manage UTXO persistence:

```bash
SETTINGS_CONTEXT=dev.[YOUR_CONTEXT] ./teranode-cli utxopersister
```

### Seeder

Seed initial blockchain data:

```bash
SETTINGS_CONTEXT=dev.[YOUR_CONTEXT] ./teranode-cli seeder --inputDir=<input-dir> --hash=<hash> [options]
```

Options:

- `--inputDir` - Input directory for UTXO set and headers (required)
- `--hash` - Hash of the UTXO set / headers to process (required)
- `--skipHeaders` - Skip processing headers
- `--skipUTXOs` - Skip processing UTXOs

### Block Data Import/Export

#### Export Blockchain to CSV

```bash
SETTINGS_CONTEXT=dev.[YOUR_CONTEXT] ./teranode-cli export-blocks --file=<file-path>
```

#### Import Blockchain from CSV

```bash
SETTINGS_CONTEXT=dev.[YOUR_CONTEXT] ./teranode-cli import-blocks --file=<file-path>
```

### Block Template Verification

Check if the current block template is valid:

```bash
SETTINGS_CONTEXT=dev.[YOUR_CONTEXT] ./teranode-cli checkblocktemplate
```

### Interactive Monitoring Tools

The CLI includes two TUI (Terminal User Interface) tools for real-time monitoring and debugging.

#### Log Viewer

View and filter logs in real-time with the interactive log viewer:

```bash
SETTINGS_CONTEXT=dev.[YOUR_CONTEXT] ./teranode-cli logs
```

Options:

- `--file` - Path to log file (default: `./logs/teranode.log`)
- `--buffer` - Number of log entries to keep in memory (default: 10000)

Key features for debugging:

- **Service filtering** (`s`): Filter logs by specific services (e.g., `p2p,validator`)
- **Log level filtering** (`+`/`-`): Adjust minimum severity level
- **Text search** (`/`): Search for specific text in messages
- **Transaction tracking** (`t`): Track a transaction ID across all services
- **Error summary** (`e`): View error counts by service
- **Pause/resume** (`p` or `space`): Pause auto-scroll to examine specific entries

Press `?` for full keyboard shortcuts or `q` to quit.

#### Node Monitor

Monitor node status with a live dashboard:

```bash
SETTINGS_CONTEXT=dev.[YOUR_CONTEXT] ./teranode-cli monitor
```

The monitor displays:

- Blockchain state (height, block count, transactions)
- FSM state with color-coded status
- Connected peers and their reputation scores
- Service health status with latency measurements
- Aerospike cluster statistics

Views:

- **Dashboard** (default): Overview of all node metrics
- **Settings** (`s`): View current configuration
- **Health** (`h`): Detailed service health table
- **Aerospike** (`a`): Cluster and namespace statistics

Press `r` to refresh manually or `q` to quit.

## Common Development Workflows

### Starting a Fresh Development Node

1. Start your Teranode node:

    ```bash
    SETTINGS_CONTEXT=dev.[YOUR_CONTEXT] go run .
    ```

2. Check the initial FSM state:

    ```bash
    SETTINGS_CONTEXT=dev.[YOUR_CONTEXT] ./teranode-cli getfsmstate
    ```

3. Transition to RUNNING state:

    ```bash
    SETTINGS_CONTEXT=dev.[YOUR_CONTEXT] ./teranode-cli setfsmstate --fsmstate running
    ```

4. Verify the FSM state change:

    ```bash
    SETTINGS_CONTEXT=dev.[YOUR_CONTEXT] ./teranode-cli getfsmstate
    ```

### Debugging Tips

- If your teranode-cli commands aren't working, ensure your `SETTINGS_CONTEXT` is correctly set
- Verify the node is actually running before attempting to change its state
- Look for error messages in both the CLI output and your node's logs
- Use the `settings` command to confirm your configuration settings are applied correctly

## Extending the CLI

Developers can extend the Teranode-CLI by adding new commands to the `cmd/teranodecli/teranodecli/cli.go` file. Follow the existing pattern for creating new commands and adding them to the command help map.

## Further Resources

- [Developer Setup Guide](../tutorials/developers/developerSetup.md)
- [Locally Running Services](locallyRunningServices.md)
- [FSM State Management](../howto/miners/minersHowToInteractWithFSM.md)
