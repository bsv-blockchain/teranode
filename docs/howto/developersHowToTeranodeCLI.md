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
- `catchingblocks` - Catching up on blocks from the network

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
SETTINGS_CONTEXT=dev.[YOUR_CONTEXT] ./teranode-cli filereader [options] [path]
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
- `--force` - Force processing even if `lastProcessed.dat` exists

### Block Data Import/Export

#### Export Blockchain to CSV

```bash
SETTINGS_CONTEXT=dev.[YOUR_CONTEXT] ./teranode-cli export-blocks --file=<file-path>
```

#### Import Blockchain from CSV

```bash
SETTINGS_CONTEXT=dev.[YOUR_CONTEXT] ./teranode-cli import-blocks --file=<file-path>
```

### Block Operations

#### Check Block Template

Check if the current block template is valid:

```bash
SETTINGS_CONTEXT=dev.[YOUR_CONTEXT] ./teranode-cli checkblocktemplate
```

#### Check Block

Fetch and validate a specific block by its hash:

```bash
SETTINGS_CONTEXT=dev.[YOUR_CONTEXT] ./teranode-cli checkblock <blockhash>
```

#### Reconsider Block

Re-validate a block that was previously marked as invalid:

```bash
SETTINGS_CONTEXT=dev.[YOUR_CONTEXT] ./teranode-cli reconsiderblock <blockhash>
```

#### Reset Block Assembly

Reset block assembly state, useful for clearing stuck transactions or resetting mining state:

```bash
SETTINGS_CONTEXT=dev.[YOUR_CONTEXT] ./teranode-cli resetblockassembly [--full-reset]
```

Options:

- `--full-reset` - Perform a full reset including clearing mempool and unmined transactions

### Aerospike Kafka Connector

Read Aerospike CDC (Change Data Capture) from Kafka, optionally filtering by transaction ID:

```bash
SETTINGS_CONTEXT=dev.[YOUR_CONTEXT] ./teranode-cli aerospikekafkaconnector --kafka-url=<kafka-url> [options]
```

Options:

- `--kafka-url` - Kafka broker URL (required, e.g., `kafka://localhost:9092/aerospike-cdc`)
- `--txid` - Filter by 64-character hex transaction ID
- `--namespace` - Filter by Aerospike namespace
- `--set` - Filter by Aerospike set (default: `txmeta`)
- `--stats-interval` - Statistics logging interval in seconds (default: 30)

### UTXO Set Validation

Validate a UTXO set file for integrity and correctness:

```bash
SETTINGS_CONTEXT=dev.[YOUR_CONTEXT] ./teranode-cli validate-utxo-set [--verbose] <utxo-set-file-path>
```

Options:

- `--verbose` - Enable verbose output showing individual UTXOs

### Database Maintenance

#### Fix Chainwork

Fix incorrect chainwork values in the blockchain database:

```bash
SETTINGS_CONTEXT=dev.[YOUR_CONTEXT] ./teranode-cli fix-chainwork --db-url=<database-url> [options]
```

Options:

- `--db-url` - Database URL, e.g. `postgres://...` or `sqlite://...` (required)
- `--dry-run` - Preview changes without updating database (default: true)
- `--batch-size` - Number of updates to batch in a transaction (default: 1000)
- `--start-height` - Starting block height (default: 650286)
- `--end-height` - Ending block height, 0 for current tip (default: 0)

**Warning**: Always run with `--dry-run=true` first to preview changes.

#### Audit Mined Set

List main-chain blocks stored with `mined_set` true whose transactions do not carry the block's ID in the UTXO store, or carry it with their outputs still locked:

```bash
SETTINGS_CONTEXT=dev.[YOUR_CONTEXT] ./teranode-cli audit-mined-set --start-height=<height> --end-height=<height> [--max-blocks=<n>]
```

Releases before PR 2180 could store such a block in two ways: a catch-up block fell from quick validation to full validation after quick validation had reserved its ID, or a `ProcessBlock` caller supplied a `block_id` and the block took full validation. The setMined worker skips every block already marked `mined_set`, so nothing tags the rest of those transactions, and a later block spending one of their outputs can fail with empty parent block IDs or a locked output.

The audit changes no data. It needs the blockchain service running and reads blocks through it. It opens the UTXO store and the subtree store from the node's settings; opening the UTXO store runs that store's normal startup, which creates any missing schema and, on Postgres, can start the index build the node itself runs.

For each block it samples two transactions: the first after the coinbase, and the first transaction of the last subtree, because a failed quick-validation attempt stamps subtrees in order and leaves the later ones untagged. Each block prints as `AFFECTED` or `UNCHECKED`, followed by a summary. `AFFECTED` means a sampled transaction lacks the block's ID, or carries it with its outputs still locked: quick validation creates transactions locked and unlocks them only after the block commits, so an attempt that fell through to full validation left the ones it had stamped locked, as does a commit whose unlock failed after the block was stored. `UNCHECKED` means the subtree file is gone, unreadable or malformed, or no sampled transaction is in the UTXO store, and it is not a pass. A block reported as tagged is not proven clean either, since only two transactions are read. Blocks on a netting UTXO store's below-checkpoint route are skipped, because full validation never stores them. Quick validation stores a block with `mined_set` true and unlocks its outputs just after, so a block being committed while the audit runs can show as locked; run the audit again for that height before acting on it.

Each audited block costs two blockchain gRPC calls, at most two subtree file reads of under 200 bytes each, and at most two UTXO store reads. `--max-blocks` (default 10,000) refuses a wider range unless raised.

Remedy: there is no in-place repair. If the audit reports an affected block, resync the node. The command exits non-zero when it finds one.

Options:

- `--start-height` - First main-chain height to audit (default: 0)
- `--end-height` - Last main-chain height to audit (required)
- `--max-blocks` - Refuse a range holding more heights than this (default: 10000)

### Benchmarking Tools

Four profiling commands for measuring block assembly performance. Each writes CPU and memory profiles to disk.

#### Subtree Benchmark

```bash
SETTINGS_CONTEXT=dev.[YOUR_CONTEXT] ./teranode-cli subtreebench [options]
```

Options:

- `--subtree-size` - Size of subtree (default: 1048576)
- `--producers` - Number of producer goroutines (default: 16)
- `--iterations` - Number of transactions to process (default: 10000000)
- `--duration` - Duration in seconds, 0 for iteration-based (default: 0)
- `--cpu-profile` - CPU profile output file (default: `cpu.prof`)
- `--mem-profile` - Memory profile output file (default: `mem.prof`)

#### Load Unmined Benchmark

```bash
SETTINGS_CONTEXT=dev.[YOUR_CONTEXT] ./teranode-cli loadunminedbench [options]
```

Options:

- `--tx-count` - Number of transactions (default: 1000000)
- `--full-scan` - Use full scan mode
- `--aerospike-url` - Aerospike URL (empty uses testcontainer)
- `--cpu-profile` - CPU profile output file (default: `loadunmined_cpu.prof`)
- `--mem-profile` - Memory profile output file (default: `loadunmined_mem.prof`)

#### Transaction Map Benchmark

```bash
SETTINGS_CONTEXT=dev.[YOUR_CONTEXT] ./teranode-cli txmapbench [options]
```

Options:

- `--subtrees` - Number of subtrees (default: 100)
- `--txs-per-subtree` - Transactions per subtree (default: 1048576)
- `--cpu-profile` - CPU profile output file (default: `createtransactionmap_cpu.prof`)
- `--mem-profile` - Memory profile output file (default: `createtransactionmap_mem.prof`)

#### Remainder Benchmark

```bash
SETTINGS_CONTEXT=dev.[YOUR_CONTEXT] ./teranode-cli remainderbench [options]
```

Options:

- `--subtrees` - Number of subtrees (default: 100)
- `--txs-per-subtree` - Transactions per subtree (default: 1048576)
- `--cpu-profile` - CPU profile output file (default: `processremaindertxanddequeue_cpu.prof`)
- `--mem-profile` - Memory profile output file (default: `processremaindertxanddequeue_mem.prof`)

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

- [Developer Setup Guide](./developerSetup.md)
- [Locally Running Services](locallyRunningServices.md)
- [FSM State Management](../howto/miners/minersHowToInteractWithFSM.md)
