# Teranode CLI Documentation

## Overview

The teranode-cli is a command-line interface tool for interacting with Teranode services. It provides various commands for maintenance, debugging, and operational tasks.

## Basic Usage

To access the CLI in a Docker container:

```bash
docker exec -it blockchain teranode-cli


Usage: teranode-cli <command> [options]

    Available Commands:
    aerospikekafkaconnector  Read Aerospike CDC from Kafka and filter by txID bin
    aerospikereader      Aerospike Reader
    bitcointoutxoset     Bitcoin to Utxoset
    checkblock           Check block - fetches a block and validates it using the block validation service
    checkblocktemplate   Check block template
    export-blocks        Export blockchain to CSV
    filereader           File Reader
    fix-chainwork        Fix incorrect chainwork values in blockchain database
    getfsmstate          Get the current FSM State
    import-blocks        Import blockchain from CSV
    loadunminedbench     Benchmark loadUnminedTransactions with CPU and memory profiling
    logs                 Interactive log viewer with filtering and search
    monitor              Live TUI dashboard for monitoring node status
    reconsiderblock      Reconsider a block that was previously marked as invalid
    remainderbench       Benchmark processRemainderTransactionsAndDequeue with CPU and memory profiling
    resetblockassembly   Reset block assembly state
    seeder               Seeder
    setfsmstate          Set the FSM State
    settings             Settings
    subtreebench         Benchmark SubtreeProcessor throughput with CPU and memory profiling
    txmapbench           Benchmark CreateTransactionMap with CPU and memory profiling
    utxopersister        Utxo Persister
    validate-utxo-set    Validate UTXO set file

    Use 'teranode-cli <command> --help' for more information about a command

```

## Available Commands

### Configuration

| Command     | Description                  | Key Options |
|-------------|------------------------------|-------------|
| `settings`  | View system configuration    | None        |

### Data Management

| Command                    | Description                                  | Key Options                                   |
|----------------------------|----------------------------------------------|-----------------------------------------------|
| `aerospikekafkaconnector`  | Read Aerospike CDC from Kafka                | `--kafka-url` - Kafka broker URL (required)   |
|                            |                                              | `--txid` - Filter by transaction ID           |
|                            |                                              | `--namespace` - Filter by namespace           |
|                            |                                              | `--set` - Filter by set (default: txmeta)     |
|                            |                                              | `--stats-interval` - Stats interval (default: 30) |
| `aerospikereader`          | Read transaction data from Aerospike         | `<txid>` - Transaction ID to lookup           |
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
| `checkblock`         | Validate an existing block    | `<blockhash>` - Hash of the block to validate                    |
| `checkblocktemplate` | Check block template validity | None                                                             |
| `seeder`             | Seed initial blockchain data  | `--inputDir` - Input directory for data                          |
|                      |                               | `--hash` - Hash of the data to process                           |
|                      |                               | `--skipHeaders` - Skip processing headers                        |
|                      |                               | `--skipUTXOs` - Skip processing UTXOs                            |
|                      |                               | `--force` - Force processing even if lastProcessed.dat exists    |
| `filereader`         | Read and process files        | `--verbose` - Enable verbose output                              |
|                      |                               | `--checkHeights` - Check heights in UTXO headers                 |
|                      |                               | `--useStore` - Use store                                         |
| `validate-utxo-set`  | Validate UTXO set file        | `--verbose` - Enable verbose output showing individual UTXOs     |
| `getfsmstate`        | Get the current FSM state     | None                                                             |
| `setfsmstate`        | Set the FSM state             | `--fsmstate` - Target FSM state                                  |
|                      |                               | &nbsp;&nbsp;Values: running, idle, catchingblocks, legacysyncing |
| `reconsiderblock`    | Reconsider an invalid block   | `<blockhash>` - Hash of the block to reconsider                 |
| `resetblockassembly` | Reset block assembly state    | `--full-reset` - Perform full reset including clearing mempool  |

### Database Maintenance

| Command              | Description                                    | Key Options                                                      |
|----------------------|------------------------------------------------|------------------------------------------------------------------|
| `fix-chainwork`      | Fix incorrect chainwork values in blockchain  | `--db-url` - Database URL (required)                            |
|                      | database                                       | `--dry-run` - Preview changes without updating (default: true)   |
|                      |                                                | `--batch-size` - Updates per transaction (default: 1000)        |
|                      |                                                | `--start-height` - Starting block height (default: 650286)      |
|                      |                                                | `--end-height` - Ending block height (default: 0 for tip)       |

### Interactive Tools

| Command   | Description                                       | Key Options                                                        |
|-----------|---------------------------------------------------|--------------------------------------------------------------------|
| `logs`    | Interactive TUI log viewer with real-time tailing | `--file` - Log file path (default: ./logs/teranode.log)            |
|           |                                                   | `--buffer` - Log entries to keep in memory (default: 10000)        |
| `monitor` | Live TUI dashboard for node monitoring            | None (uses settings from config)                                   |

## Detailed Command Reference

### Aerospike Kafka Connector

```bash
teranode-cli aerospikekafkaconnector --kafka-url=<kafka-url> [options]
```

Reads Aerospike CDC (Change Data Capture) from Kafka and filters by transaction ID.

Options:

- `--kafka-url`: Kafka broker URL (required, e.g., kafka://localhost:9092/aerospike-cdc)
- `--txid`: Filter by 64-character hex transaction ID (optional)
- `--namespace`: Filter by Aerospike namespace (optional)
- `--set`: Filter by Aerospike set (default: txmeta)
- `--stats-interval`: Statistics logging interval in seconds (default: 30)

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

### Check Block

```bash
teranode-cli checkblock <blockhash>
```

Validates an existing block by its hash. This command performs comprehensive validation including:

- Transaction validation
- Merkle tree verification
- Proof of work validation
- Consensus rule checks

**Example:**

```bash
teranode-cli checkblock 000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f
```

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
- `--force`: Force processing even if `lastProcessed.dat` exists

### Reconsider Block

```bash
teranode-cli reconsiderblock <blockhash>
```

Re-validates a block that was previously marked as invalid, allowing it to be reconsidered for inclusion in the active chain.

### Reset Block Assembly

```bash
teranode-cli resetblockassembly [--full-reset]
```

Resets the block assembly state. Useful for clearing stuck transactions or resetting mining state.

Options:

- `--full-reset`: Perform a comprehensive reset including clearing mempool and unmined transactions

### Validate UTXO Set

```bash
teranode-cli validate-utxo-set [--verbose] <utxo-set-file-path>
```

Validates a UTXO set file for integrity and correctness. This tool is useful for ensuring UTXO set integrity and detecting any inconsistencies.

Options:

- `--verbose`: Enable verbose output showing individual UTXOs

**Example:**

```bash
teranode-cli validate-utxo-set --verbose /data/utxos/utxo-set.dat
```

### Fix Chainwork

```bash
teranode-cli fix-chainwork --db-url=<database-url> [options]
```

Fixes incorrect chainwork values in the blockchain database. This command is used for database maintenance and should be used with caution.

Options:

- `--db-url`: Database URL (postgres://... or sqlite://...) (required)
- `--dry-run`: Preview changes without updating database (default: true)
- `--batch-size`: Number of updates to batch in a transaction (default: 1000)
- `--start-height`: Starting block height (default: 650286)
- `--end-height`: Ending block height (0 for current tip) (default: 0)

⚠️ **Warning**: This command modifies blockchain database records. Always run with `--dry-run=true` first to preview changes before applying them to production databases.

### Logs (Interactive Log Viewer)

```bash
teranode-cli logs [--file=<path>] [--buffer=<size>]
```

An interactive TUI (Terminal User Interface) for viewing and filtering Teranode logs in real-time. The viewer supports service filtering, log level filtering, text search, and transaction ID tracking across services.

Options:

- `--file`: Path to log file (default: `./logs/teranode.log`)
- `--buffer`: Number of log entries to keep in memory (default: 10000)

#### Keyboard Shortcuts

**Navigation:**

| Key | Action |
|-----|--------|
| `j` / `k` or `up` / `down` | Scroll line by line |
| `g` / `G` or `home` / `end` | Jump to top / bottom |
| `pgup` / `pgdn` or `b` / `f` | Page up / down |
| `ctrl+u` / `ctrl+d` | Half-page up / down |

**Filtering and Search:**

| Key | Action |
|-----|--------|
| `/` | Enter text search mode |
| `s` | Enter service filter mode (comma-separated) |
| `t` | Enter transaction ID search (64-char hex) |
| `+` / `-` | Increase / decrease minimum log level |
| `c` | Clear all filters |

**Controls:**

| Key | Action |
|-----|--------|
| `p` or `space` | Pause / resume auto-scroll |
| `e` | Toggle error summary panel |
| `r` | Toggle rate graph (logs/second) |
| `m` | Toggle mouse mode (disable for text selection) |
| `?` | Toggle help screen |
| `q` or `ctrl+c` | Quit |

#### Features

- **Real-time tailing**: Automatically follows new log entries as they are written
- **Service filtering**: Filter by one or more services (e.g., `p2p,validator,blockchain`)
- **Log level filtering**: Show only logs at or above a minimum severity (DEBUG, INFO, WARN, ERROR, FATAL)
- **Text search**: Case-insensitive search across log messages
- **Transaction ID tracking**: Track a specific transaction across all services by its 64-character hex ID
- **Error statistics**: View error and warning counts by service over the last 5 minutes
- **Rate monitoring**: Sparkline graph showing log volume over the last 30 seconds

**Example:**

```bash
# View logs from default location
teranode-cli logs

# View logs from a specific file with larger buffer
teranode-cli logs --file=/var/log/teranode/teranode.log --buffer=50000
```

### Monitor (Node Dashboard)

```bash
teranode-cli monitor
```

A live TUI dashboard for monitoring Teranode node status. Displays real-time information about blockchain state, FSM status, connected peers, service health, and Aerospike cluster statistics.

The monitor uses configuration from your settings files (`settings.conf`, `settings_local.conf`) to connect to services.

#### Keyboard Shortcuts

| Key | Action |
|-----|--------|
| `s` | Toggle Settings view |
| `h` | Toggle Health view |
| `a` | Toggle Aerospike view |
| `r` | Manual refresh |
| `j` / `k` or `up` / `down` | Scroll (in Settings/Aerospike views) |
| `g` / `G` or `home` / `end` | Jump to top / bottom |
| `esc` | Return to Dashboard |
| `q` or `ctrl+c` | Quit |

#### Views

**Dashboard (default):**

- **Blockchain panel**: Height, block count, transaction count, average block size, last block age
- **FSM State panel**: Current state (color-coded), service connectivity indicators
- **Peers panel**: Connected peer count, top peers by height with reputation scores
- **Services summary**: Compact health status for all services (BC, VAL, BV, BA, ST, P2P)
- **Aerospike summary**: Connection status, object count, disk usage

**Settings view (`s`):**

Displays all active configuration settings organized by section: General, Blockchain, P2P, Validator, Block Assembly, Kafka, Aerospike, and Asset.

**Health view (`h`):**

Detailed service health table showing:

- Service name and status (OK / DOWN / N/A)
- Response latency in milliseconds
- Status message

**Aerospike view (`a`):**

Comprehensive Aerospike statistics including:

- Cluster info (nodes, connections, namespace)
- Per-node statistics
- Namespace metrics (objects, disk usage, throughput)
- Critical alerts (stop_writes, hwm_breached)
- Latency histograms

#### Status Indicators

- Green (`✓`): Healthy / Running
- Orange: Idle / Syncing / Warning
- Red (`✗`): Down / Error
- Gray (`○`): Not configured

**Example:**

```bash
# Launch the monitor dashboard
teranode-cli monitor
```

## Benchmarking Tools

Four profiling commands for measuring block assembly performance. Each writes CPU and memory profiles to disk for analysis with `go tool pprof`.

### Subtree Benchmark

```bash
teranode-cli subtreebench [options]
```

Benchmarks SubtreeProcessor throughput.

Options:

- `--subtree-size`: Size of subtree (default: 1048576)
- `--producers`: Number of producer goroutines (default: 16)
- `--iterations`: Number of transactions to process (default: 10000000)
- `--duration`: Duration in seconds, 0 for iteration-based (default: 0)
- `--cpu-profile`: CPU profile output file (default: `cpu.prof`)
- `--mem-profile`: Memory profile output file (default: `mem.prof`)

### Load Unmined Benchmark

```bash
teranode-cli loadunminedbench [options]
```

Benchmarks loadUnminedTransactions performance.

Options:

- `--tx-count`: Number of transactions (default: 1000000)
- `--full-scan`: Use full scan mode
- `--aerospike-url`: Aerospike URL (empty uses a testcontainer)
- `--cpu-profile`: CPU profile output file (default: `loadunmined_cpu.prof`)
- `--mem-profile`: Memory profile output file (default: `loadunmined_mem.prof`)

### Transaction Map Benchmark

```bash
teranode-cli txmapbench [options]
```

Benchmarks CreateTransactionMap performance.

Options:

- `--subtrees`: Number of subtrees (default: 100)
- `--txs-per-subtree`: Transactions per subtree (default: 1048576)
- `--cpu-profile`: CPU profile output file (default: `createtransactionmap_cpu.prof`)
- `--mem-profile`: Memory profile output file (default: `createtransactionmap_mem.prof`)

### Remainder Benchmark

```bash
teranode-cli remainderbench [options]
```

Benchmarks processRemainderTransactionsAndDequeue performance.

Options:

- `--subtrees`: Number of subtrees (default: 100)
- `--txs-per-subtree`: Transactions per subtree (default: 1048576)
- `--cpu-profile`: CPU profile output file (default: `processremaindertxanddequeue_cpu.prof`)
- `--mem-profile`: Memory profile output file (default: `processremaindertxanddequeue_mem.prof`)

## Error Handling

The CLI will exit with status code 1 when:

- Invalid commands are provided
- Required arguments are missing
- Command execution fails

## Environment

The CLI is available in all Teranode containers and automatically configured to work with the local Teranode instance.
