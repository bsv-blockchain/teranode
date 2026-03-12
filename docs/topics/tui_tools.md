# TUI Tools

## Index

1. [Overview](#1-overview)
2. [Node Status Monitor](#2-node-status-monitor)
    - [2.1. Dashboard View](#21-dashboard-view)
    - [2.2. Settings View](#22-settings-view)
    - [2.3. Health View](#23-health-view)
    - [2.4. Aerospike View](#24-aerospike-view)
    - [2.5. Keyboard Shortcuts](#25-keyboard-shortcuts)
    - [2.6. Running the Monitor](#26-running-the-monitor)
3. [Interactive Log Viewer](#3-interactive-log-viewer)
    - [3.1. Log Display](#31-log-display)
    - [3.2. Filtering](#32-filtering)
    - [3.3. Error Summary](#33-error-summary)
    - [3.4. Keyboard Shortcuts](#34-keyboard-shortcuts)
    - [3.5. Running the Log Viewer](#35-running-the-log-viewer)
4. [Related Documentation](#4-related-documentation)

## 1. Overview

Teranode includes two terminal-based monitoring tools in `teranode-cli`, both built with the [Bubble Tea](https://github.com/charmbracelet/bubbletea) TUI framework and [Lip Gloss](https://github.com/charmbracelet/lipgloss) for styling:

- **Node Status Monitor** (`teranode-cli monitor`) - a live dashboard showing blockchain state, FSM status, peer connections, service health, and Aerospike statistics.
- **Interactive Log Viewer** (`teranode-cli logs`) - a real-time log tail with per-service color coding, filtering, text search, and transaction ID tracking.

Both tools run in an alternate screen buffer and refresh automatically, providing a lightweight alternative to the [web dashboard](dashboard.md) that requires no browser or additional dependencies.

## 2. Node Status Monitor

The monitor connects to Teranode's gRPC services and polls them every two seconds to display a consolidated view of node health.

### 2.1. Dashboard View

The default view shows five panels arranged in a responsive layout (side-by-side on wide terminals, stacked on narrow ones):

**Blockchain Panel**

| Field | Description |
|-------|-------------|
| Height | Maximum chain height |
| Blocks | Total block count |
| Transactions | Total transaction count (formatted with commas) |
| Avg Block Size | Average block size in human-readable units (KB, MB, etc.) |
| Avg Tx/Block | Average transactions per block |
| Last Block | Time since the most recent block (e.g. "5m ago") |

**FSM State Panel**

Displays the current Finite State Machine state with color coding:

- Green for "running"
- Yellow for "idle", "catching", or "sync" states (sync states also show a spinner)
- White for other states

Below the state, connectivity indicators show whether the Blockchain and P2P services are reachable (green checkmark or red X).

**Peers Panel**

- Connected and total known peer counts
- Top 5 connected peers sorted by height, showing truncated peer ID, height, and reputation score
- Reputation scores are color-coded: green (>= 80), yellow (>= 50), red (< 50)

**Services Health Row**

A compact one-line summary using short labels and status icons:

| Label | Service | Icons |
|-------|---------|-------|
| BC | Blockchain | checkmark = healthy, X = down, circle = not configured |
| VAL | Validator | checkmark = healthy, X = down, circle = not configured |
| BV | Block Validation | checkmark = healthy, X = down, circle = not configured |
| BA | Block Assembly | checkmark = healthy, X = down, circle = not configured |
| ST | Subtree Validation | checkmark = healthy, X = down, circle = not configured |
| P2P | P2P | checkmark = healthy, X = down, circle = not configured |

**Aerospike Summary Row**

A one-line status showing connection state, namespace name, node count, object count, disk usage percentage, and alert indicators for overloads, key contention, low available space, or low memory.

### 2.2. Settings View

Press `s` to switch to a scrollable display of the loaded Teranode settings, grouped by section:

- **General** - version, commit, network, data folder, log level
- **Blockchain** - gRPC address, listen address, HTTP listen, store URL
- **P2P** - gRPC address, listen address, HTTP address, port, listen mode, DHT mode, bootstrap/static peer counts
- **Validator** - gRPC address, listen address
- **Block Assembly** - disabled flag, gRPC address, listen address
- **Kafka** - hosts, port, partitions
- **Aerospike** - host, port
- **Asset** - HTTP address, listen address

The current settings context is shown at the top. Values that are not set appear as "(not set)".

### 2.3. Health View

Press `h` to switch to a detailed service health table:

| Column | Description |
|--------|-------------|
| SERVICE | Service name (Blockchain, Validator, Block Validation, Block Assembly, Subtree Validation, P2P) |
| STATUS | Health status with icon (checkmark OK, X DOWN, circle N/A) |
| LATENCY | Response time in milliseconds (or "-" if not applicable) |
| MESSAGE | Status message (e.g. "OK", "Connection failed", "3 peers connected") |

### 2.4. Aerospike View

Press `a` to switch to a scrollable detailed Aerospike statistics view containing:

**Cluster Info** - node count, open connections, namespace name, node names.

**Per-Node Stats** - for each node: host address, cluster size, uptime, free memory percentage, client connections, batch/scan queue depths.

**Namespace Statistics** (aggregated across nodes for multi-node clusters):

- Disk usage (used/total with percentage)
- Critical flags: `stop_writes`, `clock_skew_stop_writes`, `hwm_breached` (highlighted in red if non-zero)
- Storage metrics: available percentage, memory usage, index usage, cache read percentage
- Object metrics: objects, tombstones, evicted/expired/truncated counts
- Throughput metrics: read/write successes, errors, and timeouts (errors highlighted in red)
- Migration metrics (shown only when non-zero)

**Latencies** - operation latency histograms from the Aerospike server.

### 2.5. Keyboard Shortcuts

| Key | Action |
|-----|--------|
| `q`, `Ctrl+C` | Quit |
| `Esc` | Return to dashboard (or quit if already on dashboard) |
| `r` | Force refresh |
| `s` | Toggle settings view |
| `h` | Toggle health view |
| `a` | Toggle Aerospike view |
| `j` / `Down` | Scroll down (in settings/Aerospike views) |
| `k` / `Up` | Scroll up (in settings/Aerospike views) |
| `g` / `Home` | Scroll to top (in settings/Aerospike views) |
| `G` / `End` | Scroll to bottom (in settings/Aerospike views) |

### 2.6. Running the Monitor

**Build:**

```bash
make build-teranode-cli
```

**Launch:**

```bash
./teranode-cli monitor
```

The monitor reads Teranode's `settings.conf` (and `settings_local.conf` if present) to discover service addresses. At minimum, the Blockchain and P2P gRPC addresses must be configured for the dashboard to display data. Other services (Validator, Block Validation, Block Assembly, Subtree Validation, Aerospike) are optional and will show as "Not configured" if their addresses are not set.

## 3. Interactive Log Viewer

The log viewer tails a Teranode log file in real time, parsing each line into structured fields and rendering them with color coding and alignment.

### 3.1. Log Display

Each parsed log line is displayed with the following columns:

```
HH:MM:SS  LEVEL  SERVICE   Message text...
```

- **Timestamp** - displayed in gray (`HH:MM:SS` format)
- **Level** - color-coded: gray (DEBUG), blue (INFO), yellow (WARN), red (ERROR), magenta (FATAL)
- **Service** - padded to 8 characters, each service gets a distinct color:

| Service | Color |
|---------|-------|
| p2p | Green |
| bchn | Purple |
| validator / valid | Blue |
| prop | Orange |
| ba | Pink |
| rpc | Cyan |
| asset | Gold |
| alert | Red |
| pruner | Light purple |
| legacy | Gray |

Lines that do not match the expected log format are displayed as raw text in gray.

The header shows:

- **Title** ("TERANODE LOGS")
- **Rate sparkline** - a Unicode block-character graph showing log frequency over the last 30 seconds, with a current rate counter (e.g. "42/s"). Toggle with `r`.
- **Status indicator** - "LIVE" (green) or "PAUSED" (yellow)
- **Line counts** - filtered line count and total count (if different)

### 3.2. Filtering

The log viewer supports four types of filters that can be combined:

**Service Filter** (`s`)

Enter a comma-separated list of service names to show only those services. Press `Tab` to autocomplete from discovered service names. Services are discovered automatically as log entries are parsed.

**Log Level Filter** (`+` / `-`)

Increase or decrease the minimum log level to display. The hierarchy is: DEBUG < INFO < WARN < ERROR < FATAL. The default shows all levels.

**Text Search** (`/`)

Enter a search string to filter log entries. Matches are case-insensitive and search across the message, service name, and caller fields. Matching text is highlighted with a yellow background.

**Transaction ID Search** (`t`)

Enter a 64-character hex transaction ID to filter logs to only entries containing that ID. The ID is highlighted in matching lines.

All filters can be cleared at once with `c`. The current filter state is shown in the status bar at the bottom (e.g. "Filter: WARN+ | p2p,validator | "search text" | tx:abcd1234...").

### 3.3. Error Summary

Press `e` to toggle the error summary panel. This shows a 5-minute sliding window of error and warning counts per service, sorted by total count (descending). The panel displays up to 4 services plus a total, in the format:

```
ERRORS (5m): service1: 3E 2W | service2: 1E 0W | Total: 4E 2W
```

Error counts are shown in red and warning counts in yellow.

### 3.4. Keyboard Shortcuts

| Key | Action |
|-----|--------|
| **Navigation** | |
| `j` / `k`, `Up` / `Down` | Scroll up/down one line |
| `g` / `G`, `Home` / `End` | Go to top/bottom |
| `PgUp` / `PgDn` | Page up/down |
| `Ctrl+U` / `Ctrl+D` | Half page up/down |
| **Filtering** | |
| `/` | Open text search |
| `s` | Open service filter |
| `t` | Open transaction ID search |
| `+` / `-` | Increase/decrease minimum log level |
| `c` | Clear all filters |
| `Tab` | Autocomplete service name (in filter mode) |
| **Controls** | |
| `p`, `Space` | Pause/resume auto-scroll |
| `m` | Toggle mouse mode (off enables text selection for copy) |
| `e` | Toggle error summary panel |
| `r` | Toggle rate sparkline graph |
| `?` | Toggle full help screen |
| `q`, `Ctrl+C` | Quit |
| `Esc` | Cancel current input / close help |
| `Enter` | Confirm current input |

### 3.5. Running the Log Viewer

**Build:**

```bash
make build-teranode-cli
```

**Launch:**

```bash
./teranode-cli logs [--file <path>] [--buffer <size>]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--file` | `./logs/teranode.log` | Path to the log file to tail |
| `--buffer` | `10000` | Number of log entries to keep in memory |

The log viewer requires Teranode to be running with log output directed to a file. The recommended way is to use the log rotation script:

```bash
./scripts/run-teranode-with-logrotate.sh
```

The viewer reads the last 1,000 lines on startup and then tails new entries in real time.

## 4. Related Documentation

- [Dashboard](dashboard.md) - web-based monitoring UI
- [Settings Overview](../references/settings.md) - configuration reference for service addresses
- [Blockchain Service](services/blockchain.md) - FSM states and blockchain gRPC API
- [P2P Service](services/p2p.md) - peer management and P2P gRPC API
