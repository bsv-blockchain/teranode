# Diagnose Command

Last Modified: 26-Mar-2026

## Overview

The `diagnose` command is a comprehensive diagnostic tool for Teranode nodes. It performs two types of checks:

- **Health checks** (`--check`): Verifies that running services, infrastructure, and cross-service consistency are all healthy.
- **Configuration checks** (`--config`): Validates settings for common mistakes, security issues, and misconfigurations without needing a running node.

## Usage

```bash
# Health checks (default)
teranode-cli diagnose

# Configuration validation only (no running node needed)
teranode-cli diagnose --config

# Both health and config checks
teranode-cli diagnose --check --config

# JSON output for scripting
teranode-cli diagnose --json

# Combined
teranode-cli diagnose --check --config --json
```

### Exit Codes

| Code | Meaning |
|------|---------|
| 0 | All checks passed |
| 1 | One or more errors/failures |
| 2 | Warnings only (no errors) |

This makes it suitable for use in scripts and CI/CD pipelines:

```bash
teranode-cli diagnose --config || echo "Configuration issues detected"
```

### Settings Context

Like all CLI commands, `diagnose` respects your `SETTINGS_CONTEXT`:

```bash
SETTINGS_CONTEXT=operator ./teranode-cli diagnose --config
```

This is important because many checks are context-aware. For example, security warnings are more strict in production contexts than in dev/test.

## Health Checks (`--check`)

Health checks require a running node. They verify service reachability, infrastructure connectivity, and operational state.

### gRPC Services

Checks connectivity to each gRPC service by creating a client and calling its `Health()` RPC:

| Service | Address Setting | Start Flag |
|---------|----------------|------------|
| Blockchain | `blockchain_grpcAddress` | `startBlockchain` |
| Validator | `validator_grpcAddress` | `startValidator` |
| Block Validation | `blockvalidation_grpcAddress` | `startBlockValidation` |
| Block Assembly | `blockassembly_grpcAddress` | `startBlockAssembly` |
| Subtree Validation | `subtreevalidation_grpcAddress` | `startSubtreeValidation` |
| P2P | `p2p_grpcAddress` | `startP2P` |

If a service address is empty, the check is skipped. If the service is disabled in settings (e.g. `startValidator=false` for the current context), the skip message says so explicitly:

```
Validator gRPC  -  SKIP  -  disabled (startValidator=false)
```

### HTTP Services

Checks HTTP endpoints by sending GET requests:

| Service | Address Setting | Path |
|---------|----------------|------|
| Asset HTTP | `asset_httpListenAddress` | `/health` |
| Propagation HTTP | `propagation_httpListenAddress` | `/health` |
| Block Persister HTTP | `blockpersister_httpListenAddress` | `/health` |
| RPC | `rpc_listener_url` | `/` |
| Health Endpoint | `health_check_httpListenAddress` | `/health` |
| Profiler (pprof) | `profilerAddr` | `/debug/pprof/` |

Auth-protected endpoints (HTTP 401/403) are reported as OK with a note, since a 401 means the service is running and responding.

### Infrastructure

| Check | What It Does |
|-------|-------------|
| Kafka | Connects to brokers via `KAFKA_HOSTS` using Sarama admin client |
| Aerospike | Creates client, verifies `IsConnected()`, reports node count |
| PostgreSQL | TCP dial to `postgres_check_address` |

### Operational State

These checks provide deeper insight into the node's operational health:

| Check | Description |
|-------|-------------|
| **FSM State** | Current state machine state (IDLE, RUNNING, CATCHINGBLOCKS, LEGACYSYNCING) |
| **Chain Tip** | Block height, count, and tip freshness. Only flags as stale (FAIL) when FSM is RUNNING and tip is older than 30 minutes. During catchup or on regtest with old timestamps, an old tip is expected and reported as OK. |
| **Catchup** | Shows sync progress if catching up (peer, blocks validated, percentage) |
| **Block Assembly State** | FSM state, transaction count, queue depth, subtree count, height |
| **Mining Candidate** | Whether a candidate block exists, its height, tx count, and size |
| **P2P Peers** | Connected count, total count, max peer height. FAIL if zero connected. |
| **Banned Peers** | Count of banned peers (shown only if > 0) |

### Cross-Service Consistency

These catch subtle problems where services silently fall out of sync:

| Check | Description |
|-------|-------------|
| **BA/Chain Sync** | Compares block assembly height vs blockchain height. FAIL if drift > 2 blocks and FSM is RUNNING. During catchup, drift is expected and noted as OK. |
| **Chain vs Peers** | Compares our chain height vs best peer height. FAIL if > 10 blocks behind and FSM is RUNNING. During catchup, being behind is expected. |

### Example Output

```
Service Health Checks
=====================
SERVICE                  ADDRESS                STATUS  LATENCY  MESSAGE
Blockchain gRPC          localhost:8087         OK      0us
Validator gRPC           localhost:8081         OK      0us
Block Validation gRPC    localhost:8088         OK      0us
Block Assembly gRPC      localhost:8085         OK      0us
Subtree Validation gRPC  localhost:8086         OK      0us
P2P gRPC                 localhost:9904         OK      1ms
Asset HTTP               http://localhost:8090  OK      2ms
Propagation HTTP         http://localhost:8833  OK      389us
Block Persister HTTP     http://localhost:8083  OK      329us
RPC                      http://:9292           OK      243us    HTTP 401 (auth required)
Health Endpoint          http://localhost:8000  OK      31ms
Profiler (pprof)         http://localhost:9091  OK      942us
Kafka                    localhost:9092         OK      3ms
Aerospike                localhost:3000         OK      21ms     1 node(s)
PostgreSQL               localhost:5432         OK      151us
FSM State                -                      OK      -        RUNNING
Chain Tip                -                      OK      -        height=50000, blocks=50000, tip_age=2m
Catchup                  -                      OK      -        not catching up
Block Assembly State     -                      OK      -        state=running, txs=1523, queue=0, subtrees=3, height=50000
Mining Candidate         -                      OK      -        height=50001, txs=872, size=2.4 MB
P2P Peers                -                      OK      -        3 connected, 5 total, max_peer_height=50000
BA/Chain Sync            -                      OK      -        ba_height=50000, chain_height=50000
Chain vs Peers           -                      OK      -        our_height=50000, best_peer=50000, in sync

  22 OK, 0 FAIL, 0 SKIP
```

## Configuration Checks (`--config`)

Configuration checks analyze the settings file without requiring a running node. They are useful for validating a configuration before deployment.

### Context and Network

Shows the active configuration context and network, so you know which settings are being evaluated.

### Port Conflicts

Scans all 16+ listen addresses across services and detects conflicts. The check is IP-aware:

- `:8080` and `0.0.0.0:8080` both bind all interfaces, so they conflict
- `127.0.0.1:8080` and `10.0.0.1:8080` bind different interfaces, no conflict

Listen addresses checked: Blockchain gRPC/HTTP, Block Assembly gRPC, Block Validation gRPC, Subtree Validation gRPC, Validator gRPC/HTTP, P2P gRPC/HTTP, Propagation gRPC/HTTP, Asset HTTP, Asset Centrifuge, Block Persister HTTP, Faucet HTTP, Health Check HTTP, RPC.

### Security

| Check | Condition | Severity |
|-------|-----------|----------|
| gRPC TLS | `securityLevelGRPC = 0` | WARN in prod, INFO in dev |
| HTTP TLS | `securityLevelHTTP = 0` | WARN in prod, INFO in dev |
| TLS cert files | TLS enabled but `server_certFile` or `server_keyFile` empty | ERROR |
| gRPC admin API key | Empty or < 16 chars | WARN in prod |
| RPC authentication | Both `rpc_user` and `rpc_pass` empty | WARN in prod |

### Kafka

| Check | Condition | Severity |
|-------|-----------|----------|
| Kafka hosts | Empty | ERROR |
| Replication factor | = 1 | WARN in prod |
| Kafka TLS | Disabled | WARN in prod |
| TLS certificates | TLS enabled but no cert files | WARN |
| TLS skip verify | `true` | WARN (MITM risk) |

### RPC

Reports listener URL, max clients (WARN if default 1), and authentication status.

### P2P

| Check | Condition | Severity |
|-------|-----------|----------|
| Listen addresses | Empty | ERROR |
| NAT enabled | `true` | WARN (abuse risk on shared hosting) |
| mDNS enabled | `true` | WARN (abuse risk on shared hosting) |
| Allow private IPs | `true` in prod | WARN |
| Listen mode, DHT mode | Always shown | INFO |
| Static/bootstrap peers | Count if configured | INFO |

### Data Stores

Shows blockchain store type (SQLite/PostgreSQL), PostgreSQL connection pool settings (warns if `max_idle > max_open`), and Aerospike address.

### Retention

Warns if `global_blockHeightRetention < 144` (less than 1 day of blocks).

### Services Summary

Shows how many of the core services are configured vs unconfigured. In non-dev contexts, warns if any service addresses point to localhost.

### Policy

Shows excessive block size limit and minimum mining transaction fee.

### Observability

Shows tracing configuration (enabled/disabled, sample rate, collector URL), profiler address (with warning to ensure not publicly exposed), Prometheus endpoint, and log level.

### Data Folder

Checks that the data folder exists, is a directory, and is writable. Catches permission issues before they cause runtime failures.

### Example Output

```
Configuration Checks
====================
SEVERITY  CHECK                       VALUE                                       RECOMMENDED
INFO      Configuration context       dev
INFO      Network                     mainnet
OK        Port conflicts              none (16 listen addresses checked)
INFO      gRPC TLS                    disabled (level 0)                          Set securityLevelGRPC >= 1 for production
INFO      HTTP TLS                    disabled (level 0)                          Set securityLevelHTTP >= 1 for production
OK        gRPC admin API key          32 chars
OK        Kafka hosts                 localhost:9092
INFO      Kafka replication factor    1                                           3+ for production (data loss risk)
INFO      Kafka TLS                   disabled                                   Enable KAFKA_ENABLE_TLS for production
INFO      RPC listener                http://localhost:9292
WARN      RPC max clients             1                                           10-100+ for production (default is restrictive)
INFO      RPC authentication          disabled (no user/pass)                    Set rpc_user and rpc_pass
OK        P2P listen addresses        /ip4/0.0.0.0/tcp/9905
INFO      P2P listen mode             full
INFO      P2P DHT mode                server
OK        Block height retention      288 blocks
INFO      Blockchain store            sqlite://
INFO      PostgreSQL pool             max_open=50, max_idle=10
INFO      Aerospike                   localhost:3000
INFO      Services configured         7 of 7
INFO      Excessive block size        4.0 GB
INFO      Min mining tx fee           0.00000500
INFO      Tracing                     disabled
INFO      Log level                   INFO
OK        Data folder                 data (writable)

  5 OK, 14 INFO, 1 WARN, 0 ERROR
```

## JSON Output

The `--json` flag outputs a structured JSON object suitable for parsing with `jq` or feeding into monitoring systems:

```bash
teranode-cli diagnose --check --json | jq '.health[] | select(.status == "FAIL")'
```

The JSON structure:

```json
{
  "health": [
    {
      "service": "Blockchain gRPC",
      "address": "localhost:8087",
      "status": "OK",
      "latency": "0us"
    }
  ],
  "config": [
    {
      "severity": "WARN",
      "check": "RPC max clients",
      "current_value": "1",
      "recommended": "10-100+ for production"
    }
  ]
}
```

## Troubleshooting

### Common FAIL Scenarios

| Failure | Likely Cause | Action |
|---------|-------------|--------|
| gRPC service FAIL | Service not running or wrong address | Check service logs, verify address in settings |
| Kafka FAIL | Kafka/Redpanda not running | Start Kafka broker |
| Chain Tip stale (RUNNING) | Node stuck, blockchain service issue | Check blockchain and block validation logs |
| BA/Chain Sync drift | Block assembly lost connection to blockchain | Restart block assembly, check gRPC connectivity |
| Chain vs Peers behind | Node falling behind network | Check catchup status, peer connectivity |
| P2P Peers = 0 | No peers discovered | Check P2P listen addresses, bootstrap/static peers, firewall |
| Port conflict | Two services on same port | Change one service's listen address |

### Using with Different Contexts

To validate a production configuration from your dev machine:

```bash
SETTINGS_CONTEXT=operator teranode-cli diagnose --config
```

This evaluates all context-aware checks (security, Kafka replication, localhost warnings) using the operator context's settings.
