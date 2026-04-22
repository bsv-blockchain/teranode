# Multinode Local Testing

Run a configurable N-node teranode network locally with Docker Compose. All nodes form a full-mesh P2P network, each with its own Aerospike, Postgres database, and Kafka topics.

## Quick Start

```bash
# Build the teranode image first (if not already built)
make build

# Start a 5-node network
compose/multinode.sh up 5

# Generate blocks
compose/multinode.sh generate 1,10    # 10 blocks on node 1
compose/multinode.sh generate 1,5 3,5 # 5 blocks on node 1, then 5 on node 3

# Open all dashboards
compose/multinode.sh dashboards

# Check status
compose/multinode.sh status

# Tail logs for a specific node
compose/multinode.sh logs 2

# Tear down
compose/multinode.sh down
```

## Commands

| Command | Description |
|---|---|
| `up <N>` | Generate config and start N nodes (3-10) |
| `down` | Stop and remove all containers and volumes |
| `restart` | Restart containers (picks up config changes after `make gen-multinode`) |
| `status` | Show container status |
| `logs [node]` | Tail logs for all nodes or a specific node number |
| `dashboards` | Open all node dashboards in the browser |
| `generate <node,count> ...` | Generate blocks on specific nodes |

### Chaos commands

| Command | Description |
|---|---|
| `chaos isolate <node>` | Block peer traffic (RPC still works) |
| `chaos heal [node]` | Restore peer traffic, or all nodes if omitted |
| `chaos kill <node>` | Stop a node container |
| `chaos start <node>` | Start a stopped node container |
| `chaos pause <node>` | Freeze a node (simulates hang/GC pause) |
| `chaos unpause <node>` | Unfreeze a paused node |
| `chaos slow <node> <ms>` | Add network latency to a node (requires sudo) |
| `chaos unslow <node>` | Remove added latency from a node |

```bash
# Isolate node 3 from peers (RPC still works, can still generate blocks)
compose/multinode.sh chaos isolate 3

# Restore all isolated nodes
compose/multinode.sh chaos heal

# Simulate a 500ms network delay on node 1
compose/multinode.sh chaos slow 1 500

# Freeze node 2 (simulates a long GC pause or disk stall)
compose/multinode.sh chaos pause 2

# Kill and restart node 4
compose/multinode.sh chaos kill 4
compose/multinode.sh chaos start 4
```

The `isolate`/`heal` commands use `iptables` via `nsenter` to block traffic to other teranode containers while keeping RPC, dashboard, and shared infrastructure (Kafka, Postgres) accessible. The `slow`/`unslow` commands use `tc` (traffic control) via `nsenter`. Both require `sudo` and `iproute2` on the host. The `kill`/`start`/`pause`/`unpause` commands are pure Docker operations with no extra dependencies.

## Architecture

Each `up N` invocation generates a self-contained bundle under `compose/generated/`:

```
compose/generated/
  docker-compose-multinode.yml    # Main compose file
  settings_multinode.conf         # Per-node settings overlay
  postgres/init-multinode.sql     # DB roles and schemas for N nodes
  aerospike/aerospike-{1..N}.conf # Per-node Aerospike config
  open-dashboards.sh              # Browser launcher
  generate-blocks.sh              # Block generation helper
```

### Shared infrastructure (one instance)

- **Postgres** - one server, separate database per node (`teranode1`, `teranode2`, ...)
- **Kafka (Redpanda)** - one broker, per-node topic names via env vars
- **Jaeger** - shared tracing collector

### Per-node infrastructure

- **Aerospike** - one instance per node on ports `3010`, `3020`, ..., `3100`
- **Teranode** - each node runs all services (validator, block assembly, blockchain, P2P, etc.)

### Port scheme

Each node gets a 2000-port host range starting at 20000:

| Node | Host base | Dashboard | RPC | P2P | Health |
|------|-----------|-----------|-----|-----|--------|
| 1 | 20000 | 20090 | 21292 | 21905 | 20000 |
| 2 | 22000 | 22090 | 23292 | 23905 | 22000 |
| 3 | 24000 | 24090 | 25292 | 25905 | 24000 |
| N | 20000+(N-1)*2000 | base+90 | base+1292 | base+1905 | base |

Container-internal ports are unchanged (8000, 8090, 9292, 9905, etc.). The host mapping is `host_base + (container_port - 8000)`.

### P2P mesh

All nodes are wired in a full mesh via static peers. Each node's `p2p_static_peers` lists every other node. Bootstrap peers are disabled so discovery stays deterministic.

### Node identity

Libp2p keypairs are pre-seeded in `compose/cmd/gennodes/peer_keys.json` (10 keys). Indices 1-3 reuse the keys from `compose/settings_test.conf` so a 3-node generated stack uses the same peer IDs as the existing `docker-compose-3blasters.yml` setup. To regenerate or extend the pool:

```bash
go run ./compose/cmd/genpeerkeys -n 20 -o compose/cmd/gennodes/peer_keys.json
```

### Coinbase identification

Each node stamps `/teranodeN/` in its mined blocks via `coinbase_arbitrary_text`, so you can tell which node mined a given block.

## Makefile targets

The wrapper script is the recommended interface, but individual Makefile targets are also available:

```bash
make gen-multinode N=5                  # Generate config only (no docker)
make open-dashboards                    # Open dashboards
make generate-blocks ARGS="1,10 3,5"   # Generate blocks
```

## Troubleshooting

**Containers won't start (name conflict)**
Another compose stack may have containers with the same service names. Run `docker ps -a | grep teranode` and remove stale containers, or use `compose/multinode.sh down` first.

**Settings changes not taking effect**
Teranode reads settings at startup. After regenerating with `make gen-multinode`, restart the containers:

```bash
compose/multinode.sh restart
```

**Data directory permissions**
Containers run as root, so `data/multinode/` files are root-owned. Clean up with:

```bash
sudo rm -rf data/multinode/
```

**Postgres port conflict**
The shared Postgres binds to host port 15432. If that's in use, stop the conflicting service or edit the generated compose file.
