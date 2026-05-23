# Install Teranode with Docker

Last modified: 28-April-2026

The supported Docker path for a single-host Teranode deployment is the
[teranode-quickstart](https://github.com/bsv-blockchain/teranode-quickstart)
repository. It wraps Docker Compose with scripts for setup, startup, status
checks, RPC access, seeding, updates, and cleanup.

Use this guide when you want to run Teranode on one host for testing, network
participation, or operational evaluation. Use the Kubernetes operator instead
for multi-node production deployments or horizontally scaled configurations.

## Prerequisites

- Docker and Docker Compose v2
- Git
- A stable internet connection
- Enough CPU, memory, and SSD storage for the selected network

| Network | Recommended RAM | Minimum RAM | Disk | CPU cores |
| --- | --- | --- | --- | --- |
| mainnet | 256 GB | 128 GB | 2 TB+ | 16+ |
| testnet | 32 GB | 16 GB | 300 GB | 8+ |
| teratestnet | 32 GB | 16 GB | 100 GB | 8+ |
| regtest | 8 GB | 4 GB | 20 GB | 4+ |

Mainnet requires substantial memory because Aerospike stores the UTXO index in
memory and Teranode runs multiple services in parallel. SSD storage is strongly
recommended for every network.

## Install

Clone the quickstart repository and run the setup script:

```bash
git clone https://github.com/bsv-blockchain/teranode-quickstart.git
cd teranode-quickstart
./setup.sh
```

`setup.sh` is interactive. It checks host requirements, asks which network to
run, asks whether the node should use listen-only or full mode, generates RPC
credentials, and writes the local `.env` file.

Read the quickstart network notes before choosing a network:

```bash
less docs/NETWORKS.md
```

## Start

Start the stack from the quickstart repository root:

```bash
./start.sh
```

The script runs Docker Compose with the profiles from `.env`, waits for the
services to become healthy, and moves the node into the expected FSM state.

Check the deployment:

```bash
./status.sh
./rpc.sh getblockcount
./cli.sh getfsmstate
```

For logs:

```bash
./logs.sh
./logs.sh blockchain
```

## What Runs

Quickstart starts the Teranode microservices plus the local dependencies they
need:

| Component | Purpose |
| --- | --- |
| Teranode services | Blockchain, asset, propagation, RPC, validation, and related services |
| PostgreSQL | Blockchain state and indexes |
| Redpanda | Kafka-compatible event streaming |
| Aerospike | UTXO store |
| Nginx asset cache | Reverse proxy for asset API access |
| Prometheus and Grafana | Metrics and dashboards (optional, `monitoring` profile) |
| Kafka Console | Kafka topic inspection (optional, `monitoring` profile) |

The `COMPOSE_PROFILES` setting in `.env` selects which optional services run.
The default is `legacy,p2p,monitoring`. Drop `monitoring` for a leaner
node-only stack, or add `blockpersister` for archival mode (see [Pruning and
Archival](#pruning-and-archival)).

Most ports bind to `127.0.0.1`. The quickstart `.env` setting `HOST_IP` only
controls the asset viewer, asset cache, and P2P bindings. It does not expose
RPC, Grafana, Prometheus, PostgreSQL, Redpanda, or Aerospike.

## Choose Listen-Only or Full Mode

Listen-only mode is the default. It receives blocks and transactions from peers
without accepting inbound P2P traffic or requiring public HTTP endpoints. It is
the simplest mode for local validation, testing, and chain observation.

Full mode is for a node that should be reachable by other nodes. It requires:

- A public HTTPS asset endpoint that proxies to quickstart's asset cache
- Inbound TCP for P2P, usually port `9905`
- `asset_httpPublicAddress` and `p2p_advertise_addresses` in `.env`
- The `p2p` profile in `COMPOSE_PROFILES`

Quickstart checks declared public endpoints after startup, but firewall and
reverse-proxy configuration remain the operator's responsibility.

## P2P DHT Mode

The `p2p_dht_mode` setting in `.env` controls how the node uses the libp2p DHT
for peer discovery:

- `off` (default) — connect only to bootstrap and topic peers. No DHT scanning.
- `client` — query the DHT but do not advertise. Opens 100+ peer connections.
- `server` — full DHT participation: advertises records, routes queries.

Most operators should keep `off`. `server` is only appropriate for nodes
operating as bootstrap or relay points with a publicly reachable
`p2p_advertise_addresses`.

> **Warning**: `server` mode probes 100+ peers to build its routing table. Some
> hosting providers (Hetzner, OVH, and other abuse-sensitive networks) may flag
> this traffic as port scanning and issue abuse reports or suspend the server.
> Stay on `off` or `client` on those providers unless you have cleared the
> behaviour with them in advance.

## Pruning and Archival

By default Teranode prunes spent outputs from the UTXO store after 288 blocks
(roughly 48 hours). This keeps Aerospike's memory and disk footprint bounded.
Do not raise the pruning depth without a specific reason — every additional
block retained grows Aerospike proportionally, and mainnet UTXO churn is high.

If you need full historical block data — for indexers, explorers, or chain
analysis — enable the optional `blockpersister` service by adding it to
`COMPOSE_PROFILES` in `.env`:

```bash
COMPOSE_PROFILES=legacy,p2p,monitoring,blockpersister
```

It writes raw block data to disk in parallel with the pruner; pruning behaviour
is unchanged. Archival mode requires significant additional disk space (multiple
TB on mainnet) and is not needed for standard validation or mining.

## Seed the Node

Initial sync is faster when you seed the UTXO set from a compatible snapshot.
For teratestnet, quickstart can derive the canonical snapshot URL from the block
hash:

```bash
./seed.sh 000000002ea94a515ad9fd40d710fd249fe8610acef7b74f459446812d565187
```

For mainnet or standard testnet, provide your own compatible snapshot directory
or URL:

```bash
./seed.sh <block-hash> /path/to/seed-dir
```

See [Syncing the Blockchain](./minersHowToSyncTheNode.md) for sync and seeding
options.

## Stop

Use quickstart's stop script for a graceful shutdown:

```bash
./stop.sh
```

It moves the FSM toward an idle state before stopping the Docker Compose stack.

## Update

Check for an update:

```bash
./update.sh --check
```

Apply an update:

```bash
./update.sh
./start.sh
```

`update.sh` changes only `TERANODE_VERSION` in `.env`; `start.sh` pulls and
recreates containers as needed. See [Updating Teranode](./minersUpdatingTeranode.md).

## Reset or Switch Networks

Different networks cannot share data. To switch networks:

```bash
./stop.sh
./clean.sh --data-only
./setup.sh
./start.sh
```

See [Reset Teranode](./minersHowToResetTeranode.md) for cleanup options.

## Related Documentation

- [Configure Docker Teranode](./minersHowToConfigureTheNode.md)
- [Start and Stop Docker Teranode](./minersHowToStopStartDockerTeranode.md)
- [Syncing the Blockchain](./minersHowToSyncTheNode.md)
- [Troubleshooting Docker Teranode](./minersHowToTroubleshooting.md)
- [teranode-quickstart README](https://github.com/bsv-blockchain/teranode-quickstart)
