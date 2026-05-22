# Configure Docker Teranode

Last modified: 28-April-2026

Docker deployments are configured through the
[teranode-quickstart](https://github.com/bsv-blockchain/teranode-quickstart)
repository. Run `./setup.sh` for the initial configuration, then edit `.env`
directly for later changes.

The `.env` file is the single source of truth for a quickstart deployment. It
is not committed to Git, so local settings, passwords, and version pins do not
conflict with future `git pull` operations.

## Initial Configuration

From the quickstart repository root:

```bash
./setup.sh
```

The setup script asks for:

- Network: `mainnet`, `testnet`, `teratestnet`, or `regtest`
- Run mode: `listen_only` or `full`
- Optional archival mode
- Client name
- RPC credentials
- Host bind address for selected public-facing ports

It also writes per-network defaults such as mining fee policy, block size
limits, Docker Compose profiles, and generated secrets.

## Important Settings

| Setting | Purpose |
| --- | --- |
| `TERANODE_VERSION` | Teranode image tag. `./update.sh` updates this value. |
| `network` | Active network: `mainnet`, `testnet`, `teratestnet`, or `regtest`. |
| `COMPOSE_PROFILES` | Optional services to run, such as `legacy`, `p2p`, and `blockpersister`. |
| `listen_mode` | `listen_only` for no inbound participation, or `full` for a reachable node. |
| `HOST_IP` | Bind address for asset viewer, asset cache, and P2P only. |
| `asset_httpPublicAddress` | Public HTTPS asset API URL, including `/api/v1`, for full mode. |
| `p2p_advertise_addresses` | Public libp2p multiaddr for full mode. |
| `rpc_user`, `rpc_pass` | JSON-RPC credentials used by `./rpc.sh`. |
| `logLevel` | Service log level. Keep `INFO` unless diagnosing an issue. |

Quickstart passes lower-case and camelCase Teranode settings directly into the
containers. The key name in `.env` should match the key Teranode reads from
`settings.conf`.

## Change Configuration

1. Stop the stack:

   ```bash
   ./stop.sh
   ```

2. Edit `.env`.

3. Start the stack:

   ```bash
   ./start.sh
   ```

4. Verify the result:

   ```bash
   ./status.sh
   ```

Some settings, such as `network`, require a data reset before restart because
data volumes are network-specific. See [Reset Teranode](./minersHowToResetTeranode.md).

The examples below keep quickstart's default `monitoring` profile. Remove
`monitoring` from `COMPOSE_PROFILES` only when you intentionally want a lean
node-only stack without Grafana, Prometheus, the Aerospike exporter, or Kafka
Console.

## Listen-Only Mode

Listen-only mode is the simplest and safest default:

```env
listen_mode=listen_only
COMPOSE_PROFILES=legacy,p2p,monitoring
asset_httpPublicAddress=
p2p_advertise_addresses=
HOST_IP=127.0.0.1
```

The node receives network data but does not require public inbound P2P or a
public asset endpoint.

## Full Mode

Full mode is for a node that should participate as a reachable network peer:

```env
listen_mode=full
COMPOSE_PROFILES=legacy,p2p,monitoring
asset_httpPublicAddress=https://node.example.com/api/v1
p2p_advertise_addresses=/dns4/node.example.com/tcp/9905
```

You must provide the reverse proxy, TLS, DNS, firewall rules, and TCP reachability
outside quickstart. `./start.sh` runs a reachability check when public endpoints
are configured.

## Optional Archival Mode

Enable `blockpersister` only when you need raw historical block data for an
explorer, indexer, research workload, or backup workflow:

```env
COMPOSE_PROFILES=legacy,p2p,monitoring,blockpersister
```

Archival mode can require multiple TB of disk on mainnet. It does not disable
normal UTXO pruning.

## Ports

`HOST_IP` controls only these host bindings:

| Port | Service | Notes |
| --- | --- | --- |
| `8090` | Asset viewer UI | Always enabled |
| `8000` | Asset cache | Used by full-mode public asset API |
| `9905` | P2P | Used by full-mode inbound peer connections |

RPC (`9292`), PostgreSQL (`5432`), Redpanda (`9092`), Aerospike (`3000`), and
internal gRPC ports bind to `127.0.0.1` in quickstart. When `monitoring` is
enabled, Grafana (`3005`), Prometheus (`9090`), and Kafka Console (`8080`) also
bind to `127.0.0.1`.

## Settings Reference

- Quickstart defaults: `.env.example` in the quickstart repository
- Teranode defaults: [`settings.conf`](https://github.com/bsv-blockchain/teranode/blob/main/settings.conf)
- Docker install guide: [Install Teranode with Docker](./minersHowToInstallation.md)
