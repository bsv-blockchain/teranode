# Troubleshoot Docker Teranode

Last modified: 28-April-2026

Use the quickstart scripts first. They keep checks consistent with the Docker
Compose files and `.env` used by the deployment.

## First Checks

From the quickstart repository root:

```bash
./status.sh
./logs.sh blockchain
./logs.sh asset
```

If the stack is not running:

```bash
./start.sh
```

If `.env` is missing:

```bash
./setup.sh
```

## Container Health

Show all services:

```bash
docker compose ps
```

Inspect a health check:

```bash
docker inspect --format='{{json .State.Health}}' blockchain
```

Use service logs for failures:

```bash
./logs.sh <service-name>
```

Common services include `blockchain`, `asset`, `rpc`, `legacy`, `p2p`,
`postgres`, `aerospike`, `redpanda`, `prometheus`, and `grafana`.

## FSM Stuck in INIT

The startup FSM transition can race while services become healthy. Set the
state manually:

```bash
./cli.sh setfsmstate --fsmstate RUNNING
./cli.sh getfsmstate
```

Then check status:

```bash
./status.sh
```

## RPC Fails

Use the wrapper first:

```bash
./rpc.sh getblockchaininfo
```

If it fails:

- Confirm `rpc_user` and `rpc_pass` are set in `.env`.
- Confirm the `rpc` container is running with `docker compose ps`.
- Check RPC logs with `./logs.sh rpc`.
- Remember that quickstart binds RPC to `127.0.0.1:9292`.

## Port Already in Use

Check which process owns the port:

```bash
lsof -i :9292
```

Quickstart's common host ports are:

| Port | Service |
| --- | --- |
| `8090` | Asset viewer |
| `8000` | Asset cache |
| `9905` | P2P |
| `9292` | RPC, loopback only |
| `3005` | Grafana, loopback only |
| `9090` | Prometheus, loopback only |
| `8080` | Kafka Console, loopback only |

Change bindings in the Compose files only when you understand the security
impact.

## Full Mode Reachability Fails

Check `.env`:

```env
listen_mode=full
asset_httpPublicAddress=https://node.example.com/api/v1
p2p_advertise_addresses=/dns4/node.example.com/tcp/9905
```

Then verify:

- DNS points to the expected host.
- The reverse proxy forwards HTTPS asset API traffic to port `8000`.
- The firewall allows inbound TCP on port `9905`.
- `HOST_IP` allows the reverse proxy or firewall path to reach the quickstart
  host binding.

Rerun startup or the reachability helper after changes:

```bash
./start.sh
./lib/reachability.sh
```

## Aerospike Out of Space

The quickstart Aerospike Community Edition configuration uses a bounded local
UTXO store. If Aerospike reports device-full or stop-writes errors:

- Check host disk and memory.
- Confirm pruning settings were not increased unexpectedly.
- Consider reseeding from a newer snapshot if appropriate.
- For larger mainnet capacity, plan a custom Aerospike layout outside the basic
  quickstart defaults.

## Grafana Shows No Data

Prometheus needs time to scrape initial metrics. If dashboards remain empty:

```bash
docker compose ps prometheus
./logs.sh prometheus
```

Open Prometheus locally at <http://localhost:9090> and check target health.

## Reset Bad Local State

If containers are healthy but local data is inconsistent, reset Docker volumes:

```bash
./stop.sh
./clean.sh --data-only
./start.sh
```

For seeded recovery, run `./seed.sh` before `./start.sh`. See
[Sync Docker Teranode](./minersHowToSyncTheNode.md).

## Report Issues

Open quickstart orchestration issues in
[bsv-blockchain/teranode-quickstart](https://github.com/bsv-blockchain/teranode-quickstart).

Open Teranode service bugs in
[bsv-blockchain/teranode](https://github.com/bsv-blockchain/teranode/issues).
