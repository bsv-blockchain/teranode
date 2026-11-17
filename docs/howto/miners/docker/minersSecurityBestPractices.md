# Docker Security Best Practices

Last modified: 11-August-2026

The quickstart deployment is designed to keep most services local by default.
Do not expose internal ports unless you have a specific operational reason and
an authentication or network-control layer in front of them.

## Default Exposure

In [teranode-quickstart](https://github.com/bsv-blockchain/teranode-quickstart),
`HOST_IP` controls only these ports:

| Port | Service | Expose publicly? |
| --- | --- | --- |
| `8090` | Asset viewer UI | Only to trusted networks |
| `8000` | Asset cache | Only through a controlled HTTPS reverse proxy |
| `9905` | P2P | Required for full mode inbound peers |

These services remain bound to `127.0.0.1` in the quickstart Compose files:

- RPC: `9292`
- Grafana: `3005`
- Prometheus: `9090`
- Kafka Console: `8080`
- Redpanda/Kafka: `9092`
- PostgreSQL: `5432`
- Aerospike: `3000`
- Internal gRPC ports

Setting `HOST_IP=0.0.0.0` does not expose those loopback-only services.

## Listen-Only Mode

Listen-only mode is the safest default:

```env
listen_mode=listen_only
HOST_IP=127.0.0.1
```

Use it when the node does not need inbound P2P participation or public asset API
access.

## Full Mode

Full mode requires public reachability. At minimum:

- Expose P2P TCP on port `9905`.
- Serve the asset API through HTTPS and a reverse proxy to port `8000`.
- Set `asset_httpPublicAddress` to the public URL including `/api/v1`.
- Set `p2p_advertise_addresses` to the public libp2p multiaddr.
- Keep RPC on loopback unless a separate authenticated access layer is in place.

Do not expose raw internal service ports to the internet.

## RPC

Quickstart generates RPC credentials in `.env` and binds RPC to
`127.0.0.1:9292`. Keep it that way for normal deployments.

If remote RPC is required, use a dedicated authenticated tunnel or reverse proxy
with TLS, access logging, rate limits, and source restrictions.

## PostgreSQL Role Privileges

Teranode's PostgreSQL role (`teranode`) owns the `teranode` database and needs no
superuser rights. Deployments provisioned from this repository's Compose and
Kubernetes manifests create it as `NOSUPERUSER`. The `teranode-quickstart` stack
still creates it as `SUPERUSER`; until that is updated, apply the remediation
below regardless of when your node was provisioned.

Existing deployments are not fixed by an update. The role is created by the
`init.sql` that the `postgres` image runs only when its data directory is empty,
and the Compose stack bind-mounts that directory from the host
(`${DATA_PATH}/postgres`). Pulling a newer Teranode and restarting leaves the
role exactly as it was first created, so a node provisioned before this change
still runs with a superuser `teranode` role.

Remediate once, as the `postgres` superuser:

```bash
docker exec -i postgres psql -U postgres -c 'ALTER ROLE teranode NOSUPERUSER;'
```

Verify:

```bash
docker exec -i postgres psql -U postgres -tAc \
  "SELECT rolname, rolsuper FROM pg_roles WHERE rolname = 'teranode';"
```

Expect `teranode|f`. Object ownership is unaffected — `teranode` still owns its
database and tables, so no regrant is needed and Teranode can be left running.

The development and test Compose stacks under `compose/` and `test/` create
`miner1`, `miner2`, `miner3`, and the `coinbase*` roles the same way. If those
stacks were brought up before this change and the `postgres-data` volume was kept
(`docker compose down` without `-v`), run `ALTER ROLE <role> NOSUPERUSER;` for
each of those roles, or recreate the volume.

## Secrets

`.env` contains RPC and PostgreSQL secrets. On multi-user hosts, restrict file
permissions:

```bash
chmod 600 .env
```

Back up `.env` securely before destructive reset operations.

## Monitoring Interfaces

Grafana, Prometheus, and Kafka Console are local development and operations
interfaces. If you expose them beyond localhost, put them behind authentication
and restrict source networks.

## DHT Mode

Quickstart defaults `p2p_dht_mode=off`. Only use `server` mode if the node is
intended to act as a reachable DHT participant and the hosting provider allows
the peer probing behavior required for DHT routing.

## Host Maintenance

- Keep Docker, the host OS, and reverse-proxy software patched.
- Use host firewall rules in addition to Docker port bindings.
- Monitor disk usage; verbose logs and archival mode can grow quickly.
- Keep backup and restore procedures separate from the running quickstart
  checkout.
