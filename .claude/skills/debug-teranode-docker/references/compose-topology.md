# Teranode on Docker — Compose Topology Reference

The concrete map of how Teranode runs under docker-compose: which files exist, the
containers/ports they define, how the datastore backend changes with the settings context,
and the gotchas a debugger needs. The diag script auto-detects most of this; this file is for
when you need to reason about a specific stack or override discovery.

## Compose families

| Path | Purpose | UTXO backend | pprof on host? | Settings context |
|---|---|---|---|---|
| `deploy/docker/mainnet/docker-compose.yml` · `…/testnet/…` | **Operator single-node miner** (extends `deploy/docker/base/*`). The "run a real node" path. | **Aerospike** (EE) | **No** | `docker.m` |
| `compose/docker-compose-3blasters.yml` | **3 full nodes** + per-node aerospike/prometheus, optional blasters. `make chain-integrity-test`. | **Aerospike** (one per node) | **Yes** | `docker.teranode{1,2,3}.test` |
| `compose/generated/docker-compose-multinode.yml` | **Generated N-node** (3–10), all-in-one or split. Created by `make gen-multinode N=<n>` / `compose/multinode.sh up <n>`. | Aerospike | **Yes** | `docker.teranodeN.test` |
| `compose/docker-compose-ss.yml` | **Split single-node** — each microservice its own container; includes Toxiproxy for chaos. | **PostgreSQL** | mostly no | `docker.ss.teranode1` |
| `test/docker-compose.e2etest.yml` | E2E integration tests (3 nodes + blob/subtree stores). | Aerospike | ephemeral | `docker.teranodeN.test` |

`teranode-builder` (in dev/CI files) is a build-only helper that tags `teranode:latest`; it
doesn't run a node. `make dev` is **native** (`go run .`), not Docker — its deps come from
`scripts/dev-stack-macos.sh` or the standalone `deploy/docker/*` compose files.

## Operator single-node containers (`deploy/docker/mainnet`)

Image `ghcr.io/bsv-blockchain/teranode:<ver>`; all teranode services share a base anchor with
`SETTINGS_CONTEXT=docker.m`, `USE_LOCAL_AEROSPIKE/POSTGRES/KAFKA=true`. Published ports are
bound to `127.0.0.1`.

| Container | Host port | Notes |
|---|---|---|
| `blockchain` | — | FSM owner; health 8087 |
| `asset` | `8090` | dashboard / blockchain viewer / **FSM endpoint** `/api/v1/fsm/state` |
| `asset-cache` (nginx) | `8000` | |
| `rpc` | `9292` | JSON-RPC |
| `subtreevalidation`, `blockvalidation`, `blockassembly`, `pruner`, `legacy` | — | no host ports; `mem_limit` high on validation services |
| `postgres` (postgres:17) | `5432` | blockchain + other SQL stores |
| `kafka-shared` (redpanda) | `9092/9093` + `9096` | message bus |
| `aerospike` (EE) | `3000` | UTXO store, reachable in-net at `aerospike:3000` |
| `prometheus` `9090` · `grafana` `3005→3000` | | metrics |

> **pprof/metrics 9091 is NOT published** in the operator stack — Prometheus scrapes it
> in-network. To profile: `docker compose exec <svc> curl -s localhost:9091/debug/pprof/...`.

## Multi-node dev/CI (`compose/docker-compose-3blasters.yml`)

Image `teranode:latest`. Containers `teranode1/2/3` (all-in-one), `aerospike-1/2/3`,
`postgres` (`postgres_for_chain_integrity`), `kafka-shared`, optional `tx-blaster-1/2/3` +
`coinbase1/2/3` (profile `with-coinbase`), `prometheus-1/2/3`, `grafana`, `svnode1`.

pprof **is** published: node1 `localhost:19091`, node2 `:29091`, node3 `:39091`. Per-node
port pattern there: `teranode1` 18000/18081-18092/19091/19292/19905, etc.

**Generated multinode host-port formula** (`compose/cmd/gennodes`):
`hostPort = 20000 + (N-1)*2000 + (containerPort - 8000)`.
Node 1: dashboard/asset `20090`, RPC `21292`, **pprof `21091`**, health `20000`, p2p `21905`.
Node 2: pprof `23091`. Node 3: pprof `25091`. Aerospike per node N: service port `3000 + N*10`.
Container names: `teranodeN-multinode` (all-in-one) or `teranodeN-<svc>-multinode` (split:
svc ∈ blockchain, blockassembly, blockvalidation, subtreevalidation, validator, propagation,
p2p, asset, core).

## Split single-node (`compose/docker-compose-ss.yml`)

One container per microservice: `blockchain-1`, `p2p-1`, `validator-1`, `propagation-1`,
`blockassembly-1`, `subtreevalidation-1`, `blockvalidation-1`, `asset-1`, `coinbase-1`,
`blockpersister-1`. **UTXO + blockchain store = PostgreSQL** (no aerospike container here).
Includes `toxiproxy-postgres` / `toxiproxy-kafka` for fault injection. Only `blockchain-1`
(8082/8087) and `asset-1` (8090) publish ports by default.

## Canonical ports (settings.conf)

| Port | Service |
|---|---|
| **9091** | pprof + Prometheus `/metrics` (all teranode services) |
| 8000 health · 8081 validator gRPC · 8082 blockchain HTTP · 8084 propagation gRPC · 8085 block-assembly gRPC · 8086 subtree-validation gRPC · 8087 blockchain gRPC · 8088 block-validation gRPC | |
| 8090 | asset HTTP (dashboard / **FSM** `/api/v1/fsm/state`) |
| 8833 propagation HTTP · 9292 JSON-RPC · 9905 P2P · 9904 P2P gRPC | |
| 9092 | Kafka broker / blaster pprof (`PROFILE_PORT_TXBLASTER`) — but blaster `-profile` flag is sometimes `:7092`; **discover it** |
| 5432 postgres · 3000 aerospike · 9090 prometheus · 6831 jaeger | |

## Datastore backend by settings context

| Context | UTXO store | Blockchain store | Kafka |
|---|---|---|---|
| `docker.m` (operator) | Aerospike `aerospike:3000/utxo-store` | Postgres `postgres:5432` | `kafka-shared:9092` |
| `docker.teranodeN.test` (dev/CI) | Aerospike `aerospike-N:<port>` | Postgres | `kafka-shared:9092` |
| `docker.ss.teranode1` (split) | **Postgres** | Postgres | `kafka-shared:9092` |
| `.dev` (native) | SQLite (in-mem) | Postgres | — |

So **"check Aerospike" is wrong on the split stack** — confirm the backend first (the diag
script prints it). Configuration is hierarchical: a base key (`utxostore = …`) is overridden
by a context-suffixed key (`utxostore.docker.m = …`); the container picks the context from
the `SETTINGS_CONTEXT` env var and reads `/app/settings.conf` + mounted `/app/settings_local.conf`.

## Exec / logs cheat-sheet

```bash
docker compose ls                                            # running projects
docker compose -p <project> ps                               # containers in a project
docker compose -p <project> exec <service> sh                # shell into a service
docker exec -it <container> asinfo -v statistics             # aerospike CLI
docker exec -it postgres psql -U postgres teranode           # postgres
docker compose -p <project> logs -f <service>                # follow logs
compose/multinode.sh status | logs <node[-svc]> | chaos …    # multinode driver (no 'exec' subcmd)
```

Gotchas:

- Two image sources: operator uses pinned `ghcr.io/bsv-blockchain/teranode:<ver>`; dev/CI uses
  locally-built `teranode:latest` (some older files reference ECR images needing AWS access).
- `compose/generated/` doesn't exist until `make gen-multinode` / `multinode.sh up` runs.
- `${TEST_ID}` namespaces dev/CI networks, container names, and data dirs.
- Log lines carry ` | ERROR | ` / ` | INFO | ` severity fields — grep on those.
