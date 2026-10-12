---
name: debug-teranode-docker
description: Collect and interpret Teranode diagnostics on Docker / docker-compose. Use this whenever you are debugging a Teranode stack running in Docker — a local dev stack, a multi-node compose stack, the operator single-node mainnet/testnet stack, or CI — reached with docker / docker compose: discovering the compose project and its containers, pulling goroutine and CPU profiles (via host port or docker exec), checking Aerospike / PostgreSQL / Redpanda health, scanning container logs, and running the bundled teranode-diag-docker.sh snapshot. Trigger on mentions of docker, docker compose, compose files, container names like teranode1/blockchain/aerospike, settings contexts like docker.m or docker.teranodeN.test, or "why is my local teranode slow / stuck" when the deployment is Docker. This is the "how to collect it on Docker" layer; pair it with the debug-teranode skill for "what it means". For Kubernetes deployments use debug-teranode-k8s instead.
license: MIT
metadata:
  audience: devs, devops
  layer: deployment-docker
---

# Debug Teranode on Docker / docker-compose

Collection and discovery for Teranode running under Docker. This skill knows how to *find*
the containers and *pull* the data on a compose stack. For what the numbers mean —
architecture, the pipeline, goroutine-profile interpretation, datastore metrics — use the
**`debug-teranode`** skill; its `references/debugging-playbook.md` and
`references/datastore-health.md` are the interpretation layer this skill feeds.

> **Read-only.** Everything here only reads container state, logs, and pprof endpoints.

## Prerequisites

- `docker` (with `docker compose` v2, or `docker-compose`) and access to the running stack.
- `python3` locally (the diag script parses pprof / latency output with inline Python).
- The stack must be up: `docker compose ls` shows the project.

## Which compose stack are you on? (it changes everything)

Teranode ships several compose families. The script auto-detects the project and adapts, but
know which one you're on because pprof exposure and datastore backend differ:

| Stack | Typical containers | pprof (9091) on host? | UTXO backend | Settings context |
|---|---|---|---|---|
| **Operator single-node** (`deploy/docker/mainnet|testnet`) | `blockchain`, `asset`, `rpc`, `subtreevalidation`, `blockvalidation`, `blockassembly`, `pruner`, `aerospike`, `postgres`, `kafka-shared` | **No** — use `docker exec … curl localhost:9091` | Aerospike | `docker.m` |
| **Multi-node dev/CI** (`compose/docker-compose-3blasters.yml`, `compose/multinode.sh`) | `teranode1/2/3` (all-in-one), `aerospike-1/2/3`, `postgres`, `kafka-shared`, optional `tx-blaster-*` | **Yes** — node1 `:19091`, node2 `:29091`, … | Aerospike | `docker.teranodeN.test` |
| **Split single-node** (`compose/docker-compose-ss.yml`) | `blockchain-1`, `propagation-1`, `validator-1`, … (one container per service) | mostly no | **PostgreSQL** | `docker.ss.teranode1` |

The diag script reports the detected project, settings context, and UTXO backend in Section 1
so you don't have to guess. See `references/compose-topology.md` for the full map (files,
service names, ports, the multinode host-port formula, datastore-by-context table).

## The diagnostic snapshot: `scripts/teranode-diag-docker.sh`

A single read-only snapshot. Discovers the compose project, classifies containers
(teranode / aerospike / postgres / kafka / blaster), detects the settings context, UTXO
backend, and FSM state, then reports resource usage (`docker stats`), Aerospike / PostgreSQL /
Kafka health, aggregated goroutine profiles per teranode service, and a log error scan.

```bash
scripts/teranode-diag-docker.sh --help              # options + override env vars
scripts/teranode-diag-docker.sh                     # full snapshot of the detected project
scripts/teranode-diag-docker.sh --project my-stack  # target a specific compose project
scripts/teranode-diag-docker.sh --quick             # skip log scan + longer collections
scripts/teranode-diag-docker.sh --since 30m         # widen the log error window

scripts/teranode-diag-docker.sh | tee diag-$(date +%Y%m%d-%H%M%S).txt   # save to diff later
```

Key behaviors:

- **pprof without host ports:** in the operator stack 9091 isn't published, so the script
  pulls profiles by `docker exec`-ing `curl`/`wget` inside the container. In dev/CI stacks it
  uses the published host port automatically. You don't configure this.
- **Backend-aware:** if there's no Aerospike container (e.g. the split stack uses Postgres for
  UTXO), the Aerospike section says so and the Postgres section carries the load.
- **No absolute targets:** the summary gives directional next steps, never a "good" TPS —
  interpret with the `debug-teranode` playbook.

## Ad-hoc collection (discover container names, don't hardcode)

```bash
PROJECT=$(docker ps --format '{{.Label "com.docker.compose.project"}}\t{{.Image}}' | grep -i teranode | awk -F'\t' '{print $1}' | sort | uniq -c | sort -rn | awk 'NR==1{print $2}')
docker ps --filter "label=com.docker.compose.project=$PROJECT" \
  --format 'table {{.Names}}\t{{.Image}}\t{{.Label "com.docker.compose.service"}}\t{{.Ports}}'

# Goroutine snapshot — works whether or not 9091 is published:
CTR=teranode1   # or blockchain / propagation-1 / whatever the stack uses
docker exec "$CTR" sh -c 'curl -s localhost:9091/debug/pprof/goroutine?debug=1 || wget -qO- localhost:9091/debug/pprof/goroutine?debug=1' \
  | grep '^[0-9]* @' | sort -rn | head -20

# Batcher concurrency-semaphore pressure
docker exec "$CTR" sh -c 'curl -s localhost:9091/debug/pprof/goroutine?debug=1' | grep -c 'SetMaxConcurrent'

# 5s CPU profile (analyze: go tool pprof -top -cum cpu.prof)
docker exec "$CTR" sh -c 'curl -s "localhost:9091/debug/pprof/profile?seconds=5"' > cpu.prof

# Aerospike health (container name varies: aerospike / aerospike-1 …)
docker exec aerospike asinfo -v namespaces
docker exec aerospike asinfo -v statistics | tr ';' '\n' | grep -E 'rw_in_progress|client_connections'
docker exec aerospike asinfo -v 'latencies:' | tr ';' '\n'

# Postgres (operator stack: container 'postgres'; split stack uses it for UTXO too)
docker exec postgres psql -U postgres -c "select state, count(*) from pg_stat_activity group by 1"

# Redpanda consumer-group lag (which consumer is behind)
docker exec kafka-shared rpk group list
docker exec kafka-shared rpk group describe <group>

# Logs for one service
docker compose -p "$PROJECT" logs -f --tail=100 "$CTR"
docker logs --since 15m "$CTR" 2>&1 | grep -E '\| ERROR \|'
```

## Workflow

1. `docker compose ls` to confirm the stack is up; note the project.
2. Run `scripts/teranode-diag-docker.sh` — read Section 1 (project, settings context, UTXO
   backend, FSM state) before anything else.
3. Inert node? The FSM state explains it (IDLE / CATCHINGBLOCKS) — see the `debug-teranode`
   playbook before chasing performance.
4. Throughput problem? Find the busiest pipeline stage in the Section 6 goroutine profiles and
   follow the playbook's bottleneck method.
5. Stalled service with no CPU? Check Kafka/Redpanda consumer lag — teranode pauses on
   unhealthy Kafka.

For interpretation of everything you collect, go to **`debug-teranode`**.
