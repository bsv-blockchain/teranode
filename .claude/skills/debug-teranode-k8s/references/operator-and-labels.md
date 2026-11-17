# Teranode on Kubernetes — Operator, Labels, Ports

Concrete handles for finding and reading a Teranode deployment on k8s. These are the stable
contracts the diag script and ad-hoc commands rely on. Values were confirmed against an
operator-managed k0s cluster; treat the *mechanism* (labels/CRs) as stable and the *example
names* as illustrative.

## Pod labels (discovery contract)

| Label | Value | Use |
|---|---|---|
| `teranode.bsvblockchain.org/part-of` | `true` | On every operator-managed teranode pod. Discover teranode namespaces: `kubectl get pods -A -l teranode.bsvblockchain.org/part-of=true`. |
| `app` | service name | Per-service selector: `app=propagation`, `app=block-assembly`, `app=block-validator`, `app=blockchain`, `app=subtree-validator`, `app=asset`, `app=asset-cache`, `app=rpc`, `app=pruner`, `app=peer`, `app=coinbase`, `app=bitcoin-miner`. |
| `app` | `tx-blaster` | The load generator (StatefulSet). **Test tooling, not a node service** — usually absent in real deployments. |

Most workload pods are `Deployment → ReplicaSet → Pod`; `tx-blaster` is a `StatefulSet`.

## Teranode operator CRs (desired config + scale)

CRD group `teranode.bsvblockchain.org`. The parent CR is `clusters` (one teranode node);
each service has its own CR that behaves like a Deployment (DESIRED/CURRENT/READY).

| CR (plural) | What it configures |
|---|---|
| `clusters` | the teranode node as a whole (parent) |
| `propagations` | propagation replicas + settings |
| `validators` | validator |
| `blockassemblies` | block assembly |
| `blockvalidators` | block validation |
| `subtreevalidators` | subtree validation |
| `blockchains` | blockchain service / FSM |
| `assets` | asset server |
| `rpcs` | RPC |
| `pruners` | pruner |
| `peers` | P2P |
| `legacies` | legacy bridge |
| `coinbases`, `alertsystems`, `blockpersisters`, `utxopersisters`, `bootstraps`, `faucets` | the matching overlay services |

```bash
kubectl get clusters.teranode.bsvblockchain.org -A          # the teranode node(s)
kubectl get propagations -A                                 # replicas: DESIRED/CURRENT/READY
kubectl get propagations.teranode.bsvblockchain.org <name> -n <ns> -o yaml   # full desired spec
```

Reading config from the CR is more reliable than scraping pod env when you want the
*intended* configuration; read pod env (`kubectl exec … -- env`) for the *effective* runtime.

## Container ports (teranode service pod)

Ports are not always named on the teranode pods; these are the conventions (settings.conf):

| Port | Purpose |
|---|---|
| 9091 | **pprof + Prometheus `/metrics`** (`/debug/pprof/goroutine`, `/profile`, `/heap`) |
| 8000 | health |
| 8084 | propagation gRPC · 8833 propagation HTTP |
| 8081 validator gRPC · 8085 block-assembly gRPC · 8086 subtree-validation gRPC · 8087 blockchain gRPC · 8088 block-validation gRPC |
| 8090 | asset HTTP (dashboard / blockchain viewer / **FSM endpoint** `/api/v1/fsm/state`) |
| 9292 | JSON-RPC · 9905 P2P |
| 4040 | Delve (if the image was built with the debug entrypoint) |

`tx-blaster` pod ports are **named** and the pprof port varies by deployment — discover it:
`profiler` (pprof, e.g. 9092) and `exporter` (Prometheus, e.g. 7092). Don't assume; read the
named `profiler` port. (A common stale bug is profiling the `exporter` port by mistake.)

## Datastore operators (the stores that vary)

| Store | Operator / CRD | Discover namespaces | Reach it |
|---|---|---|---|
| **Aerospike** (UTXO) | Aerospike Kubernetes Operator · `aerospikeclusters.asdb.aerospike.com` | `kubectl get aerospikeclusters -A` | `kubectl exec -n <ns> <pod> -c <aero-container> -- asinfo …` |
| **PostgreSQL** (blockchain + other SQL stores) | CloudNativePG · `clusters.postgresql.cnpg.io` | `kubectl get clusters.postgresql.cnpg.io -A` | primary pod labeled `cnpg.io/instanceRole=primary`; `psql -U postgres` |
| **Kafka / Redpanda** (messaging) | Redpanda/Strimzi operator (CRD varies) | by image (`redpanda`/`kafka`) | broker pod; `rpk group …` for consumer lag |

Notes:

- An Aerospike pod runs the server container (commonly `aerospike-server`, but **discover it**:
  the container whose name matches `aerospike` and isn't the exporter) plus an
  `aerospike-prometheus-exporter` sidecar. Aerospike often runs `hostNetwork=true`.
- The Aerospike **DB namespace** holding UTXOs is commonly `utxo-store` but discover it with
  `asinfo -v namespaces` (a cluster may also host other namespaces, e.g. `merkle`).
- Aerospike node count is **not fixed** — list pods, never loop a hardcoded range.
- The UTXO store is **pluggable**: a deployment may use PostgreSQL or SQLite instead of
  Aerospike. Confirm the backend (the diag script prints it in Section 1) before running
  Aerospike-specific checks.

## Quick capability check before host-probes

```bash
kubectl auth can-i create pods            # must be yes
kubectl get pod <aero-pod> -n <aero-ns> -o jsonpath='{.spec.hostNetwork}'   # true ⇒ bare-metal-style
```

If `can-i create pods` is no, or the cluster is managed (EKS/GKE) and forbids privileged
hostNetwork pods, skip `--host-probes` — the in-container diagnostics still give you the
goroutine profiles, Aerospike `asinfo` stats, and CPU/throttle data that matter most.
