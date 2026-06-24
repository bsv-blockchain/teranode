---
name: debug-teranode-k8s
description: Collect and interpret Teranode diagnostics on Kubernetes. Use this whenever you are debugging a Teranode deployment that runs on k8s — k0s, EKS, GKE, or any operator-managed cluster reached with kubectl: discovering the services/pods/namespaces, pulling goroutine and CPU profiles, checking Aerospike / PostgreSQL / Redpanda health, hunting pipeline bottlenecks, or running the bundled teranode-diag.sh snapshot. Trigger on mentions of kubectl, pods, namespaces, the Teranode operator or its CRs (propagations/blockassemblies/etc), or "why is my teranode cluster slow / stuck" when the deployment is Kubernetes. This is the "how to collect it on k8s" layer; pair it with the debug-teranode skill for "what it means". For Docker / docker-compose deployments use debug-teranode-docker instead.
license: MIT
metadata:
  audience: devops
  layer: deployment-k8s
---

# Debug Teranode on Kubernetes

Collection and discovery for Teranode running on Kubernetes. This skill knows how to *find*
the pieces and *pull* the data on a k8s cluster. For what the numbers mean — architecture,
the tx/block pipeline, goroutine-profile interpretation, datastore metrics — use the
**`debug-teranode`** skill; its `references/debugging-playbook.md` and
`references/datastore-health.md` are the interpretation layer this skill feeds.

> **Read-only by default.** Everything here only reads cluster state. The single exception
> is `--host-probes`, which creates short-lived privileged pods on datastore nodes — it is
> OFF by default and described under "Host-level deep dive" below. Never run host-probes on
> a cluster where you lack authorization.

## Prerequisites

- `kubectl` configured for the target cluster (`kubectl config current-context` to confirm).
- `python3` locally (the diag script parses output with inline Python).
- Read access to the teranode + datastore namespaces; `kubectl exec` into pods.
- For `--host-probes` only: permission to create privileged hostNetwork/hostPID pods
  (works on bare-metal-style clusters like k0s; managed EKS/GKE usually forbid it).

## How discovery works (nothing is hardcoded)

Teranode on k8s is normally deployed by the **Teranode operator**, which gives every
deployment the same stable handles regardless of namespace naming:

- **Workload pods carry `app=<service>`** (`app=propagation`, `app=block-assembly`,
  `app=blockchain`, `app=asset`, `app=subtree-validator`, `app=rpc`, `app=pruner`, …) plus
  the operator label **`teranode.bsvblockchain.org/part-of=true`**. Discover teranode
  namespaces by that label; discover pods of a service by its `app` label.
- **Operator CRs** describe desired config + scale: the parent `clusters.teranode.bsvblockchain.org`
  and one CR per service (`propagations…`, `blockassemblies…`, `validators…`,
  `blockvalidators…`, `subtreevalidators…`, `blockchains…`, `pruners…`, `rpcs…`, `assets…`).
  `kubectl get propagations -A` shows DESIRED/CURRENT/READY like a Deployment.
- **Datastores run under their own operators**, discovered by CR: Aerospike via
  `aerospikeclusters.asdb.aerospike.com`, PostgreSQL via `clusters.postgresql.cnpg.io`
  (CloudNativePG), Kafka/Redpanda by image. `tx-blaster` (load tool) is a StatefulSet
  (`app=tx-blaster`); it is **test tooling, not a node service** — absent in most real
  deployments.

See `references/operator-and-labels.md` for the full label/CR/port reference. If discovery
ever comes up empty (custom labels, no operator), every handle can be pinned with an env var
(`TERANODE_NAMESPACES`, `AEROSPIKE_NAMESPACES`, `AEROSPIKE_DB_NS`, `PPROF_PORT`, … — see the
script's `--help`).

## The diagnostic snapshot: `scripts/teranode-diag.sh`

A single read-only snapshot across every layer. Discovers namespaces, pods, the Aerospike
container + DB namespace, and pprof ports at runtime, then reports: cluster overview + FSM
state, Aerospike health, PostgreSQL health, Kafka/Redpanda consumer lag, Aerospike disk I/O,
propagation/blaster/block-assembly/block-validator CPU + aggregated goroutine profiles, and
networking. Designed to be run repeatedly during a load test and diffed across runs.

```bash
scripts/teranode-diag.sh --help          # options + discovery override env vars

scripts/teranode-diag.sh                  # full snapshot, 10% pod sampling
scripts/teranode-diag.sh --quick          # skip 2-second deltas (~fast)
scripts/teranode-diag.sh --sample-pct 30  # sample 30% of pods for goroutine profiles
scripts/teranode-diag.sh --host-probes    # add host-level deep dive (privileged; see below)

# Save a run and diff two runs to see the impact of a change:
scripts/teranode-diag.sh | tee diag-$(date +%Y%m%d-%H%M%S).txt
diff <(grep -E 'cache_read_pct|batch-index|TOTAL|iowait|Blocked on|FSM' run1.txt) \
     <(grep -E 'cache_read_pct|batch-index|TOTAL|iowait|Blocked on|FSM' run2.txt)
```

| Flag | Default | Effect |
|---|---|---|
| `--quick` | off | Skip 2-second deltas (disk I/O rates, CPU breakdown, pod net throughput). Faster, no rate data. |
| `--sample-pct N` | 10 | % of pods sampled for goroutine profiles. `kubectl top` always covers 100%. Min 1 pod. |
| `--host-probes` | off | Enable the host-level deep dive (privileged pods). See below. |

The script never asserts a "good" TPS or pod count — its summary prints **directional
signals**, not pass/fail thresholds, because Teranode deployments vary. Interpret with the
`debug-teranode` playbook.

### Host-level deep dive (`--host-probes`)

Off by default. When enabled, it creates short-lived **privileged hostNetwork/hostPID** pods
on Aerospike nodes to collect what containers can't see: `perf stat` (IPC, cache misses),
`ss` socket queues/RTT/retransmits on the Aerospike port, `ethtool` NIC counters, NVMe SMART,
`iostat -x`, and node-level bandwidth. It **only works on clusters that permit such pods**
(bare-metal-style, e.g. k0s with hostNetwork Aerospike). On managed clusters (EKS/GKE) it
auto-detects the lack of permission and skips cleanly. Probe pods are created in parallel and
deleted automatically. Only use it where you're authorized to create privileged pods.

## Reading the configured settings from the operator

The operator CRs are the source of truth for desired config and scale — often more reliable
than reading pod env:

```bash
kubectl get propagations -A                              # desired/ready replicas per node
kubectl get propagations.teranode.bsvblockchain.org <name> -n <ns> -o yaml   # full spec
kubectl get clusters.teranode.bsvblockchain.org -A       # the parent teranode cluster CR
kubectl get aerospikeclusters.asdb.aerospike.com -A      # Aerospike size/image/phase
```

To see the *effective* runtime config (env + settings) of a running pod:

```bash
POD=$(kubectl get pods -n <ns> -l app=propagation -o name | head -1)
kubectl exec -n <ns> $POD -- env | grep -iE 'utxostore|batcher|concurrent|drain|kafka|spend|locked|mined|pruner' | sort
```

## Ad-hoc collection (generalized — discover, never hardcode)

Set `NS` to the target namespace, then discover pods by label and Aerospike nodes by listing
them — don't assume a fixed node count.

```bash
NS=<teranode-namespace>; AERO_NS=<aerospike-namespace>
AC=$(kubectl get pods -n "$AERO_NS" -o name | grep -v operator | head -1 | xargs -I{} \
     kubectl get -n "$AERO_NS" {} -o jsonpath='{range .spec.containers[*]}{.name}{"\n"}{end}' | grep -i aerospike | grep -vi export | head -1)

# Goroutine snapshot for one propagation pod (pprof default :9091)
POD=$(kubectl get pods -n "$NS" -l app=propagation -o name | head -1)
kubectl exec -n "$NS" "${POD#pod/}" -- wget -qO- 'http://localhost:9091/debug/pprof/goroutine?debug=1'

# Top goroutine stacks by count
kubectl exec -n "$NS" "${POD#pod/}" -- wget -qO- 'http://localhost:9091/debug/pprof/goroutine?debug=1' \
  | grep '^[0-9]* @' | sort -rn | head -20

# Batcher concurrency-semaphore pressure (blocked dispatches waiting for a slot)
kubectl exec -n "$NS" "${POD#pod/}" -- wget -qO- 'http://localhost:9091/debug/pprof/goroutine?debug=1' \
  | grep -c 'SetMaxConcurrent'

# Aerospike health across ALL nodes (discover them; do not loop a fixed range)
for P in $(kubectl get pods -n "$AERO_NS" -o name | grep -v operator); do
  P=${P#pod/}
  rw=$(kubectl exec -n "$AERO_NS" "$P" -c "$AC" -- asinfo -v statistics 2>/dev/null | tr ';' '\n' | grep '^rw_in_progress=' | cut -d= -f2)
  echo "$P: rw_in_progress=$rw"
done

# Aerospike latency on one node (discover the DB namespace name first)
DB=$(kubectl exec -n "$AERO_NS" "$P" -c "$AC" -- asinfo -v namespaces 2>/dev/null | tr ';' '\n' | head -1)
kubectl exec -n "$AERO_NS" "$P" -c "$AC" -- asinfo -v 'latencies:' | tr ';' '\n'

# 5-second CPU profile from a pod (analyze with: go tool pprof -top -cum cpu.prof)
kubectl exec -n "$NS" "${POD#pod/}" -- sh -c 'wget -qO- "http://localhost:9091/debug/pprof/profile?seconds=5"' > cpu.prof
```

## Workflow

1. Confirm the context (`kubectl config current-context`) and that it's the cluster you mean.
2. Run `scripts/teranode-diag.sh --quick` first for a fast picture; check Section 1's FSM
   state and discovered backends before anything else.
3. If a node looks inert, the FSM state explains it (IDLE / CATCHINGBLOCKS) — see the
   `debug-teranode` playbook before chasing performance.
4. For a throughput problem, take a full run, find the busiest pipeline stage in the
   aggregated goroutine profile, and follow the playbook's bottleneck method.
5. Make one change, wait for rollout + stabilization, re-run, and diff. Never trust a single
   post-change snapshot.

For interpretation of everything you collect, go to **`debug-teranode`**.
