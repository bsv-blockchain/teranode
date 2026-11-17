# Teranode Debugging Playbook (deployment-agnostic)

How to reason about *where* Teranode is bottlenecked and *why*, independent of how it's
deployed. The collection commands (kubectl exec, docker exec, port-forwards) live in the
`debug-teranode-k8s` and `debug-teranode-docker` skills; this file is the interpretation
layer they both lean on.

> **No universal throughput benchmark.** Teranode deployments vary enormously — node
> count, hardware, UTXO backend, network, tuning. There is **no "good" TPS number** and no
> fixed pod/replica count to expect. Diagnose by **comparison**, not against absolutes:
> compare a component to its own downstream, compare nodes to each other, compare a run to
> a previous run. Treat every threshold below as a *direction to look*, not a pass/fail SLA.

## Table of Contents

1. [The core method: find the bottleneck layer](#1-the-core-method-find-the-bottleneck-layer)
2. [How Teranode's tx pipeline throttles itself](#2-how-teranodes-tx-pipeline-throttles-itself)
3. [Reading goroutine profiles](#3-reading-goroutine-profiles)
4. [Reading CPU profiles](#4-reading-cpu-profiles)
5. [Finding concurrency bottlenecks](#5-finding-concurrency-bottlenecks)
6. [Host & runtime health signals](#6-host--runtime-health-signals)
7. [Measuring the impact of a change](#7-measuring-the-impact-of-a-change)
8. [Common bottleneck patterns](#8-common-bottleneck-patterns)

---

## 1. The core method: find the bottleneck layer

The tx path is a chain: **tx source → Propagation → Validator → UTXO store → Block Assembly**
(and for blocks: P2P → Block Validation → Subtree Validation → Blockchain). A chain runs at
the speed of its slowest link. **The bottleneck is the component that is AT capacity while
its downstream is underutilized.** Always locate that component before changing anything.

Quick checks, in order:

1. **Resource usage vs limits** for every component type (CPU/mem). A component pinned at
   its CPU limit is a candidate; one with idle CPU is being starved by something else.
2. **Demand-side profile** (the tx source / blaster): if its goroutines are blocked waiting
   on Propagation responses, Propagation (or below) is the limiter from its perspective.
3. **Propagation goroutine profile**: which pipeline stage has the most goroutines piled up?
   That stage is the local bottleneck (see §3 for the caveat about the last stage).
4. **UTXO store health**: is it busy (work in progress high) or idle (starved)? Idle store +
   piled-up clients = the gates between them are too narrow (§5), not the store itself.
5. **Block/queue state**: is Block Assembly's queue growing? Is Block Validation stuck
   catching up? Growing queues point at the consumer being behind.

The single most common mistake is tuning the component that *looks* busy (e.g. the last
pipeline stage, which always has the highest goroutine count) instead of the one actually
gating throughput.

---

## 2. How Teranode's tx pipeline throttles itself

Inside a Propagation pod each tx passes through stages **sequentially**:

```text
Get UTXO → ValidateScripts → Spend UTXO → Create UTXO → BA.Store → SetLocked
```

- **Get**: batch read of parent UTXOs from the UTXO store.
- **ValidateScripts**: CPU-bound script verification via BDK (cgo).
- **Spend**: mark input UTXOs spent (batch UDF, or batch write with filter expressions).
- **Create**: write new output UTXOs (batch write).
- **BA.Store**: send the validated tx to Block Assembly via gRPC (batched).
- **SetLocked**: mark the UTXO locked/unlocked (batch UDF).

**Batcher model (the key throttle).** Each store-touching stage uses a **batcher** that
collects individual items and dispatches them in batches. Two thresholds: a **size**
(max items/batch) and a **duration** (max wait before flushing) — whichever trips first
flushes. Each batcher has a **`maxConcurrent`** semaphore limiting in-flight batches; when
all slots are busy, new dispatches block. This semaphore is one of the most important
throttles in the system. Batchers also have a **drain mode**: ON = keep flushing while
items wait (good for bursty stages like Get/Create); OFF = wait for the timer between
batches (better for trickle stages like Spend/SetLocked, but risks single-item batches).

**UTXO-client behavior (Aerospike backend).**

- A connection pool per store node; `ConnectionQueueSize` caps connections per node per pod,
  and `LimitConnectionsToQueueSize=true` makes it a hard cap.
- A batch operation fans out to all store nodes owning relevant partitions; it completes
  only when **all** sub-operations on **all** nodes return — **the slowest node sets the
  batch latency**.
- **executeSingle fallback**: when a batch contains exactly **1** record, the client falls
  back to a single-key call (no pipelining, different code path). Common when drain mode is
  OFF and arrivals trickle. Wasteful at scale.

Knowing this model tells you what to look at: goroutines stuck *at the batcher semaphore*
vs *inside the client* vs *waiting on a WaitGroup* mean very different things (§3, §5).

---

## 3. Reading goroutine profiles

The goroutine dump (`/debug/pprof/goroutine?debug=1`) is the single most powerful diagnostic
— a snapshot of what every goroutine is doing right now. Sort stack groups by count
(`grep '^[0-9]* @' | sort -rn`) and read the function each group is parked in.

**Propagation pipeline stages by function** (line numbers drift across versions — match on
the **function/file name**, not the line):

| File · function | Stage |
|---|---|
| `get.go` · `(*Store).get` | Get UTXO (in batcher) |
| `Validator.go` · `getUtxoBlockHeightsAndExtendTx` | Get UTXO (awaiting result) |
| `spend.go` · `(*Store).Spend.func2` | Spend (in batcher) |
| `spend.go` · `(*Store).Spend` (WaitGroup.Wait) | Spend (awaiting result) |
| `create.go` · `(*Store).Create` | Create (in batcher) |
| `locked.go` · `(*Store).SetLocked.func1` | SetLocked (in batcher) |
| `locked.go` · `(*Store).SetLocked` (WaitGroup.Wait) | SetLocked (awaiting result) |
| `Client.go` · BA.Store batcher | Block Assembly Store |

**What the accumulation pattern means:**

- Goroutines pile up at the **slowest** stage because upstream feeds it faster than it drains.
- The **last** stage (SetLocked) naturally has a high count from pipeline accumulation —
  high count there does **not** by itself mean it's the bottleneck.
- Find the true bottleneck by which stage has *disproportionately more* goroutines than its
  position warrants. Lots at Get → store reads are slow. Lots at Spend relative to Get →
  the Spend op specifically is slow (UDF/expression overhead).

**Demand-side (tx source / blaster) patterns:**

| Parked at | Meaning |
|---|---|
| channel-receive in the pool loop | Idle pool workers — pool oversized for demand. Normal. |
| `Client.ProcessTransaction` | Active RPCs waiting on Propagation to respond — Propagation is slow. |
| `Distributor.sendViaPool` | Can't submit to the pool — input channel full because workers are all blocked on Propagation. |
| `utxoqueue` | Source UTXO queue backed up (overflow or mutex contention). |
| `propagateCancel` / `removeChild` | Context-mutex contention — a shared parent context across pool workers (see §8). |

**gRPC/Kafka infra goroutines are noise:** `transport.*`, `bufio.(*Reader).Read` (one per
connection), `sarama.(*partitionProducer).dispatch` (idle Kafka producers), `errgroup.Wait`
(normal concurrency). Don't mistake these for bottlenecks.

---

## 4. Reading CPU profiles

`go tool pprof -top -cum` on a short CPU profile (e.g. 5s) from a pod shows where CPU goes.

| Function | Category | Notes |
|---|---|---|
| `cgocall` / `VerifyScript` | Script verification | Useful work; proportional to tx rate |
| `batchCommandOperate.Execute` | Store batch I/O | Time in the client doing batch ops |
| `executeSingle` / `(*Client).Operate` | Single-key fallback | **Waste** — batches of exactly 1 item (§2) |
| `mallocgc` / `newobject` | GC / allocation | Reducible via object pooling |
| `(*histogram).Observe` / `initStats.*` | Prometheus metrics | Recording overhead; scales with tx rate, contends under concurrency |
| `Syscall6` / `RawSyscall6` | Network I/O | Kernel time for network ops |
| `runtime.schedule` / `runtime.mcall` | Go scheduler | Grows with goroutine count |

**Low CPU utilization + low throughput = I/O-bound**: the process is waiting (on the store or
network), not computing. The gap between CPU capacity and CPU used is wait time; the CPU
profile shows it as time in store-client functions and syscalls. The fix is rarely "more CPU."

---

## 5. Finding concurrency bottlenecks

The subtlest bottleneck: every individual component has headroom, but the **gates between
them** are too narrow. Two gates dominate:

- **Batcher `maxConcurrent` semaphore.** Goroutines blocked at `SetMaxConcurrent.func1` are
  batch dispatches waiting for a concurrency slot. **Many of these while the UTXO store is
  near-idle = the batcher limit is starving the store of work.** Raise `maxConcurrent`.
- **Connection-pool limit.** Each in-flight batch uses one connection per store node it
  touches. If `maxConcurrent` exceeds `ConnectionQueueSize`, the pool becomes the secondary
  gate. Total connections per store node ≈ `ConnectionQueueSize × number_of_client_pods` —
  compare to the store's max-connections setting. Symptom: goroutines blocked *inside* the
  client (not at the semaphore) while the store reports low work-in-progress.

Rule of thumb: **client goroutines piled up + store idle ⇒ open the gates** (concurrency /
connections), don't tune the store.

---

## 6. Host & runtime health signals

Universal systems signals (not Teranode-specific). Useful **relatively** — compare nodes and
runs. None is a fixed SLA.

| Signal | Look-here direction | Meaning |
|---|---|---|
| iowait (kernel CPU) | rising / high vs peers | CPU stalled waiting on disk |
| IPC (instructions/cycle) | low vs peers | memory-stalled CPU; on NUMA hosts suspect missing pinning |
| CPU cache-miss % | high vs peers | random-access memory pattern |
| schedstat wait ratio | high vs peers | threads waiting for CPU → node oversubscribed |
| cgroup `nr_throttled` | >0 and climbing | process hitting its CPU limit (throttled) |
| TCP retransmit ratio | high vs peers | packet loss on that path |
| `rwnd_limited` conns | many | receiver can't keep up (its side is the limiter) |
| softnet drops / time-squeeze | >0 | kernel network stack overflowing its budget |
| NIC drops / errors / pause frames | >0 | physical link / ring-buffer pressure |
| NVMe media errors | >0 | drive hardware fault |

`nr_throttled` climbing on a component is the cleanest "this pod/container is CPU-capped"
signal. Everything else is read by comparison.

---

## 7. Measuring the impact of a change

1. Wait for the changed component to fully restart/roll out.
2. Let the system stabilize (a couple of minutes) before trusting numbers.
3. Snapshot goroutine + CPU profiles and store latency, the same way you did before.
4. Compare **like-for-like** against the pre-change snapshot.

**Direction of improvement** (not absolute targets):

- Goroutines spread more evenly across pipeline stages (no single stage dominating).
- UTXO store doing *more* work (work-in-progress up) at stable-or-better latency.
- Client CPU utilization *up* (more useful work, less waiting).
- Throughput up on the dashboard.

**Direction of regression — revert and investigate:**

- One stage's goroutine count explodes while others are flat (the change hurt that stage).
- Store latency degrades (the change pushed too much load → contention).
- Client CPU drops (more waiting — wrong way).
- Throughput down.

Always change **one thing at a time** so the snapshot diff is attributable.

---

## 8. Common bottleneck patterns

**Propagation low CPU, high goroutine count** → I/O-bound, waiting on the UTXO store.
Look at store latency, batcher `maxConcurrent`, connection-pool size.

**UTXO store idle (low work-in-progress) but Propagation goroutines piling up** → concurrency
gates too narrow (§5). Look at batcher `maxConcurrent`, `ConnectionQueueSize`, goroutines at
`SetMaxConcurrent.func1`.

**One store node much hotter than its peers** → hot node drags every batch (slowest-node
rule). Causes: uneven data distribution, TTL-cleanup/defrag on that node, or different/noisy
hardware. Compare per-node stats (see `datastore-health.md`).

**Tx source piled up at `ProcessTransaction`** → Propagation is the limiter from its view.
Drop into Propagation's profile to find which stage.

**Spend goroutines explode after switching Spend to filter-expressions** → the expression
path can be slower than the Lua-UDF path. Compare batch-sub-write (expression) vs
batch-sub-udf (Lua) latency.

**High executeSingle in the CPU profile** → batcher sending single-item batches (§2). Either
enable drain mode (if arrivals are bursty) or accept it (if genuinely one-at-a-time).

**Throughput dips during block mining** → `setMined` + Pruner share store capacity with the
live pipeline. Look at store latency during mining; larger mined-batch sizes cut round trips;
TTL-based pruning offloads deletes to the store's background cleanup but adds load.

**Prometheus metrics eating CPU** → `(*histogram).Observe` high in the CPU profile; histogram
atomics contend at high tx rates. Proportional to throughput.

**Context-mutex contention in the tx source** → goroutines at `propagateCancel`/`removeChild`.
Caused by `context.WithTimeout(sharedParentCtx, …)` creating O(n) contention on the parent's
child-tracking. Fix: derive from `context.Background()` (tradeoff: loses graceful-shutdown
propagation).
