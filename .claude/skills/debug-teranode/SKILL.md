---
name: debug-teranode
description: Teranode architecture knowledge base and deployment-agnostic debugging methodology. Use this whenever you are reasoning about Teranode internals — the microservice pipeline, transaction or block lifecycle, the UTXO/subtree/block data model, the blockchain FSM states, the pluggable stores (Aerospike/PostgreSQL/SQLite/Kafka/Redpanda), or diagnosing performance bottlenecks, stuck transactions, or blocks that won't validate. Start here for any "how does Teranode work", "why is Teranode slow", or "why is X stuck" question, and use it as the shared foundation beneath the debug-teranode-k8s and debug-teranode-docker skills, which add the environment-specific data collection on top of this reasoning layer.
license: MIT
metadata:
  audience: devops, engineers
  layer: knowledge-base
---

# Debug Teranode — Architecture & Debugging Brain

This skill is the **deployment-agnostic** layer: what Teranode is, how its pieces relate,
what the data looks like, and how to reason about where it's bottlenecked or stuck. It holds
**no** kubectl/docker commands on purpose — those live in the deployment skills:

- **Running on Kubernetes** (k0s/EKS/GKE, operator-managed, kubectl) → use **`debug-teranode-k8s`**.
- **Running on Docker / docker-compose** (local dev stack, CI) → use **`debug-teranode-docker`**.

Those skills handle *collecting* the data; come back here to *interpret* it. If you don't yet
know which environment you're in, ask, or check for `kubectl` vs `docker` access.

## The 60-second mental model

Teranode is BSV's horizontally-scaled node: a set of Go microservices targeting >1M tx/s,
with **no mempool** and an **unbounded block size** made tractable by **subtrees**
(continuously-broadcast, power-of-2 batches of tx IDs that peers pre-validate). The tx path
is a pipeline; debugging is almost always about finding the slowest link.

```text
tx source ─▶ Propagation ─▶ Validator ─▶ UTXO store ─▶ Block Assembly ─▶ (mining) ─▶ Blockchain
                                              ▲                                          │
peers ─▶ P2P ─▶ Block Validation ─▶ Subtree Validation ──┘            notifications ◀────┘
```

- **Validator → Block Assembly is direct gRPC** (`Store()`); most other hops are **Kafka**.
- The node only mines / creates subtrees in FSM state **RUNNING**. **IDLE = does nothing**;
  **CATCHINGBLOCKS = syncing, no mining** (and does not auto-revert on error).
- Stores are **pluggable** — confirm the backend before assuming Aerospike.

That's enough to route a question. For anything deeper, read the references below.

## References — read the one that fits the question

| File | Read it when you need… |
|---|---|
| `references/architecture.md` | Service responsibilities & data flow, tx/block lifecycle, **data-model schemas** (tx, UTXO record, subtree, block, header), stores, Kafka topics, design decisions, FSM, glossary. The "how does it work" reference. Has a TOC — jump to the section. |
| `references/debugging-playbook.md` | **Performance / throughput** debugging: finding the bottleneck layer, the batcher/concurrency model, reading goroutine & CPU profiles, host signals, measuring change impact, common bottleneck patterns. |
| `references/datastore-health.md` | What store metrics mean: Aerospike (`asinfo`, latency histograms, nsup, connection-pool math), PostgreSQL (`pg_stat_*`, replication, slow `StoreBlock`), Kafka/Redpanda (**consumer lag** → which service is behind). |

These are loaded on demand — don't paste them wholesale; open the relevant file and pull
what the task needs.

## Symptom → where to look

| Symptom | First move | Reference |
|---|---|---|
| "Teranode is slow / low TPS" | Find the bottleneck layer (compare each component to its downstream); read goroutine profiles | playbook §1, §3 |
| "Propagation is busy but TPS is low" | I/O-bound vs concurrency-gated? Check store latency, batcher `maxConcurrent`, connection pool | playbook §2, §5 |
| UTXO store looks idle but clients pile up | Open the concurrency gates, don't tune the store | playbook §5 |
| One store node hotter than the rest | Hot-node hunt: per-node stats, nsup, data skew | datastore-health §2 |
| Tx stuck "not mined" | Check UTXO `locked` (two-phase commit) + `unminedSince`; Block Assembly unmined-tx reload | architecture §5, §10 |
| Tx rejected | Double-spend (first-seen)? `ErrTxMissingParent`? policy/min-fee? frozen UTXO? | architecture §3, §10 |
| Block won't validate | PoW/merkle/MTP/coinbase (→ invalid) vs missing-subtree/timeout (→ recoverable retry) | architecture §4, §10 |
| Node "doing nothing" | Check **FSM state** — IDLE needs driving to RUNNING; stuck in CATCHINGBLOCKS is by design on error | architecture §8, §10 |
| A service stalled with no errors | Kafka/Redpanda health — Teranode pauses on unhealthy Kafka; check consumer lag | datastore-health §4 |
| Reorg / chain split confusion | Block Assembly (not Block Validation) builds on the strongest chain; chainwork, not height | architecture §4, §8 |

## Working principles

- **Diagnose by comparison, never against an absolute.** Teranode has no universal "good"
  TPS, pod count, or latency. Compare a component to its downstream, a node to its peers, a
  run to a prior run. Do not invent benchmark numbers — if you need a baseline, capture one.
- **Locate before you tune.** Name the bottleneck layer and the evidence for it before
  proposing any change. The component that *looks* busy (e.g. the last pipeline stage) is
  often not the one gating throughput.
- **The goroutine profile is the highest-signal tool** for the live tx pipeline; the FSM
  state is the highest-signal tool for a node that seems inert.
- **Confirm the backend.** Stores are pluggable — verify Aerospike vs Postgres vs SQLite
  before running backend-specific checks.
- **Be honest about uncertainty.** `architecture.md` flags spots where the docs themselves
  are ambiguous; for consensus-critical conclusions, verify against the code, not just this
  distillation.
