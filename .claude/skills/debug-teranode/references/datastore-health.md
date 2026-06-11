# Teranode Datastore Health Reference (deployment-agnostic)

Teranode's stores are **pluggable** and a given deployment may use any combination. Before
running store checks, first establish *which backend is actually in use* (read the
`utxostore` / `blockchain_store` / `KAFKA_HOSTS` settings, or the deployment skill's
discovery step). Then read this file for what the numbers mean. This is interpretation only
— the `debug-teranode-k8s` / `debug-teranode-docker` skills show how to *reach* each store.

> As in the playbook: **no absolute SLAs.** Read every metric by comparison — node vs node,
> now vs a baseline run. The "direction" columns say where to look, not what's "passing."

## 1. Which backend is in use?

| Store | Common backends | How it's selected |
|---|---|---|
| **UTXO store** | Aerospike (production reference), PostgreSQL, SQLite (dev/in-mem), Memory, Null | `utxostore` URL scheme (`aerospike://`, `postgres://`, `sqlite://`, `memory://`) |
| **Blockchain store** | PostgreSQL (production), SQLite (test) | `blockchain_store` URL scheme |
| **Coinbase / other SQL stores** | PostgreSQL, SQLite | their respective URLs |
| **Blob (tx/subtree) store** | File, S3 / S3-compatible, HTTP, Memory, Null | `blockstore` / `subtreestore` URL scheme |
| **Messaging** | Kafka, Redpanda (Kafka-API-compatible) | `KAFKA_HOSTS` |

A "check Aerospike" instinct is wrong if the UTXO store is Postgres. Confirm first.

---

## 2. Aerospike (UTXO store)

Reached via `asinfo` inside the Aerospike server container. The Teranode UTXO data lives in
an Aerospike **namespace** (their word for a database) — commonly `utxo-store`, but
**discover it** (`asinfo -v namespaces`) rather than assuming.

**Always compare across ALL nodes.** A batch waits for the slowest node, so one hot node
caps the whole cluster.

### Key statistics (`asinfo -v statistics`, `asinfo -v 'namespace/<ns>'`)

| Stat | Direction to look | Meaning |
|---|---|---|
| `rw_in_progress` | low *with* low throughput = starved (look upstream); high = busy | active read/write ops in flight |
| `client_connections` | near `proto-fd-max` = connection pressure | total client connections |
| `batch_index_queue` | growing | batch-index processing falling behind |
| `cache_read_pct` | falling toward disk-bound territory | % of reads served from cache vs disk |
| `write_q` (per device) | non-zero / growing | disk write queue behind |
| `objects` / `non_expirable_objects` | uneven across nodes | data-distribution skew (drives hot nodes) |

`rw_in_progress` interpreted against the service-thread count tells you utilization. The most
useful read is **idle store + busy clients ⇒ the bottleneck is the gates above the store**
(see playbook §5), not the store.

### Latency histograms (`asinfo -v 'latencies:'`)

Format: `{namespace}-operation:unit,rate,>1ms%,>2ms%,>4ms%,>8ms%,>16ms%,…`

| Operation | What it measures |
|---|---|
| `batch-index` | overall batch command latency (network + all sub-ops) |
| `batch-sub-read` | individual read sub-ops within a batch |
| `batch-sub-write` | individual write sub-ops (Create, SetLocked via expressions) |
| `batch-sub-udf` | individual UDF sub-ops (Spend/SetLocked via Lua) |
| `read` / `write` / `udf` | single-key ops — a spike here hints at the **executeSingle fallback** (playbook §2) |

Rising `>1ms` / `>8ms` buckets on **one** node vs peers = that node is the drag. Compare the
expression path (`batch-sub-write`) vs the Lua path (`batch-sub-udf`) when diagnosing Spend.

### nsup (namespace supervisor / TTL cleanup)

When DAH/TTL expiry is on, the `nsup` thread scans and deletes expired records.

| Stat | Direction | Meaning |
|---|---|---|
| `nsup_cycle_duration` | > `nsup-period` ⇒ nsup runs continuously | last scan duration |
| `nsup_cycle_deleted_pct` | high | heavy cleanup this cycle |
| `non_expirable_objects` vs `objects` | uneven across nodes | uneven TTL load → hot nodes |
| `nsup-threads` | too few ⇒ longer cycles | cleanup parallelism |

Heavy nsup on one node is a classic hot-node cause and shows up as that node's latency
diverging during/after block mining (when `setMined` + Pruner generate deletions).

### Config worth dumping (`asinfo -v 'get-config:…'`)

`replication-factor`, `read-page-cache`, `post-write-cache`, defrag settings,
`service-threads`, `batch-index-threads`, `proto-fd-max`, `partition-tree-sprigs`,
`nsup-period`, `nsup-threads`. These explain *why* a node behaves differently from its peers.

### Connection-pool math (client side)

`ConnectionQueueSize` (per node, per client pod) × number of client pods ≈ total connections
hitting each Aerospike node — compare to `proto-fd-max`. With
`LimitConnectionsToQueueSize=true` the per-pod cap is hard, so if `maxConcurrent` >
`ConnectionQueueSize` the pool, not the server, is the gate.

---

## 3. PostgreSQL (blockchain store, and UTXO store when configured as SQL)

Reached via `psql` (or the operator's status). Production Teranode typically runs Postgres
via an operator (e.g. CloudNativePG) with a primary + replicas.

| Check | Query / source | Direction to look |
|---|---|---|
| Active vs idle connections | `pg_stat_activity` (count by `state`) | near `max_connections` = pool exhaustion |
| Long / stuck queries | `pg_stat_activity` where `state='active'` ordered by `query_start` | a query running for minutes blocks others |
| Lock waits | `pg_locks` joined to `pg_stat_activity` (`granted=false`) | blocked backends = contention |
| Replication lag | `pg_stat_replication` (`write/flush/replay_lag`) | rising lag = replica behind; read traffic stale |
| Cache hit ratio | `pg_stat_database` (`blks_hit / (blks_hit+blks_read)`) | falling = more disk reads |
| Slow `StoreBlock` / catchup | block insert rate | the known recursive-CTE + cache-invalidation slowdown on long chains (architecture.md §6) |
| Table bloat / autovacuum | `pg_stat_user_tables` (`n_dead_tup`, `last_autovacuum`) | high dead tuples = vacuum behind |

For an operator-managed cluster, also check the operator's own view: is the cluster
**Healthy**, who is **primary**, are replicas **streaming**, any failover in progress. A
catchup slowdown is frequently the blockchain store, not the UTXO store — don't only look at
Aerospike.

---

## 4. Kafka / Redpanda (messaging)

Teranode pauses processing when Kafka is unhealthy and resumes when it recovers, so a "stuck"
service is often a Kafka problem. Redpanda speaks the Kafka API; the same checks apply
(`rpk` or any Kafka admin tool).

| Check | Direction to look | Meaning |
|---|---|---|
| **Consumer-group lag** per topic | growing | consumer can't keep up with the producer (the consumer service is the bottleneck) |
| Under-replicated / offline partitions | >0 | broker/disk problem; durability at risk |
| Partition count vs consumer concurrency | — | effective concurrency = `partitions / consumer_ratio`, *not* the `kafkaWorkers` setting |
| Broker disk usage | near full | Redpanda/Kafka will throttle or stall producers |
| Topic throughput (msgs/s in vs out) | out << in | backlog building |

Map lag to the responsible consumer:

| Topic | Lagging consumer = bottleneck |
|---|---|
| `validatortxs` | Validator behind tx ingress |
| `txmeta` | Subtree Validation behind |
| `blocks` | Block Validation behind / catching up |
| `subtrees` | Subtree Validation behind |
| `blocksFinal` | Block Persister behind |
| `rejectedTx` | P2P behind (low impact) |

Lag on `blocks` while the node is in **CATCHINGBLOCKS** is expected during sync — correlate
with FSM state before calling it a bug.

---

## 5. Blob store (file / S3)

Less metric-rich, but check: backend reachability (S3 endpoint / mount present), free space
(file backend filling up stalls subtree/tx writes), and DAH expiry working (old `.subtree` /
`.tx` blobs being cleaned). For S3-compatible backends, watch request error/throttle rates.
A failing blob store surfaces as Subtree/Block Validation errors fetching subtree or tx data.
