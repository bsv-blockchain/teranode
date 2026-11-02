# Probe Service

## Index

1. [Description](#1-description)
2. [How to Enable the Probe](#2-how-to-enable-the-probe)
3. [Security](#3-security)
4. [The Dashboard](#4-the-dashboard)
    - [4.1 The Six Stages](#41-the-six-stages)
    - [4.2 Kafka Flow Strips and Consumer Lag](#42-kafka-flow-strips-and-consumer-lag)
    - [4.3 Drain Time](#43-drain-time)
    - [4.4 The Lanes View](#44-the-lanes-view)
5. [Alarms](#5-alarms)
    - [5.1 Alarm States](#51-alarm-states)
    - [5.2 Learned Baselines](#52-learned-baselines)
    - [5.3 Floors](#53-floors)
    - [5.4 Cause Chains and Start of Chain](#54-cause-chains-and-start-of-chain)
    - [5.5 Source Alarms](#55-source-alarms)
    - [5.6 Snooze and the Alarm Log](#56-snooze-and-the-alarm-log)
6. [The State File](#6-the-state-file)
7. [Data Store Requirements](#7-data-store-requirements)
    - [7.1 Aerospike](#71-aerospike)
    - [7.2 PostgreSQL](#72-postgresql)
    - [7.3 Kafka](#73-kafka)
    - [7.4 Tx Meta Cache](#74-tx-meta-cache)
8. [Fault Behaviour](#8-fault-behaviour)
9. [HTTP API](#9-http-api)
10. [Configuration Options (Settings Flags)](#10-configuration-options-settings-flags)
11. [Limits](#11-limits)
12. [Troubleshooting](#12-troubleshooting)
13. [Directory Structure and Main Files](#13-directory-structure-and-main-files)

## 1. Description

The Probe service shows the live state of one Teranode node in a web dashboard.
It shows where transactions and blocks flow, where they wait, and which stage is slow.

The probe is an overlay service. It is off by default.
It only reads. It never writes to a Teranode service or to a data store.

The probe does these tasks:

- It samples the Prometheus metrics of the node services every 2 seconds.
- It reads the FSM state, the chain tip and the peer heights from the Blockchain and P2P services.
- It reads Aerospike, PostgreSQL and Kafka every 15 seconds.
- It compares each important reading with a normal value that it learns for this node.
- It raises an alarm with fixed text when a reading stays outside its normal range.
- It sends a new sample to each open dashboard over a WebSocket.

## 2. How to Enable the Probe

The probe starts only when you enable it and set a token.

Do these steps:

1. Set `probe_authToken` to a long random value in your local settings.
2. Start the node with `-probe=1`, or set `startProbe = true`.
3. Open `http://localhost:8100/` in a browser on the same host.
4. Type the token in the login form.

An all-in-one node runs the probe in the same process as the other services.
The probe then reads the metrics of all services from the process registry. You do not set `probe_metricsTargets`.

```shell
SETTINGS_CONTEXT=dev.[YOUR_CONTEXT] go run . -probe=1
```

In Kubernetes, run the probe as its own service with `-all=0 -probe=1`.
A probe in its own pod cannot see the metrics registry of the other pods. Set `probe_metricsTargets` to the metrics endpoint of each service, for example `blockvalidation:9091|validator:9091`.
The probe scrapes a `host:port` entry at `http://host:port/metrics` and a `host:port/path` entry at `http://host:port/path`. It uses an entry with a scheme, such as `https://validator:9091/metrics`, as given. It ignores empty entries. A node with a custom metrics path needs the path or a full URL.
The probe also needs the Blockchain and P2P gRPC addresses from the normal settings.

## 3. Security

The probe protects its data with a static bearer token.

- Every API request must send `Authorization: Bearer <token>`. The probe compares the token in constant time.
- A request with no token or a wrong token gets `401` and an empty body.
- The WebSocket must send `{"auth": "<token>"}` as its first message within 5 seconds. Otherwise the probe closes it with code `4401`.
- When the probe is enabled and `probe_authToken` is empty, the node refuses to start.
- The probe never writes the token or a data store credential to the log.

The probe binds to `localhost:8100` by default. Only a browser on the same host can reach it.

To reach the probe in Kubernetes, do these steps:

1. Set `probe_httpListenAddress` to `:8100`.
2. Put a reverse proxy with TLS in front of the probe.
3. Expose only the proxy. Do not expose port 8100 directly.

The probe itself serves plain HTTP. It has no TLS of its own.

The dashboard keeps the token only in the session storage of the browser tab. It does not use a cookie, and it asks the browser not to save the token. When you close the tab, the browser forgets the token.

The probe serves the dashboard files (`/`, `/app.js`, `/view.js`, `/style.css`) without a token. These files contain no node data. All data comes from the API and the WebSocket, which need the token.
The dashboard pages send a strict Content Security Policy. Scripts, styles and connections are limited to the probe origin.

The probe is read-only toward every store:

- It reads Prometheus metrics.
- It calls read methods on the Blockchain and P2P gRPC clients.
- It sends Aerospike info commands. It does not read or write records.
- It runs PostgreSQL queries in a `READ ONLY` transaction.
- It reads Kafka offsets through the admin API. It never joins a consumer group and never commits an offset.

## 4. The Dashboard

The dashboard needs a desktop browser with a window width of 1280 px or more.

The home page shows the six stages of the node from left to right. Arrows between the stages show the flows. A panel LED shows the worst status of the readings in that stage.
Click a stage to open its detail view. The detail view shows each reading with its normal value and a 10 minute sparkline.

Colour shows only status:

- Green: the reading is normal.
- Amber: a warning alarm is active.
- Red: a critical alarm is active.
- Grey: no data. The panel shows the age of the last good sample.

When a source fails, every value from that source goes grey with the age of its last good sample. This includes the FSM chip and the tip chip (Blockchain source), the best peer height (P2P source), the Kafka lag on the flow strips and in the Kafka topics panel (Kafka source) and the Aerospike and PostgreSQL tanks. The block height chips and the drain text keep their last values during an outage.

### 4.1 The Six Stages

| Stage | Readings | Source |
|-------|----------|--------|
| Ingress | Propagation tx/s, subtree intake/s, peers, rejected tx/s, subtree queue, age of the oldest queued subtree | Propagation, Subtree Validation and P2P metrics |
| Validator | Validated tx/s, latency p99, queue fill, spend batch fill, errors/s | Validator metrics (see the note below the table), Aerospike spend batch metrics |
| Tx meta cache | Hit rate, misses/s, evictions/s, inserts/s, fill, current and previous generation share, entries, size, capacity | Tx meta cache metrics |
| Block Assembly | State (running or paused), tip lag in blocks, age of the oldest queued transaction, ingest queue fill, subtree fill | Block Assembly metrics |
| Blockchain | FSM state, tip height, best peer height, fork depth, blocks not yet persisted, dropped subscribers, heights of queued blocks | Blockchain gRPC, P2P gRPC (best peer height), Block Validation and Blockchain metrics |
| Data stores | Aerospike and PostgreSQL disk fill, size, read latency, reads/s, writes/s, PostgreSQL commits/s | Aerospike info protocol, PostgreSQL statistics views, UTXO store metrics |

A reading shows `no data` when its metric does not exist on this node. For example, the Data stores stage shows no data for a store that the node does not use.
The persister backlog is a lower bound. The probe asks for at most 25 blocks and shows "25 or more" at that limit.
The Blockchain panel shows the count of waiting blocks and the lowest three heights. A "+N more" marker shows how many waiting blocks the panel does not list.
The probe reads the peer heights from the P2P service as its own `p2p` source. When that call fails, the best peer height goes grey with its age, and the tip chip turns grey. The FSM state, the tip and the backlog stay live. After 90 seconds the Blockchain stage raises its source alarm.

The Data stores tanks show reads and writes on separate lines:

- Aerospike reads are the sum of the rates of `teranode_aerospike_txmeta_get`, `teranode_aerospike_txmeta_get_multi_n` and `teranode_aerospike_utxo_get`.
- Aerospike writes are the sum of the rates of `teranode_aerospike_utxo_store`, `teranode_aerospike_utxo_spend`, `teranode_aerospike_txmeta_set_mined_batch_n`, `teranode_aerospike_utxo_reset` and `teranode_aerospike_utxo_delete`.
- PostgreSQL reads are rows returned and fetched (`tup_returned + tup_fetched` in `pg_stat_database`). PostgreSQL writes are rows inserted, updated and deleted. Both show no data on the first sample, because a rate needs two samples.

When a tank has more notes than fit below it, the last note line says how many more notes the stage detail view shows.

The validated tx/s reading comes from `teranode_validator_transactions_validate`. This histogram also counts the transactions that block validation checks and the transactions that fail validation. The reading carries the note "includes block-validation checks and rejected transactions". During catch-up the reading shows block-validation work. The latency p99 covers the script and policy checks only. It does not include the parent reads and the UTXO spend.

### 4.2 Kafka Flow Strips and Consumer Lag

The arrows between the stages are the Kafka flow strips. Each strip shows a rate from the service metrics.
The probe also reads the consumer-group lag of the matching Kafka topic. The lag is the sum, over all partitions and all groups that read the topic, of the end offset minus the committed offset.

The probe reads these topics from the Kafka settings:

- `tx-input`: the topic in `kafka_validatortxsConfig`
- `subtrees`: `kafka_subtreesTopic`
- `txmeta`: `kafka_txmetaTopic`
- `blocks`: `kafka_blocksTopic`
- `blocks-final`: `kafka_blocksFinalTopic`

A group that has subscribed but has not committed an offset shows the full topic length as lag.

### 4.3 Drain Time

The probe adds the queued blocks and the subtree queue to get one backlog.
The block part is the count of waiting blocks, up to the persister limit of about 25. It is not limited to the heights that the panel shows. While the FSM is in `CATCHINGBLOCKS`, the block part is at least the gap between the best peer height and the tip.
It fits a line through the backlog samples of the last 5 minutes. The drain time is the backlog divided by the drain rate.
When the backlog grows, the dashboard shows "growing" and no time.

### 4.4 The Lanes View

The links in the top right corner switch between the Stages view and the Lanes view. The Lanes view opens at `#/lanes`. Both views show the same snapshot.

The Lanes view groups the readings by what the node does, not by service:

| Lane | What it shows |
|------|---------------|
| Transactions | Raw transactions that arrive, the validation result and the transactions that the block assembler holds |
| Subtrees | Subtrees that peers announce, the subtrees that are already known, and the missing transactions that the node fetches |
| Blocks | Blocks that peers announce and that the node validates, the tip height and the fork depth |
| Mining | The block assembler state, the candidates that miners fetch and the solutions that the node submits |

The Tx meta cache and Data stores panels follow the lanes as shared panels. An arrow between two lanes glows only while work flows.

On an idle node, the rate readings are 0. The totals in the Blocks and Mining lanes count from the start of the node and change only when a block or a solution arrives.

Three readings come from metrics that only recent Teranode builds export. An older node shows "no data" for them:

- Transactions that the node skipped because the UTXO store already holds them (`teranode_validator_existing_tx_total`).
- Missing subtrees of a block that the node still has to fetch (`teranode_subtreevalidation_block_missing_subtrees_outstanding` and `teranode_subtreevalidation_block_missing_subtrees_total`).
- The age of the last mining candidate that block assembly served (`teranode_blockassembly_last_mining_candidate_timestamp_seconds`). The age grows while no miner fetches a candidate.

Below the lanes, two more panels show shared infrastructure.

The Blob stores panel shows the subtree store and the block store. For each store it shows reads per second, writes per second, errors per second and the p99 latency of reads and writes. Teranode counts these calls in a wrapper around each blob store:

- `teranode_blob_reads_total`: calls to `Exists`, `Get` and `GetIoReader`
- `teranode_blob_writes_total`: calls to `Set`, `SetFromReader`, `SetDAH` and `Del`
- `teranode_blob_errors_total`: failed calls. A not-found result and a call that the caller cancels are not errors.
- `teranode_blob_read_duration_seconds` and `teranode_blob_write_duration_seconds`: call duration

All five metrics carry a `store` label: `tx`, `subtree`, `temp`, `block` or `block_persister`. The panel shows `subtree` and `block`. The latency of `GetIoReader` is the time to open the reader. A p99 reading shows "no data" when no call finished in the last interval.

The Kafka topics panel lists every Kafka topic of the node, worst first. A topic shows the total lag of all consumer groups, with a bar on a log scale. A topic with no consumer group shows "no consumers". The bar has no colour thresholds, because a normal lag depends on the topic. The list uses the topics from the Kafka settings. When the Kafka source fails, the panel shows the age of the last good sample and no lag.

## 5. Alarms

All alarm text is fixed text in the probe. The probe does not generate text.

### 5.1 Alarm States

| Condition | Result |
|-----------|--------|
| A reading stays outside its tolerance for 30 s | Amber alarm |
| A reading stays beyond its floor, or far outside its tolerance, for 60 s | Red alarm |
| A reading stays normal for 60 s | The alarm clears |
| A reading has no data for 60 s | The alarm clears |

"Far outside" means more than three tolerances from the normal value.

### 5.2 Learned Baselines

The probe learns a normal value and a tolerance for each alarm reading on this node.

- The baseline is a weighted mean with a half-life of 1 hour. A new sample counts more than an old sample.
- The tolerance is the larger of three standard deviations and 5% of the mean. Some readings also have a fixed minimum tolerance.
- After 5 samples, the probe clamps each new sample to the mean plus or minus eight tolerances. One spike cannot move the baseline far.
- A baseline is ready after 30 samples and `probe_baselineWarmup` (default 30 minutes).

Before the baseline is ready, the reading shows the note `Calibrating` and raises no alarm from its baseline.
After a restart, the tx meta cache is cold. For its first 30 samples, or while the cache is less than 10% full, the hit rate shows `Warming` and raises no alarm.

**The baseline freezes while an alarm is pending or active.** The probe learns only from normal samples. This stops an alarm from teaching the probe that the bad value is normal.
If the node moves to a new normal level for good, the alarm stays. Use one of these ways out:

1. Snooze the alarm.
2. Set a floor for the reading (see [5.3 Floors](#53-floors)).
3. Stop the node, delete the state file, and start the node again. This resets all baselines, and calibration starts again.

### 5.3 Floors

A floor is a fixed limit that you set for one reading. A reading beyond its floor for 60 s raises a red alarm, also during calibration.

Set floors in `probe_floors` as a pipe-separated list of `metric=value` entries:

```conf
probe_floors = cache.hitRate=0.9|validator.queueFill=0.8
```

For `cache.hitRate`, a value below the floor is bad. For all other keys, a value above the floor is bad.

| Floor key | Unit |
|-----------|------|
| `cache.hitRate` | fraction from 0 to 1 |
| `stores.aerospike.readP99Sec` | seconds |
| `validator.latencyP99Sec` | seconds |
| `validator.queueFill` | fraction from 0 to 1 |
| `assembly.queueHeadAgeSec` | seconds |
| `assembly.tipLagBlocks` | blocks |
| `ingress.subtreeQueueOldestSec` | seconds |
| `chain.persisterBehind` | blocks |

The node refuses to start when a floor entry has one of these faults:

- It is not in the `metric=value` format.
- Its value is not a finite number.
- It names a key twice.
- Its key is not in this table.

### 5.4 Cause Chains and Start of Chain

One slow component often causes alarms in other stages. The probe sorts alarms into two cause chains:

| Chain | Order |
|-------|-------|
| Throughput | 1. Tx meta cache hit rate, 2. Aerospike read latency, 3. Validator latency and queue, 4. Block Assembly input and tip lag |
| Catch-up | 1. FSM `CATCHINGBLOCKS`, 2. Block persister behind, 3. Subtree queue age, 4. Validator latency and queue |

The probe uses the catch-up chain while the FSM is in `CATCHINGBLOCKS`. Otherwise it uses the throughput chain.
The active alarm that is earliest in the chain gets the tag "Start of chain". Start your investigation there.
While the node catches up, the "Node is catching up" alarm is first in the catch-up chain. It has the tag during the whole catch-up, unless you snooze it.

### 5.5 Source Alarms

The probe tracks the health of each of its sources: `metrics`, `chain`, `p2p`, `kafka`, `aerospike` and `postgres`. The `p2p` source feeds the Blockchain stage.
When a source has no fresh data for more than 60 seconds, the readings of its stages go grey. After 30 more seconds, the probe raises an amber "source has no data" alarm for each stage that the source feeds.
In a test, an unreachable PostgreSQL URL raised this alarm after about 94 seconds. The node kept on processing blocks.

### 5.6 Snooze and the Alarm Log

A snooze applies to one alarm ID. It applies to all dashboards, and it survives a restart.
A snoozed alarm stays in the list with a snoozed mark. It does not take the "Start of chain" tag.
A snooze lasts `probe_snoozeDefault` (default 15 minutes) unless you give a duration of 1 to 1440 minutes.

The probe writes each raised alarm and each change of severity to the alarm log. The log keeps entries for `probe_alarmLogRetention` (default 7 days).

## 6. The State File

The probe keeps one local JSON file at `probe_statePath`. When the setting is empty, the file is `probe_state.json` in the working directory of the node.

The file holds these items:

- The learned baselines
- The snoozes
- The alarm log

The probe writes the file atomically. It writes a temporary file, syncs it, and renames it over the old file. A crash cannot leave half a file.
The probe saves the baselines once a minute. It saves a snooze or an alarm within about 1 second.

When the file is not valid JSON at startup, the probe moves it aside to `<path>.corrupt-<UTC timestamp>`, for example `probe_state.json.corrupt-20261028T170018Z`. It logs one warning line and starts with an empty store. It never deletes the corrupt file.
The probe refuses to start when the directory of the state file cannot be created or is not writable.

## 7. Data Store Requirements

### 7.1 Aerospike

The probe reads Aerospike from `probe_aerospikeURL`. When that setting is empty and the UTXO store is Aerospike, the probe uses the UTXO store URL. The probe then connects with the node's own read-write credentials. It sends only info commands, but we recommend a read-only user in `probe_aerospikeURL`.
The probe sends info commands to every node of the cluster. It adds the size of all nodes, and it shows the highest disk fill and the highest read latency.

Read latency p99 needs `enable-benchmarks-read true` on the namespace. Without it, the read latency reading shows no data.

### 7.2 PostgreSQL

The probe reads PostgreSQL from `probe_postgresURL`. When that setting is empty and the blockchain store is PostgreSQL, the probe uses the blockchain store URL. The probe then connects with the node's own read-write credentials. It uses only `READ ONLY` transactions, but we recommend a read-only role in `probe_postgresURL`.

- PostgreSQL cannot report the size of its disk through SQL. Set `probe_postgresDiskCapacityGB` to see the disk fill. When it is 0, disk fill shows "disk capacity not set".
- PostgreSQL has no p99 latency without extensions. With `pg_stat_statements`, the probe shows the mean execution time over the sample interval, labelled "mean". Without it, the reading shows "needs pg_stat_statements".
- The probe does not force `sslmode`. When the URL has no `sslmode`, the default of the PostgreSQL driver applies. The probe passes through `sslmode`, `sslrootcert`, `sslcert`, `sslkey`, `connect_timeout` and `application_name`. It drops other URL parameters.

### 7.3 Kafka

The probe connects to the brokers in `KAFKA_HOSTS` (`kafka_hosts`) and reads consumer-group lag through the Kafka admin API.
The probe user needs permission to describe topics and groups and to read offsets.

The probe client does not use TLS or SASL. A cluster that requires either shows no Kafka data.
The probe treats Kafka as in-memory when `KAFKA_SCHEMA` is `memory` or a Kafka config URL uses `memory://`, and no Kafka config URL uses `kafka://`. Then the probe does not connect to `kafka_hosts`. Kafka is then not configured for the probe. The flow strips show no lag, and no Kafka source alarm is raised.

### 7.4 Tx Meta Cache

The tx meta cache is a ring cache. After its first pass it stays full, and old entries make room for new ones. A full cache is normal.
The live signals are the hit rate and the eviction rate. A falling hit rate with a high share of entries from the previous generation means that the cache is too small for the load.

The tx meta cache publishes two gauges for the probe:

- `teranode_tx_meta_cache_bytes`: the bytes that the cache uses now
- `teranode_tx_meta_cache_capacity_bytes`: the bytes that the cache can use

The probe shows fill as bytes divided by capacity.

## 8. Fault Behaviour

A fault in the probe never stops the node.

- The probe recovers every panic in its own goroutines and logs it.
- A slow or hung source times out. It cannot stop a sampling tick.
- When a source fails, the probe logs one warning line with the cause. Credentials in URLs are masked. When the source works again, the probe logs one line. The probe does not repeat the warning on every sample.
- A WebSocket client that does not read cannot block the sampler or other clients.
- After five consecutive panics, the probe stops itself. Only the probe stops. The node keeps running.
- When the probe stops itself, it logs one error line. Its health check still returns `200` with the message "probe stopped after repeated failures; the node must be restarted to resume probe monitoring; the node itself is not affected".
- To start the probe again, restart the node.

## 9. HTTP API

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/snapshot` | The latest sample as JSON |
| GET | `/api/history?stage=<stage>&minutes=<1-10>` | The readings of one stage over the last minutes. `stage` is one of `ingress`, `validator`, `cache`, `assembly`, `chain`, `stores` |
| GET | `/api/alarms?since=<minutes or RFC3339 time>` | The alarm log. The default is the last 24 hours |
| POST | `/api/snooze` | Body `{"id": "<alarm id>", "minutes": <0-1440>}`. 0 means `probe_snoozeDefault` |
| POST | `/api/unsnooze` | Body `{"id": "<alarm id>"}` |
| GET | `/health` | Probe health |
| GET | `/ws` | WebSocket. Send `{"auth": "<token>"}` first. Each message is one sample |

Every path except `/ws` and the dashboard files needs the bearer token.
The probe accepts at most 16 WebSocket clients at the same time. A further client gets `503`.

## 10. Configuration Options (Settings Flags)

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `startProbe` | bool | `false` | Start the Probe service. The `-probe=1` flag does the same |
| `probe_authToken` | string | empty | Bearer token for every request. Required. An empty value stops the probe at startup |
| `probe_httpListenAddress` | string | `localhost:8100` | Bind address of the dashboard and the API |
| `probe_sampleInterval` | duration | `2s` | Interval between samples of the service metrics, the Blockchain and P2P |
| `probe_slowSampleInterval` | duration | `15s` | Interval between samples of Aerospike, Kafka and PostgreSQL |
| `probe_baselineWarmup` | duration | `30m` | Minimum learning time before a baseline is ready |
| `probe_snoozeDefault` | duration | `15m` | Snooze length when the request gives no duration |
| `probe_alarmLogRetention` | duration | `168h` | Age after which the probe removes alarm log entries |
| `probe_metricsTargets` | []string | empty | Pipe-separated metrics endpoints. A `host:port` entry is scraped at `http://host:port/metrics`, a `host:port/path` entry at `http://host:port/path`. An entry with a scheme is used as given. Empty entries are ignored. Empty means the registry of the probe process |
| `probe_statePath` | string | empty | Path of the state file. Empty means `probe_state.json` in the working directory |
| `probe_aerospikeURL` | url | empty | Aerospike URL. Empty means the UTXO store URL when it is Aerospike |
| `probe_postgresURL` | url | empty | PostgreSQL URL. Empty means the blockchain store URL when it is PostgreSQL |
| `probe_postgresDiskCapacityGB` | float64 | `0` | Disk capacity of the PostgreSQL volume in GB (1 GB = 1e9 bytes). 0 means unknown |
| `probe_floors` | []string | empty | Pipe-separated `metric=value` floors. See [5.3 Floors](#53-floors) |

## 11. Limits

- The probe shows one node. It does not combine nodes.
- The dashboard needs a desktop window width of 1280 px or more.
- The stage detail view is the only drilldown page. There are no other named drilldown pages.
- The probe does not correlate metrics. The cause chains use a fixed order.
- History covers the last 10 minutes only. The probe does not keep long-term series. Use Prometheus and Grafana for that.
- The Kafka client has no TLS or SASL support.

## 12. Troubleshooting

| Symptom | Cause | Action |
|---------|-------|--------|
| The node does not start, and the log says `probe_authToken must be set` | The probe is enabled without a token | Set `probe_authToken`, or disable the probe |
| The browser shows nothing at `http://<host>:8100/` | The probe binds to `localhost` by default | Use an SSH tunnel, or set `probe_httpListenAddress` and put a TLS reverse proxy in front |
| The login form says the token is wrong | The token does not match `probe_authToken` | Check the setting. Leading and trailing spaces are removed from the setting |
| All service readings are grey on a probe in its own pod | `probe_metricsTargets` is empty or points to the wrong endpoints | Set the metrics `host:port` or URL of each service |
| The FSM and the tip are grey | The probe cannot reach the Blockchain service | Check the Blockchain gRPC address |
| The tip chip is grey and says "peer no data" | The probe cannot reach the P2P service | Check the P2P gRPC address. FSM and tip stay live without P2P |
| The tip chip is grey and says "no peers" | The P2P service has no peers | Check the peer connections of the node |
| Aerospike read latency is grey | Read benchmarks are off | Set `enable-benchmarks-read true` on the namespace |
| PostgreSQL disk fill says "disk capacity not set" | `probe_postgresDiskCapacityGB` is 0 | Set the volume size in GB |
| An alarm stays after the node changed for good | The baseline is frozen while the alarm is active | Snooze the alarm, set a floor, or reset the baselines (see [5.2](#52-learned-baselines)) |
| The log shows `state file was corrupt; moved it to ...` | The state file was not valid JSON | No action needed. The old file stays next to the new one |
| The health check says "probe stopped after repeated failures" | Five consecutive panics stopped the probe | Collect the log, then restart the node |
| A 17th or later browser tab gets no data, and the API returns `503` on `/ws` | More than 16 WebSocket clients | Close unused tabs |

## 13. Directory Structure and Main Files

```text
/services/probe/
├── Server.go        # Service lifecycle, HTTP server, self-stop
├── http.go          # Routes, token check, dashboard files and headers
├── api.go           # REST handlers
├── hub.go           # WebSocket hub and client writers
├── pipeline.go      # Sampler, alarm engine and drain wiring; state file handling
├── sampler.go       # Fast and slow sampling loops and history
├── faults.go        # Panic counting and self-stop
├── alarm/           # Baselines, floors, rules, texts, engine, drain time, state store
├── collect/         # Metrics, chain, Kafka, Aerospike and PostgreSQL collectors
├── guard/           # Panic recovery helper
├── snapshot/        # JSON contract between the probe and the dashboard
└── ui/              # Embedded dashboard (HTML, CSS, JavaScript)
```

For the settings struct, see `settings/probe_settings.go`.
