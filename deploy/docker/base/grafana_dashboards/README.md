# Grafana Dashboards

Dashboards in this directory are auto-provisioned into Grafana via `main.yaml`.
The **In-Grafana title** column is the dashboard's `.title` field — that is the
name you search for in the Grafana UI, and it does not always match the file
name.

## Teranode Service Overview

**File:** `teranode/teranode-overview-dashboard.json`
(in-Grafana title: **Teranode Service Overview**, uid `cde0cde9-5698-4433-b851-ccae709ee2b2`)

The primary day-to-day operational dashboard, covering the main Teranode
services and pipeline stages. Start here for general health checks. Most panels
are on the always-expanded `Teranode Overview` row; the `Teranode Latency` row
is collapsed by default and holds the latency heatmaps.

## Other dashboards in this stack

| In-Grafana title | File | uid | Scope | Requires |
| ---------------- | ---- | --- | ----- | -------- |
| Aerospike Batch Index Bottleneck Diagnostic (1024 Buffers) | `teranode/teranode-batch-index-dashboard.json` | `stwh6cx` | Aerospike batch-index buffer pool: queue depth, delay, buffer churn | Aerospike Prometheus exporter (`aerospike_node_stats_batch_index_*`) |
| Latency View | `aerospike/aerospike-latency.json` | `ZoeGW1DBk` | UTXO-store dependency latency | Aerospike Prometheus exporter |
| Namespace View | `aerospike/aerospike-namespace.json` | `zGcUKcDZz2` | UTXO-store dependency namespace stats | Aerospike Prometheus exporter |

All three read Aerospike exporter metrics, not Teranode's own metrics. `Aerospike
Batch Index Bottleneck Diagnostic` is named after its first panel, *Dispatcher
Batch Queue*, but every panel queries
`aerospike_node_stats_batch_index_*{cluster_name="$cluster"}` — it diagnoses the
Aerospike server's batch-index buffer pool, not a Teranode dispatcher queue. Its
panels are empty unless an Aerospike exporter is being scraped and the `$cluster`
variable resolves.

## Not shipped here

The `compose` stack (`compose/grafana/dashboards/`) additionally ships
**BlockAssembler State Monitoring** (`blockassembly-state.json`, documented in
that directory's own README) that is not present in this docker-base stack.
Conversely, this stack's `Teranode Service Overview` and Aerospike dashboards are
not present under `compose/grafana/dashboards/`.

Copying a dashboard JSON between the two stacks works only if its datasource
references survive the move. Both stacks provision their Prometheus datasources
without an explicit `uid` (see `deploy/docker/base/grafana_datasource.yaml` and
`compose/grafana/datasources/main.yaml`), so Grafana generates a uid at startup
and any dashboard that hard-codes one will render "datasource not found" on every
panel. Every querying panel in this directory therefore resolves its datasource
through a `datasource`-type template variable (`${prometheus}` in the Teranode
dashboards, `${DS_AEROSPIKE_PROMETHEUS}` in the Aerospike ones), which Grafana
renders as a dropdown; the only hard-coded uids left are on row headers, which
issue no queries. Keep that pattern when copying a dashboard across, and note
that the Aerospike dashboards additionally need an Aerospike exporter in the
target stack's Prometheus.
