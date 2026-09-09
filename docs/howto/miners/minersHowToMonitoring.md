# Monitoring Teranode

This guide covers the dashboards Teranode ships out of the box, the metrics
that distinguish a healthy node from a degraded one, and how to get started
with alerting. It applies to both the Docker quickstart and Kubernetes
operator deployments — the metrics and the dashboard files are the same; what
differs is how you reach Grafana/Prometheus and which dashboards are provisioned
for you.

## Dashboards

Teranode ships pre-built Grafana dashboards. Not all of them are provisioned by
every stack — the `Provisioned by` column says which one mounts each file:

| Dashboard | File | Provisioned by | What it shows |
| --- | --- | --- | --- |
| Teranode Service Overview | `deploy/docker/base/grafana_dashboards/teranode/teranode-overview-dashboard.json` | Operator Docker stacks (`deploy/docker/mainnet`, `deploy/docker/testnet`, `deploy/docker/monitoring`) | Top-level health across all services: throughput, latencies, error rates |
| Aerospike Batch Index Bottleneck Diagnostic | `deploy/docker/base/grafana_dashboards/teranode/teranode-batch-index-dashboard.json` | Operator Docker stacks | Aerospike batch index buffer pressure, a common scaling bottleneck |
| Aerospike Namespace | `deploy/docker/base/grafana_dashboards/aerospike/aerospike-namespace.json` | Operator Docker stacks | Namespace-level memory, disk, and object counts |
| Aerospike Latency | `deploy/docker/base/grafana_dashboards/aerospike/aerospike-latency.json` | Operator Docker stacks | Read/write/batch latency buckets for the UTXO store |
| BlockAssembler State Monitoring | `compose/grafana/dashboards/blockassembly-state.json` | Nothing on the operator paths — **import by hand** (only the developer multi-node stacks `compose/docker-compose-ss.yml` and `test/docker-compose-host.yml` mount it) | Block assembler state timeline (`running`, `reorging`, `movingUp`, ...), state durations, and transition rates — see [dashboard notes](https://github.com/bsv-blockchain/teranode/blob/main/compose/grafana/dashboards/README.md) |

The Grafana service in the Docker stacks mounts a single dashboards directory,
`deploy/docker/base/grafana_dashboards`, so the four dashboards in that directory
— and only those — are provisioned automatically. On the quickstart, Grafana and
Prometheus come up with the `monitoring` compose profile, which is part of the
default `COMPOSE_PROFILES` (`legacy,p2p,monitoring`). Grafana is then reachable
at `http://localhost:3005` and Prometheus at `http://localhost:9090` (both
loopback-only by default). See [Installing with
Docker](docker/minersHowToInstallation.md) and its [Troubleshooting
guide](docker/minersHowToTroubleshooting.md) if Grafana shows no data.

The BlockAssembler State Monitoring dashboard is **not** provisioned on either
operator path. To use it, import
`compose/grafana/dashboards/blockassembly-state.json` manually (Grafana →
Dashboards → Import → Upload JSON file), or drop the file into
`deploy/docker/base/grafana_dashboards/teranode/` before starting the stack so
the existing provisioning picks it up.

On Kubernetes, Prometheus and Grafana are not deployed by the Teranode
operator itself — point your cluster's existing Prometheus at the Teranode
services' metrics endpoints and import all of the dashboards above manually
(Grafana → Dashboards → Import).

## Metrics: Healthy vs. Degraded

The full metric catalogue is in the [Prometheus Metrics
Reference](../../references/prometheusMetrics.md). The signals below are the
ones worth watching first — most are visible directly on the Service Overview
dashboard.

### FSM and sync state

`teranode_blockchain_fsm_current_state` is a **numeric** gauge, not a string — it
carries the enum ordinal of the blockchain FSM state:

| Value | State |
| --- | --- |
| 0 | `IDLE` |
| 1 | `RUNNING` |
| 2 | `CATCHINGBLOCKS` |

A healthy node sits on `1` (`RUNNING`) once initial sync completes. Stuck on `2`
(`CATCHINGBLOCKS`) or oscillating indicates a sync problem; see [Syncing the
Node](docker/minersHowToSyncTheNode.md). The gauge is written on FSM transitions
only, so the series is absent until the node makes its first transition — use
`absent(...)` if you want to alert on that case as well.

Also watch:

- `teranode_blockvalidation_catchup_active` and
  `teranode_blockvalidation_processing_blocks_stuck` — non-zero for extended
  periods means the node has fallen behind or a block is wedged.

### Block assembly

`teranode_blockassembly_current_state` is a **numeric** gauge too:

| Value | State |
| --- | --- |
| 0 | `starting` |
| 1 | `running` |
| 2 | `resetting` |
| 4 | `blockchainSubscription` |
| 5 | `reorging` |
| 6 | `movingUp` |
| 7 | `reconciling` |

Value `3` is unused — that state was removed. A healthy node spends almost all of
its time on `1` (`running`).

`teranode_blockassembly_state_duration_seconds` (histogram) and
`teranode_blockassembly_state_transitions_total` (counter) carry the state in a
`state` / `from` / `to` label. The label values are the lower-camelCase names in
the table above — `running`, `movingUp`, `blockchainSubscription`, and so on —
**not** the capitalised names the dashboard panels display, so a selector such as
`{state="Running"}` matches nothing. Long dwell time in `reorging` or `movingUp`
is worth investigating.

Also watch:

- `teranode_blockassembly_best_block_height` vs. your own chain tip — a gap
  that doesn't close means block assembly is falling behind validation.

### Validation and propagation errors

- `teranode_validator_invalid_transactions` and
  `teranode_propagation_invalid_transactions` — a sustained non-zero rate
  (rather than occasional spikes from normal network traffic) suggests policy
  misconfiguration or an upstream data problem.
- `teranode_aerospike_utxo_errors`, `teranode_aerospike_txmeta_errors`, and
  the SQL-store equivalents (`teranode_sql_utxo_errors`) — any sustained rate
  here points at store-level trouble (disk, memory pressure, connectivity),
  not the chain itself.

### Fork and reorg activity

- `teranode_blockvalidation_fork_count` and
  `teranode_blockvalidation_fork_orphaned_total` — occasional forks are
  normal; a rapidly growing fork count or frequent orphaning suggests network
  or peering issues.

### Cache and store pressure

- `teranode_tx_meta_cache_hits` vs. `teranode_tx_meta_cache_misses` — a
  degrading hit ratio under steady load usually precedes increased UTXO store
  latency.
- `teranode_aerospike_utxo_create_batch` / `_spend_batch` (histograms) —
  rising batch durations are an early indicator of Aerospike contention,
  before it shows up as user-visible slowdown.

## Starter Alerting

Teranode does not ship Prometheus alerting rules — alert thresholds depend on
your hardware, network, and risk tolerance, so this is left to the operator.

Both state metrics are numeric gauges, so state alerts must be written as
numeric comparisons; a string comparison such as
`teranode_blockchain_fsm_current_state != "RUNNING"` is not valid PromQL and
will be rejected. As a starting point, consider:

```yaml
groups:
  - name: teranode-starter
    rules:
      # Blockchain FSM not RUNNING (1) for 5 minutes.
      - alert: TeranodeFSMNotRunning
        expr: teranode_blockchain_fsm_current_state != 1
        for: 5m
        annotations:
          summary: "Blockchain FSM is not RUNNING"

      # Block assembly stuck outside running (1) for 5 minutes. Tune the window
      # to the block time you expect on your network.
      - alert: TeranodeBlockAssemblyNotRunning
        expr: teranode_blockassembly_current_state != 1
        for: 5m
        annotations:
          summary: "Block assembly is not in the running state"

      # A block is wedged in validation.
      - alert: TeranodeBlockProcessingStuck
        expr: teranode_blockvalidation_processing_blocks_stuck > 0
        for: 2m
        annotations:
          summary: "Block validation reports stuck blocks"

      # Sustained store or validation errors.
      - alert: TeranodeErrorRate
        expr: |
          rate(teranode_validator_invalid_transactions[5m]) > 0
          or rate(teranode_aerospike_utxo_errors[5m]) > 0
          or rate(teranode_sql_utxo_errors[5m]) > 0
        for: 10m
        annotations:
          summary: "Sustained validation or UTXO store errors"

      # Reorgs happening more than once every 10 seconds.
      - alert: TeranodeFrequentReorgs
        expr: sum(rate(teranode_blockassembly_state_transitions_total{to="reorging"}[5m])) > 0.1
        for: 5m
        annotations:
          summary: "Frequent block assembly reorgs"
```

Note the `to="reorging"` label value: the `state`, `from`, and `to` labels carry
the lower-camelCase state names listed above, so capitalised selectors return an
empty series and the alert silently never fires.

The [BlockAssembler State Monitoring dashboard
notes](https://github.com/bsv-blockchain/teranode/blob/main/compose/grafana/dashboards/README.md#alerting-rules)
carry further examples in the same style.

## Related Documentation

- [Prometheus Metrics Reference](../../references/prometheusMetrics.md)
- [Installing with Docker](docker/minersHowToInstallation.md)
- [Installing with Kubernetes](kubernetes/minersHowToInstallation.md)
- [Troubleshooting (Docker)](docker/minersHowToTroubleshooting.md)
- [Troubleshooting (Kubernetes)](kubernetes/minersHowToTroubleshooting.md)
