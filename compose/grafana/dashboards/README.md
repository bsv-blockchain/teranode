# Grafana Dashboards

## BlockAssembler State Monitoring

**File:** `blockassembly-state.json`

Comprehensive monitoring dashboard for BlockAssembler internal state transitions and durations.

### Panels

#### 1. State Timeline

- **Type:** State Timeline
- **Shows:** Visual timeline of state changes over time
- **Colors:**
    - Green: Running (normal operation)
    - Purple: BlockchainSubscription (processing new block)
    - Light Blue: MovingUp (advancing chain tip)
    - Orange: Resetting (full reset)
    - Red: Reorging (handling chain reorg)
    - Yellow: Starting (initialization)
    - Blue: Reconciling (catching the tip up after startup or a missed notification)

#### 2. State Durations (P50/P95/P99)

- **Type:** Time Series
- **Shows:** Percentile distribution of time spent in each state
- **Use:** Identify performance bottlenecks
- **Example:** If P95 BlockchainSubscription is 5s, 95% of blocks process in under 5s

#### 3. State Transitions (per second)

- **Type:** Time Series
- **Shows:** Rate of state transitions
- **Format:** `from → to`
- **Use:** Understand state change frequency and patterns

#### 4. Current State

- **Type:** Stat (single value)
- **Shows:** Current state with color coding
- **Updates:** Real-time (5s refresh)

#### 5. Time in Current State

- **Type:** Stat with gauge
- **Shows:** How long in current state
- **Thresholds:**
    - Green: < 5s (normal)
    - Yellow: 5-10s (slow)
    - Red: > 10s (investigate)

#### 6. State Distribution (Last 5 min)

- **Type:** Pie Chart
- **Shows:** Percentage of time in each state
- **Use:** Understand where BlockAssembler spends most time

#### 7. State Entries (Last 5 min)

- **Type:** Stat (horizontal)
- **Shows:** How many times each state was entered
- **Use:** Identify high-frequency states

#### 8. State Transition Matrix

- **Type:** Table
- **Shows:** All state transitions with rates
- **Sorted:** By transition frequency
- **Use:** Understand common state flows

### Key Metrics

**Captured even between Prometheus scrapes:**

1. `teranode_blockassembly_current_state`
   - Gauge: Current state as a number — 0 `starting`, 1 `running`, 2 `resetting`,
     4 `blockchainSubscription`, 5 `reorging`, 6 `movingUp`, 7 `reconciling`
     (3 is unused; see `StateStrings` in `services/blockassembly/BlockAssembler.go`)

2. `teranode_blockassembly_state_transitions_total{from, to}`
   - Counter: Total transitions between states
   - Incremented on every state change
   - `from` / `to` label values are the lower-camelCase names above (`running`,
     `movingUp`, ...), not the capitalised labels the panels display

3. `teranode_blockassembly_state_duration_seconds{state}`
   - Histogram: Time spent in each state
   - Buckets: 1ms, 10ms, 100ms, 500ms, 1s, 2s, 5s, 10s, 30s, 60s
   - `state` label values are the same lower-camelCase names

### Common Use Cases

#### Debugging Slow Block Processing

**Question:** Why is block processing slow?

**Dashboard View:**

```text
State Timeline: [Running][BlockchainSubscription (5s)][Running]
State Durations: P95 - blockchainSubscription = 5.2s
```

**Conclusion:** Block processing consistently takes ~5s

#### Identifying Slow Tip Advances

**Question:** Is the assembler slow to advance onto a new tip?

**Dashboard View:**

```text
State Transitions: running → movingUp: 0.02/sec
State Durations: P95 - movingUp = 3.5s
```

**Conclusion:** Each tip advance takes ~3.5s, which eats into the time available
for mining on the new tip

#### Detecting Frequent Reorgs

**Question:** How often do reorgs happen?

**Dashboard View:**

```text
State Entries: reorging = 15 (in last 5m)
Transition Matrix: running → reorging: 0.05/sec
```

**Conclusion:** Reorgs happening every 20 seconds (investigate)

### Alerting Rules

The block-assembler alert rules (stuck-state, slow tip advance, frequent
reorgs, tip lag, repeated processing failures) live in a single canonical file,
[`deploy/docker/base/blockassembly.rules.yml`](../../../deploy/docker/base/blockassembly.rules.yml).
Edit that file to change the rules — this README no longer carries a copy.

Every Prometheus config that a stack in this repo actually mounts references it
via `rule_files` and bind-mounts it at `/etc/prometheus/blockassembly.rules.yml`
(`compose/prometheus/prometheus.yml` and `prometheus-microservices.yml` are not
wired into any compose file and are left alone):

| Stack | Config |
| --- | --- |
| Local / mainnet / testnet docker | [`deploy/docker/base/prometheus.yml`](../../../deploy/docker/base/prometheus.yml) |
| Monitoring-only docker | [`deploy/docker/monitoring/prometheus.yml`](../../../deploy/docker/monitoring/prometheus.yml) |
| 3-node compose (`docker-compose-ss.yml`, `docker-compose-3blasters.yml`) | [`prometheus-1.yml`](../../prometheus/prometheus-1.yml), `-2`, `-3` |
| Host-network tests (`test/docker-compose-host.yml`) | [`prometheus-host-1.yml`](../../prometheus/prometheus-host-1.yml), `-2`, `-3` |

Alert *delivery* is only wired in the `deploy/docker/base` stack, which runs an
Alertmanager (UI on <http://localhost:9094>) configured in
[`deploy/docker/base/alertmanager.yml`](../../../deploy/docker/base/alertmanager.yml).
That Alertmanager has **no notifier attached** — firing alerts are visible in
its UI and in Prometheus (`/alerts`), but nothing is sent anywhere until a
receiver is added. In the other stacks the rules are evaluated and visible in
the Prometheus UI, but there is no Alertmanager at all.

Note: `teranode_blockassembly_current_state` is a numeric gauge, so the
`BlockAssemblerStuckInState` rule compares it against the state number
(`running` is `1`) — a string comparison such as `!= "running"` is not valid
PromQL.

Changes to the rules are covered by unit tests in
[`deploy/docker/base/blockassembly.rules_test.yml`](../../../deploy/docker/base/blockassembly.rules_test.yml),
run in CI by
[`.github/workflows/prometheus_rules.yaml`](../../../.github/workflows/prometheus_rules.yaml)
and locally with:

```bash
promtool check rules deploy/docker/base/blockassembly.rules.yml
cd deploy/docker/base && promtool test rules blockassembly.rules_test.yml
```

### Import Instructions

1. Open Grafana
2. Navigate to Dashboards → Import
3. Upload `blockassembly-state.json`
4. Select Prometheus datasource
5. Click Import

### Requirements

- Prometheus datasource configured
- Teranode metrics endpoint accessible
- Grafana 9.0 or higher (for State Timeline panel)
