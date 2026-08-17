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

**Example Prometheus alerts:**

`teranode_blockassembly_current_state` is a numeric gauge, so compare it against
the state number (`running` is `1`) — a string comparison such as
`!= "running"` is not valid PromQL.

```yaml
# Alert if stuck in any non-running state for >30s
- alert: BlockAssemblerStuckInState
  expr: |
    teranode_blockassembly_current_state != 1
    and
    (time() - timestamp(teranode_blockassembly_current_state) > 30)
  for: 1m
  annotations:
    summary: "BlockAssembler stuck in non-running state"

# Alert if movingUp P95 > 1s
- alert: SlowTipAdvance
  expr: |
    histogram_quantile(0.95,
      sum(rate(teranode_blockassembly_state_duration_seconds_bucket{state="movingUp"}[5m])) by (le)
    ) > 1
  for: 5m
  annotations:
    summary: "Block assembly tip advance is slow (P95 > 1s)"

# Alert if reorg frequency is high
- alert: FrequentReorgs
  expr: |
    sum(rate(teranode_blockassembly_state_transitions_total{to="reorging"}[5m])) > 0.1
  for: 5m
  annotations:
    summary: "Reorgs happening more than once per 10 seconds"
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
