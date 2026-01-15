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
  - Blue: GetMiningCandidate (serving miners)
  - Purple: BlockchainSubscription (processing new block)
  - Light Blue: MovingUp (advancing chain tip)
  - Orange: Resetting (full reset)
  - Red: Reorging (handling chain reorg)
  - Yellow: Starting (initialization)

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
   - Gauge: Current state (0-6)

2. `teranode_blockassembly_state_transitions_total{from, to}`
   - Counter: Total transitions between states
   - Incremented on every state change

3. `teranode_blockassembly_state_duration_seconds{state}`
   - Histogram: Time spent in each state
   - Buckets: 1ms, 10ms, 100ms, 500ms, 1s, 2s, 5s, 10s, 30s, 60s

### Common Use Cases

#### Debugging Slow Block Processing
**Question:** Why is block processing slow?

**Dashboard View:**
```
State Timeline: [Running][BlockchainSubscription (5s)][Running]
State Durations: P95 BlockchainSubscription = 5.2s
```

**Conclusion:** Block processing consistently takes ~5s

#### Identifying Mining Candidate Bottlenecks
**Question:** Are miners waiting for candidates?

**Dashboard View:**
```
State Transitions: Running → GetMiningCandidate: 5/sec
State Durations: P95 GetMiningCandidate = 200ms
```

**Conclusion:** Serving 5 candidates/sec, each takes ~200ms

#### Detecting Frequent Reorgs
**Question:** How often do reorgs happen?

**Dashboard View:**
```
State Entries: Reorging = 15 (in last 5m)
Transition Matrix: Running → Reorging: 0.05/sec
```

**Conclusion:** Reorgs happening every 20 seconds (investigate)

### Alerting Rules

**Example Prometheus alerts:**

```yaml
# Alert if stuck in any non-Running state for >30s
- alert: BlockAssemblerStuckInState
  expr: |
    teranode_blockassembly_current_state != 1
    and
    (time() - timestamp(teranode_blockassembly_current_state) > 30)
  for: 1m
  annotations:
    summary: "BlockAssembler stuck in non-Running state"

# Alert if GetMiningCandidate P95 > 1s
- alert: SlowMiningCandidateGeneration
  expr: |
    histogram_quantile(0.95,
      sum(rate(teranode_blockassembly_state_duration_seconds_bucket{state="getMiningCandidate"}[5m])) by (le)
    ) > 1
  for: 5m
  annotations:
    summary: "Mining candidate generation is slow (P95 > 1s)"

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
