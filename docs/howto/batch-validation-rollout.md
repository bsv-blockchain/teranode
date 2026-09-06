# Batch Validation Rollout & Revert Runbook

## What

`validator_useBatchValidation` (default `false`) routes
`ProcessTransactionBatch` through the new six-phase native path that
issues one Aerospike `BatchOperate` per phase per batch (Phases A–F:
GetParents → CPU → Spend → Create → BA-submit/SetLocked → TxMeta Kafka).

See spec: `docs/superpowers/specs/2026-05-14-end-to-end-batch-validation-design.md`
(local-only; gitignored).

## Rollout sequence

1. Build is on `main`, flag default `false`. No behavior change.
2. **Synthetic bench (Phase 0 in spec).** Run the local benchmark and
   record baseline numbers:

   ```bash
   go test -bench=BenchmarkValidateBatch_FallbackVsNative -benchmem -run=^$ ./services/validator -v
   ```

3. **Scale-1 single pod.** Set `validator_useBatchValidation=true` in
   the env for ONE propagation pod; restart pod; observe for 2 hours.
4. **Scale-1 cluster.** Flip the flag cluster-wide on scale-1; observe
   for 24 hours.
5. **Scale-2.** Repeat steps 3–4.
6. **Cleanup (after 2 weeks).** Per spec follow-up F3, delete unused
   go-batcher constructors in `stores/utxo/aerospike`.

## Revert criteria

Flip flag back to `false` if any of the following hold for 15 minutes
during steps 3–5:

- Per-tx p99 latency > 2× the pre-rollout baseline
- Per-tx error rate > baseline + 0.5 %
- BA rejection rate > baseline + 0.5 %
- Aerospike error rate > baseline + 0.1 %

## How to revert

1. Set `validator_useBatchValidation=false` in the affected pods' env.
2. Restart pods. `validator.ValidateBatch` immediately falls back to
   the per-tx fan-out path; `ProcessTransactionBatch` reverts to its
   legacy errgroup loop via Kafka.
3. File an issue with goroutine profile and Aerospike client metrics
   captured just before the revert.

## Phase failure attribution

`ValidationResult.Phase` tags which phase of the six-phase pipeline
failed a per-tx outcome. Use these tags when investigating per-tx
errors in the native path:

| Phase                | Cause when set on Err                              |
|----------------------|----------------------------------------------------|
| `PhaseGetParents`    | Parent tx not in Aerospike (intra-batch or genuine)|
| `PhaseCPU`           | Format or script validation rejected the tx        |
| `PhaseSpend`         | Aerospike Spend Lua refused (likely double-spend)  |
| `PhaseCreate`        | CREATE_ONLY collision, or tx needs external storage|
| `PhaseBlockAssembly` | BA rejected the tx; tx left locked for reconciler  |
| `PhaseSetLocked`     | Aerospike unlock write failed AFTER BA accepted    |

`PhaseSetLocked` errors are rare and worrying — tx is unaccounted for
between Aerospike and BA. Surface to the team immediately.

## Metrics to watch (per pod)

- `validator_batch_validate_total` — count of ValidateBatch calls
- `validator_batch_validate_phase_duration_seconds{phase=...}` — histogram per phase
- `validator_batch_validate_per_tx_errors_total{phase=...}` — counter per phase
- Existing per-tx latency / error gauges (unchanged)
- Aerospike client `rw_in_progress` and BatchOperate latency histograms

(Several of the new metrics are not yet wired; this section is the
desired observability surface. Wiring them up is captured as a
follow-up.)

## Single-tx coalescer (v2 on same branch)

When `validator_useBatchValidation=true` AND **no Kafka producer is
configured**, propagation routes single-tx callers (gRPC
`ProcessTransaction`, HTTP `/tx`) through an in-process TxCoalescer
that gathers concurrent submissions and dispatches them to
`validator.ValidateBatch` in batches.

When Kafka IS configured, single-tx callers continue to publish to
Kafka unchanged. The coalescer is bypassed.

### Settings

| Setting | Default | Effect |
|---|---|---|
| `validator_batchMaxSize` | 1024 | Max items in a coalescer batch before flush. |
| `validator_batchMaxWait` | 5ms | Max time the coalescer holds a partial batch before flushing. |
| `validator_batchMaxConcurrent` | 64 | Max concurrent in-flight ValidateBatch flush goroutines. 0 = unbounded. |

### Latency floor

The coalescer adds up to `validator_batchMaxWait` of additional
latency under low load (single-tx submissions wait for a partial
batch to time out). Tune down if low-latency single-tx is more
important than throughput in your deployment.

### When this matters

- Deployments NOT using Kafka (test, dev, edge clusters) — coalescer
  exercises the multi-tx validator op.
- Production deployments using Kafka — no change; the coalescer is
  inactive. Validator-side Kafka consumer batching (follow-up F1) is
  the lever for those.

## Related follow-ups

- F1 (spec section 7): switch the validator's Kafka consumer to consume
  in batches and call `ValidateBatch`. Biggest remaining throughput
  lever after this work lands.
- F2: intra-batch dependency layering if F1's data shows the v1
  "fail and let propagation retry" strategy hurts throughput.
- F3: delete unused go-batcher constructors in `stores/utxo/aerospike`
  once stable for 2 weeks.
- F4: revisit `aerospike_batchPolicy` defaults — likely tighter
  per-phase deadline.
