# PostgreSQL tuning for the high-throughput UTXO store

**Updated model (2026-06-09, measured):** with sane WAL/checkpoint GUCs the
sustained workload is **PostgreSQL server-CPU-bound, not disk-bound** — under
full load postgres consumes ~12 of 16 cores (client ~2, 0% idle), and
stretching `checkpoint_timeout` 90s→30min cut full-page-image WAL ~20× with
**zero** TPS change. The binding constraint on the *balanced* (create == reclaim,
table bounded) rate is the **per-row reclaim pipeline's CPU**: DAH sweep
enumerate + stamp UPDATE + doomed scan + cascade DELETE measured at ~25–30% of
all server CPU (`pg_stat_statements`). The earlier "shared NVMe write budget"
theory below this section is retained for history but was disproven on this
host once `max_wal_size` (found at 1GB, with `min_wal_size`=2GB!) was restored
to the playbook value.

## Measured results (PostgreSQL 18.2, M3 Max 16-core 64 GB macOS, NVMe, 10K workers)

| scenario | result |
|---|---|
| **No-prune hot-path ceiling (current code)** | **median ~112K TPS** (range 105–143K), shared_buffers=8GB |
| **Concurrent create+prune, table gated ≤1.5M rows** | **~75K honest plateau; best stable runs 88.4K median CV 4.2%** |
| Prune reclaim under load | ~86K/s when cache-resident; degrades to ~23–30K/s once the table outgrows the hot set (metastable — hence the table-size gate in the harness) |
| Unit tests | full `stores/utxo/postgres` suite passes |

Progression of the sustained-with-prune median during the 2026-06-09 session:
63.1K (GUC fix baseline) → 70.6K (BRIN/HOT indexes) → 75.7K (harness miner fix)
→ 88.4K best / ~75K honest (two-step bounded sweep + interleaved prune +
SetMinedMulti RETURNING + table gate).

> **Route to ≥100K balanced on this host:** the gap between the 112K no-prune
> ceiling and the ~75K balanced rate *is* the per-row reclaim pipeline.
> Generational reclaim (sub-partition txs/spends by height bucket; prune =
> relocate survivors + DETACH+DROP the aged bucket leaves; delete the whole
> stamp/watermark machinery) removes that pipeline. See
> `stores/utxo/throughput_designd_spike_test.go` for the Phase-4A instrument.

### Two TEST-HARNESS bugs that were silently corrupting measurements (fixed)

These made the pruned benchmark mis-measure for many runs; both are in
`stores/utxo/throughput_test.go`'s `cleanDB`:

1. **`dah_watermark` not reset.** `cleanDB` dropped `txs`/`spends` but not
   `dah_watermark`, so each concurrent run inherited the PREVIOUS run's high
   watermark (e.g. 984) — higher than the new run's max height (743). The DAH
   sweep's `sweepDAHUpTo(safeTip)` then hit `toH <= watermark` and returned
   immediately, **so prune silently never engaged** and the table grew unbounded.
   This was the root cause of the "prune is flaky / doesn't keep up" symptom (the
   first run after a fresh DB worked; every run after silently did nothing). NOTE:
   production is unaffected — the watermark persists and advances correctly there;
   only the test (which `DROP`s and recreates) hit it. Fix: `cleanDB` now drops
   `dah_watermark` (recreated seeded to 0).
2. **`txs_raw` not dropped** (during the since-reverted raw_tx split experiment):
   ~43M orphaned rows (~12 GB) accumulated across runs, thrashing cache.

### Two DAH-sweep bugs fixed (correctness + reliability, not just throughput)

Reaching a *reliably bounded* table under concurrent load required fixing two real
defects in the deferred DAH sweep (both affect production pruning at scale, not
just the benchmark):

1. **Watermark truncation (`sweepDAHUpTo`).** The sweep processed `(watermark,
   safeTip]` in ONE `LIMIT`-bounded call, then advanced the watermark to `safeTip`
   unconditionally. When a range held more than `LIMIT` candidates (a startup
   backlog, or any sustained high-throughput window), it stamped `LIMIT` of them
   and the watermark jumped PAST the rest — orphaning them to the slow keyspace
   backstop. Fix: advance the watermark in bounded **height steps**
   (`dahSweepHeightStep`) so no single sweep truncates.

2. **Plan instability (`sweepDAHRange`).** The candidate-enumeration CTE is run via
   pgx prepared statements, so its plan is cached per connection. The planner could
   lock in a bad GENERIC plan against cold/skewed early-run stats and reuse it for
   the connection's life — manifesting as the sweep **non-deterministically stamping
   ~0** under load (same config, opposite outcome run-to-run; the table then grew
   unbounded). Fix: `SET LOCAL plan_cache_mode = force_custom_plan` in the sweep
   transaction so it re-plans each call against current estimates.

With both fixed (plus per-partition parallel sweep + cascade delete, `fillfactor=50`
on `txs` to keep the per-tx UPDATEs HOT, and the GUCs below), concurrent
create+prune is stable at **~65K TPS with the table bounded** (prune keeps pace;
reclaim capacity is 5.38× in isolation).

### ≥100K / 150K-with-prune feasibility (verdict revised 2026-06-09)

SUPERSEDED: the earlier verdict ("~65–70K balanced is the wall; 100K needs a
Linux host or the outputs fold") was measured under GUC drift (`max_wal_size`
silently at 1GB) and with three since-fixed CPU sinks (HOT-blocking partial
btrees, the `enable_nestloop=off` full-partition sweep joins, and a serialised
sweep-then-delete prune cycle). With those fixed the no-prune ceiling on this
same macOS box is **~112K** and the balanced rate ~75K honest / 88K best.

The remaining gap to ≥100K balanced is the per-row reclaim pipeline itself
(~25–30% of server CPU), not storage and not macOS: the route is **generational
DROP-partition reclaim** (height-bucket sub-partitions; prune = survivor
relocation + DETACH+DROP; the DAH stamp/watermark/doomed machinery is deleted
outright). Stacked with hot-path byte-diet levers (INT4 narrowing, array
packing) the projected balanced rate is ~110–125K on this host.

Still true: eager inline DAH stamping remains rejected — it violates the
"inline ops only tag heights" invariant (`TestSetMinedTagsHeightAndDoesNotStampInline`)
and re-adds a row-locking UPDATE to the spend hot path.

Reproduce:

```bash
# No-prune sustained (12 min):
THROUGHPUT_WORKERS=10000 THROUGHPUT_TIER2=0 THROUGHPUT_REPS=180 THROUGHPUT_VERBOSE=1 \
  go test ./stores/utxo/ -run TestThroughput_QueueStoreStable -v -timeout 25m
# Concurrent create+prune, table bounded (use -count=1 to bypass the go test cache):
THROUGHPUT_WORKERS=10000 THROUGHPUT_TIER2=0 THROUGHPUT_REPS=16 THROUGHPUT_VERBOSE=1 \
  go test ./stores/utxo/ -run TestThroughput_QueueStorePruned -count=1 -v -timeout 20m
# Prune reclaim capacity (isolated):
PRUNE_DRAIN_TXS=1000000 PRUNE_DRAIN_WORKERS=200 \
  go test ./stores/utxo/ -run TestThroughput_PruneDrainCapacity -count=1 -v -timeout 12m
```

## Required server GUCs (ALTER SYSTEM + restart/reload)

```sql
-- Cache: hold the index working set. Size to the WORKLOAD, not just host RAM:
--   * No-prune (table grows unbounded): large — 24GB on a 64GB host.
--   * Pruned steady-state (table bounded to ~active set, ~1-2GB): SMALL is better.
--     8GB is ample, and the smaller shared-memory segment avoids the macOS SIGBUS
--     ceiling under a heavy concurrent client (see note). 6GB used for the
--     aggressive concurrent create+prune dev runs.
ALTER SYSTEM SET shared_buffers = '8GB';           -- restart (6-8GB pruned; 24GB no-prune)
ALTER SYSTEM SET effective_cache_size = '24GB';

-- Long checkpoint cycles: every checkpoint re-arms a full-page image for every
-- hot page on next touch, and the random-hash access pattern touches EVERY index
-- leaf each cycle — 90s cycles produced ~20x the FPI WAL of 30min cycles
-- (measured; TPS-neutral on NVMe but the WAL volume matters on lesser disks and
-- for replication). checkpoint_completion_target=0.9 keeps the flush smooth.
-- WARNING: min_wal_size must stay BELOW max_wal_size — this instance was found
-- running max=1GB/min=2GB, which forced a checkpoint every ~10s under load.
ALTER SYSTEM SET max_wal_size = '16GB';
ALTER SYSTEM SET min_wal_size = '2GB';
ALTER SYSTEM SET checkpoint_timeout = '30min';
ALTER SYSTEM SET checkpoint_completion_target = 0.9;
ALTER SYSTEM SET bgwriter_lru_maxpages = 1000;
ALTER SYSTEM SET bgwriter_delay = '10ms';
ALTER SYSTEM SET bgwriter_lru_multiplier = 10.0;
ALTER SYSTEM SET backend_flush_after = '256kB';
ALTER SYSTEM SET wal_compression = 'lz4';

-- Group commit: amortise fsync across the concurrent committers (durability kept).
ALTER SYSTEM SET commit_delay = 500;
ALTER SYSTEM SET commit_siblings = 5;

-- Autovacuum keeps up with the per-tx dead-tuple churn (SetLocked + SetMinedMulti
-- both UPDATE txs; see schema.go leaves at cost_limit=8000). Keep
-- autovacuum_max_workers × maintenance_work_mem WELL under free RAM: 8×1GB=8GB of
-- vacuum memory on top of shared_buffers was a SIGBUS trigger under concurrent
-- prune load. 6 workers × 256MB = 1.5GB is the safe envelope on this box.
ALTER SYSTEM SET autovacuum_max_workers = 6;       -- restart
ALTER SYSTEM SET autovacuum_naptime = '5s';
ALTER SYSTEM SET maintenance_work_mem = '256MB';   -- workers × this must fit RAM

-- Planner: NVMe random reads are cheap.
ALTER SYSTEM SET random_page_cost = 1.1;
ALTER SYSTEM SET effective_io_concurrency = 200;
```

`synchronous_commit` stays **on** (enforced per-connection in `store.go`) — never
disable it; it is a durability/consensus requirement.

## SIGBUS / memory ceiling (important on memory-constrained or shared hosts)

On hosts where PostgreSQL shares RAM with a heavy client (e.g. a dev box running
the benchmark, or a co-located service), an over-large `shared_buffers` under
memory pressure can make a backend fail to fault a shared-memory page →
`SIGBUS` → crash-recovery (no data loss with `synchronous_commit=on`, but a
throughput cliff). Observed at `shared_buffers=44GB` on a 64 GB host while the
benchmark client also consumed many GB.

Guidance: keep `shared_buffers` + peak client/other RSS + autovacuum
(`autovacuum_max_workers × maintenance_work_mem`) comfortably under physical RAM.
`24GB` was the sweet spot on a 64 GB host for the no-prune sustained run; a
dedicated DB host can go higher. Pruned/steady-state workloads keep the table
bounded and need far less cache.

## In-repo changes that pair with this config

- **`schema.go`**: `numPartitions = 8` (spreads autovacuum/index work); per-leaf
  autovacuum tuning set on the leaf partitions (parent-level storage params are a
  no-op for autovacuum scheduling) — `txs` leaves vacuum aggressively
  (`scale_factor=0.01`) to bound the `SetLocked` dead-tuple churn.
- **`pruner_provider.go`**: set-based cascade delete (`DELETE … WHERE col = ANY($batch)`)
  across parallel workers over disjoint hash slices — ~8× the per-hash baseline,
  reclaiming >200K tx/s.
- **`dah_sweep.go`**: DAH sweep + backstop use pre-aggregated `JOIN`s instead of
  per-candidate correlated subqueries.

## Execution model (v14): no clocks on healthy work

The DAH sweep must handle ANY number of records per band. Its work is bounded by
unit size — `band_heights` heights per band, `max_windows_per_call` bands per
CALL — and never by wall-clock. There is deliberately no CALL timeout, no
statement_timeout, and no lock_timeout on this path. History showed why: every
wall-clock bound eventually crossed a legitimate work size and turned into an
infinite retry treadmill (cancel mid-band → rollback → retry the same band),
which is strictly worse than slow progress — three production incidents,
including a 36-hour silent stall that filled a 400GB disk and crashed postgres.

Safety comes from structure instead:

- **Interruption-proof progress:** each band commits fold + stamp + watermark
  advance atomically inside the procedure. Crash, cancel, deploy, or connection
  loss costs at most the in-flight band.
- **No deadlocks between DAH writers:** the sweep fold and the reconciler both
  take `pg_try_advisory_xact_lock(20240684 + partition)`, so they are mutually
  exclusive per partition. The reconciler skips (without advancing its rotation
  cursor) when the sweep holds the partition.
- **Clock immunity:** the maintenance pool pins `statement_timeout=0`,
  `lock_timeout=0`, `idle_in_transaction_session_timeout=0` at connect time, so
  a server/role-level ops default can never kill a healthy band mid-fold.
- **Errors back off, never spin:** any CALL or probe error sleeps
  `PostgresDAHSweepIdleIntervalMillis` before retrying and logs at Warnf with
  the SQLSTATE. A postgres outage produces a handful of quiet retries per
  minute, not a log flood.

### The stagnation alarm (the only "timeout" left — it kills nothing)

`dah_stagnation.go` runs a 60s ticker, independent of every CALL, on the MAIN
pool (so a saturated maintenance pool cannot blind it). One rule, no exceptions:
**watermark frozen + backlog > 0** escalates on wall-clock time since that
partition last advanced — Warnf at `PostgresDAHSweepStallAlertSeconds/2`,
Errorf every tick past the threshold plus the
`teranode_postgres_utxo_dah_sweep_stalled{partition}` gauge. It is deliberately blind to WHY
progress stopped, so every cause lands in the same loud place: wedged backend,
`dah_sweep_control.enabled=false` left off after maintenance, broken tip
source, orphaned advisory lock after a kill -9, plan regression, IO stall.

**Runbook when it fires:**

1. Is a CALL in flight and doing work? `SELECT pid, state, wait_event_type,
   wait_event, now()-query_start FROM pg_stat_activity WHERE query LIKE 'CALL
   dah_sweep_batch%';` — `IO/DataFileRead` = healthy giant band, leave it alone.
2. Waiting on a lock? `SELECT pg_blocking_pids(<pid>);` then look the blocker
   up in pg_stat_activity. Decide manually — nothing auto-cancels by design.
3. No CALL at all? Check `SELECT enabled FROM dah_sweep_control;` (a forgotten
   kill switch) and that the node's block height source is advancing.
4. CALLs erroring? The `[dahCursor]` Warnf lines carry the SQLSTATE.

### Tuning for chain density

`band_heights` (control table, live — the proc re-reads it every CALL) is the
work-quantum knob: it bounds how much one band folds and therefore how much a
single interruption can cost. 5000 is fine for sparse eras; the 2018-19
stress-test region needed 500 (~215K spends/partition/band). If bands start
taking hours, shrink it — never add a timeout.

Considered and deferred (2026-07-06): a pg_stat_activity wait-event sampler
that classifies WHY a stall happened ("wedged on lock held by pid X" vs
"healthy IO grind") in the alert itself. Deferred because every real incident
so far was caused by the timeouts, not by lock wedges; revisit if a genuine
wedge ever pages (runbook step 2 covers it manually meanwhile).
