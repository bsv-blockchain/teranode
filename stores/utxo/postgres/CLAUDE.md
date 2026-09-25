# PostgreSQL UTXO store — working notes

This is the high-throughput PostgreSQL UTXO store (the branch that became PR #684).
It is an alternative backend to the Aerospike store and the older `stores/utxo/sql`
store, built for one thing: sustained UTXO throughput on a single Postgres instance.
If you are editing this package, read this first — it records the design decisions
that look wrong until you know why, and the experiments already tried and rejected so
you don't spend a week re-discovering them.

> **Naming legacy:** the throughput benchmarks are called `TestThroughput_QueueStore*`
> and older notes call this "the queue store". That name is historical — an early
> pg_notify / single-materializer design was abandoned. **The store today is a
> direct-write store.** There is no pg_notify, no materializer. Ignore any reference
> to those; they describe dead iterations.

## What it is (current shape — `schema.go` is the authority)

Two LOGGED, hash-partitioned tables, **8 partitions each** (`numPartitions = 8`):

- **`txs`** — one row per transaction, keyed by the 32-byte hash (no surrogate id, no
  foreign keys). It is deliberately *wide*: `raw_tx` (LZ4 blob), the per-output UTXO
  data **packed** into the row (a flat `utxo_hashes` bytea, `out_spendables`/`out_frozens`
  bitmaps, scalar counts) rather than a separate `outputs` table, plus arrays for
  `block_ids` / `block_heights` / `subtree_idxs` / `conflicting_children`, and the
  `delete_at_height` (DAH) used for pruning. A single row lookup returns everything a
  spend needs — **zero JOINs on the hot path.**
- **`spends`** — append-only. A spend is the *presence of a row* (`UNIQUE(prev_tx_hash,
  prev_output_idx)`), not a nullable column on `txs`. A double-spend is a unique-constraint
  violation. This is the single most important design choice — see the invariants.

Writes go through coalescing batchers + `pgx` pipelining + bulk `UNNEST` CTEs. Pruning
is a server-side DAH sweep (stamps `delete_at_height`) feeding a `pending_deletes`
side-table that the pruner drains. Tuning, GUCs and the sweep execution model live in
**`TUNING.md`** — this file does not repeat them.

## Hard invariants — do not violate these

These are load-bearing. Several are locked by tests; all were paid for with real incidents
or measured regressions.

1. **Never `UNLOGGED`. `synchronous_commit` stays `on`** (enforced per-connection in
   `store.go`). This holds financial/consensus data — durability is non-negotiable.
   Because durability is fixed, **WAL bytes/sec is the cost to minimise structurally**
   (append-only INSERT ≪ update-in-place WAL). fsync *count* is a non-issue — batchers
   group-commit thousands of txs per commit.

2. **Spends stay append-only. Never convert a spend into a row-locking `UPDATE` on `txs`.**
   The "UPDATE utxos SET spending_data=… WHERE spending_data IS NULL" pattern saturates
   3–4× lower at 10K concurrent workers — page-latch contention serialises concurrent
   UPDATEs on the same hot pages, and `fillfactor` + HOT does **not** fix it. Spent-state
   as row-presence is what lets this store scale.

3. **The DAH sweep is spends-driven. Never add a btree on `txs.mined_at_height`.**
   `mined_at_height` is set by a *later* UPDATE (txs are inserted unmined), so it is
   physically uncorrelated → any BRIN on it degenerates to a full-partition scan, and a
   btree "fixes" the read only by adding write-amplification that **breaks HOT on the mine
   path** (the 83%→34% regression the schema exists to avoid). The sweep enumerates
   candidates from `spends` by `spent_at_height` (append-ordered → correlated → cheap) via
   a composite btree on `spends`. "Spends enumerate, txs decides."

4. **Inline ops only *tag* heights — they never *stamp* `delete_at_height`.** Setting the
   DAH is the deferred sweep's job, not the spend/mine hot path (stamping inline re-adds
   the row-locking UPDATE from invariant 2). Locked by
   `TestSetMinedTagsHeightAndDoesNotStampInline` (and the sibling Spend contract). The one
   sanctioned exception is a genuinely zero-spendable tx (all-`OP_RETURN`), stamped inline
   in `SetMinedMulti` because the sweep can never see it — and only because
   `delete_at_height` is BRIN-indexed so that write stays HOT.

5. **A delete must be authorised by ground truth, never by a drift-prone counter.** An
   earlier design maintained an additive "spent progress" counter and stamped the DAH when
   it hit the output count; on reorg re-folds the counter drifted *up*, stamped a
   still-live tx, and the pruner cascade-deleted it → `TX_NOT_FOUND` data-loss wedge. The
   fix is an **idempotent spent-outputs bitmap fold** (set bits, don't add — drift becomes
   impossible). If you touch the fold/stamp logic, preserve idempotency.

6. **`numPartitions = 8` is deliberate — do not drop it to 1.** Burst benchmarks favour a
   single partition (no fan-out), but under sustained churn the per-tx `locked`-flag UPDATE
   makes ~1 dead tuple/tx, and one autovacuum worker on one huge partition cannot keep up
   (dead tuples grow unbounded to 24M+ at 60K TPS). 8 leaves let autovacuum workers vacuum
   in parallel. (Separately: **never RANGE-partition by height** — a spend references its
   parent by txid with no height, so by-hash lookup would have to probe all partitions:
   measured 21× slower. Hash partitioning is correct.)

7. **Keep the tx row single and wide. Don't split it into more tables.** Measured, all
   rejected: splitting `txs` into core/archive/flags regressed (more server-side
   parse/plan/execute per op); a `LEFT JOIN` to a "sparse" flags table costs the *same* per
   row as INNER JOIN (sparseness saves memory, not CPU) — Spend latency went 3.4× worse;
   covering `INCLUDE` columns on the PK break HOT when the column is UPDATEd. Reads happen
   at every spend, writes once per tx lifecycle → flags belong ON `txs`.

8. **Bulk create must chunk by bytes.** `pgx` has a ~1 GiB protocol message cap; fat blocks
   blew it on bulk create. Creates are byte-bounded chunked — keep that bound if you touch
   the create path.

## The DAH sweep / pruner (orientation — details in `TUNING.md`)

Two stages: **(1) set DAH** — a server-side procedure (`dah_sweep_proc.go`, bootstrapped
in `schema.go`; there is no in-process fallback) stamps `delete_at_height` on
fully-spent+mined txs; **(2) delete** — the pruner (`pruner_provider.go`) drains
`pending_deletes` set-based across parallel workers over disjoint hash slices, cascading
`spends`→`txs` in one txn.

Two things worth knowing before you change sweep behaviour:

- **No wall-clock timeouts on the sweep (the "v14 no-clocks" model).** Every timeout we
  ever added eventually crossed a legitimate large band and became an infinite
  cancel→rollback→retry treadmill — three production incidents, one a 36-hour silent stall
  that filled a 400 GB disk. Progress is made interruption-safe by committing per band
  inside the procedure; safety comes from structure (per-partition advisory locks
  `20240684+partition` shared by sweep and reconciler) and from `dah_stagnation.go`, a
  monitor that *alarms but kills nothing*. Don't reintroduce a timeout — shrink
  `band_heights` instead. Full rationale + runbook in `TUNING.md`.
- **Never disable the pruner.** It is load-bearing; if it stops, the disk fills. Make
  pruning cheaper, never turn it off.

## Rejected experiments — don't re-try these

- **Alternative batcher dispatch** — four separate experiments, all bench-rejected. The
  current design (K workers per shard-slot + `pgx.SendBatch` pipelining + *static* per-op
  batch sizes) wins each time:
    - *go-batcher v2 `NewWithPool`* (one accumulator goroutine feeding a callback pool): the
      single accumulator caps throughput once thousands of submitters call `Put` — our K
      parallel accumulations beat it 65–89% at the meaty tiers. (Aerospike adopted it; that's
      correct for Aerospike's different concurrency profile, and doesn't generalise here.)
    - *Dynamic batch sizes* (compute each op's size from `getBatchSize × K_get / K_op`):
      regressed 11–48% at every tier. Get mostly duration-triggers with tiny batches and the
      K workers fire jittered not in lockstep, so the assumed "big synchronised burst" never
      materialises; larger Create/Unlock batches also cost more per WAL commit. Keep static
      per-op sizes; tune each op from measurement, not a model.
    - *`locked` side-table* (move the `locked` flag off `txs` into a sparse table, read via
      `EXISTS`): +144% at the 1K tier (removes Create-vs-SetLocked contention) but −40–44% at
      10K–50K because the correlated `EXISTS` doubles read-side index lookups and defeats
      partition pruning. Net reject. (If ever revisited: a `LEFT JOIN … USING(hash)` form or a
      partial index `WHERE locked=true` might avoid the subquery cost — unbenched.)
    - *Goroutine fan-out entry points* (`Store.CreateBatch`, `Validator.ValidateBatch`, per-tx
      spend grouping; upstream #748/#750/#752, all closed): neutral to 1.3–3.7× *regression*.
      The existing per-call batchers already pipeline N statements into one round-trip via
      `pgx.SendBatch`, which a sequential batch loop can't beat. **Lesson: thousands of
      goroutines parked on a batcher's done-channel is not a perf bug — parked goroutines are
      ~1 KB and cost nothing; measure end-to-end wall time before "optimising" them away.**
- **Generational DROP-PARTITION reclaim / greenfield "epoch-slab" store** — the by-hash
  lookup tax (~2.1× via a directory hop) roughly trades against the reclaim it frees, and it
  only wins if garbage *clusters* by an insert-time key, which is true for tight-chain
  test workloads but **false for mainnet** (spend-time is decorrelated from create-time, so
  prunable txs scatter across all epochs → relocation degrades toward a full-table rewrite).
  Per-row stamp+delete is the right tool for mainnet's scattered garbage. No store-only
  design beat ~82K sustained on the reference box.

## Benchmarking

The arbiter is `./stores/utxo/` — `TestThroughput_QueueStoreStable` (no-prune hot-path
ceiling), `TestThroughput_QueueStorePruned` (concurrent create+prune, table bounded),
`TestThroughput_PruneDrainCapacity` (isolated reclaim). Reproduce commands are in
`TUNING.md`.

**Two caveats that have burned measurements before:**

- The **pruned bench models tight chains** (each worker spends its parent immediately) =
  the *teratestnet* pattern, not mainnet (where most UTXOs stay live and spend-time is
  decorrelated). Its reclaim-bound number is teratestnet-relevant; don't project it onto
  mainnet.
- On a **shared dev box** (Postgres + benchmark client on one machine), an over-large
  `shared_buffers` under memory pressure triggers `SIGBUS` crash-recovery — a throughput
  cliff, not data loss. Keep `shared_buffers` + client RSS + autovacuum memory comfortably
  under physical RAM. Reliable signal lives at the ≥10K (saturation) and ≤500 (cold) tiers;
  the 1K–5K middle is noisy — take a median of 3+ runs.

## Map

- `schema.go` — table DDL, partitioning, fillfactor, autovacuum-per-leaf (**the schema
  authority**; this doc summarises, it does not override).
- `create.go` / `spend.go` / `get.go` / `mined.go` — the hot path.
- `dah_sweep_proc.go` / `dah_sweep.go` — the server-side sweep procedure and driver.
- `dah_reconcile.go` — counter/bitmap healing (shares the sweep's advisory lock).
- `dah_stagnation.go` — the stall monitor (alarms, kills nothing).
- `pruner_provider.go` — the set-based cascade delete.
- `pending_unmined_projector.go` — startup backfill, gated by the clean-shutdown marker.
- `TUNING.md` — GUCs, the measured performance model, the sweep execution model, runbook.
- `OVERVIEW.html` — generated overview.
