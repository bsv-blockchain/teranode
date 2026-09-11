# Recover replayed confirmed transactions

`teranode-cli recoverreplayedtransactions` audits the persisted Aerospike store
for confirmed transactions recreated after pruning, and missing replay markers
on surviving parents. One job inventories the store, authenticates local chain
evidence, applies proven repairs when requested, and verifies persisted results.
Audit is the default. It never purges all unmined transactions.

`resetblockassembly` rebuilds assembly from persisted state. It can reload these
recreated records; clearing assembly alone does not repair the store.

## Operator preconditions

1. Put the node in **IDLE**. Stop/drain every writer sharing the UTXO, blockchain
   and archive stores, or restart services into an inactive configuration.
   Include propagation/validator, block/subtree validation, assembly and retries,
   block persister, pruner/blob deletion, legacy ingestion, other replicas and
   DAH/background cleanup. Disable automatic restart/resume during recovery.
   Restarting services alone does not prove that writes have stopped.
2. Keep the existing blockchain metadata service reachable in IDLE, plus SQL,
   Aerospike and retained archives. Prevent external chain/FSM changes. The job
   uses existing blockchain read RPCs and a direct Aerospike client; it does not
   start normal UTXO cleaners or construct a blockchain store that runs migrations.
3. Preserve backups and incident evidence. Use a private directory on durable
   storage, owned by the operator, mode `0700`. Allow enough disk for the complete
   store census, targeted chain evidence and mutation before-images. Files are
   private `0600` files. Keep them out of automatic cleanup paths.
4. Run only one recovery job against the store, including across hosts. The work
   directory lock prevents concurrent use of that directory; it is not a
   distributed store lease.

The CLI reads persisted `fsm_state` through the existing blockchain API and
accepts only `IDLE`. Missing state or a failed read stops the job. It checks state
and the pinned tip throughout scans, before mutations and during verification;
a watchdog also cancels work if these checks fail. There is no force override
and no automatic transition to IDLE or RUNNING.

**These checks detect changes; they are not an atomic write lock.** Operators
must isolate writers. IDLE alone does not stop all already-running background
workers: for example, the pruner's startup gate and catchup pause do not form a
maintenance barrier. `--maintenance` acknowledges the isolation procedure.

## Invocation

Use the node's normal settings context, Aerospike namespace/set, output batch
size, external transaction store and subtree archive configuration.

```bash
# Read-only audit; no UTXO writes.
teranode-cli recoverreplayedtransactions \
  --work-dir /var/lib/teranode/recovery-audit

# Fresh apply: inventory, evidence, repair and store verification in one job.
teranode-cli recoverreplayedtransactions \
  --work-dir /var/lib/teranode/recovery-apply --apply --maintenance

# Explicitly resume that interrupted apply, while writers remain stopped.
teranode-cli recoverreplayedtransactions \
  --work-dir /var/lib/teranode/recovery-apply --apply --maintenance --resume
```

These are alternatives, not three mandatory stages. A fresh apply includes its
own complete audit. A new run requires an empty work directory; an audit's
artifacts cannot be silently repurposed as an apply. Resume requires the same
store identity, configuration, compatible artifact version and pinned tip.

The default timeout is 30 minutes; use `--timeout` for a longer maintenance
window. `--concurrency` limits Aerospike scan node fan-out (default 1, range 1–64).
Archive parsing remains bounded and sequential. Full history scans can take much
longer than the default timeout; plan the window before starting.

## Evidence and repair scope

The census covers masters and pagination records, including unmined records,
missing/inconsistent mined metadata, locked/conflicting/creating states,
parent spend references, absent children lacking markers and affected
dependencies. It does not depend on current assembly membership. Unsupported
schemas or unknown page owners remain findings; they are never disposable data.

The evidence source is the node's validated local canonical chain at the pinned
tip. No external SV Node RPC is required. The job authenticates retained raw
transactions against subtree and block Merkle commitments and verifies header
ancestry. A fully-spent classification requires positive canonical inclusion
and a correctly ordered canonical spending transaction for every spendable
output, using the existing chain-era burned-output policy. Empty mined metadata,
a missing UTXO record or local spent flags alone never authorize deletion.

The history scan starts at genesis to make absence claims conservative. Missing,
malformed or oversized archives create coverage gaps. Positive inclusion/spend
proofs may still authorize repairs despite unrelated gaps; missing coverage
cannot prove that a transaction is unconfirmed or an output is live. The index
retains targeted evidence and its dependencies, not all historical raw bytes.
Subtree transaction data is streamed, with bounded individual transaction and
block parsing; blocks exceeding limits remain unresolved.

Repairs are limited to positively proven recreated fully-spent records and
missing markers for absent fully-spent children. Normally mined dependent
records and legitimate unconfirmed transactions are preserved. Confirmed
records with live outputs, unexplained flags, unsafe parent ownership or expiry,
and incomplete evidence remain untouched. This recovers this incident's
persisted state; it is not general reconstruction of arbitrary UTXO corruption.

For each repair the job preserves original input spend owners, writes and reads
back required parent/page replay markers, and conditionally deletes only sealed
records using their generations. It preserves native bin types and expiration,
refuses finite-expiry replay guards, does not unspend inputs and does not delete
retained archive blobs. The durable journal records intent before remote writes
and verified outcomes afterward. Shared parents and dependency order are tracked.

## Interrupted work and results

Keep writers stopped on failure and retain the entire work directory. Explicit
resume reconciles only this job's exact expected before/after states. Interrupted
read-only phases can be rebuilt; a partial scan is never treated as complete.
Foreign changes, a different tip or unexplained disappearance stop recovery.
Do not edit/delete a journal to bypass a failure or restore before-images over
a chain that has advanced. There is no automatic rollback.

Progress goes to stderr. The final JSON summary goes to stdout and `report.json`;
`manifest.sqlite`, `history.sqlite`, `journal.sqlite` and `job.json` retain the
private audit, evidence, mutation journal and phase checkpoint.

| Exit | Meaning |
| --- | --- |
| 0 | Complete audit or complete store repair; JSON distinguishes `applied`. |
| 1 | Unsafe precondition, interruption, operational failure or failed verification. |
| 2 | Unresolved findings remain, even if independent components were repaired. |

A complete graph allows independent proven repairs while unknown components
remain untouched. Unresolvable ownership prevents safe apply. Read the counts,
classifications and reasons; an empty assembly is not proof of recovery.

After successful store repair, **restart assembly and the other writers through
the normal operational procedure** so stale in-memory state cannot survive.
Then resume the FSM and verify a fresh valid mining candidate and the next tip
transition. `restart_required` is explicit after mutations, including interrupted
attempts. The CLI finishes store verification while IDLE; it cannot prove those
later runtime observations and never resumes the node itself.
