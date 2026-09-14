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
5. On release 0.15, stores using `utxostore_utxoBatchSize=1` must have
   `aerospike_enable_spend_filter_expressions=false` on every writer before
   writers resume. The expression spend path does not enforce replay markers;
   recovery does not fix that path. Keep the store's existing output batch size.

The CLI reads persisted `fsm_state` through the existing blockchain API and
accepts only `IDLE`. Missing state or a failed read stops the job. It checks state
and the pinned tip throughout scans, before mutations and during verification;
a watchdog also cancels work if these checks fail. There is no force override
and no automatic transition to IDLE or RUNNING.

**These checks detect changes; they are not an atomic write lock.** Operators
must isolate writers. IDLE alone does not stop all already-running background
workers: for example, the pruner's startup gate and catchup pause do not form a
maintenance barrier. `--maintenance` acknowledges the isolation procedure.

On release 0.15, legacy sync automatically requests RUN when current, including
from IDLE. Stop legacy sync and other automatic transition sources throughout
recovery; avoiding manual FSM transitions is insufficient. The blockchain service
changes its runtime FSM before persisting the new state. A persistence failure
is logged but the transition command can still report success, leaving persisted
`IDLE` behind a different runtime state. Resolve any FSM persistence errors;
the CLI's persisted-state check cannot establish that writers are inactive.

Isolate promptly, before further reorg/conflict processing. Release 0.15
`ProcessConflicting` can clear parent spend references to absent children when
their replay markers are missing, erasing evidence that recovery needs to discover
those children. A complete recovery report cannot certify evidence already
erased before the census. Preserve incident backups for separate investigation
if this processing may already have occurred.

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
store identity, configuration (including the reorg retention policy), compatible
artifact version and pinned tip. Artifacts from older versions without the
retention policy are not accepted by this version; never edit a journal or its
version fields to bypass that refusal.

The default timeout is 30 minutes; use `--timeout` for a longer maintenance
window. `--concurrency` limits Aerospike scan node fan-out (default 1, range 1–64).
Archive parsing remains bounded and sequential. Full history scans can take much
longer than the default timeout; plan the window before starting and explicitly
set `--timeout` to cover it. Mainnet-scale runtime has not been measured. The
scan uses at most two body passes, expands unresolved ancestry transitively with
a depth limit of 256 and separate limits of 10,000 processed records and 10,000
input edges across all roots, and batches header writes and guard checks.
Reaching a limit leaves unresolved evidence;
it never authorizes a repair. The watchdog still checks maintenance every five
seconds, and phase boundaries check it synchronously.

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
output, using the existing chain-era burned-output policy. Both confirmation
and the latest authenticated spend must be outside the reorg retention window:
`max(confirmation height, last spend height) + retention <= pinned tip height`.
Retention is the greater of `global_blockHeightRetention` and the effective UTXO
retention including its adjustment. A zero retention policy is refused by the
command. Discovery seals this policy; apply rechecks it and fresh evidence before
writing markers or deleting records. Recent fully-spent records remain unresolved
so a reorg can still recover their transactions. Empty mined metadata,
a missing UTXO record or local spent flags alone never authorize deletion.

The history scan starts at genesis to make absence claims conservative. Missing,
malformed or oversized archives create coverage gaps. Positive inclusion/spend
proofs may still authorize repairs despite unrelated gaps; missing coverage
cannot prove that a transaction is unconfirmed or an output is live. The index
retains targeted evidence and its dependencies, not all historical raw bytes.
Subtree transaction data is streamed, with bounded individual transaction and
block parsing; blocks exceeding limits remain unresolved.

Repairs are limited to positively proven recreated fully-spent records and
missing markers for absent fully-spent children. A marker on the parent record
that owns the spent output is sufficient, including a pagination record without
a master marker; this matches normal pruning and replay validation. Normally mined dependent
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
A completed, sealed history index is reused only if every fresh census target
is already indexed; any target missing from the index requires rebuilding.
An unfinished body scan still restarts from its read-only phase boundary, so a
larger maintenance timeout is essential for large stores. SIGINT, SIGTERM and
SIGHUP cancel through the job context and attempt to persist the final report;
SIGKILL and power loss cannot run that cleanup.
Foreign changes, a different tip or unexplained disappearance stop recovery.
Do not edit/delete a journal to bypass a failure or restore before-images over
a chain that has advanced. There is no automatic rollback.

Mutation journal commits use SQLite `synchronous=EXTRA` with DELETE journaling,
including directory synchronization before remote writes. Journal schema and
identity header are committed together; resume may initialize a provably empty
journal left before that commit, but refuses existing headerless state or foreign
schemas. [SQLite durability details](https://www.sqlite.org/pragma.html#pragma_synchronous).

Progress goes to stderr. The final JSON summary goes to stdout and `report.json`;
`manifest.sqlite`, `history.sqlite`, `journal.sqlite` and `job.json` retain the
private audit, evidence, mutation journal and phase checkpoint.

| Exit | Meaning |
| --- | --- |
| 0 | Complete audit with no repairable or unresolved findings, or complete store repair; JSON distinguishes `applied`. |
| 1 | Unsafe precondition, interruption, operational failure or failed verification. |
| 2 | Audit found repairable records, or unresolved findings remain even if independent components were repaired. |

`complete` describes whether classification/verification finished; a complete
audit with repairable records returns exit 2 and performs no mutations.

A complete graph allows independent proven repairs while unknown components
remain untouched. Unresolvable ownership prevents safe apply. Read the counts,
classifications and reasons; an empty assembly is not proof of recovery.

After successful store repair, **restart assembly and the other writers through
the normal operational procedure** so stale in-memory state cannot survive.
Then resume the FSM and verify a fresh valid mining candidate and the next tip
transition. `restart_required` is explicit after mutations, including interrupted
attempts. The CLI finishes store verification while IDLE; it cannot prove those
later runtime observations and never resumes the node itself.

The restart step above refers to assembly and the other application writers.
The recovery backend currently uses generation-guarded Aerospike deletes without
`DurableDelete`. On persistent namespaces, an Aerospike cold restart can resurrect
older deleted record versions. Durable deletes prevent that using tombstones,
but Community Edition rejects that policy, so it is not enabled unconditionally.
Keep the mutation journal and backups. After an Aerospike cold restart, keep
writers isolated and run a fresh audit in a new work directory before resuming;
a completed journal cannot certify newly resurrected records. See
[Aerospike durable deletes](https://aerospike.com/docs/database/learn/architecture/durable-deletes)
and [cold restart behavior](https://aerospike.com/docs/database/manage/database/cold-start).
