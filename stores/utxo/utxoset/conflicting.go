package utxoset

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/util"
)

// conflictingInputsSQL reads what each named transaction spends, and proves the store holds it.
//
// It reads tx_inpoints rather than the serialized body for two reasons. The body is dropped
// once its partition ages out, which is the ordinary steady state rather than an error, so a
// design resting on it would work for 288 blocks and then stop. And this store writes the body
// in plain rather than extended form, meaning its inputs carry no value or locking script, so
// even a body that is still present could not answer this.
//
// BOTH HOMES, because this statement does not know which of them still holds the transaction.
// A transaction that lost a double-spend race is very often mined -- it arrives in a block on
// the fork being abandoned, and conflict resolution has to read what it spent so those spends
// can be undone. Its identity row carries the inpoints until the deep stamp deletes it, and
// its containment row carries a copy from then on. Reading the identity table alone reported
// a stamped transaction as not held at all, so SetConflicting failed instead of resolving the
// race.
//
// The identity arm has THE LEAF AS A SCALAR and the txids as an array, so it runs once per leaf
// group: see leafGroups for the measurements that reject the other two key shapes. It replaced
// the paired `unnest(l[],t[]) JOIN tx_ident` form, whose plan flips with statistics.
//
// The containment arm puts the keys on the OUTSIDE of a LATERAL with an OFFSET 0 fence, the
// shape minedByTxidSQL uses, so it is one primary-key descent per key per live partition rather
// than a hash join against every partition read whole. Every containment row of one transaction
// carries the same inpoints, copied from the identity row or from an earlier row, so which row
// answers does not matter; the ORDER BY only makes the choice repeatable.
//
// tx_inpoints IS NOT NULL makes a BLOCK-PATH membership row a not-found here, deliberately.
// Such a row records that a transaction is in a block, not what it spends, so it cannot be a
// conflict participant: only coinbases take that path at the tip, and below the checkpoint
// nothing conflicts. Reading it as "spends nothing" would be worse than refusing it, because an
// empty input set reports a transaction with no counter-spender at all, and that is the answer
// that lets a double spend through.
//
// A hash that comes back missing from both arms is the not-found answer.
const conflictingInputsSQL = `
SELECT i.txid, i.tx_inpoints
  FROM tx_ident i
 WHERE i.leaf = $1::smallint
   AND i.txid = ANY($2::bytea[])
UNION ALL
SELECT k.txid, m.tx_inpoints
  FROM unnest($2::bytea[]) AS k(txid)
 CROSS JOIN LATERAL (
   SELECT m.tx_inpoints
     FROM tx_mined m
    WHERE m.txid = k.txid
      AND m.tx_inpoints IS NOT NULL
    ORDER BY m.mined_height, m.block_id
    LIMIT 1 OFFSET 0
 ) AS m`

// noteConflictingChildrenSQL records the contest on every parent whose UTXO is wanted.
//
// A transaction that loses a double-spend race is kept rather than discarded, because
// resolving the race later has to find it, and finding it means asking the PARENT whose UTXO
// was contested. Without this there is no route from a contested UTXO back to the transactions
// competing for it.
//
// It writes to conflict_children, keyed on the parent's txid alone, and that is what lets a
// MINED parent be contested. The row this used to update lived on tx_ident, which a mined
// transaction does not have -- the longest-chain stamp moved it into tx_mined -- so the note
// was a zero-row update for exactly the parents that matter, and silently succeeded.
//
// Two consequences of the move are worth stating, because both were rules here before. It no
// longer has to be a separate statement from the flag flip to avoid PostgreSQL's
// one-update-per-row-per-statement rule, since it updates no row; it stays separate because
// the note must go FIRST, so a note that fails leaves the transaction unmarked rather than
// marked but unfindable. And the 32-byte boundary test is gone with the packed column it
// guarded: one row per child cannot be matched straddling its neighbours.
//
// Run on BOTH values of the flag, matching both reference stores. Removing an entry is
// RemoveFromConflictingChildren's job.
//
// $1 is the height, $2 the parents and $3 the children, one element per (parent, child) pair.
// ON CONFLICT DO NOTHING against the partition's own unique index makes a repeat free; the
// reader still says DISTINCT, because the index is per partition. See the schema comment.
const noteConflictingChildrenSQL = `
INSERT INTO conflict_children (noted_height, parent_txid, child_txid)
SELECT DISTINCT $1::int, p.ptxid, p.child
  FROM unnest($2::bytea[], $3::bytea[]) AS p(ptxid, child)
ON CONFLICT DO NOTHING`

// setConflictingSQL flips the flag on every row that carries it. conflictingReadSQL then reports
// both answers the caller needs.
//
// THE FLIP AND THE READ ARE SEPARATE STATEMENTS, and the split is a correctness requirement.
// runConflictingPlan runs this statement once per leaf group, in ascending leaf order, and then
// runs the read: N+1 statements in one READ COMMITTED transaction, which keeps every row lock it
// takes until the commit, across all of those round trips. A child whose spend of one of these
// UTXOs is in flight holds that UTXO's row lock, so the UTXO update below waits for it; when the
// child commits, the row is gone and the update skips it. The child's journal row committed in
// the same instant, and the read that names the children has to see it. Inside one statement it
// cannot: every part of a statement reads the snapshot taken when the statement began, and READ
// COMMITTED's re-check after a lock wait covers only the row being updated, not the other parts
// of the statement. Measured with the child's spend held open in a transaction
// (TestSetConflictingSeesAChildWhoseSpendCommitsWhileItWaits): the one-statement form marked the
// parent and returned no children, so the child stayed spendable and unmarked with nothing left
// to find it. As a later statement in the same transaction the read takes a fresh snapshot,
// taken after the last flip finished waiting, so every child whose spend committed before that
// flip is seen. A spend that comes later finds the conflicting bit set and is refused by its own
// flag mask, including one that was waiting on a flip's row lock, whose predicate READ COMMITTED
// re-evaluates against the new row version (TestSpendWaitingOnSetConflictingIsRefused).
//
// Holding locks across round trips widens the window for a deadlock with a caller that takes the
// same rows in another order. Within each statement the UTXO update runs before the identity and
// membership arms, while setLockedOnce takes the identity rows first, so a concurrent SetLocked on
// an overlapping set can deadlock with this. SetConflicting runs the whole transaction under
// withDeadlockRetry, the cure the store uses for the same race between SetLocked and the mine.
//
// BOTH rows, for the reason setLockedSQL gives: the identity row is what a metadata read
// shows, the UTXO row is what the spend path reads, and the spend path never looks at the
// identity row. Moving only one leaves a transaction reporting itself conflicting while its
// UTXOs stay spendable. The UTXO bit is what makes the spend statement's flag mask refuse the
// delete.
//
// The statement runs ONCE PER LEAF GROUP, in ascending leaf order, with the leaf as a scalar
// and every array a bound constant. That is what keeps each arm keyed and linear whatever the
// planner's estimates say, because no arm joins anything to a table.
//
// The UTXO arm finds its rows in two steps inside one statement. The ARRAY sub-select walks each
// transaction's packed-key range through a LATERAL with OFFSET 0, which can only be planned as a
// per-key index probe on this leaf's partition, with the full 32-byte id rechecked on the heap.
// The update then takes those keys as an index condition, one descent per distinct key, and
// rechecks the full id against the txid array. The pool runs force_custom_plan (store.go), so that
// array is a constant in every plan and Postgres hashes it; under a generic plan it would be a
// parameter and scanned per row, which is still linear in outputs, because the array holds one
// entry per transaction of the leaf group, not one per output. The packed key
// carries the id prefix first precisely so a transaction's UTXOs are one index range, and the
// full id is still rechecked because the prefix is non-unique by design.
//
// Three earlier shapes were measured and rejected, on the test container's Postgres 16 with 2M
// UTXO rows over 8 partitions plus the flagged rows, 1,000 outputs per transaction
// (TestSetConflictingIsLinearInOutputs pins the result):
//
//   - A plain join against the unnest, with the range as a join filter. The planner hash-joined
//     on the transaction id, which no UTXO index covers, over a sequential scan of every
//     partition: on Hetzner mainnet on 2026-10-23, 4.56 billion rows to flag 16 transactions,
//     about 40 minutes for every block with conflicts.
//   - The fenced lookup with the update matching the found keys as three independent
//     ANY (ARRAY(SELECT ...)) filters over all eight partitions. An array from a sub-select is
//     never hashed, so each candidate row scanned the arrays element by element: 1.3 s at 16,000
//     outputs and 25-29 s at 64,000, quadratic in outputs.
//   - The fenced lookup with the update joined to it on (leaf, ukey, txid), setLockedSQL's
//     shape. Linear under good estimates (61-70 ms at 16,000 outputs, 246-270 ms at 64,000),
//     but with enable_nestloop off, which stands in for the estimates that pick a hash join, it
//     became a hash join over a sequential scan of every partition, 4.9 s at 16,000.
//
// This shape measured 46-57 ms at 16,000 outputs and 159-227 ms at 64,000, the same with nested
// loops on or off. The identity and membership arms match the per-leaf constant arrays
// directly, in setLockedIdentSQL's leaf-scalar shape, and
// TestConflictingStatementsNeverScanTheirTables pins all of it.
//
// The flag is flipped on THREE things, not two, for the same reason setLockedSQL flips three.
// A transaction lives in exactly one of tx_ident and tx_mined and this statement does not know
// which; a contested parent is very often mined, which is the whole reason the note itself
// became a side table. minedRow.toMeta reads Conflicting straight off tx_mined.flags, copied
// once by the move and updated by nothing afterwards, so without the membership arm marking a
// mined transaction conflicting set the UTXO bit and not the bit Get reports -- the mirror
// image of the failure setLockedSQL's own comment warns about. The membership arm is a plain
// txid equality because tx_mined's primary key leads with txid.
//
// Every arm writes ONLY ROWS WHOSE BIT CHANGES, as setLockedSQL and clearMarkerSQL do. A cascade
// revisits shared descendants and a clear runs over rows that are already clear, and rewriting
// those rows would lock them for nothing and widen the overlap with SetLocked and the mine that
// withDeadlockRetry has to cure. Skipping them does not weaken the wait described above. A UTXO
// row whose bit is still clear in the statement's snapshot is a candidate, so the update still
// waits on a child's in-flight spend of it. A row whose bit is already set in the snapshot was set
// by a committed flip, and a spend that starts after that is refused by its flag mask, so no spend
// of it can be in flight for the update to wait on. The one spend the
// mask lets through is conflict resolution's own, with IgnoreConflicting, made for the winner of
// a contest. The read names it a child only if it committed before the read. It is the winner,
// so it is not one the cascade should mark. On a clear, a UTXO row that is already clear is not
// skipped from the read, only from the write: a child that spent it was not marked through it,
// and a spend of it committed before the read is still named.
//
// SKIPPING UNCHANGED ROWS MEANS OPPOSITE FLIPS NO LONGER SERIALIZE. The reasoning above covers
// in-flight spends only, not an in-flight flip in the other direction. Say a clear of a
// transaction is running and not yet committed when a set of the same transaction starts. The
// set's snapshot still shows the bit set, so every arm skips the row without waiting on the
// clear's row lock, and the clear's value is what stands once it commits. When every arm wrote
// its rows, the set waited on the clear, re-checked the row and applied its own value. Nothing
// in this store serializes the two calls, so a caller of SetConflicting must not run opposite
// flips on the same transaction at the same time: the losing call would act on its affected
// outputs and children as if its flip had landed. No path that overlaps them is known today.
const setConflictingSQL = `
WITH ident AS (
    UPDATE tx_ident i
       SET flags = CASE WHEN $5::boolean THEN i.flags |  $6::smallint
                                         ELSE i.flags & ~$6::smallint END
     WHERE i.leaf = $1::smallint AND i.txid = ANY ($2::bytea[])
       AND ((i.flags & $6::smallint) <> 0) <> $5::boolean
),
mined AS (
    UPDATE tx_mined m
       SET flags = CASE WHEN $5::boolean THEN m.flags |  $6::smallint
                                         ELSE m.flags & ~$6::smallint END
     WHERE m.txid = ANY ($2::bytea[])
       AND ((m.flags & $6::smallint) <> 0) <> $5::boolean
)
UPDATE utxo u
   SET flags = CASE WHEN $5::boolean THEN u.flags |  $6::smallint
                                     ELSE u.flags & ~$6::smallint END
 WHERE u.leaf = $1::smallint
   AND u.ukey = ANY (ARRAY(
         SELECT c.ukey
           FROM unnest($2::bytea[], $3::uuid[], $4::uuid[]) AS k(txid, lo, hi)
          CROSS JOIN LATERAL (
            SELECT x.ukey
              FROM utxo x
             WHERE x.leaf = $1::smallint AND x.ukey >= k.lo AND x.ukey <= k.hi AND x.txid = k.txid
            OFFSET 0
          ) AS c))
   AND u.txid = ANY ($2::bytea[])
   AND ((u.flags & $6::smallint) <> 0) <> $5::boolean`

// conflictingReadSQL reads back the spends to undo and the next level of the cascade, after
// setConflictingSQL has run in the same transaction. See setConflictingSQL for why it is a
// separate statement.
//
// The 'parent' rows are the spends to be undone, and the join to the journal is OUTER with a
// guard, in two parts, for two different reasons.
//
// The journal row is what makes a spend undoable, so an input that still has one is reported
// with its full undo payload. An input with NO journal row is reported too, but only when the
// parent UTXO is currently LIVE at that outpoint. That case is not hypothetical bookkeeping:
// it is what a crash between the unspend and the unlock leaves behind, and the shared
// conflict-resolution replay depends on seeing it. The driver takes the parents named here as
// the set to unlock at the end of the run, so an input omitted because its UTXO was already
// restored is a parent left LOCKED FOREVER with no operator signal. Both reference stores
// report every input unconditionally for exactly this reason.
//
// Handing back a record for a UTXO that is already live is safe here because Unspend counts it:
// its live_before arm makes a restore of an already-restored UTXO a no-op success rather than a
// missing-UTXO failure, and its held arm puts the caller's hold on such a UTXO, so the record
// costs one probe and is acted on exactly as one with a journal row would be.
//
// The probe is a LATERAL with an OFFSET 0 fence rather than a plain EXISTS, and the fence is
// the whole difference. Written as `OR EXISTS (SELECT 1 FROM utxo ...)` the planner is free to
// pull the subquery up and hash it, and it does: measured on this schema at 40,000 UTXOs across
// all eight partitions with 500 keys, the plan carried a hashed SubPlan over a Seq Scan of
// every one of utxo_p0..p7, 30.6 ms, and its cost grows with the UTXO table rather than with
// the batch. Fenced, it is one index descent per key on the leaf partition's ukey index with
// the full 32-byte txid rechecked on the heap -- the bound schema.go requires of every by-txid
// UTXO access, because the packed key is a non-unique 96-bit prefix that can find a row but
// must never justify acting on one.
//
// An input with neither a journal row nor a live UTXO is still omitted, and that is the half of
// the old rule that was right. Its undo payload has aged out of the journal, or the UTXO was
// re-spent by a different transaction since; either way this store cannot restore it, and
// Unspend FAILS the whole restore if a single record it is given cannot be accounted for. A
// cascade rooted on a transaction older than journal retention therefore still stops early,
// which is a real limit of delete-on-spend rather than something to paper over.
//
// The 'child' rows are the next level of the cascade. The journal is the only place this store
// can answer "who took this UTXO", because the UTXO row is destroyed the moment it is spent.
const conflictingReadSQL = `
WITH k AS (
    SELECT * FROM unnest($1::int[], $2::bytea[], $3::uuid[], $4::uuid[])
        AS t(ref, txid, lo, hi)
),
p AS (
    SELECT * FROM unnest($5::int[], $6::bytea[], $7::smallint[], $8::bytea[], $9::uuid[], $10::int[])
        AS t(ref, child, pleaf, ptxid, pukey, pvout)
)
SELECT 'parent'::text AS kind, p.ref, p.ptxid, p.pvout,
       j.satoshis, j.script, j.hash_override, NULL::bytea AS spender
  FROM p
  LEFT JOIN LATERAL (
    SELECT sj.ukey, sj.satoshis, sj.script, sj.hash_override
      FROM spend_journal sj
     WHERE sj.ukey = p.pukey AND sj.txid = p.ptxid AND sj.spending_txid = p.child
    OFFSET 0
  ) AS j ON TRUE
  LEFT JOIN LATERAL (
    SELECT 1 AS live
      FROM utxo u
     WHERE u.leaf = p.pleaf AND u.ukey = p.pukey AND u.txid = p.ptxid
     LIMIT 1 OFFSET 0
  ) AS lv ON TRUE
 WHERE j.ukey IS NOT NULL OR lv.live IS NOT NULL
UNION ALL
SELECT 'child'::text, k.ref, NULL::bytea, NULL::int,
       NULL::bigint, NULL::bytea, NULL::bytea, j.spending_txid
  FROM k
 CROSS JOIN LATERAL (
   SELECT sj.spending_txid
     FROM spend_journal sj
    WHERE sj.ukey >= k.lo AND sj.ukey <= k.hi AND sj.txid = k.txid
   OFFSET 0
 ) AS j
ORDER BY 1 DESC, 2`

// SetConflicting marks transactions as having lost a double-spend race, or clears that mark.
//
// It returns two things the caller cannot get anywhere else. The first is the set of parent
// outputs these transactions consumed, in a form this store's own Unspend can restore, which is
// exactly what conflict resolution does with it. The second is the set of transactions that
// spent THESE transactions' outputs, which is the next level of the walk that demotes a loser's
// whole descendant tree.
//
// The spender recorded on each returned record is the transaction that took the UTXO. Its input
// index is the position in the flattened inpoint list, which is parent-major and therefore NOT
// the transaction's original input order, because the stored inpoints deduplicate parents.
// Nothing reads it today; it is stated here so a later reader does not assume otherwise.
//
// Both answers are bounded by the journal's retention. Beyond that this store cannot say who
// took a UTXO, so a cascade rooted on an older transaction stops early. That is a real limit of
// delete-on-spend rather than a gap to paper over.
//
// The caller must not run a set and a clear on the same transaction at the same time. The store
// does not serialize them. A call that starts while the other is uncommitted still sees its own
// target value in its snapshot, writes nothing, and returns as if its flip had landed, and the
// other call's value stands once it commits (see setConflictingSQL).
func (s *Store) SetConflicting(ctx context.Context, txHashes []chainhash.Hash,
	value bool) ([]*utxo.Spend, []chainhash.Hash, error) {
	if len(txHashes) == 0 {
		return nil, nil, nil
	}

	// One entry per DISTINCT transaction. The cascade can name the same transaction twice when
	// two of its parents were both contested, and asking twice would duplicate both answers.
	named := make([]chainhash.Hash, 0, len(txHashes))
	seen := make(map[chainhash.Hash]struct{}, len(txHashes))

	for _, h := range txHashes {
		if _, dup := seen[h]; dup {
			continue
		}

		seen[h] = struct{}{}

		named = append(named, h)
	}

	inpoints, err := s.readConflictingInputs(ctx, named)
	if err != nil {
		return nil, nil, err
	}

	plan := s.planConflicting(named, inpoints)

	// The height is read ONCE and used for both the partition and the row, because a second read
	// that crossed a leaf boundary would insert into a partition that does not exist.
	notedHeight := s.GetBlockHeight()

	// The note's partition BEFORE the transaction is opened, never inside it: the DDL needs its
	// own pool connection, and taking one while holding a transaction from the same pool
	// deadlocks the pool under concurrency, with no timeout.
	if len(plan.pChild) > 0 {
		if err := s.ensureSpendJournalPartition(ctx, notedHeight); err != nil {
			return nil, nil, err
		}
	}

	var (
		affected []*utxo.Spend
		children []chainhash.Hash
	)

	// The transaction holds row locks across one statement per leaf group, and a concurrent
	// SetLocked on an overlapping set takes the same rows in another order (see
	// setConflictingSQL). PostgreSQL stops one of the two as a deadlock, and this runs the whole
	// transaction again from the start.
	err = withDeadlockRetry(ctx, "set_conflicting", func() error {
		var runErr error

		affected, children, runErr = s.setConflictingOnce(ctx, plan, notedHeight, value)

		return runErr
	})
	if err != nil {
		return nil, nil, err
	}

	return affected, children, nil
}

// setConflictingOnce is one attempt of SetConflicting's write, in one database transaction: the
// note, the flag flips and the read-back commit or fail together.
func (s *Store) setConflictingOnce(ctx context.Context, plan *conflictingPlan, notedHeight uint32,
	value bool) ([]*utxo.Spend, []chainhash.Hash, error) {
	dbTx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, nil, errors.NewStorageError("[utxoset][SetConflicting] begin", err)
	}

	committed := false

	defer func() {
		if !committed {
			_ = dbTx.Rollback(ctx)
		}
	}()

	// The note runs first, so a failure here leaves the transactions unmarked rather than
	// marked but unfindable.
	if len(plan.pChild) > 0 {
		if _, err = dbTx.Exec(ctx, noteConflictingChildrenSQL,
			int32(notedHeight), plan.pTxid, plan.pChild); err != nil { //nolint:gosec // a chain height fits int32
			return nil, nil, errors.NewStorageError("[utxoset][SetConflicting] note parents", err)
		}
	}

	affected, children, err := s.runConflictingPlan(ctx, dbTx, plan, value)
	if err != nil {
		return nil, nil, err
	}

	if err = dbTx.Commit(ctx); err != nil {
		return nil, nil, errors.NewStorageError("[utxoset][SetConflicting] commit", err)
	}

	committed = true

	return affected, children, nil
}

// readConflictingInputs fetches what each named transaction spends, and reports any the store
// does not hold.
func (s *Store) readConflictingInputs(ctx context.Context,
	named []chainhash.Hash) (map[chainhash.Hash]subtree.TxInpoints, error) {
	txids := make([][]byte, 0, len(named))
	for i := range named {
		txids = append(txids, named[i][:])
	}

	out := make(map[chainhash.Hash]subtree.TxInpoints, len(named))

	// One round trip per leaf group, because the identity arm needs the leaf as a scalar to
	// keep its plan an index scan. The membership arm rides along in the same statement rather
	// than taking a round trip of its own: it does not need the leaf, and splitting the keys
	// across groups costs it nothing, since it is one primary-key descent per key either way.
	for _, g := range leafGroups(txids) {
		if err := s.readInputsForLeaf(ctx, g, out); err != nil {
			return nil, err
		}
	}

	if len(out) == len(named) {
		return out, nil
	}

	// Absent means the store does not hold it, which both reference stores report as an error
	// rather than skipping. Bounded, so one bad batch cannot produce an error the size of a
	// block.
	missing := make([]error, 0, 4)

	for i := range named {
		if _, ok := out[named[i]]; ok {
			continue
		}

		if len(missing) < 10 {
			missing = append(missing,
				errors.NewTxNotFoundError("[utxoset][SetConflicting] %s", named[i].String()))
		}
	}

	return nil, errors.Join(missing...)
}

// readInputsForLeaf runs both arms of conflictingInputsSQL for one leaf group, folding the
// answers into out.
func (s *Store) readInputsForLeaf(ctx context.Context, g leafBatch,
	out map[chainhash.Hash]subtree.TxInpoints) error {
	rows, err := s.pool.Query(ctx, conflictingInputsSQL, g.leaf, g.txids)
	if err != nil {
		return errors.NewStorageError("[utxoset][SetConflicting] read inputs", err)
	}

	defer rows.Close()

	for rows.Next() {
		var (
			txid []byte
			raw  []byte
		)

		if err := rows.Scan(&txid, &raw); err != nil {
			return errors.NewStorageError("[utxoset][SetConflicting] scan inputs", err)
		}

		var h chainhash.Hash

		copy(h[:], txid)

		if len(raw) == 0 {
			// A coinbase stores no inpoints. It spends nothing, so it contributes no parents.
			out[h] = subtree.NewTxInpoints()
			continue
		}

		ip, ierr := subtree.NewTxInpointsFromBytes(raw)
		if ierr != nil {
			return errors.NewStorageError("[utxoset][SetConflicting] decode inpoints %s", h.String(), ierr)
		}

		out[h] = ip
	}

	if err := rows.Err(); err != nil {
		return errors.NewStorageError("[utxoset][SetConflicting] input rows", err)
	}

	return nil
}

// conflictingPlan is the argument set for one call of the statements above, flattened across
// however many transactions were named, in the same shape the spend and create paths use.
type conflictingPlan struct {
	// One element per named transaction.
	kRef  []int32
	kTxid [][]byte
	kLo   [][16]byte
	kHi   [][16]byte
	named []chainhash.Hash

	// The same transactions split by leaf, built once from kTxid, kLo and kHi.
	leaves []conflictingLeafKeys

	// One element per input, across every named transaction.
	pRef   []int32
	pChild [][]byte
	pLeaf  []int16
	pTxid  [][]byte
	pUkey  [][16]byte
	pVout  []int32
	pVin   []int
	pOwner []int
}

// planConflicting flattens the named transactions and their inputs into arrays.
func (s *Store) planConflicting(named []chainhash.Hash,
	inpoints map[chainhash.Hash]subtree.TxInpoints) *conflictingPlan {
	p := &conflictingPlan{named: named}

	for i := range named {
		p.kRef = append(p.kRef, int32(i)) //nolint:gosec // bounded by batch size
		p.kTxid = append(p.kTxid, named[i][:])
		p.kLo = append(p.kLo, Pack(named[i][:], 0))
		p.kHi = append(p.kHi, Pack(named[i][:], ^uint32(0)))

		ip := inpoints[named[i]]
		flat := ip.GetTxInpoints()

		for vin, in := range flat {
			// The coinbase placeholder is not a real parent, and following it would look up a
			// transaction that does not exist. Skipped cleanly rather than left as a hole.
			if in.Hash == subtree.CoinbasePlaceholderHashValue {
				continue
			}

			parent := in.Hash

			p.pRef = append(p.pRef, int32(len(p.pOwner))) //nolint:gosec // bounded by batch size
			p.pChild = append(p.pChild, named[i][:])
			p.pLeaf = append(p.pLeaf, LeafFor(parent[:]))
			p.pTxid = append(p.pTxid, parent[:])
			p.pUkey = append(p.pUkey, Pack(parent[:], in.Index))
			p.pVout = append(p.pVout, int32(in.Index)) //nolint:gosec // a vout fits int32
			p.pVin = append(p.pVin, vin)
			p.pOwner = append(p.pOwner, i)
		}
	}

	p.groupByLeaf()

	return p
}

// conflictingLeafKeys is one leaf group of a conflictingPlan's named transactions, with each
// transaction's packed-key range, the arguments setConflictingSQL takes.
type conflictingLeafKeys struct {
	leaf  int16
	txids [][]byte
	lo    [][16]byte
	hi    [][16]byte
}

// groupByLeaf splits the named transactions by leaf, in the lock order leafIndexGroups decides
// for every batch writer. It copies kTxid, kLo and kHi rather than packing the ranges again, so
// the flip and the read take their keys from one source.
func (p *conflictingPlan) groupByLeaf() {
	groups := leafIndexGroups(p.kTxid)
	p.leaves = make([]conflictingLeafKeys, 0, len(groups))

	for _, g := range groups {
		keys := conflictingLeafKeys{
			leaf:  g.leaf,
			txids: make([][]byte, len(g.idx)),
			lo:    make([][16]byte, len(g.idx)),
			hi:    make([][16]byte, len(g.idx)),
		}

		for j, i := range g.idx {
			keys.txids[j] = p.kTxid[i]
			keys.lo[j] = p.kLo[i]
			keys.hi[j] = p.kHi[i]
		}

		p.leaves = append(p.leaves, keys)
	}
}

// runConflictingPlan flips the flags and then reads back both answers, as separate statements so
// the read sees every spend that committed while a flip waited on its row locks. q must be a
// transaction for them to be atomic; SetConflicting always passes one.
func (s *Store) runConflictingPlan(ctx context.Context, q querier, p *conflictingPlan,
	value bool) ([]*utxo.Spend, []chainhash.Hash, error) {
	for _, g := range p.leaves {
		if _, err := q.Exec(ctx, setConflictingSQL,
			g.leaf, g.txids, g.lo, g.hi, value, FlagConflicting); err != nil {
			return nil, nil, errors.NewStorageError("[utxoset][SetConflicting] set", err)
		}
	}

	rows, err := q.Query(ctx, conflictingReadSQL,
		p.kRef, p.kTxid, p.kLo, p.kHi,
		p.pRef, p.pChild, p.pLeaf, p.pTxid, p.pUkey, p.pVout)
	if err != nil {
		return nil, nil, errors.NewStorageError("[utxoset][SetConflicting] read", err)
	}

	var (
		affected []*utxo.Spend
		children []chainhash.Hash
	)

	childSeen := make(map[chainhash.Hash]struct{})

	for rows.Next() {
		var (
			kind         string
			ref          int32
			ptxid        []byte
			pvout        *int32
			satoshis     *int64
			script       []byte
			hashOverride []byte
			spender      []byte
		)

		if err := rows.Scan(&kind, &ref, &ptxid, &pvout, &satoshis, &script, &hashOverride, &spender); err != nil {
			rows.Close()
			return nil, nil, errors.NewStorageError("[utxoset][SetConflicting] scan", err)
		}

		if kind == "child" {
			var h chainhash.Hash

			copy(h[:], spender)

			// Deduplicated on the way out: a transaction that spends several outputs of one
			// parent would otherwise be named once per output.
			if _, dup := childSeen[h]; dup {
				continue
			}

			childSeen[h] = struct{}{}

			children = append(children, h)

			continue
		}

		sp, serr := p.spendFor(ref, ptxid, pvout, satoshis, script, hashOverride)
		if serr != nil {
			rows.Close()
			return nil, nil, serr
		}

		affected = append(affected, sp)
	}

	rows.Close()

	if err := rows.Err(); err != nil {
		return nil, nil, errors.NewStorageError("[utxoset][SetConflicting] rows", err)
	}

	return affected, children, nil
}

// spendFor turns one journal row into the restorable record conflict resolution hands back to
// Unspend.
func (p *conflictingPlan) spendFor(ref int32, ptxid []byte, pvout *int32, satoshis *int64,
	script []byte, hashOverride []byte) (*utxo.Spend, error) {
	// The vout comes from the plan's own inpoint rather than from the journal, so it is never
	// absent. A missing one would mean the statement returned a row the plan did not ask for.
	if pvout == nil {
		return nil, errors.NewStorageError("[utxoset][SetConflicting] row %d names no output", ref)
	}

	parent, err := chainhash.NewHash(ptxid)
	if err != nil {
		return nil, errors.NewStorageError("[utxoset][SetConflicting] parent hash", err)
	}

	child := p.named[p.pOwner[ref]]
	vout := uint32(*pvout) //nolint:gosec // a vout is never negative

	sp := &utxo.Spend{
		TxID:         parent,
		Vout:         vout,
		SpendingData: spendpkg.NewSpendingData(&child, p.pVin[ref]),
	}

	// The reassigned identity wins when there is one, exactly as the spend path treats it.
	if len(hashOverride) > 0 {
		if h, herr := chainhash.NewHash(hashOverride); herr == nil {
			sp.UTXOHash = h
			return sp, nil
		}
	}

	// No journal row means the UTXO is already live, so there is no captured amount or script
	// to compute the UTXO's identity from and the record carries none. Nothing in this store
	// reads it -- Unspend restores on the outpoint and the spender, and that is all the driver
	// does with these -- and inventing one from a UTXO row would be a second, unverified
	// opinion about a value the journal is the record for.
	if satoshis == nil {
		return sp, nil
	}

	if h, herr := util.UTXOHash(parent, vout, bscript.NewFromBytes(script), uint64(*satoshis)); herr == nil { //nolint:gosec // satoshis are never negative
		sp.UTXOHash = h
	}

	return sp, nil
}
