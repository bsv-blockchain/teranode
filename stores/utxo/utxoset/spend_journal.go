package utxoset

import (
	"context"
	"fmt"
	"sort"

	"github.com/bsv-blockchain/teranode/errors"
)

// SpendJournalPartitionBlocks is the width of one journal leaf. The pruner drops whole leaves,
// so retention is granular to this. At the measured frontier (~20,000 spends/block) a leaf
// holds roughly 960,000 rows.
const SpendJournalPartitionBlocks = 48

// DefaultSpendJournalRetentionBlocks is how far back a spend stays undoable.
//
// 1440, not the 288 the design originally proposed. ParentPreservationBlocks is 1440 and
// its own settings documentation says in bold "DO NOT reduce below 1440 as this risks
// invalidating legitimate transaction resubmissions" -- so a 288-block journal would
// have compiled, passed tests, and then silently failed to protect a resubmitted
// transaction ten days later. The measured UTXO growth slope (251/block, not the 802
// assumed) leaves the budget at roughly 60-66% even at this depth, so the correct number
// is affordable.
//
// Steady-state leaf count is retention/SpendJournalPartitionBlocks + 1 = 31 tables. Bounded, and
// dropped as the chain advances.
const DefaultSpendJournalRetentionBlocks = 1440

// spendJournalSQL deletes the UTXO row AND captures its payload in one statement.
//
// One statement, not merely one transaction. A data-modifying CTE guarantees the delete
// and the journal insert see the same rows and commit together -- there is no ordering,
// no second round trip, and no window in which a coin is gone with nothing recording how
// to put it back. The outer SELECT still returns satoshis and script, so the spend
// remains its own decorate fetch.
//
// Every parameter is an ARRAY, including the height and the spending transaction, so one
// statement serves one transaction or a thousand. That is what lets the spend path batch
// without a second copy of these predicates existing somewhere: the single-transaction path
// is a batch of one.
//
// This is now the ONLY spend statement. There was a journal-free twin of it, used below
// the checkpoint, whose predicates had to be kept identical by hand; it is deleted rather
// than flagged off, because two copies of a consensus predicate is a defect waiting for
// one of them to be edited. The predicates that authorise the delete are the full 32-byte
// txid recheck (the ukey is a non-unique 96-bit prefix and can only locate, never
// authorise), the refusing-flag mask, and the maturity test. classifySQL deliberately omits
// the last two, so an excluded row surfaces as frozen, conflicting, locked or immature rather
// than as spent.
//
// THE FLAG MASK IS PER KEY, not a constant, because two of the three flags it tests are
// waivable and the waiver belongs to the caller rather than to the coin. Conflict resolution
// spends the promoted winner through the very lock and conflicting mark it set a moment
// earlier -- that is what WithIgnoreLocked and WithIgnoreConflicting are for -- while an
// ordinary validator spend of the same coin must be refused. The mask used to be the literal 5
// and neither option had any effect: a locked coin was spendable by anybody, which made the
// lock decorative, and an ignored conflicting flag still refused. Frozen has no waiver in any
// store and is always in the mask. See spendGuardMask.
//
// The flag test is written (flags & 5) < 1 and not (flags & 1) = 0 AND (flags & 4) = 0,
// which it is equal to for every value a smallint can hold, because the planner can estimate
// one and not the other. It has no statistics for a bit-mask expression, so an equality on
// one is given the default selectivity of one row in two hundred, and two of them one in
// forty thousand. That told the planner almost no coin survives the test, and with a batch
// of keys on the other side it chose to walk the whole coin table once PER KEY, since a
// table of one row is cheap to walk. Measured on a 40,000-row table: a 64-key batch took
// 3 ms until the table crossed the size where that plan won, then 45 ms, and with
// materialisation disabled 180 ms. An inequality on an expression without statistics is
// given one third, which is wrong by three rather than by forty thousand, and it is enough
// for the planner to reach for the index or hash the batch instead. The single-key form of
// this statement never showed the problem, because with one key a walk per key is one walk.
//
// The outer SELECT carries hash_override out with the payload, and it costs nothing: the
// DELETE's RETURNING already reads the whole row. It is non-NULL only on a coin ReAssignUTXO
// has moved to a new owner, and on such a coin the satoshis and the script beside it are the
// OLD output's -- the reassign interface has no room for the new ones. claimMismatch reads it
// to decide which of the two authentications applies, so it has to travel with them rather
// than be fetched separately, or a coin could be reassigned between the delete and the check.
//
// The journal row copies the coin's mined_height and block_id along with the rest of the
// payload, and they cost nothing extra: the DELETE already carries the whole row, so this is
// two more columns on a RETURNING that was already reading them. They are what makes a
// fully-spent parent older than the membership retention still answerable -- see
// readSpentParents in lookup.go and the spend_journal comment in schema.go. They add no
// placeholder, because both values come off the deleted row rather than from the caller.
// $7 is the per-key flag mask, and it is the only placeholder added since.
const spendJournalSQL = `
WITH k AS (
    SELECT * FROM unnest($1::smallint[], $2::uuid[], $3::bytea[], $4::int[],
                         $5::int[], $6::bytea[], $7::smallint[])
        AS t(leaf, ukey, txid, vin, spent_height, spending_txid, mask)
),
del AS (
    DELETE FROM utxo u USING k
     WHERE u.leaf           = k.leaf
       AND u.ukey           = k.ukey
       AND u.txid           = k.txid
       AND (u.flags & k.mask) < 1
       AND u.spendable_from <= k.spent_height
    RETURNING k.vin, k.spent_height, k.spending_txid, u.satoshis, u.created_height,
              u.spendable_from, u.flags, u.mined_height, u.block_id, u.ukey, u.txid,
              u.script, u.hash_override
),
journal AS (
    INSERT INTO spend_journal (spent_height, satoshis, created_height, spendable_from,
                           flags, mined_height, block_id, ukey, txid, spending_txid,
                           script, hash_override)
    SELECT d.spent_height, d.satoshis, d.created_height, d.spendable_from, d.flags,
           d.mined_height, d.block_id, d.ukey, d.txid, d.spending_txid, d.script,
           d.hash_override
      FROM del d
)
SELECT d.vin, d.satoshis, d.script, d.hash_override FROM del d`

// ensureSpendJournalPartition creates the spend-journal leaf covering height, if absent.
//
// Each leaf gets ONE index, the packed-key btree, which is what every restore probes. There
// was a second, a block-range summary over a block-applied mark, and it existed only for a
// question the reclaimer asked once per retiring leaf. Nothing asks it now: retiring a
// transaction's identity is dropping the tx_mined window it lives in, so the journal is read
// only by a restore, and only by outpoint.
//
// Called on the spend path, so it must be cheap and idempotent. CREATE TABLE IF NOT
// EXISTS is both, and a leaf covers SpendJournalPartitionBlocks heights so this is a no-op for
// all but one spend in that many.
func (s *Store) ensureSpendJournalPartition(ctx context.Context, height uint32) error {
	leaf := height / SpendJournalPartitionBlocks

	// Only touch the catalog when the leaf actually changes -- once every
	// SpendJournalPartitionBlocks heights, not once per spend.
	//
	// The cache holds leaf+1 so that its zero value means "nothing cached yet" rather
	// than "leaf 0 is already created". Storing the leaf directly made leaf 0 permanently
	// unreachable: a fresh store starts at zero, and the first spend below height 48 would
	// see a hit, skip the DDL, and fail the insert with "no partition found for row" --
	// precisely the initial-sync case.
	if s.journalLeaf.Load() == leaf+1 {
		return nil
	}

	// One writer per process. CREATE TABLE IF NOT EXISTS is NOT concurrency-safe in
	// PostgreSQL: simultaneous attempts race on the pg_type row and raise a unique
	// violation rather than quietly agreeing. With the spend phase running thousands of
	// goroutines, that race is the common case rather than the rare one.
	s.journalDDL.Lock()
	defer s.journalDDL.Unlock()

	// Re-check under the lock: the winner may have created it while we queued.
	if s.journalLeaf.Load() == leaf+1 {
		return nil
	}

	lo := leaf * SpendJournalPartitionBlocks
	hi := lo + SpendJournalPartitionBlocks

	// The conflict-bookkeeping window is created in the SAME statement, over the SAME range,
	// and that is the invariant it rests on: a note lands at the store's current height, which
	// is the height whose journal leaf the spend path has already ensured, so the window a
	// note needs exists whenever its journal leaf does. Two separate ensure calls, or a window
	// created on first note, would leave the one case that matters -- a conflict noted at a
	// height nothing has spent at -- failing with "no partition found for row".
	//
	// The unique index is PER WINDOW rather than on the parent. A unique index on a
	// partitioned table must include the partition key, and (noted_height, parent_txid,
	// child_txid) would admit the same pair twice at two heights, so it would buy nothing the
	// reader's DISTINCT does not already have to do. Per window it makes the repeated note
	// inside one window -- the common case, since a losing transaction is re-offered -- an
	// ON CONFLICT DO NOTHING that writes nothing at all.
	//
	// It is also the ONE index, and that is deliberate rather than an omission. Every read of
	// this table is by parent_txid, which LEADS the pair, so a second index on (parent_txid)
	// alone can only duplicate work the planner already does here: measured at 500 keys
	// against 40,000 rows in six windows, with both indexes present, the read and the removal
	// both chose the pair index and neither touched the parent-only one. A second index on a
	// table written on every double-spend is write amplification for nothing.
	ddl := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS spend_journal_%[1]d PARTITION OF spend_journal
  FOR VALUES FROM (%[2]d) TO (%[3]d)
  WITH (fillfactor = 100,
        autovacuum_vacuum_scale_factor = 0,
        autovacuum_vacuum_threshold    = 50000);
CREATE INDEX IF NOT EXISTS spend_journal_%[1]d_ukey ON spend_journal_%[1]d (ukey);
CREATE TABLE IF NOT EXISTS conflict_children_%[1]d PARTITION OF conflict_children
  FOR VALUES FROM (%[2]d) TO (%[3]d);
CREATE UNIQUE INDEX IF NOT EXISTS conflict_children_%[1]d_pair
    ON conflict_children_%[1]d (parent_txid, child_txid);`, leaf, lo, hi)

	if _, err := s.pool.Exec(ctx, ddl); err != nil {
		return errors.NewStorageError("[utxoset] create spend-journal partition %d", leaf, err)
	}

	// Only record the leaf once the DDL has actually succeeded. Marking it up front would
	// make a transient failure permanent -- every later spend would see a cache hit, skip
	// the retry, and fail on a partition that was never created.
	s.journalLeaf.Store(leaf + 1)

	// The drop deliberately does NOT happen here, though crossing into a new leaf is
	// exactly the moment old ones fall out of retention. It used to, and the argument was
	// sound for a catalog operation: dropping a partition is constant time, and a
	// background job that falls behind is the failure mode that dominated the previous
	// store. What it missed is that DETACH CONCURRENTLY waits for every open transaction
	// on the parent. From a spend, with thousands of goroutines behind it, that stalls the
	// pipeline; and because a spend must not fail over old history that could not be
	// discarded, the only available response to an error was to swallow it. It swallowed a
	// real one for the entire life of the branch.
	//
	// The pruner service drives it instead: see GetPrunerService in pruner.go.

	return nil
}

// partitionLeafSQL lists every leaf of one height-partitioned parent, in whichever state it
// is in. The parent's name is substituted twice: once as the regclass the state columns are
// judged against, and once as the name prefix that identifies an orphan.
//
// Three states matter, and all three were verified against PostgreSQL 17 rather than
// assumed:
//
//   - ATTACHED. The normal case. relispartition is true and pg_inherits has the row.
//   - ORPHANED. A crash between DETACH and DROP leaves a fully standalone table:
//     relispartition goes FALSE and the pg_inherits row is GONE. A listing that joins
//     pg_inherits can never see it again, so it would leak forever. Found here by name
//     within the parent's own schema, which is the only thing left that identifies it.
//   - DETACH PENDING. A crash DURING a concurrent detach leaves inhdetachpending set.
//     PostgreSQL then refuses any further ATTACH or DETACH on the parent -- "partition
//     already pending detach", hinting at FINALIZE -- so an unhandled one wedges every
//     future drop rather than leaking quietly.
//
// Scoped to the schema that the parent's regclass resolves to, so this agrees with the
// unqualified DETACH below about which table it means.
const partitionLeafSQL = `
SELECT c.relname,
       c.relispartition,
       COALESCE(i.inhdetachpending, false)
  FROM pg_class c
  LEFT JOIN pg_inherits i
         ON i.inhrelid = c.oid AND i.inhparent = '%[1]s'::regclass
 WHERE c.relnamespace = (SELECT relnamespace FROM pg_class WHERE oid = '%[1]s'::regclass)
   AND c.relkind  = 'r'
   AND c.relname ~ '^%[1]s_[0-9]+$'`

// journalLeafParents are the two parents whose leaves retire TOGETHER on the journal's
// cutoff, oldest first, in one pass.
//
// conflict_children is here rather than on a horizon of its own because its retention is not
// an independent choice. It records which losing transactions contest a parent's coin, and
// what conflict resolution DOES with that answer is restore the losing spends out of the
// journal. A note whose journal leaf has been dropped names a race that can no longer be
// undone, so keeping it past the journal would be keeping an answer nothing can act on.
var journalLeafParents = []string{"spend_journal", "conflict_children"}

// leafState is one partition of one of those parents, and which of the three states it is in.
type leafState struct {
	parent        string
	name          string
	leaf          uint32
	attached      bool
	detachPending bool
}

// listPartitionLeaves reads the leaves of one partitioned parent.
func (s *Store) listPartitionLeaves(ctx context.Context, parent string) ([]leafState, error) {
	rows, err := s.pool.Query(ctx, fmt.Sprintf(partitionLeafSQL, parent))
	if err != nil {
		return nil, errors.NewStorageError("[utxoset] list %s leaves", parent, err)
	}

	var out []leafState

	for rows.Next() {
		l := leafState{parent: parent}
		if err := rows.Scan(&l.name, &l.attached, &l.detachPending); err != nil {
			rows.Close()
			return nil, errors.NewStorageError("[utxoset] scan %s leaf", parent, err)
		}

		// A name carrying no leaf number is not one of ours. Parsing here rather than in the
		// drop loop keeps it in one place, so the ordering below and the cutoff test can never
		// disagree about which leaf a table is.
		if _, err := fmt.Sscanf(l.name, parent+"_%d", &l.leaf); err != nil {
			continue
		}

		out = append(out, l)
	}

	rows.Close()

	if err := rows.Err(); err != nil {
		return nil, errors.NewStorageError("[utxoset] list %s leaves", parent, err)
	}

	return out, nil
}

// DropSpendJournalPartitionsBelow drops journal leaves entirely below height, and returns how
// many it dropped.
//
// This is the whole story: DROP TABLE, O(1), no scan, no vacuum, no per-row work to fall
// behind on. It is idempotent, which is what lets the caller treat a crash as nothing worse
// than a repeat: every leaf below the cutoff is dropped on every call, in whatever state the
// last attempt left it, so a partial run is simply redone.
//
// The journal is undo insurance and nothing else. It used to be the prune engine as well --
// a retiring leaf was the work list of parents to re-examine -- and the ordering of a pruner
// session was built around that. Nothing reads a retiring leaf now, so it can be dropped in
// any order with respect to the rest of the session.
func (s *Store) DropSpendJournalPartitionsBelow(ctx context.Context, height uint32) (int, error) {
	return s.dropSpendJournalPartitionsBelow(ctx, height)
}

// dropSpendJournalPartitionsBelow drops journal leaves, and the conflict-bookkeeping windows
// that retire with them, below height.
//
// Both parents are listed and merged into ONE ordered pass rather than dropped in two loops.
// The three-state handling is per table, because a crash between detaching the journal leaf
// and detaching its conflict window leaves the two in different states, and the ordering
// argument below is about age rather than about which parent a table belongs to.
//
// The count returned is the number of tables dropped across both parents, which is what the
// pruner logs. It is not a leaf count and does not claim to be.
func (s *Store) dropSpendJournalPartitionsBelow(ctx context.Context, height uint32) (int, error) {
	var leaves []leafState

	for _, parent := range journalLeafParents {
		found, err := s.listPartitionLeaves(ctx, parent)
		if err != nil {
			return 0, err
		}

		leaves = append(leaves, found...)
	}

	// OLDEST FIRST, and the ordering is load-bearing once there is a backlog.
	//
	// The listing query has no ORDER BY, so without this the catalog hands leaves back in
	// whatever order it scanned them, which shifts as tables are created and dropped. With
	// one leaf retiring every 48 blocks and nothing behind, order is irrelevant. With
	// thousands outstanding it decides which work gets done before the session ends, and a
	// session ends when the daemon is restarted rather than when the work runs out.
	//
	// Two things follow. Old leaves are the cheap ones, measured at six to thirteen times
	// less work than leaves near the frontier, so taking them first retires more of them per
	// session. And the oldest surviving leaf only becomes a usable progress measure if the
	// oldest is what gets attacked; in catalog order it can sit untouched indefinitely while
	// newer leaves churn, which is exactly what the mainnet box showed with leaf 4,676 still
	// present while the session worked on 9,353.
	//
	// The parent name breaks the tie so the order is total rather than dependent on the sort
	// being stable, which sort.Slice is not.
	sort.Slice(leaves, func(i, j int) bool {
		if leaves[i].leaf != leaves[j].leaf {
			return leaves[i].leaf < leaves[j].leaf
		}

		return leaves[i].parent < leaves[j].parent
	})

	cutoff := height / SpendJournalPartitionBlocks
	dropped := 0

	for _, l := range leaves {
		if l.leaf >= cutoff {
			continue
		}

		switch {
		case l.detachPending:
			// FINALIZE is the only way out of this state, and until it runs no other
			// partition of this table can be detached either.
			if _, err := s.pool.Exec(ctx,
				fmt.Sprintf(`ALTER TABLE %s DETACH PARTITION %s FINALIZE`, l.parent, l.name)); err != nil {
				return dropped, errors.NewStorageError("[utxoset] finalize detach of leaf %s", l.name, err)
			}

		case l.attached:
			// DETACH CONCURRENTLY first, then drop the now-standalone table. A bare DROP
			// TABLE on an attached partition briefly takes ACCESS EXCLUSIVE on the
			// PARENT, which would stall every concurrent spend; detaching concurrently
			// does not. It cannot run inside a transaction block, which is why this is a
			// single-statement Exec on its own and not folded into one.
			if _, err := s.pool.Exec(ctx,
				fmt.Sprintf(`ALTER TABLE %s DETACH PARTITION %s CONCURRENTLY`, l.parent, l.name)); err != nil {
				return dropped, errors.NewStorageError("[utxoset] detach leaf %s", l.name, err)
			}

		default:
			// Already standalone: a previous session was interrupted between its DETACH
			// and its DROP. Nothing to detach, just finish the job.
		}

		if _, err := s.pool.Exec(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s`, l.name)); err != nil {
			return dropped, errors.NewStorageError("[utxoset] drop leaf %s", l.name, err)
		}

		dropped++
	}

	return dropped, nil
}
