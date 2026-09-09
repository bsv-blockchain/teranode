package utxoset

import (
	"context"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
)

// removeConflictingChildrenSQL deletes the recorded contest between named parents and named
// children.
//
// It is a DELETE of rows rather than a rebuild of a packed list, and that is the whole
// difference the side table makes here. The list used to be a concatenation of 32-byte ids in
// one column, so a removal had to split it, drop the named entries and reassemble the rest in
// order -- with the membership test aligned to a 32-byte boundary, because a plain substring
// search matches a window straddling two neighbouring entries and as a remover deletes bytes
// that were never an entry, corrupting both neighbours and leaving a length the reader
// rejects. One row per (parent, child) makes that entire class of defect impossible.
//
// The pairs arrive as two parallel arrays, one element per removal, and every window is
// searched: a pair can have been noted at any height still inside the journal's retention, and
// the caller does not know which. Each window carries an index on (parent_txid).
//
// Idempotence is free, and a no-op costs nothing. A parent the store never held, a pair that
// was never noted, and a second run after the first succeeded all match no row, so they write
// no row, no journal and no vacuum debt.
const removeConflictingChildrenSQL = `
DELETE FROM conflict_children c
 USING unnest($1::bytea[], $2::bytea[]) AS k(parent, child)
 WHERE c.parent_txid = k.parent
   AND c.child_txid  = k.child`

// removeBlockIDsSQL strips named blocks from the transactions that claim them.
//
// The arrays are FLATTENED to one element per transaction and block pair, the same shape the
// create statement uses for its coin arrays, because a SQL array is rectangular and each
// removal carries a ragged list. The grouping folds them back into one list per transaction
// before the update runs, for the same one-update-per-row reason as above.
//
// The unpacking lives in mh_strip rather than being inlined, for the same reason mh_max exists:
// the shift that would silently strip nothing is a trap worth stating once, beside the other
// reader of the same packed form. It also appears twice here, so inlining would mean two copies
// of that trap.
//
// The two extra conditions are what make a replayed rewind free. The first skips a row with
// nothing to strip, covering both an absent membership and the empty residue left behind when a
// transaction loses its last block. The second skips a row that claims none of the blocks being
// removed. Together they mean a second run writes no row, no journal and no vacuum debt.
//
// The mempool marker is deliberately untouched, matching both reference stores. This call does
// not claim to know whether the chain still contains the transaction, only which blocks the
// caller has stopped believing in.
//
// This statement reaches the IDENTITY table only, and under the reshape that is half the job:
// a settled transaction has no identity row at all, its membership is one row per block in
// tx_mined. removeMinedBlockIDsSQL is the other half, and the two run together. Splitting them
// rather than folding them into one statement is the honest shape, because the two tables hold
// membership in different forms -- a packed list in one column against a row per block -- and a
// combined statement would need both anyway.
const removeBlockIDsSQL = `
WITH k AS (
    SELECT leaf, txid, array_agg(DISTINCT block_id) AS ids
      FROM unnest($1::smallint[], $2::bytea[], $3::bigint[]) AS t(leaf, txid, block_id)
     GROUP BY leaf, txid
)
UPDATE tx_ident i
   SET membership = mh_strip(i.membership, k.ids)
  FROM k
 WHERE i.leaf = k.leaf
   AND i.txid = k.txid
   AND octet_length(i.membership) > 0
   AND mh_strip(i.membership, k.ids) IS DISTINCT FROM i.membership`

// removeMinedBlockIDsSQL deletes the membership rows naming blocks the caller has rewound.
//
// A settled transaction's membership is not a packed list to strip but a row per block, so
// removing a block is a DELETE rather than an UPDATE, which is what makes this arm idempotent
// for free: a second run matches nothing.
//
// tx_mined's primary key leads with txid, so this is one descent per pair. The pairs are the
// same flattened arrays removeBlockIDsSQL takes, so a caller cannot pass one shape to one
// statement and another shape to the other.
//
// If this leaves a transaction with no membership row and no identity row, the transaction is
// then answered by its own coin -- one block id, from the coin's stamp -- or by the journal
// step, or not at all. That is the tool's caller's decision to make: the rewind is being told
// which blocks to stop believing in, and it is not this store's place to decide what the
// transaction becomes afterwards. It is the same silence the identity arm already keeps about
// a transaction that loses its last block.
const removeMinedBlockIDsSQL = `
DELETE FROM tx_mined m
 USING unnest($1::bytea[], $2::int[]) AS k(txid, block_id)
 WHERE m.txid = k.txid
   AND m.block_id = k.block_id`

// RemoveFromConflictingChildren takes transactions off their parents' contested-coin lists.
//
// Called only by the offline rewind tool, which runs with the node stopped. A pair naming a
// parent the store does not hold, or a child that was never noted, is a silent no-op rather
// than an error, because a rewind re-run after a crash must not fail on the work it already
// did.
func (s *Store) RemoveFromConflictingChildren(ctx context.Context, removals []utxo.ConflictingChildRemoval) error {
	if len(removals) == 0 {
		return nil
	}

	parents := make([][]byte, 0, len(removals))
	children := make([][]byte, 0, len(removals))

	for _, r := range removals {
		if r.ParentHash == nil || r.ChildHash == nil {
			continue
		}

		parents = append(parents, r.ParentHash[:])
		children = append(children, r.ChildHash[:])
	}

	if len(parents) == 0 {
		return nil
	}

	if _, err := s.pool.Exec(ctx, removeConflictingChildrenSQL, parents, children); err != nil {
		return errors.NewStorageError("[utxoset][RemoveFromConflictingChildren]", err)
	}

	return nil
}

// RemoveBlockIDs makes transactions stop claiming blocks the caller has rewound.
//
// Called only by the offline rewind tool. A transaction the store does not hold, or a block it
// never claimed, is a silent no-op, for the same crash-replay reason.
//
// BOTH homes are stripped. A transaction lives in exactly one of tx_ident and tx_mined, and the
// caller does not know which, so a rewind that reached only the identity table found mempool
// and fork-limbo rows and silently missed every settled transaction -- a partial rewind with no
// signal, which is the worst outcome available to a tool for recovering from a bad chain state.
// The two statements run in ONE transaction so a crash cannot leave a transaction stripped in
// one table and not the other.
func (s *Store) RemoveBlockIDs(ctx context.Context, removals []utxo.BlockIDsRemoval) error {
	if len(removals) == 0 {
		return nil
	}

	// Flattened to one element per transaction and block pair, because a SQL array is
	// rectangular and each removal carries a list of its own length.
	leaves := make([]int16, 0, len(removals))
	txids := make([][]byte, 0, len(removals))
	blockIDs := make([]int64, 0, len(removals))
	minedIDs := make([]int32, 0, len(removals))

	for _, r := range removals {
		if r.TxHash == nil {
			continue
		}

		for _, id := range r.BlockIDs {
			leaves = append(leaves, LeafFor(r.TxHash[:]))
			txids = append(txids, r.TxHash[:])
			blockIDs = append(blockIDs, int64(id))
			minedIDs = append(minedIDs, int32(id)) //nolint:gosec // a block id fits int32
		}
	}

	if len(txids) == 0 {
		return nil
	}

	dbTx, err := s.pool.Begin(ctx)
	if err != nil {
		return errors.NewStorageError("[utxoset][RemoveBlockIDs] begin", err)
	}

	if _, err := dbTx.Exec(ctx, removeBlockIDsSQL, leaves, txids, blockIDs); err != nil {
		_ = dbTx.Rollback(ctx)

		return errors.NewStorageError("[utxoset][RemoveBlockIDs]", err)
	}

	if _, err := dbTx.Exec(ctx, removeMinedBlockIDsSQL, txids, minedIDs); err != nil {
		_ = dbTx.Rollback(ctx)

		return errors.NewStorageError("[utxoset][RemoveBlockIDs] membership", err)
	}

	if err := dbTx.Commit(ctx); err != nil {
		return errors.NewStorageError("[utxoset][RemoveBlockIDs] commit", err)
	}

	return nil
}
