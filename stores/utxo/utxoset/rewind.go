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

// removeMinedBlockIDsSQL deletes the containment rows naming blocks the caller has rewound.
//
// Containment is one row per (transaction, block), so removing a block is a DELETE, which is
// what makes this idempotent for free: a second run matches nothing. It is the one statement
// RemoveBlockIDs runs. The identity table used to carry a packed block list that a second
// statement stripped in the same transaction; that column is gone, containment has one home,
// and the identity row of a transaction seen before its block is untouched by a rewind.
//
// tx_mined's primary key leads with txid, so this is one descent per pair per live window. It
// deliberately carries no height bound: the tool runs offline with the node stopped and names
// blocks the caller has stopped believing in at any depth.
//
// The unmined marker is deliberately untouched, matching both reference stores. This call does
// not claim to know whether the chain still contains the transaction, only which blocks the
// caller has stopped believing in.
//
// If this leaves a transaction with no containment row and no identity row, the transaction is
// then answered by its own UTXO -- one block id, from the UTXO's pair -- or by the journal
// step, or not at all. That is the tool's caller's decision to make: the rewind is being told
// which blocks to stop believing in, and it is not this store's place to decide what the
// transaction becomes afterwards. The pair the UTXOs, undo copies and preserved rows carry is
// left as it stands, which the design records as the boundary the rewind tool's preflight has
// to respect.
const removeMinedBlockIDsSQL = `
DELETE FROM tx_mined m
 USING unnest($1::bytea[], $2::int[]) AS k(txid, block_id)
 WHERE m.txid = k.txid
   AND m.block_id = k.block_id`

// RemoveFromConflictingChildren takes transactions off their parents' contested-UTXO lists.
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
func (s *Store) RemoveBlockIDs(ctx context.Context, removals []utxo.BlockIDsRemoval) error {
	if len(removals) == 0 {
		return nil
	}

	// Flattened to one element per transaction and block pair, because a SQL array is
	// rectangular and each removal carries a list of its own length.
	txids := make([][]byte, 0, len(removals))
	blockIDs := make([]int32, 0, len(removals))

	for _, r := range removals {
		if r.TxHash == nil {
			continue
		}

		for _, id := range r.BlockIDs {
			txids = append(txids, r.TxHash[:])
			blockIDs = append(blockIDs, int32(id)) //nolint:gosec // a block id fits int32
		}
	}

	if len(txids) == 0 {
		return nil
	}

	if _, err := s.pool.Exec(ctx, removeMinedBlockIDsSQL, txids, blockIDs); err != nil {
		return errors.NewStorageError("[utxoset][RemoveBlockIDs] containment", err)
	}

	return nil
}
