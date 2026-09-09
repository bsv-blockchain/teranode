package utxoset

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
)

// emptyUnminedIterator is an iterator over nothing.
//
// It is a real value rather than a nil interface on purpose: the block assembly caller
// checks only the error and then uses the iterator (BlockAssembler.go:2629-2636), so
// handing back a nil would move a startup error into a nil dereference, which is a worse
// failure in a worse place.
type emptyUnminedIterator struct{}

func (emptyUnminedIterator) Next(_ context.Context) ([]*utxo.UnminedTransaction, error) {
	return nil, nil
}
func (emptyUnminedIterator) Err() error   { return nil }
func (emptyUnminedIterator) Close() error { return nil }

// QueryOldUnminedTransactions finds none, same reason.
func (s *Store) QueryOldUnminedTransactions(_ context.Context, _ uint32) ([]chainhash.Hash, error) {
	return nil, nil
}

// preserveParentSQL copies each named transaction's EARLIEST membership row into the
// preservation table, or extends the life of a copy already there.
//
// The earliest row by seq is the transaction's longest-chain stamp rather than a fork one, and
// that is the same rule firstMinedRowSQL relies on when it stamps a retiring window's coins:
// since task 9 a transaction only reaches the membership table by a longest-chain stamp or a
// block-path create, and a fork stamp can only ever append to a row that already exists. So
// the first row is the block this parent really was mined into, and preserving any other one
// would keep a loser alive as the answer.
//
// A hash with NO membership row copies nothing, and that is right in both of the ways it can
// happen. A parent still in the mempool is held by its identity row, which stays for as long
// as the transaction is unmined, so there is nothing to preserve and nothing to lose. A parent
// whose window has already gone cannot be recovered from here -- the row this statement copies
// is the only place those facts lived -- and inventing a row from a coin would put facts in a
// table that promises to hold what membership held.
//
// ON CONFLICT takes the GREATEST of the two heights rather than the new one. The pruner names
// a parent again on every cycle its child is still waiting, each time with a further-out
// expiry, but a second child of the same parent can be younger, and writing its shorter
// expiry over the longer one would retire the parent while the older child still needed it.
//
// The keys sit on the OUTSIDE of a LATERAL with an OFFSET 0 fence, the shape minedByTxidSQL
// and firstMinedRowSQL use and for the identical reason: one primary-key descent per key per
// live window rather than a hash join against every window read whole.
const preserveParentSQL = `
INSERT INTO preserved_parent (txid, mined_height, block_id, subtree_idx, created_height,
                              fee, size_in_bytes, tx_inpoints, locktime, created_at, flags,
                              preserve_until)
SELECT k.txid, m.mined_height, m.block_id, m.subtree_idx, m.created_height,
       m.fee, m.size_in_bytes, m.tx_inpoints, m.locktime, m.created_at, m.flags, $2::int
  FROM unnest($1::bytea[]) AS k(txid)
 CROSS JOIN LATERAL (
   SELECT m.mined_height, m.block_id, m.subtree_idx, m.created_height, m.size_in_bytes,
          m.fee, m.tx_inpoints, m.locktime, m.created_at, m.flags
     FROM tx_mined m
    WHERE m.txid = k.txid
    ORDER BY m.seq
    LIMIT 1 OFFSET 0
 ) AS m
    ON CONFLICT (txid) DO UPDATE
   SET preserve_until = GREATEST(preserved_parent.preserve_until, EXCLUDED.preserve_until)`

// PreserveTransactions keeps a parent answerable past the membership window that would
// otherwise have retired it, because a still-unmined child needs its facts to be validated
// against on the day it is finally mined.
//
// The old justification for doing nothing here was that this store's reclaim consults the
// spender's status rather than racing a clock, so a parent with a live child could never be
// deleted out from under it. That is still true of the COIN, and it is not enough. Membership
// is dropped by height, whole windows at a time, and a parent whose coins are all spent has no
// coin left to answer from either: 1440 blocks after its block, the parent is simply gone. That
// is the right answer for every parent except the one whose child never got mined, and the
// pruner names exactly those (PreserveParentsOfOldUnminedTransactions). This is where the
// answer for them survives.
//
// It is one statement for the whole batch, because the pruner hands over every parent of every
// old unmined transaction at once -- thousands of hashes on a node whose mempool has stalled.
//
// The hashes are deduplicated first. ON CONFLICT DO UPDATE cannot touch the same row twice in
// one statement, so a repeated hash is a hard error from postgres rather than a wasted probe.
// The pruner deduplicates through a map today; this does not depend on it.
func (s *Store) PreserveTransactions(ctx context.Context, txIDs []chainhash.Hash,
	preserveUntilHeight uint32) error {
	if len(txIDs) == 0 {
		return nil
	}

	seen := make(map[chainhash.Hash]struct{}, len(txIDs))
	txids := make([][]byte, 0, len(txIDs))

	for i := range txIDs {
		if _, dup := seen[txIDs[i]]; dup {
			continue
		}

		seen[txIDs[i]] = struct{}{}

		txids = append(txids, txIDs[i][:])
	}

	// A height fits an int32 for the life of the chain, the same cast every height column on
	// this store is written through.
	until := int32(preserveUntilHeight) //nolint:gosec // a height fits an int32

	if _, err := s.pool.Exec(ctx, preserveParentSQL, txids, until); err != nil {
		return errors.NewStorageError("[utxoset][PreserveTransactions] preserve %d parents until %d",
			len(txids), preserveUntilHeight, err)
	}

	return nil
}

// ProcessExpiredPreservations drops the preservations that have run out.
//
// A preservation is a promise with a deadline, and this is the only thing that ends it. Left
// alone the table would grow without bound and, worse, would keep answering for parents nothing
// needs any more -- the exact unbounded retention the aerospike store's Phase 1b exists to
// avoid, expressed here as rows rather than as bins.
//
// STRICTLY less than the current height, so a preservation is honoured through the whole of the
// height it names. The pruner passes the tip's height on every cycle, so the row leaves on the
// first block past its deadline.
//
// There is nothing to re-stamp, unlike the aerospike store, whose Phase 1b has to hand the
// parent back to the delete-at-height pruner. Here the row IS the preservation: once it is
// gone the parent is reclaimed by the same dropped window every other transaction is, with no
// second mechanism to hand it to.
//
// This runs on a background timer, so an error is not fatal but is logged on every cycle.
func (s *Store) ProcessExpiredPreservations(ctx context.Context, currentHeight uint32) error {
	height := int32(currentHeight) //nolint:gosec // a height fits an int32

	tag, err := s.pool.Exec(ctx,
		`DELETE FROM preserved_parent WHERE preserve_until < $1::int`, height)
	if err != nil {
		return errors.NewStorageError("[utxoset][ProcessExpiredPreservations] expire below %d", currentHeight, err)
	}

	if n := tag.RowsAffected(); n > 0 {
		s.logger.Infof("[utxoset] expired %d preserved parents at height %d", n, currentHeight)
	}

	return nil
}
