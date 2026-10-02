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

// preserveParentSQL copies each named parent's block facts into the preservation table, from the
// first of four sources that has them, or extends the life of a copy already there.
//
// The sources, in order, are what makes a preserved row never name a loser. First, the parent's
// surviving containment row in a window the stamp has COMPLETED, below $7: the stamp deleted
// that window's losers, so the row left is the winner, and it carries the full payload. Second,
// a containment row at or above $7 means the parent's window is not yet completed: the parent is
// SKIPPED this cycle, not preserved, and retried next cycle, because a row there can still be a
// loser and a thin copy written first would never be upgraded (the conflict clause updates only
// the expiry). Third and fourth, only when no attached window holds a row for the parent at all:
// a live UTXO with a non-zero pair, then an undo copy with one. Those two give a thin row, the
// pair and the flags with no fee, size or inputs, which is what the read path was answering for
// this parent the moment before anyway. The locked bit is masked with $8, because a row this
// deep never supplies it.
//
// $6 is the dropped floor. Skipping a parent is safe only until its window drops, and a window
// cannot drop without a completion record, which the stamp writes only after it has run; after
// that the window lives at least 1,728 more blocks and this pass runs every cycle.
//
// ON CONFLICT takes the GREATEST of the two heights rather than the new one. The pruner names
// a parent again on every cycle its child is still waiting, each time with a further-out
// expiry, but a second child of the same parent can be younger, and writing its shorter
// expiry over the longer one would retire the parent while the older child still needed it.
//
// The keys sit on the OUTSIDE of a LATERAL with an OFFSET 0 fence, the shape minedByTxidSQL
// uses and for the identical reason: one primary-key descent per key per attached window
// rather than a hash join against every window read whole.
const preserveParentSQL = `
INSERT INTO preserved_parent (txid, mined_height, block_id, subtree_idx, created_height,
                              fee, size_in_bytes, tx_inpoints, locktime, created_at, flags,
                              preserve_until)
SELECT k.txid, s.mined_height, s.block_id, s.subtree_idx, s.created_height,
       s.fee, s.size_in_bytes, s.tx_inpoints, s.locktime, s.created_at,
       s.flags & ~$8::smallint, $5::int
  FROM unnest($1::smallint[], $2::bytea[], $3::uuid[], $4::uuid[]) AS k(leaf, txid, lo, hi)
 CROSS JOIN LATERAL (
   (SELECT 1 AS pri, m.mined_height, m.block_id, m.subtree_idx, m.created_height,
           m.fee, m.size_in_bytes, m.tx_inpoints, m.locktime, m.created_at, m.flags
      FROM tx_mined m
     WHERE m.txid = k.txid
       AND m.mined_height >= $6::int
       AND m.mined_height <  $7::int
     ORDER BY (m.tx_inpoints IS NULL), m.mined_height, m.block_id LIMIT 1)
   UNION ALL
   (SELECT 2, m.mined_height, m.block_id, m.subtree_idx, m.created_height,
           NULL, NULL, NULL, NULL, NULL, m.flags
      FROM tx_mined m
     WHERE m.txid = k.txid
       AND m.mined_height >= $7::int
     LIMIT 1)
   UNION ALL
   (SELECT 3, u.mined_height, u.block_id, 0, u.created_height,
           NULL, NULL, NULL, NULL, NULL, u.flags
      FROM utxo u
     WHERE u.leaf = k.leaf AND u.ukey >= k.lo AND u.ukey <= k.hi AND u.txid = k.txid
       AND u.mined_height > 0
     ORDER BY u.ukey LIMIT 1)
   UNION ALL
   (SELECT 4, j.mined_height, j.block_id, 0, j.created_height,
           NULL, NULL, NULL, NULL, NULL, j.flags
      FROM spend_journal j
     WHERE j.ukey >= k.lo AND j.ukey <= k.hi AND j.txid = k.txid
       AND j.mined_height > 0
     ORDER BY j.ukey LIMIT 1)
   ORDER BY pri LIMIT 1 OFFSET 0
 ) AS s
 WHERE s.pri <> 2
    ON CONFLICT (txid) DO UPDATE
   SET preserve_until = GREATEST(preserved_parent.preserve_until, EXCLUDED.preserve_until)`

// renewPreservedSQL extends the preservation of every named parent that already has one. The
// insert above copies a parent only from its block window, a live UTXO or a spend-journal row,
// and a parent fully spent in its block loses all three once its window and journal partitions
// drop. Without this the row stopped renewing then, and lapsed while its child still waited:
// on 2026-09-24 that left 283 parents unpreserved on mainnet. The row itself is the source for
// its own renewal, as the preserveUntil on the protected record is on the other stores.
const renewPreservedSQL = `
UPDATE preserved_parent p
   SET preserve_until = GREATEST(p.preserve_until, $2::int)
 WHERE p.txid = ANY($1::bytea[])
   AND p.preserve_until < $2::int`

// preserveClassifySQL sorts the parents that still have no preserved row after the insert into
// the ones that are held elsewhere or will be reached, and the ones with no source at all. It
// runs once per leaf group with the leaf as a scalar. A parent with an identity row is held by
// it; a parent with a containment row at or above the completion floor, $3, will be reached
// when its window completes. Any other is a hole in the retention arithmetic, and counted.
const preserveClassifySQL = `
SELECT k.txid,
       EXISTS (SELECT 1 FROM tx_ident i
                WHERE i.leaf = $1::smallint AND i.txid = k.txid) AS has_ident,
       EXISTS (SELECT 1 FROM tx_mined m
                WHERE m.txid = k.txid
                  AND m.mined_height >= $3::int
                LIMIT 1 OFFSET 0) AS waiting
  FROM unnest($2::bytea[]) AS k(txid)
 WHERE NOT EXISTS (SELECT 1 FROM preserved_parent p WHERE p.txid = k.txid)`

// PreserveTransactions keeps a parent answerable past the containment window that would
// otherwise have retired it, because a still-unmined child needs its facts to be validated
// against on the day it is finally mined.
//
// The old justification for doing nothing here was that this store's reclaim consults the
// spender's status rather than racing a clock, so a parent with a live child could never be
// deleted out from under it. That is still true of the UTXO, and it is not enough. Containment
// is dropped by height, whole windows at a time, and a parent whose UTXOs are all spent has no
// UTXO left to answer from either: 1440 blocks after its block, the parent is simply gone. That
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

	floors, err := s.Floors(ctx)
	if err != nil {
		return err
	}

	leaves, ids, los, his := liveUTXOArgs(txids)

	if _, err := s.pool.Exec(ctx, preserveParentSQL, leaves, ids, los, his, until,
		int32(floors.DroppedFloor), int32(floors.StampCompleteFloor), FlagLocked); err != nil { //nolint:gosec // heights fit int32
		return errors.NewStorageError("[utxoset][PreserveTransactions] preserve %d parents until %d",
			len(txids), preserveUntilHeight, err)
	}

	if _, err := s.pool.Exec(ctx, renewPreservedSQL, txids, until); err != nil {
		return errors.NewStorageError("[utxoset][PreserveTransactions] renew %d preserved parents until %d",
			len(txids), preserveUntilHeight, err)
	}

	// The classification of what was not preserved is off the block path and exists for the
	// two counters; a failure there is logged and does not fail the pass.
	for _, g := range leafGroups(txids) {
		rows, err := s.pool.Query(ctx, preserveClassifySQL, g.leaf, g.txids, int32(floors.StampCompleteFloor)) //nolint:gosec // a height fits int32
		if err != nil {
			s.logger.Warnf("[utxoset][PreserveTransactions] classify unpreserved parents: %v", err)

			return nil
		}

		var waiting, noSource int

		for rows.Next() {
			var (
				txid          []byte
				hasIdent, due bool
			)

			if err := rows.Scan(&txid, &hasIdent, &due); err != nil {
				rows.Close()
				s.logger.Warnf("[utxoset][PreserveTransactions] classify scan: %v", err)

				return nil
			}

			if hasIdent || due {
				waiting++
			} else {
				noSource++
			}
		}

		rows.Close()

		if waiting > 0 {
			preserveWaiting.Add(float64(waiting))
		}

		if noSource > 0 {
			preserveNoSource.Add(float64(noSource))
			s.logger.Errorf("[utxoset][PreserveTransactions] %d parents named for preservation have no containment row, no identity row, no live UTXO and no undo copy", noSource)
		}
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
