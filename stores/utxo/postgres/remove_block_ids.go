package postgres

import (
	"context"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
)

// RemoveBlockIDs trims the supplied block IDs from each transaction's packed
// mined_info bytea, dropping the whole 12-byte record (block_id, height,
// subtree_idx) for every matching block ID so the three fields can never
// misalign. Idempotent: a block ID that is absent matches nothing.
func (s *Store) RemoveBlockIDs(ctx context.Context, removals []utxo.BlockIDsRemoval) error {
	if len(removals) == 0 {
		return nil
	}

	txn, err := s.pool.Begin(ctx)
	if err != nil {
		return errors.NewStorageError("failed to begin tx", err)
	}

	defer func() {
		_ = txn.Rollback(ctx)
	}()

	// Trim the supplied block IDs from the packed mined_info bytea in a SINGLE
	// atomic UPDATE, re-aggregating every stride-aligned 12-byte record whose
	// block_id (the first 4 bytes, int4send-encoded) is NOT in the removal set.
	// Because the whole record drops together, the block_id/height/subtree_idx
	// triple can never misalign — the class of bug the parallel-array form once had
	// (array_remove touching only block_ids). Idempotent: a block ID that is absent
	// matches nothing. The removal set is compared as 4-byte big-endian slices so
	// the match is exact and stride-aligned (offsets 0,12,24,...), never a bare
	// substring that could straddle two records.
	const q = `
		UPDATE txs t SET
			mined_info = COALESCE((
				SELECT string_agg(substr(t.mined_info, g.i * 12 + 1, 12), ''::bytea ORDER BY g.i)
				FROM generate_series(0, octet_length(t.mined_info) / 12 - 1) AS g(i)
				WHERE substr(t.mined_info, g.i * 12 + 1, 4) <> ALL(ARRAY(SELECT int4send(x) FROM unnest($1::int[]) AS x))
			), ''::bytea)
		WHERE t.hash = $2`

	for _, r := range removals {
		if r.TxHash == nil {
			return errors.NewInvalidArgumentError("txHash must be non-nil")
		}

		if len(r.BlockIDs) == 0 {
			continue
		}

		ids := make([]int32, len(r.BlockIDs))
		for i, id := range r.BlockIDs {
			ids[i] = int32(id)
		}

		if _, err = txn.Exec(ctx, q, ids, r.TxHash[:]); err != nil {
			return errors.NewStorageError("failed to remove block_ids", err)
		}
	}

	if err = txn.Commit(ctx); err != nil {
		return errors.NewStorageError("failed to commit remove block_ids tx", err)
	}

	return nil
}
