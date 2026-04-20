// Package sql implements the blockchain.Store interface using SQL database backends.
//
// This file implements DeleteBlock, which physically removes a single block row
// from the blocks table. It is used by repair tooling such as cmd/rewindblockchain
// to rewind the chain when the UTXO store has drifted out of sync with on-disk
// subtree files.
package sql

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util/tracing"
)

// DeleteBlock physically removes a single block row from the blocks table.
// It is idempotent: if the row does not exist, no error is returned.
//
// The blocks table has a self-referencing foreign key (parent_id REFERENCES
// blocks(id)) with no ON DELETE CASCADE, so callers must order deletions so
// that no surviving row still points at a deleted block. Rewind tooling
// achieves this by iterating in strict descending height order.
//
// This method does not rebuild on_main_chain, reset caches, or update the
// off-chain block ID set. Callers performing bulk rewinds should invoke the
// same cache/flag-rebuild sequence used by InvalidateBlock exactly once after
// all deletions are complete.
func (s *SQL) DeleteBlock(ctx context.Context, blockHash *chainhash.Hash) error {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "sql:DeleteBlock")
	defer deferFn()

	if blockHash == nil {
		return errors.NewInvalidArgumentError("block hash cannot be nil")
	}

	s.logger.Debugf("DeleteBlock %s", blockHash.String())

	res, err := s.db.ExecContext(ctx, `DELETE FROM blocks WHERE hash = $1`, blockHash.CloneBytes())
	if err != nil {
		return errors.NewStorageError("error deleting block", err)
	}

	if n, affErr := res.RowsAffected(); affErr == nil && n == 0 {
		s.logger.Warnf("DeleteBlock: block %s did not exist", blockHash.String())
	}

	return nil
}
