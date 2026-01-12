package sql

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
)

// ClearBlockMinedSet resets mined_set to false for the specified block.
// This triggers the background job to re-process transaction states.
// Used during fork handling when blocks move from main chain to side chain.
func (s *SQL) ClearBlockMinedSet(ctx context.Context, blockHash *chainhash.Hash) error {
	s.logger.Debugf("ClearBlockMinedSet %s", blockHash.String())

	// Invalidate response cache to ensure cached blocks reflect updated mined_set field
	defer s.ResetResponseCache()

	q := `
		UPDATE blocks
		SET mined_set = false
		WHERE hash = $1
	`

	res, err := s.db.ExecContext(ctx, q, blockHash.CloneBytes())
	if err != nil {
		return errors.NewStorageError("error clearing block mined_set", err)
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return errors.NewStorageError("error checking rows affected", err)
	}

	if rowsAffected == 0 {
		s.logger.Warnf("ClearBlockMinedSet: block %s not found", blockHash.String())
	}

	return nil
}
