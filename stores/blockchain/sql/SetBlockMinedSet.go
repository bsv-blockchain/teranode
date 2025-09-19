package sql

import (
	"context"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

func (s *SQL) SetBlockMinedSet(ctx context.Context, blockHash *chainhash.Hash) error {
	s.logger.Debugf("SetBlockMinedSet %s", blockHash.String())

	q := `
		UPDATE blocks
		SET mined_set = true
		WHERE hash = $1
	`

	res, err := s.db.ExecContext(ctx, q, blockHash.CloneBytes())
	if err != nil {
		return errors.NewStorageError("error updating block mined_set", err)
	}

	// check if the block was updated
	if rows, _ := res.RowsAffected(); rows <= 0 {
		return errors.NewStorageError("block %s mined_set was not updated", blockHash.String())
	}

	// Invalidate response cache to ensure cached blocks reflect updated processed_at timestamp
	s.ResetResponseCache()

	if err = s.ResetBlocksCache(ctx); err != nil {
		return errors.NewStorageError("error clearing caches", err)
	}

	return nil
}
