package sql

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util"
)

// SetBlockPersistedAt updates the persisted_at timestamp for a block.
func (s *SQL) SetBlockPersistedAt(ctx context.Context, blockHash *chainhash.Hash) error {
	s.logger.Debugf("SetBlockPersistedAt %s", blockHash.String())

	// Invalidate response cache to ensure cached blocks reflect updated persisted_at timestamp
	defer s.ResetResponseCache()

	var q string

	if s.engine == util.Postgres {
		q = `
			UPDATE blocks
			SET persisted_at = CURRENT_TIMESTAMP
			WHERE hash = $1
		`
	} else {
		q = `
			UPDATE blocks
			SET persisted_at = datetime('now')
			WHERE hash = $1
		`
	}

	res, err := s.db.ExecContext(ctx, q, blockHash.CloneBytes())
	if err != nil {
		return errors.NewStorageError("error updating block persisted_at timestamp", err)
	}

	// check if the block was updated
	if rows, _ := res.RowsAffected(); rows <= 0 {
		return errors.NewStorageError("block %s persisted_at timestamp was not updated", blockHash.String())
	}

	return nil
}
