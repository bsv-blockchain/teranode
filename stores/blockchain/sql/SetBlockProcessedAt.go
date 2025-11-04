package sql

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util"
)

// SetBlockProcessedAt updates the processed_at timestamp for a block.
func (s *SQL) SetBlockProcessedAt(ctx context.Context, blockHash *chainhash.Hash, clear ...bool) error {
	s.logger.Debugf("SetBlockProcessedAt %s", blockHash.String())

	// Invalidate response cache to ensure cached blocks reflect updated processed_at timestamp
	defer s.ResetResponseCache()

	var q string

	if len(clear) > 0 && clear[0] {
		q = `
			UPDATE blocks
			SET processed_at = NULL
			WHERE hash = $1
		`
	} else {
		if s.engine == util.Postgres {
			q = `
				UPDATE blocks
				SET processed_at = CURRENT_TIMESTAMP
				WHERE hash = $1
		`
		} else {
			q = `
				UPDATE blocks
				SET processed_at = datetime('now')
				WHERE hash = $1
			`
		}
	}

	res, err := s.db.ExecContext(ctx, q, blockHash.CloneBytes())
	if err != nil {
		return errors.NewStorageError("error updating block processed_at timestamp", err)
	}

	// check if the block was updated
	if rows, _ := res.RowsAffected(); rows <= 0 {
		return errors.NewStorageError("block %s processed_at timestamp was not updated", blockHash.String())
	}

	return nil
}
