package sql

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
)

func (s *SQL) SetBlockMinedSet(ctx context.Context, blockHash *chainhash.Hash) error {
	s.logger.Debugf("SetBlockMinedSet %s", blockHash.String())

	// Invalidate response cache to ensure cached blocks reflect updated mined_set field
	defer s.ResetResponseCache()

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

	return nil
}
