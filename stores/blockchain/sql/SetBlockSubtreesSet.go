package sql

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
)

func (s *SQL) SetBlockSubtreesSet(ctx context.Context, blockHash *chainhash.Hash) error {
	s.logger.Infof("SetBlockSubtreesSet %s", blockHash.String())

	// Invalidate response cache to ensure cached blocks reflect updated subtrees_set field
	defer s.ResetResponseCache()

	q := `
		UPDATE blocks
		SET subtrees_set = true
		WHERE hash = $1
	`

	res, err := s.db.ExecContext(ctx, q, blockHash.CloneBytes())
	if err != nil {
		return errors.NewStorageError("error updating block subtrees_set", err)
	}

	// check if the block was updated
	if rows, _ := res.RowsAffected(); rows <= 0 {
		return errors.NewStorageError("block %s subtrees_set was not updated", blockHash.String())
	}

	return nil
}
