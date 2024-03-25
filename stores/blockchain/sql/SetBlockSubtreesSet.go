package sql

import (
	"context"
	"fmt"

	"github.com/libsv/go-bt/v2/chainhash"
)

func (s *SQL) SetBlockSubtreesSet(ctx context.Context, blockHash *chainhash.Hash) error {
	s.logger.Infof("SetBlockSubtreesSet %s", blockHash.String())

	q := `
		UPDATE blocks
		SET subtrees_set = true
		WHERE hash = $1
	`
	res, err := s.db.ExecContext(ctx, q, blockHash.CloneBytes())
	if err != nil {
		return fmt.Errorf("error updating block subtrees_set: %v", err)
	}

	// check if the block was updated
	if rows, _ := res.RowsAffected(); rows <= 0 {
		return fmt.Errorf("block %s subtrees_set was not updated", blockHash.String())
	}

	return nil
}
