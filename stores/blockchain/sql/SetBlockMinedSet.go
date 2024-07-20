package sql

import (
	"context"
	"fmt"

	"github.com/libsv/go-bt/v2/chainhash"
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
		return fmt.Errorf("error updating block mined_set: %v", err)
	}

	// check if the block was updated
	if rows, _ := res.RowsAffected(); rows <= 0 {
		return fmt.Errorf("block %s mined_set was not updated", blockHash.String())
	}

	return nil
}
