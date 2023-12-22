package sql

import (
	"context"
	"fmt"

	"github.com/libsv/go-bt/v2/chainhash"
)

func (s *SQL) InvalidateBlock(ctx context.Context, blockHash *chainhash.Hash) error {
	s.logger.Infof("InvalidateBlock %s", blockHash.String())

	exists, err := s.GetBlockExists(ctx, blockHash)
	if err != nil {
		return fmt.Errorf("error checking block exists: %v", err)
	}
	if !exists {
		return fmt.Errorf("block %s does not exist", blockHash.String())
	}

	// update the block to invalid in sql
	q := `
		UPDATE blocks
		SET invalid = true
		WHERE hash = $1
	`
	if _, err = s.db.ExecContext(ctx, q, blockHash.String()); err != nil {
		return fmt.Errorf("error updating block to invalid: %v", err)
	}

	return nil
}
