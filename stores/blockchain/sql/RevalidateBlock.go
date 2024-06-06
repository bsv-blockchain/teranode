package sql

import (
	"context"
	"fmt"

	"github.com/libsv/go-bt/v2/chainhash"
)

func (s *SQL) RevalidateBlock(ctx context.Context, blockHash *chainhash.Hash) error {
	s.logger.Infof("InvalidateBlock %s", blockHash.String())

	s.blocksCache.RebuildBlockchain(nil, nil) // reset cache so that GetBlockExists goes to the DB

	exists, err := s.GetBlockExists(ctx, blockHash)
	if err != nil {
		return fmt.Errorf("error checking block exists: %v", err)
	}
	if !exists {
		return fmt.Errorf("block %s does not exist", blockHash.String())
	}

	// recursively update all children blocks to invalid in 1 query
	q := `
		UPDATE blocks
		SET invalid = false
		WHERE hash = $1
	`
	if _, err = s.db.ExecContext(ctx, q, blockHash.CloneBytes()); err != nil {
		return fmt.Errorf("error updating block to invalid: %v", err)
	}

	if err := s.ResetBlocksCache(ctx); err != nil {
		return fmt.Errorf("error clearing caches: %v", err)
	}
	s.ResetResponseCache()

	return nil
}
