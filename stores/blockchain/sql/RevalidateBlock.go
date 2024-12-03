package sql

import (
	"context"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/libsv/go-bt/v2/chainhash"
)

func (s *SQL) RevalidateBlock(ctx context.Context, blockHash *chainhash.Hash) error {
	s.logger.Infof("InvalidateBlock %s", blockHash.String())

	s.blocksCache.RebuildBlockchain(nil, nil) // reset cache so that GetBlockExists goes to the DB

	exists, err := s.GetBlockExists(ctx, blockHash)
	if err != nil {
		return errors.NewStorageError("error checking block exists", err)
	}

	if !exists {
		return errors.NewStorageError("block %s does not exist", blockHash.String())
	}

	// recursively update all children blocks to invalid in 1 query
	q := `
		UPDATE blocks
		SET invalid = false
		WHERE hash = $1
	`
	if _, err = s.db.ExecContext(ctx, q, blockHash.CloneBytes()); err != nil {
		return errors.NewStorageError("error updating block to invalid", err)
	}

	if err := s.ResetBlocksCache(ctx); err != nil {
		return errors.NewStorageError("error clearing caches", err)
	}

	s.ResetResponseCache()

	return nil
}
