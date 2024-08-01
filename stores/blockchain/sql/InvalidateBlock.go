package sql

import (
	"context"
	"database/sql"
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/libsv/go-bt/v2/chainhash"
)

func (s *SQL) InvalidateBlock(ctx context.Context, blockHash *chainhash.Hash) error {
	s.logger.Infof("InvalidateBlock %s", blockHash.String())

	s.blocksCache.RebuildBlockchain(nil, nil) // reset cache so that GetBlockExists goes to the DB

	exists, err := s.GetBlockExists(ctx, blockHash)
	if err != nil {
		return errors.NewStorageError("error checking block exists", err)
	}
	if !exists {
		return errors.NewStorageError("block %s does not exist", blockHash.String(), errors.ErrNotFound)
	}

	// recursively update all children blocks to invalid in 1 query
	q := `
		WITH RECURSIVE children AS (
			SELECT id, hash, previous_hash
			FROM blocks
			WHERE hash = $1
			UNION
			SELECT b.id, b.hash, b.previous_hash
			FROM blocks b	
			INNER JOIN children c ON c.hash = b.previous_hash	
		)
		UPDATE blocks
		SET invalid = true
		WHERE id IN (SELECT id FROM children)
	`
	var res sql.Result
	if res, err = s.db.ExecContext(ctx, q, blockHash.CloneBytes()); err != nil {
		return errors.NewStorageError("error updating block to invalid", err)
	}

	// check if the block was updated
	if rows, _ := res.RowsAffected(); rows <= 0 {
		return errors.NewStorageError("block %s was not updated to invalid", blockHash.String())
	}

	if err = s.ResetBlocksCache(ctx); err != nil {
		return errors.NewStorageError("error clearing caches", err)
	}
	s.ResetResponseCache()

	return nil
}
