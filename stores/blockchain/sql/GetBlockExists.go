package sql

import (
	"context"
	"database/sql"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/libsv/go-bt/v2/chainhash"
)

func (s *SQL) GetBlockExists(ctx context.Context, blockHash *chainhash.Hash) (bool, error) {
	start, stat, ctx := tracing.StartStatFromContext(ctx, "GetBlockExists")
	defer func() {
		stat.AddTime(start)
	}()

	exists, ok := s.blocksCache.GetExists(*blockHash) // resets whenever a new block is added
	if ok {
		return exists, nil
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	q := `
		SELECT
	     b.height
		FROM blocks b
		WHERE b.hash = $1
	`

	var height uint32
	if err := s.db.QueryRowContext(ctx, q, blockHash[:]).Scan(
		&height,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			s.blocksCache.SetExists(*blockHash, false) // resets whenever a new block is added
			return false, nil
		}
		return false, err
	}

	s.blocksCache.SetExists(*blockHash, true) // resets whenever a new block is added
	return true, nil
}
