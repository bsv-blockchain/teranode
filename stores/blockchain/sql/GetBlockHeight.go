package sql

import (
	"context"
	"database/sql"
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/libsv/go-bt/v2/chainhash"
)

func (s *SQL) GetBlockHeight(ctx context.Context, blockHash *chainhash.Hash) (uint32, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "sql:GetBlockHeight")
	defer deferFn()

	_, meta, err := s.blocksCache.GetBlockHeader(*blockHash)
	if err != nil {
		return 0, errors.NewStorageError("error in GetBlockHeight", err)
	}
	if meta != nil {
		return meta.Height, nil
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

	if err = s.db.QueryRowContext(ctx, q, blockHash[:]).Scan(
		&height,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, errors.NewStorageError("error in GetBlockHeight", err)
		}
		return 0, err
	}

	return height, nil
}
