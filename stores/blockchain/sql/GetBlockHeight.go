package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
)

func (s *SQL) GetBlockHeight(ctx context.Context, blockHash *chainhash.Hash) (uint32, error) {
	start, stat, ctx := util.StartStatFromContext(ctx, "GetBlockHeight")
	defer func() {
		stat.AddTime(start)
	}()

	_, meta, err := s.blocksCache.GetBlockHeader(*blockHash)
	if err != nil {
		return 0, fmt.Errorf("error in GetBlockHeight: %w", err)
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

	if err := s.db.QueryRowContext(ctx, q, blockHash[:]).Scan(
		&height,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, fmt.Errorf("error in GetBlockHeight: %w", err)
		}
		return 0, err
	}

	return height, nil
}
