package sql

import (
	"context"
	"database/sql"
	"errors"

	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

func (s *SQL) GetBlockExists(ctx context.Context, blockHash *chainhash.Hash) (bool, error) {
	start := gocore.CurrentNanos()
	defer func() {
		stats.NewStat("GetBlock").AddTime(start)
	}()

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
			return false, nil
		}
		return false, err
	}

	return true, nil
}
