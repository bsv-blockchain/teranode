package sql

import (
	"context"
	"database/sql"
	"errors"

	"github.com/TAAL-GmbH/arc/blocktx/store"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
	"github.com/ordishs/gocore"
)

func (s *SQL) GetBlockHeight(ctx context.Context, blockHash *chainhash.Hash) (uint64, error) {
	start := gocore.CurrentNanos()
	defer func() {
		gocore.NewStat("blockchain").NewStat("GetBlock").AddTime(start)
	}()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	q := `
		SELECT
			b.height
		FROM blocks b
		WHERE b.hash = $1
	`

	var height uint64
	var err error

	if err = s.db.QueryRowContext(ctx, q, blockHash[:]).Scan(
		&height,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, store.ErrBlockNotFound
		}
		return 0, err
	}

	return height, nil
}
