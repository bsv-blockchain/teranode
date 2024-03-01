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

	cacheId := chainhash.HashH([]byte(fmt.Sprintf("GetBlockHeight-%s", blockHash.String())))
	cached := cache.Get(cacheId)
	if cached != nil && cached.Value() != nil {
		if cacheData, ok := cached.Value().(uint32); ok && cacheData != 0 {
			s.logger.Debugf("GetBlockHeight cache hit")
			return cacheData, nil
		}
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
	var err error

	if err = s.db.QueryRowContext(ctx, q, blockHash[:]).Scan(
		&height,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, fmt.Errorf("error in GetBlockHeight: %w", err)
		}
		return 0, err
	}

	cache.Set(cacheId, height, cacheTTL)

	return height, nil
}
