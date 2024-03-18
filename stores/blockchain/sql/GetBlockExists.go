package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
)

func (s *SQL) GetBlockExists(ctx context.Context, blockHash *chainhash.Hash) (bool, error) {
	start, stat, ctx := util.StartStatFromContext(ctx, "GetBlockExists")
	defer func() {
		stat.AddTime(start)
	}()

	// the cache will be invalidated by the StoreBlock function when a new block is added, or after cacheTTL seconds
	cacheId := chainhash.HashH([]byte(fmt.Sprintf("GetBlockExists-%s", blockHash.String())))
	cached := cache.Get(cacheId)
	if cached != nil && cached.Value() != nil {
		if exists, ok := cached.Value().(bool); ok {
			s.logger.Debugf("GetBlockExists cache hit")
			return exists, nil
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
	if err := s.db.QueryRowContext(ctx, q, blockHash[:]).Scan(
		&height,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}

	cache.Set(cacheId, true, cacheTTL)

	return true, nil
}
