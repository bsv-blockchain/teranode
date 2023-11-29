package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
)

// GetBlockHeaderIDs returns the block header ids from the given block hash and number of headers
// this is used internally for setting blocks to mined, where we only save the id of the block header and compare that
func (s *SQL) GetBlockHeaderIDs(ctx context.Context, blockHashFrom *chainhash.Hash, numberOfHeaders uint64) ([]uint32, error) {
	start, stat, ctx := util.StartStatFromContext(ctx, "GetBlockHeaders")
	defer func() {
		stat.AddTime(start)
	}()

	// the cache will be invalidated by the StoreBlock function when a new block is added, or after cacheTTL seconds
	cacheId := chainhash.HashH([]byte(fmt.Sprintf("GetBlockHeaderIDs-%d-%s", numberOfHeaders, blockHashFrom)))
	cached := cache.Get(cacheId)
	if cached != nil && cached.Value() != nil {
		if cacheData, ok := cached.Value().([]uint32); ok && cacheData != nil {
			s.logger.Debugf("GetBlockHeaderIDs cache hit")
			return cacheData, nil
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	ids := make([]uint32, 0, numberOfHeaders)

	q := `
		SELECT
			 b.id
		FROM blocks b
		WHERE id IN (
			SELECT id FROM blocks
			WHERE id IN (
				WITH RECURSIVE ChainBlocks AS (
					SELECT id, parent_id, height
					FROM blocks
					WHERE hash = $1
					UNION ALL
					SELECT bb.id, bb.parent_id, bb.height
					FROM blocks bb
					JOIN ChainBlocks cb ON bb.id = cb.parent_id
					WHERE bb.id != cb.id
				)
				SELECT id FROM ChainBlocks
				LIMIT $2
			)
		)
		ORDER BY height DESC
	`
	rows, err := s.db.QueryContext(ctx, q, blockHashFrom[:], numberOfHeaders)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ids, nil
		}
		return nil, fmt.Errorf("failed to get headers: %w", err)
	}
	defer rows.Close()

	var id uint32
	for rows.Next() {
		if err = rows.Scan(
			&id,
		); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		ids = append(ids, id)
	}

	cache.Set(cacheId, ids, cacheTTL)

	return ids, nil
}
