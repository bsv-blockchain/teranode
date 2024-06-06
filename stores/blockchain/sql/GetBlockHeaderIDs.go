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
	start, stat, ctx := util.StartStatFromContext(ctx, "GetBlockHeaderIDs")
	defer func() {
		stat.AddTime(start)
	}()

	_, metas, err := s.blocksCache.GetBlockHeaders(blockHashFrom, numberOfHeaders)
	if err != nil {
		return nil, fmt.Errorf("error in GetBlockHeaderIDs: %w", err)
	}
	if metas != nil {
		blockIds := make([]uint32, 0, len(metas))
		for _, meta := range metas {
			blockIds = append(blockIds, meta.ID)
		}
		return blockIds, nil
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

	return ids, nil
}
