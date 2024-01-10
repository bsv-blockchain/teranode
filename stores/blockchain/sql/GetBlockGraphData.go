package sql

import (
	"context"
	"fmt"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/util"
)

func (s *SQL) GetBlockGraphData(ctx context.Context, periodMillis uint64) (*model.BlockDataPoints, error) {
	start, stat, ctx := util.StartStatFromContext(ctx, "GetBlockGraphData")
	defer func() {
		stat.AddTime(start)
	}()

	q := `
		WITH RECURSIVE ChainBlocks AS (
			SELECT
			 id
			,parent_id
			,block_time
			,tx_count
			FROM blocks
			WHERE id IN (0, (SELECT id FROM blocks ORDER BY chain_work DESC, id ASC LIMIT 1))
			UNION ALL
			SELECT
			 b.id
			,b.parent_id
			,b.block_time
			,b.tx_count
			FROM blocks b
			INNER JOIN ChainBlocks cb ON b.id = cb.parent_id
			WHERE b.parent_id != 0
		)
		SELECT block_time, tx_count from ChainBlocks
		WHERE block_time >= $1
	`

	blockDataPoints := &model.BlockDataPoints{}

	rows, err := s.db.QueryContext(ctx, q, periodMillis/1000) // Remember, periodMillis is in milliseconds, but block_time is in seconds
	if err != nil {
		return nil, fmt.Errorf("failed to get block data: %w", err)
	}

	defer rows.Close()

	for rows.Next() {
		dataPoint := &model.DataPoint{}

		err := rows.Scan(
			&dataPoint.Timestamp,
			&dataPoint.TxCount,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to read data point: %w", err)
		}

		blockDataPoints.DataPoints = append(blockDataPoints.DataPoints, dataPoint)
	}

	return blockDataPoints, nil
}
