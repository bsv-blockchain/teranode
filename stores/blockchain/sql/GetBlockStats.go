package sql

import (
	"context"
	"fmt"
	"time"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/util"
)

func (s *SQL) GetBlockStats(ctx context.Context) (*model.BlockStats, error) {
	start, stat, ctx := util.StartStatFromContext(ctx, "GetBlockStats")
	defer func() {
		stat.AddTime(start)
	}()

	q := `
		WITH RECURSIVE ChainBlocks AS (
			SELECT
			 id
			,parent_id
			,tx_count
			,height
			,size_in_bytes
			,block_time
			FROM blocks
			WHERE id IN (0, (SELECT id FROM blocks ORDER BY chain_work DESC, id ASC LIMIT 1))
			UNION ALL
			SELECT
			 b.id
			,b.parent_id
			,b.tx_count
			,b.height
			,b.size_in_bytes
			,b.block_time
			FROM blocks b
			INNER JOIN ChainBlocks cb ON b.id = cb.parent_id
			WHERE b.parent_id != 0
		)
		SELECT count(1), sum(tx_count), max(height), avg(size_in_bytes), avg(tx_count) from ChainBlocks
		WHERE block_time >= $1
	`

	blockStats := &model.BlockStats{}

	blockTime := time.Now().UTC().Add(-24 * time.Hour).Unix()

	err := s.db.QueryRowContext(ctx, q, blockTime).Scan(
		&blockStats.BlockCount,
		&blockStats.TxCount,
		&blockStats.MaxHeight,
		&blockStats.AvgBlockSize,
		&blockStats.AvgTxCountPerBlock,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get stats: %w", err)
	}

	return blockStats, nil
}
