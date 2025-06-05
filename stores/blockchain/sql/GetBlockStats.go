package sql

import (
	"context"
	"fmt"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/tracing"
)

func (s *SQL) GetBlockStats(ctx context.Context) (*model.BlockStats, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "sql:GetBlockStats")
	defer deferFn()

	tweak := "X'00'"

	if s.GetDBEngine() == util.Postgres {
		tweak = "'\\x00'::bytea"
	}

	q := fmt.Sprintf(`
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
		SELECT 
			COALESCE(count(1), 0),
			COALESCE(sum(tx_count), 0),
			COALESCE(max(height), 0),
			COALESCE(avg(size_in_bytes), 0),
			COALESCE(avg(tx_count), 0),
			COALESCE(min(block_time), 0),
			COALESCE(max(block_time), 0),
			COALESCE((SELECT chain_work FROM blocks WHERE id > 0 ORDER BY chain_work DESC, id ASC LIMIT 1), %s)
		FROM ChainBlocks
		WHERE id > 0
	`, tweak)

	blockStats := &model.BlockStats{}

	err := s.db.QueryRowContext(ctx, q).Scan(
		&blockStats.BlockCount,
		&blockStats.TxCount,
		&blockStats.MaxHeight,
		&blockStats.AvgBlockSize,
		&blockStats.AvgTxCountPerBlock,
		&blockStats.FirstBlockTime,
		&blockStats.LastBlockTime,
		&blockStats.ChainWork,
	)
	if err != nil {
		return nil, errors.NewStorageError("failed to get stats", err)
	}

	// add 1 to the block count to include the genesis block, which is excluded from the query
	blockStats.BlockCount += 1

	return blockStats, nil
}
