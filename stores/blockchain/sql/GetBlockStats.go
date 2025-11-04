// Package sql implements the blockchain.Store interface using SQL database backends.
// It provides concrete SQL-based implementations for all blockchain operations
// defined in the interface, with support for different SQL engines.
//
// This file implements the GetBlockStats method, which retrieves statistical information
// about the blockchain. These statistics are essential for monitoring blockchain health,
// analyzing network performance, and providing insights into the blockchain's growth and
// characteristics. The implementation uses a recursive Common Table Expression (CTE) in SQL
// to efficiently traverse the main chain from the current tip backward, calculating various
// metrics such as block count, transaction count, average block size, and time between blocks.
// This functionality supports Teranode's monitoring, analytics, and diagnostic capabilities.
package sql

import (
	"context"
	"fmt"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/tracing"
)

// GetBlockStats retrieves statistical information about the blockchain.
// This implements the blockchain.Store.GetBlockStats interface method.
//
// The method calculates various metrics about the blockchain's state and performance,
// providing insights into the network's health, growth patterns, and processing efficiency.
// These statistics are valuable for monitoring, analytics, diagnostics, and capacity planning
// in Teranode's high-throughput architecture.
//
// The implementation uses a recursive SQL query to efficiently traverse the main blockchain
// from the current tip backward, calculating aggregate statistics such as total blocks,
// total transactions, average block size, and average time between blocks. It handles
// database engine differences (PostgreSQL vs SQLite) with appropriate SQL syntax adjustments.
//
// Statistics calculated include:
//   - Total number of blocks in the main chain
//   - Total number of transactions across all blocks
//   - Maximum block height (chain length)
//   - Average block size in bytes
//   - Average number of transactions per block
//   - Average time between blocks (mining rate)
//
// Parameters:
//   - ctx: Context for the database operation, allowing for cancellation and timeouts
//
// Returns:
//   - *model.BlockStats: A structure containing the calculated blockchain statistics
//   - error: Any error encountered during calculation, specifically:
//   - StorageError for database errors or processing failures
func (s *SQL) GetBlockStats(ctx context.Context) (*model.BlockStats, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "sql:GetBlockStats")
	defer deferFn()

	// Try to get from response cache
	// Use a fixed cache key for stats since they're global for the entire blockchain
	statsCacheKey := chainhash.HashH([]byte("GetBlockStats"))

	cached := s.responseCache.Get(statsCacheKey)
	if cached != nil && cached.Value() != nil {
		if cacheData, ok := cached.Value().(*model.BlockStats); ok {
			return cacheData, nil
		}
	}

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

	// Cache the stats result
	s.responseCache.Set(statsCacheKey, blockStats, s.cacheTTL)

	return blockStats, nil
}
