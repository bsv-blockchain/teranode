// Package sql implements the blockchain.Store interface using SQL database backends.
// It provides concrete SQL-based implementations for all blockchain operations
// defined in the interface, with support for different SQL engines.
//
// This file implements the GetBlockGraphData method, which retrieves time-series data
// about blocks in the main chain for visualization and analytics purposes. This functionality
// is important for monitoring blockchain performance, analyzing transaction throughput,
// and visualizing network activity over time. The implementation uses a recursive Common
// Table Expression (CTE) in SQL to efficiently traverse the blockchain structure from the
// current tip backward, collecting timestamp and transaction count data for blocks within
// a specified time period. This data can be used to generate graphs and charts for blockchain
// analytics dashboards and monitoring tools.
package sql

import (
	"context"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/util/tracing"
)

// GetBlockGraphData retrieves time-series data about blocks for visualization and analytics.
// This implements the blockchain.Store.GetBlockGraphData interface method.
//
// The method collects timestamp and transaction count data for blocks in the main chain
// within a specified time period. This data is valuable for monitoring blockchain performance,
// analyzing transaction throughput patterns, and visualizing network activity over time.
// Such analytics are essential for capacity planning, performance optimization, and
// identifying trends or anomalies in Teranode's high-throughput blockchain processing.
//
// The implementation uses a recursive SQL query to efficiently traverse the main blockchain
// from the current tip backward, collecting data points for blocks with timestamps greater
// than or equal to the specified period. It handles time unit conversion between the
// millisecond-based input parameter and the second-based block timestamps in the database.
//
// Parameters:
//   - ctx: Context for the database operation, allowing for cancellation and timeouts
//   - periodMillis: The time period in milliseconds from which to collect block data;
//     only blocks with timestamps greater than or equal to this value will be included
//
// Returns:
//   - *model.BlockDataPoints: A structure containing arrays of timestamps and transaction
//     counts for blocks within the specified period, suitable for graphing and analytics
//   - error: Any error encountered during data collection, specifically:
//   - StorageError for database errors or processing failures
func (s *SQL) GetBlockGraphData(ctx context.Context, periodMillis uint64) (*model.BlockDataPoints, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "sql:GetBlockGraphData")
	defer deferFn()

	// Query to get block graph data from the main chain.
	// Uses idx_chain_work_valid to quickly find the best block, then traverses
	// back through the chain using idx_parent_id.
	//
	// The query starts from both genesis (id=0) and the best block, walking back
	// through parent links. The final WHERE clause filters to only blocks within
	// the requested time period.
	q := `
		WITH RECURSIVE ChainBlocks AS (
			SELECT
			 id
			,parent_id
			,block_time
			,tx_count
			FROM blocks
			WHERE id IN (
				0,
				(SELECT id FROM blocks WHERE id > 0 ORDER BY chain_work DESC, id ASC LIMIT 1)
			)
			AND EXISTS (SELECT 1 FROM blocks WHERE id > 0)
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
		SELECT block_time, tx_count FROM ChainBlocks
		WHERE block_time >= $1
	`

	blockDataPoints := &model.BlockDataPoints{}

	rows, err := s.db.QueryContext(ctx, q, periodMillis/1000) // Remember, periodMillis is in milliseconds, but block_time is in seconds
	if err != nil {
		return nil, errors.NewStorageError("failed to get block data", err)
	}

	defer rows.Close()

	for rows.Next() {
		dataPoint := &model.DataPoint{}

		if err = rows.Scan(
			&dataPoint.Timestamp,
			&dataPoint.TxCount,
		); err != nil {
			return nil, errors.NewStorageError("failed to read data point", err)
		}

		blockDataPoints.DataPoints = append(blockDataPoints.DataPoints, dataPoint)
	}

	return blockDataPoints, nil
}
