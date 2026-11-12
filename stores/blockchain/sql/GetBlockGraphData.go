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
	"database/sql"
	"fmt"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/util/tracing"
)

// graphBucketSeconds returns the bucket size in seconds used to aggregate the
// block graph series for a given total timestamp range, or 0 for per-block
// resolution. The asset HTTP handler holds the same ladder, but re-deriving a
// rung from an already-bucketed series is not always idempotent (30d buckets
// are not a multiple of 1w buckets), so this store reports the rung it used
// via BlockDataPoints.BucketSeconds and the handler must not re-aggregate when
// that is non-zero. Keep the two ladders in step regardless.
func graphBucketSeconds(rangeSeconds int64) int64 {
	switch {
	case rangeSeconds <= 86400: // <= 24h: per-block resolution
		return 0
	case rangeSeconds <= 604800: // <= 1w: 1h buckets
		return 3600
	case rangeSeconds <= 7776000: // <= 3m (90d): 6h buckets
		return 21600
	case rangeSeconds <= 31536000: // <= 1y: 1d buckets
		return 86400
	case rangeSeconds <= 157680000: // <= 5y: 1w buckets
		return 604800
	default: // > 5y: 30d buckets
		return 2592000
	}
}

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
	// prefix carries the recursive CTE (rebuild path only); source is the
	// FROM/WHERE shared by the range pre-query and the data query, so both
	// always select over exactly the same set of blocks.
	var prefix, source string

	if s.mainChainRebuilding.Load() > 0 {
		prefix = `
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
			  AND b.block_time >= $1
		)
	`
		source = `
		FROM ChainBlocks
		WHERE block_time >= $1
	`
	} else {
		// Mirror the original CTE exactly. The CTE's anchor is `id IN (0, best)`
		// so genesis is explicitly included; the recursive step has
		// `WHERE b.parent_id != 0` (needed because genesis's parent_id
		// self-references to 0) which inadvertently drops the height-1 block
		// (whose parent_id equals genesis's id, 0). So: keep genesis, keep
		// anything with parent_id != 0, drop the height-1 block.
		source = `
		FROM blocks
		WHERE on_main_chain = true
		  AND (id = 0 OR parent_id != 0)
		  AND block_time >= $1
	`
	}

	// Remember, periodMillis is in milliseconds, but block_time is in seconds.
	periodSeconds := periodMillis / 1000

	// Pre-query the timestamp range so the bucket size can be chosen before any
	// row is materialised. This is an aggregate-only scan: no per-block row
	// leaves the database.
	var (
		minTS, maxTS sql.NullInt64
		rowCount     int64
	)

	if err := s.db.QueryRowContext(ctx, prefix+`SELECT MIN(block_time), MAX(block_time), COUNT(*)`+source, periodSeconds).Scan(
		&minTS, &maxTS, &rowCount,
	); err != nil {
		return nil, errors.NewStorageError("failed to get block data range", err)
	}

	bucketSeconds := int64(0)
	if rowCount > 1 && minTS.Valid && maxTS.Valid {
		bucketSeconds = graphBucketSeconds(maxTS.Int64 - minTS.Int64)
	}

	q := prefix + `SELECT block_time, tx_count` + source + `ORDER BY block_time ASC`

	if bucketSeconds > 0 {
		// bucketSeconds comes from graphBucketSeconds, never from the caller,
		// so interpolating it keeps the expression portable across engines
		// (repeating a positional placeholder is not).
		bucketExpr := fmt.Sprintf("(block_time / %d) * %d", bucketSeconds, bucketSeconds)
		// CAST keeps the sum a BIGINT: Postgres widens SUM(bigint) to numeric,
		// which will not scan into the uint64 data point.
		q = prefix + `SELECT ` + bucketExpr + `, CAST(SUM(tx_count) AS BIGINT)` + source +
			`GROUP BY ` + bucketExpr + ` ORDER BY 1 ASC`
	}

	blockDataPoints := &model.BlockDataPoints{BucketSeconds: bucketSeconds}

	rows, err := s.db.QueryContext(ctx, q, periodSeconds)
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
