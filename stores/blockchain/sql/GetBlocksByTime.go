// Package sql implements the blockchain.Store interface using SQL database backends.
// It provides concrete SQL-based implementations for all blockchain operations
// defined in the interface, with support for different SQL engines.
//
// This file implements the GetBlocksByTime method, which retrieves block hashes for blocks
// inserted within a specific time range. This functionality is particularly useful for
// blockchain analytics, auditing, and monitoring systems that need to analyze block
// production rates or examine blocks created during specific time periods. The implementation
// uses a simple SQL query with time-based filtering to efficiently retrieve blocks based on
// their insertion timestamps rather than blockchain height or hash, providing an alternative
// access pattern for blockchain data that complements the traditional block height and
// hash-based retrieval methods.
package sql

import (
	"context"
	"fmt"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

// GetBlocksByTime retrieves block hashes for blocks inserted within a specific time range.
// This method provides a time-based access pattern for blockchain data, complementing the
// traditional block height and hash-based retrieval methods.
//
// Time-based block retrieval is particularly valuable for several use cases:
//   - Blockchain analytics that examine block production rates over time
//   - Auditing systems that need to analyze blocks created during specific time periods
//   - Monitoring tools that track blockchain performance and growth patterns
//   - Debugging and diagnostic processes that investigate blockchain behavior during
//     specific time windows
//
// The implementation uses a straightforward SQL query that filters blocks based on their
// insertion timestamps in the database. This approach provides an efficient way to retrieve
// blocks based on when they were added to the node's database rather than their position
// in the blockchain or their cryptographic identifiers.
//
// Note that the insertion time (when the block was added to this specific node's database)
// is distinct from the block's timestamp field (which is set by miners and subject to
// consensus rules). This method uses the former, making it particularly useful for
// node-specific analytics and diagnostics.
//
// Parameters:
//   - ctx: Context for the database operation, allowing for cancellation and timeouts
//   - fromTime: The start of the time range (inclusive) for block insertion timestamps
//   - toTime: The end of the time range (inclusive) for block insertion timestamps
//
// Returns:
//   - [][]byte: An array of block hashes (in raw byte format) for blocks inserted within
//     the specified time range
//   - error: Any error encountered during the database operation
func (s *SQL) GetBlocksByTime(ctx context.Context, fromTime, toTime time.Time) ([][]byte, error) {
	// Try to get from response cache using derived cache key
	// Use operation-prefixed key to be consistent with other operations
	fromTimeString := fromTime.Format("2006-01-02 15:04:05 +0000")
	toTimeString := toTime.Format("2006-01-02 15:04:05 +0000")

	cacheID := chainhash.HashH([]byte(fmt.Sprintf("GetBlocksByTime-%s-%s", fromTimeString, toTimeString)))

	cached := s.responseCache.Get(cacheID)
	if cached != nil {
		if hashes, ok := cached.Value().([][]byte); ok {
			return hashes, nil
		}
	}

	q := `
    SELECT hash FROM blocks
	WHERE inserted_at >='%s' AND inserted_at <= '%s'
    `
	// var Hash []byte

	formattedQuery := fmt.Sprintf(q, fromTimeString, toTimeString)

	rows, err := s.db.QueryContext(ctx, formattedQuery)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	// blocks := make([]*model.Block, 0)
	hashes := make([][]byte, 0)

	for rows.Next() {
		// block := &model.Block{}
		hash := []byte{}
		err := rows.Scan(
			&hash,
		)

		if err != nil {
			fmt.Printf("Error scanning row: %v\n", err)
			return nil, err
		}

		hashes = append(hashes, hash)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Cache the result in response cache
	s.responseCache.Set(cacheID, hashes, s.cacheTTL)

	return hashes, nil
}
