// Package sql implements the blockchain.Store interface using SQL database backends.
// It provides concrete SQL-based implementations for all blockchain operations
// defined in the interface, with support for different SQL engines.
//
// This file implements the GetLastNInvalidBlocks method, which retrieves information about
// recently invalidated blocks in the blockchain. This functionality is essential for monitoring
// blockchain reorganizations, fork resolution processes, and diagnosing consensus issues.
// The implementation includes efficient caching to optimize performance for repeated queries
// and retrieves detailed information about invalidated blocks regardless of which fork they
// belong to. This method is particularly important for Teranode's high-throughput architecture
// where chain reorganizations must be efficiently tracked and analyzed to maintain consensus
// integrity across the network.
package sql

import (
	"context"
	"fmt"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/util/tracing"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

// GetLastNInvalidBlocks retrieves information about recently invalidated blocks in the blockchain.
// This implements the blockchain.Store.GetLastNInvalidBlocks interface method.
//
// The method retrieves detailed information about blocks that have been marked as invalid,
// regardless of which fork they belong to. This functionality is essential for monitoring
// blockchain reorganizations, diagnosing consensus issues, and analyzing fork resolution
// processes. In Teranode's high-throughput architecture, tracking invalidated blocks is
// critical for maintaining consensus integrity and understanding chain reorganization events.
//
// Invalid blocks typically result from chain reorganizations, where a competing chain with
// greater cumulative proof-of-work becomes the new main chain, causing blocks in the previous
// main chain to be invalidated. This method helps track these events and provides visibility
// into the blockchain's fork resolution processes.
//
// The implementation uses a response cache to optimize performance for repeated queries,
// which is particularly important for frequently accessed diagnostic data. It constructs
// an SQL query to retrieve invalid blocks ordered by height in descending order, providing
// the most recent invalidations first.
//
// Parameters:
//   - ctx: Context for the database operation, allowing for cancellation and timeouts
//   - n: The number of most recently invalidated blocks to retrieve
//
// Returns:
//   - []*model.BlockInfo: An array of block information structures containing details
//     such as hash, height, timestamp, transaction count, and size for each invalidated block
//   - error: Any error encountered during retrieval, specifically:
//   - StorageError for database errors or processing failures
func (s *SQL) GetLastNInvalidBlocks(ctx context.Context, n int64) ([]*model.BlockInfo, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "sql:GetLastNInvalidBlocks")
	defer deferFn()

	// Create a unique cache ID for this query
	cacheID := chainhash.HashH([]byte(fmt.Sprintf("GetLastNInvalidBlocks-%d", n)))

	// Check if we have a cached result
	cached := s.responseCache.Get(cacheID)
	if cached != nil && cached.Value() != nil {
		if cacheData, ok := cached.Value().([]*model.BlockInfo); ok && cacheData != nil {
			s.logger.Debugf("GetLastNInvalidBlocks cache hit")
			return cacheData, nil
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Query to get the last n invalid blocks, ordered by inserted_at descending (most recent first)
	q := `
		SELECT
		 b.version
		,b.block_time
		,b.n_bits
		,b.nonce
		,b.previous_hash
		,b.merkle_root
		,b.tx_count
		,b.size_in_bytes
		,b.coinbase_tx
		,b.height
		,b.inserted_at
		FROM blocks b
		WHERE invalid = true
		ORDER BY height DESC
		LIMIT $1
	`

	rows, err := s.db.QueryContext(ctx, q, n)
	if err != nil {
		return nil, errors.NewStorageError("failed to get invalid blocks", err)
	}
	defer rows.Close()

	// Process the query results using the common helper function
	blockInfos, err := s.processBlockRows(rows)
	if err != nil {
		return nil, err
	}

	// Cache the result
	s.responseCache.Set(cacheID, blockInfos, s.cacheTTL)

	return blockInfos, nil
}
