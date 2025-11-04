// Package sql implements the blockchain.Store interface using SQL database backends.
// It provides concrete SQL-based implementations for all blockchain operations
// defined in the interface, with support for different SQL engines.
//
// This file implements the GetBlockHeight method, which retrieves the height of a block
// in the blockchain by its hash. Block height is a critical property representing the
// block's position in the chain, with the genesis block at height 0 and each subsequent
// block incrementing by 1. The implementation uses the blockchainCache for optimized
// lookups, avoiding database queries for frequently accessed blocks. This optimization
// is important for Teranode's high-throughput architecture, where block height lookups
// are common during transaction validation and block processing.
package sql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util/tracing"
)

// GetBlockHeight retrieves the height of a block from the database by its hash.
// This implements the blockchain.Store.GetBlockHeight interface method.
//
// The method first checks an in-memory cache for the block header metadata to avoid database queries.
// If not found in cache, it executes a focused SQL query that only retrieves the height field
// rather than fetching the entire block data for efficiency.
//
// Parameters:
//   - ctx: Context for the database operation, allows for cancellation and timeouts
//   - blockHash: The unique hash identifier of the block to query
//
// Returns:
//   - uint32: The height of the block in the blockchain (0 if not found)
//   - error: Any error encountered during retrieval, specifically:
//   - BlockNotFoundError if the block does not exist
//   - StorageError for other database errors
func (s *SQL) GetBlockHeight(ctx context.Context, blockHash *chainhash.Hash) (uint32, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "sql:GetBlockHeight")
	defer deferFn()

	// Try to get from response cache using derived cache key
	// Use operation-prefixed key to avoid conflicts with other cached data
	cacheID := chainhash.HashH([]byte(fmt.Sprintf("GetBlockHeight-%s", blockHash.String())))

	cached := s.responseCache.Get(cacheID)
	if cached != nil {
		// Check if it's a cached height value
		if height, ok := cached.Value().(uint32); ok {
			return height, nil
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	q := `
		SELECT
			b.height
		FROM blocks b
		WHERE b.hash = $1
	`

	var height uint32

	if err := s.db.QueryRowContext(ctx, q, blockHash[:]).Scan(
		&height,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, errors.NewBlockNotFoundError("error in GetBlockHeight", err)
		}

		return 0, errors.NewStorageError("error in GetBlockHeight", err)
	}

	// Cache the height result
	s.responseCache.Set(cacheID, height, s.cacheTTL)

	return height, nil
}
