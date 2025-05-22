// Package sql implements the blockchain.Store interface using SQL database backends.
// It provides concrete SQL-based implementations for all blockchain operations
// defined in the interface, with support for different SQL engines.
package sql

import (
	"context"
	"database/sql"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/tracing"
	"github.com/libsv/go-bt/v2/chainhash"
)

// GetBlockExists checks if a block exists in the database by its hash.
// This implements the blockchain.Store.GetBlockExists interface method.
//
// The method first checks an in-memory cache to avoid unnecessary database queries.
// If not found in cache, it executes a lightweight SQL query that only retrieves
// the block height (rather than the entire block data) to determine existence.
// The result is then stored in the cache for future queries.
//
// Parameters:
//   - ctx: Context for the database operation, allows for cancellation and timeouts
//   - blockHash: The unique hash identifier of the block to check
//
// Returns:
//   - bool: True if the block exists in the database, false otherwise
//   - error: Any error encountered during database operations (nil if the block simply doesn't exist)
func (s *SQL) GetBlockExists(ctx context.Context, blockHash *chainhash.Hash) (bool, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "sql:GetBlockExists")
	defer deferFn()

	// Check if the existence information is already in cache
	// The cache entry resets whenever a new block is added to maintain consistency
	exists, ok := s.blocksCache.GetExists(*blockHash)
	if ok {
		return exists, nil
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
			// Block doesn't exist - cache this result to avoid future database lookups
			// The cache entry resets whenever a new block is added to maintain consistency
			s.blocksCache.SetExists(*blockHash, false)
			return false, nil
		}

		return false, err
	}

	// Block exists - cache this result to avoid future database lookups
	// The cache entry resets whenever a new block is added to maintain consistency
	s.blocksCache.SetExists(*blockHash, true)

	return true, nil
}
