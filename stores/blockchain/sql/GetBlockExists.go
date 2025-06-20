// Package sql implements the blockchain.Store interface using SQL database backends.
// It provides concrete SQL-based implementations for all blockchain operations
// defined in the interface, with support for different SQL engines.
//
// This file implements the GetBlockExists method, which efficiently checks for the
// existence of a block in the blockchain by its hash. Block existence verification is
// a fundamental operation in blockchain systems, used extensively during transaction
// validation, block processing, and chain synchronization. In Teranode's high-throughput
// architecture, this operation must be extremely efficient as it may be called thousands
// of times per second during peak processing.
//
// The implementation uses a multi-tier optimization strategy:
//  1. First checks a dedicated existence cache that stores only block hash to existence mappings
//  2. If not found in cache, executes a minimal SQL query that only checks existence without
//     retrieving full block data
//  3. Updates the cache with the result to optimize future queries for the same block
//
// This approach significantly reduces database load and improves response times for this
// frequently called operation. The existence cache is carefully managed to ensure consistency
// with the database state, being invalidated whenever blocks are added or removed from the
// blockchain.
package sql

import (
	"context"
	"database/sql"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/util/tracing"
	"github.com/libsv/go-bt/v2/chainhash"
)

// GetBlockExists checks if a block exists in the database by its hash.
// This implements the blockchain.Store.GetBlockExists interface method.
//
// Block existence verification is a critical and frequently performed operation in
// blockchain systems. It's used during transaction validation to verify block references,
// during block processing to check for duplicate blocks, and during chain synchronization
// to determine which blocks need to be requested from peers. In Teranode's high-throughput
// architecture, this method is optimized for maximum performance.
//
// The implementation follows a tiered approach to minimize database load:
//  1. First checks the dedicated existence cache using the block hash as key
//  2. If not found in cache (cache miss), executes an optimized SQL query
//     that only checks for existence without retrieving block data
//  3. Updates the existence cache with the result to benefit future queries
//
// The SQL query is carefully designed to be as lightweight as possible, only checking
// for the presence of a block hash in the blocks table without retrieving any columns.
// This approach is significantly more efficient than querying for block data or even
// just the block height, especially for databases with proper indexing on the hash column.
//
// Parameters:
//   - ctx: Context for the database operation, allowing for cancellation and timeouts
//   - blockHash: The unique hash identifier of the block to check
//
// Returns:
//   - bool: True if the block exists in the database, false otherwise
//   - error: Any error encountered during the database operation, specifically:
//   - StorageError for database access errors
//   - nil if the operation was successful (even if the block doesn't exist)
func (s *SQL) GetBlockExists(ctx context.Context, blockHash *chainhash.Hash) (bool, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "sql:GetBlockExists")
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
