// Package sql implements the blockchain.Store interface using SQL database backends.
// It provides concrete SQL-based implementations for all blockchain operations
// defined in the interface, with support for different SQL engines.
//
// This file implements the GetBlockHeaderIDs method, which retrieves a sequence of
// block header IDs starting from a specified block hash. In Teranode's architecture
// for BSV, this method plays a critical role in block mining status tracking and
// chain traversal operations. The implementation uses a multi-tier caching strategy
// for performance optimization and falls back to a recursive SQL Common Table Expression
// (CTE) query when cache misses occur. This method supports Teranode's high-throughput
// transaction processing by providing efficient access to block header identifiers
// without requiring the full header data to be loaded. This is particularly important
// for operations that only need to track or reference blocks by their internal database IDs.
package sql

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util/tracing"
)

// GetBlockHeaderIDs retrieves a sequence of block header database IDs starting from a specified block.
//
// This method is primarily used for internal blockchain operations that require efficient
// access to block identifiers without loading complete header data. It's particularly
// important for operations like updating mining status flags, where only the internal
// database IDs of blocks are needed for efficient database operations.
//
// The implementation employs a multi-tier approach for optimal performance:
//
// 1. First attempts to retrieve header IDs from the in-memory blocks cache
//   - Provides O(1) lookup time when the requested blocks are in the cache
//   - Significantly reduces database load for frequently accessed recent blocks
//   - Returns immediately if the cache contains the requested headers
//
// 2. Falls back to a recursive SQL query using Common Table Expressions (CTEs) when cache misses occur
//   - Efficiently traverses the blockchain graph starting from the specified block
//   - Retrieves the specified number of header IDs in descending height order
//   - Uses database indexing for optimal query performance
//
// The recursive CTE approach is particularly well-suited for blockchain's linked-list
// structure, as it allows efficient traversal of the chain without requiring multiple
// separate queries. This is critical for Teranode's high-throughput BSV implementation,
// where database efficiency directly impacts node performance.
// Parameters:
//   - ctx: Context for the database operation, allowing for cancellation and timeouts
//   - blockHashFrom: Hash of the starting block to retrieve IDs from
//   - numberOfHeaders: Maximum number of header IDs to retrieve
//
// Returns:
//   - []uint32: Array of block header database IDs in descending height order
//   - error: Any error encountered during retrieval, specifically:
//   - StorageError for database query or scan errors
//
// The method is optimized for performance with a multi-tier approach:
//  1. First checks the in-memory blocks cache for the requested headers
//  2. If found in cache, extracts and returns just the IDs without database access
//  3. On cache miss, uses a recursive SQL CTE query to efficiently traverse the blockchain
//  4. Returns an empty slice with nil error if no matching blocks are found
//
// This method is particularly important for mining-related operations where blocks need
// to be efficiently marked as mined without loading their complete data.
func (s *SQL) GetBlockHeaderIDs(ctx context.Context, blockHashFrom *chainhash.Hash, numberOfHeaders uint64) ([]uint32, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "sql:GetBlockHeaderIDs",
		tracing.WithTag("blockHashFrom", blockHashFrom.String()),
		tracing.WithTag("numberOfHeaders", strconv.FormatUint(numberOfHeaders, 10)),
	)
	defer deferFn()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Try to get from response cache using derived cache key
	// Use operation-prefixed key to avoid conflicts with other cached data
	cacheID := chainhash.HashH([]byte(fmt.Sprintf("GetBlockHeaderIDs-%s-%d", blockHashFrom.String(), numberOfHeaders)))

	cached := s.responseCache.Get(cacheID)
	if cached != nil {
		if cacheData, ok := cached.Value().([]uint32); ok {
			return cacheData, nil
		}
	}

	// Pre-allocate slice capacity with a reasonable cap to balance performance and safety.
	//
	// Background:
	// - The SQL query LIMIT clause constrains actual results, but numberOfHeaders can be
	//   arbitrarily large (e.g., when global_blockHeightRetention=999999999)
	// - Pre-allocating based on numberOfHeaders directly would cause: make([]uint32, 0, 2_000_000_000)
	//   → 8GB allocation → instant OOM on 4Gi memory limit
	//
	// Benefits of capped pre-allocation:
	// - Prevents OOM from misconfigured settings (safe regardless of numberOfHeaders value)
	// - Optimal performance for common cases (<10k headers): zero reallocations
	// - Minimal memory overhead: caps pre-allocation at 40KB (10k * 4 bytes)
	//
	// Drawbacks of capped pre-allocation:
	// - For queries returning >10k results: slice will need ~log₂(n/10000) reallocations
	//   Example: 870k results requires ~6 reallocations with ~870k extra copies
	//   Performance cost: ~1-2ms additional (negligible compared to SQL query time)
	//
	// Alternative considered (no pre-allocation):
	// - make([]uint32, 0) would be safest but causes ~20 reallocations for 870k results
	// - The capped approach provides better performance with acceptable safety tradeoff
	const reasonableInitialCapacity = 10_000
	initialCap := numberOfHeaders
	if initialCap > reasonableInitialCapacity {
		initialCap = reasonableInitialCapacity
	}
	ids := make([]uint32, 0, initialCap)

	q := `
		WITH RECURSIVE ChainBlocks AS (
			SELECT id, parent_id
			FROM blocks
			WHERE hash = $1
			UNION ALL
			SELECT bb.id, bb.parent_id
			FROM blocks bb
			JOIN ChainBlocks cb ON bb.id = cb.parent_id
			WHERE bb.id != cb.id
		)
		SELECT id FROM ChainBlocks
		LIMIT $2
	`
	rows, err := s.db.QueryContext(ctx, q, blockHashFrom[:], numberOfHeaders)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ids, nil
		}

		return nil, errors.NewStorageError("failed to get headers", err)
	}

	defer rows.Close()

	var id uint32
	for rows.Next() {
		if err = rows.Scan(
			&id,
		); err != nil {
			return nil, errors.NewStorageError("failed to scan row", err)
		}

		ids = append(ids, id)
	}

	// Cache the result in response cache
	s.responseCache.Set(cacheID, ids, s.cacheTTL)

	return ids, nil
}
