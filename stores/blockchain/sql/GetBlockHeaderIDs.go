// Package sql implements the blockchain.Store interface using SQL database backends.
// It provides concrete SQL-based implementations for all blockchain operations
// defined in the interface, with support for different SQL engines.
//
// This file implements the GetBlockHeaderIDs method, which retrieves a sequence of
// block header IDs starting from a specified block hash. In Teranode's architecture
// for BSV, this method plays a critical role in block mining status tracking and
// chain traversal operations.
//
// The implementation uses a multi-tier strategy:
//
//  1. In-memory cache (responseCache or chainWalkCache, depending on
//     useInMemoryChainCheck) to short-circuit repeated requests.
//  2. An on_main_chain fast path that filters by the partial index and a
//     height range derived from the start block — used whenever the start
//     hash is on the main chain and no rebuild is in flight.
//  3. A recursive SQL Common Table Expression (CTE) fallback that walks
//     parent_id pointers — used for fork tips, unknown hashes, and during
//     main-chain rebuilds.
//
// This method supports Teranode's high-throughput transaction processing by
// providing efficient access to block header identifiers without requiring
// the full header data to be loaded. This is particularly important for
// operations that only need to track or reference blocks by their internal
// database IDs.
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
//  1. First attempts to retrieve header IDs from the in-memory blocks cache.
//     - Provides O(1) lookup time when the requested blocks are in the cache.
//     - Significantly reduces database load for frequently accessed recent blocks.
//     - Returns immediately if the cache contains the requested headers.
//
//  2. On cache miss, dispatches to buildGetBlockHeaderIDsQuery which picks
//     between two SQL strategies:
//     - on_main_chain fast path: a single backward index scan on the
//     partial index, restricted to the height range derived from the
//     start block. Used whenever the start hash is on the main chain
//     and no rebuild is in flight.
//     - Recursive CTE fallback: walks parent_id pointers from the start
//     block. Used for fork tips, unknown hashes, and during main-chain
//     rebuilds.
//
// The fast path replaces an O(N) recursive walk with a contiguous index
// scan and is ~3-6x faster on small datasets, expected 10-20x on
// production-sized DBs. The CTE remains authoritative for the cases where
// on_main_chain cannot answer correctly. This is critical for Teranode's
// high-throughput BSV implementation, where database efficiency directly
// impacts node performance.
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
//  1. First checks the in-memory blocks cache for the requested headers.
//  2. If found in cache, extracts and returns just the IDs without database access.
//  3. On cache miss, dispatches to the on_main_chain fast path or the recursive
//     CTE fallback via buildGetBlockHeaderIDsQuery.
//  4. Returns an empty slice with nil error if no matching blocks are found.
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

	// Use chain walk cache when in-memory mode is on (survives StoreBlock wipes),
	// otherwise fall back to response cache (original behavior).
	cache := s.responseCache
	cacheTTL := s.cacheTTL
	if s.useInMemoryChainCheck {
		cache = s.chainWalkCache
		cacheTTL = chainWalkCacheTTL
	}

	cacheID := chainhash.HashH([]byte(fmt.Sprintf("GetBlockHeaderIDs-%s-%d", blockHashFrom.String(), numberOfHeaders)))
	cacheOp := cache.Begin(cacheID)

	cached := cacheOp.Get()
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

	// Try the on_main_chain fast path; fall back to the recursive CTE on fork
	// tips, unknown hashes, or while a main-chain rebuild is in flight. Same
	// semantics as buildGetBlockHeadersQuery — see comment there.
	q, args := s.buildGetBlockHeaderIDsQuery(ctx, blockHashFrom, numberOfHeaders)
	rows, err := s.db.QueryContext(ctx, q, args...)

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

	// Only cache non-empty results. Empty results can occur when GetBlockHeaderIDs
	// is called for a block hash that hasn't been stored yet (e.g., checkOldBlockIDs
	// during ValidateBlock runs before AddBlock). Caching empty results in chainWalkCache
	// causes persistent failures because chainWalkCache survives StoreBlock cache wipes.
	if len(ids) > 0 {
		cacheOp.Set(ids, cacheTTL)
	}

	return ids, nil
}

// buildGetBlockHeaderIDsQuery returns the SQL query and args for GetBlockHeaderIDs.
// The fast path uses the on_main_chain partial index when the start hash is on
// the main chain. Otherwise the recursive CTE walks parent_id pointers and is
// authoritative for fork tips and rebuilds.
func (s *SQL) buildGetBlockHeaderIDsQuery(ctx context.Context, blockHashFrom *chainhash.Hash, numberOfHeaders uint64) (string, []interface{}) {
	if s.mainChainRebuilding.Load() == 0 {
		var (
			onMain      bool
			startHeight uint32
		)
		// Resolve start-block height in the probe so the main query binds it as
		// a literal parameter. This (a) lets the planner pick the
		// idx_on_main_chain_height partial index for the height range, and
		// (b) eliminates the intra-query race that a same-query subquery
		// evaluated twice would have.
		if scanErr := s.db.QueryRowContext(ctx,
			`SELECT COALESCE(on_main_chain, false), COALESCE(height, 0)
			 FROM blocks WHERE hash = $1 LIMIT 1`,
			blockHashFrom[:],
		).Scan(&onMain, &startHeight); scanErr == nil && onMain {
			fastPath := `
		SELECT b.id
		FROM blocks b
		WHERE b.on_main_chain = true
		  AND b.height <= $1
		  AND b.height > $1 - $2
		ORDER BY b.height DESC
		LIMIT $2
	`
			return fastPath, []interface{}{startHeight, numberOfHeaders}
		}
	}

	cte := `
		WITH RECURSIVE ChainBlocks AS (
			SELECT id, parent_id, 1 AS depth
			FROM blocks
			WHERE hash = $1
			UNION ALL
			SELECT bb.id, bb.parent_id, cb.depth + 1
			FROM blocks bb
			JOIN ChainBlocks cb ON bb.id = cb.parent_id
			WHERE bb.id != cb.id
			  AND cb.depth < $2
		)
		SELECT id FROM ChainBlocks
		LIMIT $2
	`
	return cte, []interface{}{blockHashFrom[:], numberOfHeaders}
}
