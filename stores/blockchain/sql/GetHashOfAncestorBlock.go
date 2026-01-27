// Package sql implements the blockchain.Store interface using SQL database backends.
// It provides concrete SQL-based implementations for all blockchain operations
// defined in the interface, with support for different SQL engines.
//
// This file implements the GetHashOfAncestorBlock method, which retrieves the hash of an
// ancestor block at a specified depth in the blockchain. This functionality is essential
// for blockchain traversal, fork detection, and chain reorganization processes. The
// implementation uses a recursive Common Table Expression (CTE) in SQL to efficiently
// navigate backward through the blockchain structure by following the parent-child
// relationships between blocks. This method is particularly important for Teranode's
// high-throughput architecture where efficient blockchain traversal is critical for
// maintaining consensus and validating chain integrity.
package sql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util/tracing"
)

// GetHashOfAncestorBlock retrieves the hash of an ancestor block at a specified depth.
// This implements the blockchain.Store.GetHashOfAncestorBlock interface method.
//
// The method traverses the blockchain backward from a specified block to find an ancestor
// at a given depth. For example, with depth=1, it returns the parent block's hash; with
// depth=2, it returns the grandparent block's hash, and so on. This traversal is essential
// for various blockchain operations including fork detection, chain reorganization, and
// validation of block relationships.
//
// The implementation uses a recursive SQL query with a depth counter to efficiently navigate
// the blockchain structure by following the parent-child relationships between blocks. The
// depth counter bounds the recursion to prevent infinite loops, and the query naturally
// terminates when the chain ends (parent_id becomes NULL).
//
// Parameters:
//   - ctx: Context for the database operation, allowing for cancellation and timeouts
//   - hash: The hash of the starting block from which to traverse backward
//   - depth: The number of generations to go back in the blockchain
//
// Returns:
//   - *chainhash.Hash: The hash of the ancestor block at the specified depth, if found
//   - error: Any error encountered during traversal, specifically:
//   - BlockNotFoundError if the starting block or an ancestor at the specified depth doesn't exist
//   - StorageError for database errors or if the operation times out
func (s *SQL) GetHashOfAncestorBlock(ctx context.Context, hash *chainhash.Hash, depth int) (*chainhash.Hash, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "sql:GetHashOfAncestorBlock")
	defer deferFn()

	// Try to get from response cache using derived cache key
	// Use operation-prefixed key to be consistent with other operations
	cacheID := chainhash.HashH([]byte(fmt.Sprintf("GetHashOfAncestorBlock-%s-%d", hash.String(), depth)))
	cacheOp := s.responseCache.Begin(cacheID)

	cached := cacheOp.Get()
	if cached != nil {
		if ancestorHash, ok := cached.Value().(*chainhash.Hash); ok {
			return ancestorHash, nil
		}
	}

	// Note: Removed the 1-second timeout that was here. The timeout was causing
	// context.DeadlineExceeded errors on busy databases, which led to incorrect
	// difficulty calculations in the caller (Difficulty.CalcNextWorkRequired).
	// The caller's context should control the timeout if needed.

	var pastHash []byte

	q := `WITH RECURSIVE ChainBlocks AS (
		SELECT
			id,
			hash,
			parent_id,
			0 AS depth  -- Start at depth 0 for the input block
		FROM
			blocks
		WHERE
			hash = $1

		UNION ALL

		SELECT
			b.id,
			b.hash,
			b.parent_id,
			cb.depth + 1
		FROM
			blocks b
		INNER JOIN
			ChainBlocks cb ON b.id = cb.parent_id
		WHERE
			cb.depth < $2
			AND b.id != cb.id  -- Prevent infinite loop on self-referential parent_id (genesis)
	)
	SELECT
	hash
	FROM
		ChainBlocks
	WHERE
		depth = $2  -- This will now correctly get the block $2 blocks back
	ORDER BY
		depth DESC
	LIMIT 1`

	if err := s.db.QueryRowContext(ctx, q, hash[:], depth).Scan(
		&pastHash,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errors.NewStorageError("can't get hash %d before block %s", depth, hash.String(), errors.ErrNotFound)
		}

		return nil, errors.NewStorageError("can't get hash %d before block %s", depth, hash.String(), err)
	}

	ph, err := chainhash.NewHash(pastHash)
	if err != nil {
		return nil, errors.NewProcessingError("failed to convert pastHash", err)
	}

	// Cache the result in response cache
	cacheOp.Set(ph, s.cacheTTL)

	return ph, nil
}
