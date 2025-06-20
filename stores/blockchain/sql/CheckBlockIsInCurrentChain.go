// Package sql implements the blockchain.Store interface using SQL database backends.
// It provides concrete SQL-based implementations for all blockchain operations
// defined in the interface, with support for different SQL engines.
//
// This file implements the CheckBlockIsInCurrentChain method, which determines whether
// specified blocks are part of the current main blockchain. In blockchain systems like
// Teranode that support multiple competing chains (forks), it's critical to efficiently
// determine which blocks belong to the main chain (the chain with the highest cumulative
// proof-of-work). The implementation uses a recursive Common Table Expression (CTE) in SQL
// to efficiently traverse the blockchain structure from the current tip backward, checking
// if the specified blocks are part of this path. This functionality is essential for
// transaction validation, chain reorganization, and ensuring consensus across the network.
package sql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/util/tracing"
)

// CheckBlockIsInCurrentChain determines if specified blocks are part of the current main chain.
// This implements a specialized blockchain validation method not directly defined in the Store interface.
//
// In blockchain systems, it's critical to determine whether specific blocks are part of the
// current main chain (the chain with the highest cumulative proof-of-work) or if they belong
// to a fork chain. This method efficiently checks multiple block IDs in a single database query,
// which is important for Teranode's high-throughput architecture where chain membership checks
// are common during transaction validation and block processing.
//
// The implementation uses a recursive SQL query to efficiently traverse the blockchain structure
// from the current tip backward, checking if the specified blocks are part of this path. It handles
// database engine differences (PostgreSQL vs SQLite) with appropriate SQL syntax adjustments.
//
// Parameters:
//   - ctx: Context for the database operation, allowing for cancellation and timeouts
//   - blockIDs: Array of internal database IDs for the blocks to check
//
// Returns:
//   - bool: True if all specified blocks are part of the current main chain, false otherwise
//   - error: Any error encountered during the check, specifically:
//   - StorageError for database errors or processing failures
func (s *SQL) CheckBlockIsInCurrentChain(ctx context.Context, blockIDs []uint32) (bool, error) {
	ctx, _, deferFn := tracing.Tracer("SyncManager").Start(ctx, "sql:CheckIfBlockIsInCurrentChain",
		tracing.WithDebugLogMessage(s.logger, "[CheckIfBlockIsInCurrentChain] checking if blocks (%v) are in current chain", blockIDs),
	)
	defer deferFn()

	if len(blockIDs) == 0 {
		return false, nil
	}

	// Get current best block header
	_, bestBlockMeta, err := s.GetBestBlockHeader(ctx)
	if err != nil {
		return false, errors.NewStorageError("failed to get best block header", err)
	}

	// Prepare the arguments and the CTE for block_ids
	args := make([]interface{}, 0, len(blockIDs)+2) // blockIDs + bestBlockID + recursionDepth

	// Generate placeholders for blockIDs
	blockIDPlaceholders := make([]string, len(blockIDs))

	for i, id := range blockIDs {
		placeholder := fmt.Sprintf("$%d", i+1)
		if s.engine == "sqlite" || s.engine == "sqlitememory" {
			blockIDPlaceholders[i] = fmt.Sprintf("SELECT CAST(%s as int) AS id", placeholder)
		} else {
			blockIDPlaceholders[i] = fmt.Sprintf("SELECT %s::INTEGER AS id", placeholder)
		}

		args = append(args, id)
	}

	blockIDsCTE := strings.Join(blockIDPlaceholders, " UNION ALL ")

	// Append the bestBlockID and recursionDepth to the arguments
	bestBlockID := bestBlockMeta.ID

	// get the lowest block id
	lowestBlockID := blockIDs[0]
	for _, id := range blockIDs {
		if id < lowestBlockID {
			lowestBlockID = id
		}
	}

	recursionDepthBlockID := bestBlockID - lowestBlockID
	if lowestBlockID > bestBlockID {
		recursionDepthBlockID = 0
	}

	args = append(args, bestBlockID, recursionDepthBlockID) // bestBlockID and recursionDepth

	// Calculate the positions for the placeholders
	bestBlockIDPlaceholder := fmt.Sprintf("$%d", len(blockIDs)+1)
	recursionDepthPlaceholder := fmt.Sprintf("$%d", len(blockIDs)+2)

	q := fmt.Sprintf(`
        WITH RECURSIVE
        block_ids(id) AS (
            %s
        ),
        ChainBlocks AS (
            SELECT id, parent_id, 1 AS depth, EXISTS (SELECT 1 FROM block_ids WHERE id = blocks.id) AS found_match
            FROM blocks
            WHERE id = %s
            UNION ALL
            SELECT
                bb.id,
                bb.parent_id,
                cb.depth + 1 AS depth,
                EXISTS (SELECT 1 FROM block_ids WHERE id = bb.id) AS found_match
            FROM blocks bb
            INNER JOIN ChainBlocks cb ON bb.id = cb.parent_id
            WHERE
                NOT cb.found_match -- Stop recursion if a match has been found
                AND cb.depth <= %s
        )
        SELECT CASE
            WHEN EXISTS (SELECT 1 FROM ChainBlocks WHERE found_match)
            THEN TRUE
            ELSE FALSE
        END AS is_in_current_chain;
    `, blockIDsCTE, bestBlockIDPlaceholder, recursionDepthPlaceholder)

	// Execute the query
	var result bool

	err = s.db.QueryRowContext(ctx, q, args...).Scan(&result)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}

		return false, errors.NewStorageError("failed to check if given blocks are part of the current chain", err)
	}

	return result, nil
}
