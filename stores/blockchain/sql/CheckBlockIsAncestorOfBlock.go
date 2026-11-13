// Package sql implements the blockchain.Store interface using SQL database backends.
// It provides concrete SQL-based implementations for all blockchain operations
// defined in the interface, with support for different SQL engines.
//
// This file implements the CheckBlockIsAncestorOfBlock method, which determines whether
// specified blocks are ancestors of a given block. This is essential for double-spend
// detection on fork blocks where we need to check against the fork's ancestor chain
// rather than the main chain. When the specified block is on the main chain, a main-chain
// block at or below its height is answered by one indexed probe. Every other answer uses a
// recursive Common Table Expression (CTE) that walks parent links back from the specified
// block, checking if the specified block IDs are part of this path.
package sql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util/tracing"
)

// CheckBlockIsAncestorOfBlock determines if specified blocks are ancestors of a given block.
// This is similar to CheckBlockIsInCurrentChain but checks against a specific block's
// ancestor chain rather than the main chain tip.
//
// This method is used for double-spend detection on fork blocks. When processing a fork block,
// we need to check if transactions were already mined in ancestor blocks of that specific fork,
// not the main chain. This allows the same transaction to legitimately exist in blocks on
// different forks without being flagged as a double-spend.
//
// Parameters:
//   - ctx: Context for the database operation, allowing for cancellation and timeouts
//   - blockIDs: Array of internal database IDs for the blocks to check
//   - blockHash: Hash of the block to check ancestors for
//
// Returns:
//   - bool: True if any specified block is an ancestor of the given block, false otherwise
//   - error: Any error encountered during the check
func (s *SQL) CheckBlockIsAncestorOfBlock(ctx context.Context, blockIDs []uint32, blockHash *chainhash.Hash) (bool, error) {
	ctx, _, deferFn := tracing.Tracer("SyncManager").Start(ctx, "sql:CheckBlockIsAncestorOfBlock",
		tracing.WithDebugLogMessage(s.logger, "[CheckBlockIsAncestorOfBlock] checking if blocks (%v) are ancestors of %s", blockIDs, blockHash.String()),
	)
	defer deferFn()

	if len(blockIDs) == 0 {
		return false, nil
	}

	if s.mainChainRebuilding.Load() == 0 {
		found, err := s.ancestorOnMainChainBelowAnchor(ctx, blockIDs, blockHash)
		if err != nil {
			return false, err
		}

		if found {
			return true, nil
		}
		// No match does not mean no ancestor: the anchor may be on a fork, or a flag may be
		// transiently false. The walk below decides every false answer.
	}

	// Get the block header for the specified block hash to get its ID
	_, blockMeta, err := s.GetBlockHeader(ctx, blockHash)
	if err != nil {
		return false, errors.NewStorageError("failed to get block header", err)
	}

	// Prepare the arguments and the CTE for block_ids
	args := make([]interface{}, 0, len(blockIDs)+2) // blockIDs + targetBlockID + recursionDepth

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

	// Append the targetBlockID and recursionDepth to the arguments
	targetBlockID := blockMeta.ID

	// get the lowest block id to determine recursion depth
	lowestBlockID := blockIDs[0] //nolint:gosec // length is checked on line 52
	for _, id := range blockIDs {
		if id < lowestBlockID {
			lowestBlockID = id
		}
	}

	recursionDepthBlockID := targetBlockID - lowestBlockID
	if lowestBlockID > targetBlockID {
		recursionDepthBlockID = 0
	}

	args = append(args, targetBlockID, recursionDepthBlockID) // targetBlockID and recursionDepth

	// Calculate the positions for the placeholders
	targetBlockIDPlaceholder := fmt.Sprintf("$%d", len(blockIDs)+1)
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
        END AS is_ancestor;
    `, blockIDsCTE, targetBlockIDPlaceholder, recursionDepthPlaceholder)

	// Execute the query
	var result bool

	err = s.db.QueryRowContext(ctx, q, args...).Scan(&result)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}

		return false, errors.NewStorageError("failed to check if given blocks are ancestors of the specified block", err)
	}

	return result, nil
}

// ancestorOnMainChainBelowAnchor is the cheap positive answer for CheckBlockIsAncestorOfBlock.
// When the anchor is on the main chain, its ancestors are exactly the main-chain blocks at or
// below its height, so one primary-key probe per id answers it however deep the id sits. The
// recursive walk costs one row per block between the anchor and the match.
//
// It only ever answers true. It finds nothing when the anchor is on a fork, when the anchor is
// unknown, or when no id is a main-chain block at or below the anchor, and the caller then walks.
// The anchor's flag and height are read in the same statement as the candidates, not from the
// header cache, so a reorg cannot slip between them. Callers must check mainChainRebuilding first:
// it trusts the on_main_chain flag exactly as GetBlockHeaderIDs' fast path does.
func (s *SQL) ancestorOnMainChainBelowAnchor(ctx context.Context, blockIDs []uint32, blockHash *chainhash.Hash) (bool, error) {
	for start := 0; start < len(blockIDs); start += maxIDsPerCheckBatch {
		end := start + maxIDsPerCheckBatch
		if end > len(blockIDs) {
			end = len(blockIDs)
		}

		batch := blockIDs[start:end]

		placeholders := make([]string, len(batch))
		args := make([]interface{}, 0, len(batch)+1)

		for i, id := range batch {
			placeholders[i] = fmt.Sprintf("$%d", i+1)
			args = append(args, id)
		}

		args = append(args, blockHash[:])

		q := fmt.Sprintf(`
			SELECT 1 FROM blocks b
			WHERE b.id IN (%s)
			  AND b.on_main_chain = true
			  AND b.height <= (SELECT a.height FROM blocks a WHERE a.hash = $%d AND a.on_main_chain = true)
			LIMIT 1`, strings.Join(placeholders, ","), len(batch)+1)

		var found int

		err := s.db.QueryRowContext(ctx, q, args...).Scan(&found)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				continue
			}

			return false, errors.NewStorageError("failed to check main-chain ancestors of the specified block", err)
		}

		return true, nil
	}

	return false, nil
}
