// Package sql implements the blockchain.Store interface using SQL database backends.
package sql

import (
	"context"
	"database/sql"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/util/tracing"
)

const (
	statusActive       = "active"        // The tip of the main chain
	statusValidFork    = "valid-fork"    // A valid fork that's not the main chain
	statusValidHeaders = "valid-headers" // Headers are valid but the block hasn't been fully validated
	statusHeadersOnly  = "headers-only"  // Only headers have been downloaded
	statusInvalid      = "invalid"       // The block is invalid
)

// GetChainTips retrieves information about all known tips in the block tree.
// This implements the blockchain.Store.GetChainTips interface method.
//
// The method identifies chain tips by finding blocks that have no children in the
// blocks table. It then determines which tip belongs to the main chain (highest
// chain_work) and calculates branch lengths for side chains by tracing back to
// find the common ancestor with the main chain.
//
// Parameters:
//   - ctx: Context for the database operation, allows for cancellation and timeouts
//
// Returns:
//   - []*model.ChainTip: Array of chain tip information
//   - error: Any error encountered during retrieval
func (s *SQL) GetChainTips(ctx context.Context) ([]*model.ChainTip, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "sql:GetChainTips")
	defer deferFn()

	// Try to get from response cache using derived cache key
	// Use operation-prefixed key to be consistent with other operations
	cacheID := chainhash.HashH([]byte("GetChainTips"))
	cacheOp := s.responseCache.Begin(cacheID)

	cached := cacheOp.Get()
	if cached != nil {
		if tips, ok := cached.Value().([]*model.ChainTip); ok {
			return tips, nil
		}
	}

	// First, get the best block (main chain tip) to determine which is active
	bestHeader, _, err := s.GetBestBlockHeader(ctx)
	if err != nil {
		return nil, errors.NewStorageError("failed to get best block header", err)
	}

	// Optimized query: Use LEFT JOIN anti-pattern instead of NOT EXISTS
	// This allows better use of idx_parent_id index and avoids correlated subquery
	// A block is a chain tip if no other block references it as parent
	q := `
		SELECT
			b.hash,
			b.height,
			b.chain_work,
			b.invalid,
			b.subtrees_set,
			b.processed_at IS NOT NULL as fully_processed
		FROM blocks b
		LEFT JOIN blocks children ON children.parent_id = b.id AND children.id != b.id
		WHERE children.id IS NULL
		ORDER BY b.chain_work DESC, b.id ASC
	`

	rows, err := s.db.QueryContext(ctx, q)
	if err != nil {
		return nil, errors.NewStorageError("failed to query chain tips", err)
	}
	defer rows.Close()

	var chainTips []*model.ChainTip

	for rows.Next() {
		var (
			hashBytes      []byte
			height         uint32
			chainWork      []byte
			invalid        bool
			subtreesSet    bool
			fullyProcessed bool
		)

		if err := rows.Scan(&hashBytes, &height, &chainWork, &invalid, &subtreesSet, &fullyProcessed); err != nil {
			return nil, errors.NewStorageError("failed to scan chain tip row", err)
		}

		// Convert hash bytes to chainhash.Hash for proper string representation
		tipHash, err := chainhash.NewHash(hashBytes)
		if err != nil {
			return nil, errors.NewStorageError("failed to create hash from bytes", err)
		}

		hash := tipHash.String()

		// For a block to be "valid-fork", it needs fullyProcessed = true
		// For a block to be "valid-headers", it needs subtreesSet = true

		/*
			Only fully processed blocks can be "valid-fork"
			Only blocks with subtrees set can be "valid-headers"
			Everything else starts as "headers-only" and gets upgraded based on these conditions
		*/

		// Determine status
		status := statusHeadersOnly // default

		switch {
		case invalid:
			status = statusInvalid // This branch contains at least one invalid block
		case tipHash.String() == bestHeader.Hash().String():
			status = statusActive // This is the tip of the active main chain
		case fullyProcessed:
			status = statusValidFork // This branch is not part of the active chain, but is fully validated
		case subtreesSet:
			status = statusValidHeaders // All blocks are available for this branch, but they were never fully validated
		}
		// If none of the above, it remains "headers-only" - Not all blocks for this branch are available, but the headers are valid

		// Calculate branch length for non-active tips
		branchLen := uint32(0)
		if status != statusActive {
			branchLen, err = s.calculateBranchLength(ctx, hashBytes, bestHeader.Hash())
			if err != nil {
				// Log error but continue with branchLen = 0
				s.logger.Warnf("Failed to calculate branch length for tip %s: %v", hash, err)
			}
		}

		chainTip := &model.ChainTip{
			Height:    height,
			Hash:      hash,
			Branchlen: branchLen,
			Status:    status,
		}

		chainTips = append(chainTips, chainTip)
	}

	if err := rows.Err(); err != nil {
		return nil, errors.NewStorageError("error iterating chain tip rows", err)
	}

	// Cache the result in response cache
	cacheOp.Set(chainTips, s.cacheTTL)

	return chainTips, nil
}

// calculateBranchLength calculates the length of a branch from a tip back to
// the common ancestor with the main chain.
func (s *SQL) calculateBranchLength(ctx context.Context, tipHashBytes []byte, mainChainTipHash *chainhash.Hash) (uint32, error) {
	// Convert tipHashBytes to chainhash.Hash
	tipHash, err := chainhash.NewHash(tipHashBytes)
	if err != nil {
		return 0, errors.NewStorageError("failed to create hash from bytes", err)
	}

	branchLength := uint32(0)
	currentHash := tipHash

	for {
		q := `SELECT parent_id, height FROM blocks WHERE hash = $1`

		var (
			parentID sql.NullInt64
			height   uint32
		)

		err := s.db.QueryRowContext(ctx, q, currentHash.CloneBytes()).Scan(&parentID, &height)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				// Reached genesis or orphaned block
				break
			}

			return 0, errors.NewStorageError("failed to query parent block", err)
		}

		if !parentID.Valid {
			// Reached genesis block
			break
		}

		branchLength++

		// Get parent hash
		q = `SELECT hash FROM blocks WHERE id = $1`

		var parentHashBytes []byte

		err = s.db.QueryRowContext(ctx, q, parentID.Int64).Scan(&parentHashBytes)
		if err != nil {
			return 0, errors.NewStorageError("failed to query parent hash", err)
		}

		currentHash, err = chainhash.NewHash(parentHashBytes)
		if err != nil {
			return 0, errors.NewStorageError("failed to create parent hash", err)
		}

		// Check if this block is in the main chain by walking back from main tip
		isInMainChain, err := s.isBlockInMainChain(ctx, currentHash, mainChainTipHash)
		if err != nil {
			return 0, errors.NewStorageError("failed to check if block is in main chain", err)
		}

		if isInMainChain {
			// Found common ancestor
			break
		}

		// Prevent infinite loops - limit to reasonable branch length
		if branchLength > 1000 {
			s.logger.Warnf("Branch length calculation exceeded 1000 blocks, stopping")
			break
		}
	}

	return branchLength, nil
}

// isBlockInMainChain checks if a given block is part of the main chain
// by walking back from the main chain tip
func (s *SQL) isBlockInMainChain(ctx context.Context, blockHash, mainChainTipHash *chainhash.Hash) (bool, error) {
	if blockHash.String() == mainChainTipHash.String() {
		return true, nil
	}

	currentHash := mainChainTipHash
	for i := 0; i < 1000; i++ { // Prevent infinite loops
		if currentHash.String() == blockHash.String() {
			return true, nil
		}

		// Get parent
		q := `SELECT parent_id FROM blocks WHERE hash = $1`

		var parentID sql.NullInt64

		err := s.db.QueryRowContext(ctx, q, currentHash.CloneBytes()).Scan(&parentID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return false, nil
			}

			return false, errors.NewStorageError("failed to query parent block", err)
		}

		if !parentID.Valid {
			// Reached genesis
			return false, nil
		}

		// Get parent hash
		q = `SELECT hash FROM blocks WHERE id = $1`

		var parentHashBytes []byte

		err = s.db.QueryRowContext(ctx, q, parentID.Int64).Scan(&parentHashBytes)
		if err != nil {
			return false, errors.NewStorageError("failed to query parent hash", err)
		}

		currentHash, err = chainhash.NewHash(parentHashBytes)
		if err != nil {
			return false, errors.NewStorageError("failed to create parent hash", err)
		}
	}

	return false, nil
}
