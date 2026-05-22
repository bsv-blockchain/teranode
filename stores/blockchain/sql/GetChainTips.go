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
func (s *SQL) GetChainTips(ctx context.Context) ([]*model.ChainTip, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "sql:GetChainTips")
	defer deferFn()

	// Try to get from response cache using derived cache key
	cacheID := chainhash.HashH([]byte("GetChainTips"))
	cacheOp := s.responseCache.Begin(cacheID)

	cached := cacheOp.Get()
	if cached != nil {
		if tips, ok := cached.Value().([]*model.ChainTip); ok {
			return tips, nil
		}
	}

	var (
		tips []*model.ChainTip
		err  error
	)

	if s.useInMemoryChainCheck {
		tips, err = s.getChainTipsUncached(ctx)
	} else {
		tips, err = s.getChainTipsSQL(ctx)
	}
	if err != nil {
		return nil, err
	}

	// Cache the result in response cache
	cacheOp.Set(tips, s.cacheTTL)

	return tips, nil
}

// getChainTipsUncached retrieves chain tips directly from the database, bypassing the
// GetChainTips response cache. Note that getBestBlockID may use its own caching; only the
// chain-tip query itself is guaranteed fresh. Uses in-memory offChainBlockIDs for branch length.
func (s *SQL) getChainTipsUncached(ctx context.Context) ([]*model.ChainTip, error) {
	_, bestHash, err := s.getBestBlockID(ctx)
	if err != nil {
		return nil, errors.NewStorageError("failed to get best block ID", err)
	}

	rows, err := s.queryChainTipRows(ctx)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return s.processChainTipRows(ctx, rows, func(tipHash *chainhash.Hash) bool {
		return *tipHash == *bestHash
	}, func(ctx context.Context, hashBytes []byte) (uint32, error) {
		return s.calculateBranchLength(ctx, hashBytes)
	})
}

// getChainTipsSQL uses GetBestBlockHeader and SQL-based chain walking (original behavior).
func (s *SQL) getChainTipsSQL(ctx context.Context) ([]*model.ChainTip, error) {
	bestHeader, _, err := s.GetBestBlockHeader(ctx)
	if err != nil {
		return nil, errors.NewStorageError("failed to get best block header", err)
	}

	rows, err := s.queryChainTipRows(ctx)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	mainChainTipHash := bestHeader.Hash()
	return s.processChainTipRows(ctx, rows, func(tipHash *chainhash.Hash) bool {
		return tipHash.String() == bestHeader.Hash().String()
	}, func(ctx context.Context, hashBytes []byte) (uint32, error) {
		return s.calculateBranchLengthSQL(ctx, hashBytes, mainChainTipHash)
	})
}

// queryChainTipRows returns the raw rows for chain tips.
func (s *SQL) queryChainTipRows(ctx context.Context) (*sql.Rows, error) {
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
	return rows, nil
}

type isActiveFn func(tipHash *chainhash.Hash) bool
type branchLenFn func(ctx context.Context, hashBytes []byte) (uint32, error)

// processChainTipRows processes chain tip rows using the provided active-check and branch-length functions.
func (s *SQL) processChainTipRows(ctx context.Context, rows *sql.Rows, isActive isActiveFn, branchLen branchLenFn) ([]*model.ChainTip, error) {
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

		tipHash, err := chainhash.NewHash(hashBytes)
		if err != nil {
			return nil, errors.NewStorageError("failed to create hash from bytes", err)
		}

		hash := tipHash.String()

		status := statusHeadersOnly
		switch {
		case invalid:
			status = statusInvalid
		case isActive(tipHash):
			status = statusActive
		case fullyProcessed:
			status = statusValidFork
		case subtreesSet:
			status = statusValidHeaders
		}

		branchLength := uint32(0)
		if status != statusActive {
			branchLength, err = branchLen(ctx, hashBytes)
			if err != nil {
				s.logger.Warnf("Failed to calculate branch length for tip %s: %v", hash, err)
			}
		}

		chainTips = append(chainTips, &model.ChainTip{
			Height:    height,
			Hash:      hash,
			Branchlen: branchLength,
			Status:    status,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, errors.NewStorageError("error iterating chain tip rows", err)
	}

	return chainTips, nil
}

// calculateBranchLength uses the in-memory offChainBlockIDs set for O(1) main-chain
// membership checks (used when useInMemoryChainCheck is true).
func (s *SQL) calculateBranchLength(ctx context.Context, tipHashBytes []byte) (uint32, error) {
	q := `SELECT id, parent_id FROM blocks WHERE hash = $1`

	var (
		currentID uint32
		parentID  uint32
	)
	if err := s.db.QueryRowContext(ctx, q, tipHashBytes).Scan(&currentID, &parentID); err != nil {
		return 0, errors.NewStorageError("failed to query tip block", err)
	}

	s.offChainBlockIDsMu.RLock()
	offChain := s.offChainBlockIDs
	s.offChainBlockIDsMu.RUnlock()

	const maxBranchWalk = 1000

	branchLength := uint32(0)
	for branchLength < maxBranchWalk {
		branchLength++

		if _, isOffChain := offChain[parentID]; !isOffChain {
			break
		}

		if parentID == currentID {
			break // genesis self-reference
		}
		var nextParentID uint32
		if err := s.db.QueryRowContext(ctx, `SELECT parent_id FROM blocks WHERE id = $1`, parentID).Scan(&nextParentID); err != nil {
			return branchLength, errors.NewStorageError("failed to query parent block", err)
		}
		currentID = parentID
		parentID = nextParentID
	}

	if branchLength >= maxBranchWalk {
		s.logger.Warnf("calculateBranchLength: hit %d iteration cap for tip starting at block %d — possible cycle or unexpectedly deep fork", maxBranchWalk, currentID)
	}

	return branchLength, nil
}

// calculateBranchLengthSQL uses SQL chain walking to find the common ancestor with
// the main chain (used when useInMemoryChainCheck is false — original behavior).
func (s *SQL) calculateBranchLengthSQL(ctx context.Context, tipHashBytes []byte, mainChainTipHash *chainhash.Hash) (uint32, error) {
	tipHash, err := chainhash.NewHash(tipHashBytes)
	if err != nil {
		return 0, errors.NewStorageError("failed to create hash from bytes", err)
	}

	branchLength := uint32(0)
	currentHash := tipHash

	var unusedHeight uint32
	for {
		var parentID sql.NullInt64
		err := s.db.QueryRowContext(ctx, `SELECT parent_id, height FROM blocks WHERE hash = $1`, currentHash.CloneBytes()).Scan(&parentID, &unusedHeight)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				break
			}
			return 0, errors.NewStorageError("failed to query parent block", err)
		}

		if !parentID.Valid {
			break
		}

		branchLength++

		var parentHashBytes []byte
		err = s.db.QueryRowContext(ctx, `SELECT hash FROM blocks WHERE id = $1`, parentID.Int64).Scan(&parentHashBytes)
		if err != nil {
			return 0, errors.NewStorageError("failed to query parent hash", err)
		}

		currentHash, err = chainhash.NewHash(parentHashBytes)
		if err != nil {
			return 0, errors.NewStorageError("failed to create parent hash", err)
		}

		isInMainChain, err := s.isBlockInMainChain(ctx, currentHash, mainChainTipHash)
		if err != nil {
			return 0, errors.NewStorageError("failed to check if block is in main chain", err)
		}
		if isInMainChain {
			break
		}

		if branchLength > 1000 {
			s.logger.Warnf("Branch length calculation exceeded 1000 blocks, stopping")
			break
		}
	}

	return branchLength, nil
}

// isBlockInMainChain checks if a given block is part of the main chain
// by walking back from the main chain tip (used by SQL fallback path).
func (s *SQL) isBlockInMainChain(ctx context.Context, blockHash, mainChainTipHash *chainhash.Hash) (bool, error) {
	if blockHash.String() == mainChainTipHash.String() {
		return true, nil
	}

	currentHash := mainChainTipHash
	for i := 0; i < 1000; i++ {
		if currentHash.String() == blockHash.String() {
			return true, nil
		}

		var parentID sql.NullInt64
		err := s.db.QueryRowContext(ctx, `SELECT parent_id FROM blocks WHERE hash = $1`, currentHash.CloneBytes()).Scan(&parentID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return false, nil
			}
			return false, errors.NewStorageError("failed to query parent block", err)
		}

		if !parentID.Valid {
			return false, nil
		}

		var parentHashBytes []byte
		err = s.db.QueryRowContext(ctx, `SELECT hash FROM blocks WHERE id = $1`, parentID.Int64).Scan(&parentHashBytes)
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
