// Package sql implements the blockchain.Store interface using SQL database backends.
// It provides concrete SQL-based implementations for all blockchain operations
// defined in the interface, with support for different SQL engines.
//
// This file implements the GetSuitableBlock method, which retrieves information needed
// for mining new blocks. In Bitcoin-based blockchains like BSV, mining requires knowledge
// of the current blockchain state, including difficulty targets and timestamps from recent
// blocks. The implementation uses a recursive Common Table Expression (CTE) in SQL to
// efficiently retrieve a sequence of blocks from the current tip backward, then calculates
// the median time and appropriate difficulty target based on Bitcoin consensus rules.
// This functionality is essential for Teranode's mining support and integration with
// mining pools.
package sql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/tracing"
)

// GetSuitableBlock retrieves information needed for mining a new block.
// This implements the blockchain.Store.GetSuitableBlock interface method.
//
// The method retrieves data necessary for mining operations, including the appropriate
// difficulty target (nBits) and median time of recent blocks. In Bitcoin-based blockchains,
// mining requires adherence to consensus rules regarding difficulty adjustments and
// timestamp validation, which depend on the state of recent blocks in the chain.
//
// The implementation uses a recursive SQL query to efficiently retrieve a sequence of blocks
// from the specified starting block backward. It then processes these blocks to calculate
// the median time (used for timestamp validation) and determine the appropriate difficulty
// target based on Bitcoin consensus rules. This information is essential for miners to
// construct valid block headers and perform proof-of-work calculations.
//
// Parameters:
//   - ctx: Context for the database operation, allowing for cancellation and timeouts
//   - hash: The hash of the current tip block from which to calculate mining parameters
//
// Returns:
//   - *model.SuitableBlock: A structure containing the necessary information for mining,
//     including block height, previous block hash, difficulty target, and median time
//   - error: Any error encountered during retrieval or calculation, specifically:
//   - BlockNotFoundError if the specified block doesn't exist
//   - StorageError for database errors or processing failures
func (s *SQL) GetSuitableBlock(ctx context.Context, hash *chainhash.Hash) (*model.SuitableBlock, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "sql:GetSuitableBlock")
	defer deferFn()

	// Try to get from response cache using derived cache key
	// Use operation-prefixed key to be consistent with other operations
	cacheID := chainhash.HashH([]byte(fmt.Sprintf("GetSuitableBlock-%s", hash.String())))

	cached := s.responseCache.Get(cacheID)
	if cached != nil {
		if suitableBlock, ok := cached.Value().(*model.SuitableBlock); ok {
			return suitableBlock, nil
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		id       int
		parentID int
	)

	q := `WITH RECURSIVE block_chain AS (
		SELECT
			id,
			hash,
			parent_id,
			n_bits,
			height,
			block_time,
			chain_work,
			1 as depth
		FROM
			blocks
		WHERE
			hash = $1

		UNION ALL

		SELECT
			b.id,
			b.hash,
			b.parent_id,
			b.n_bits,
			b.height,
			b.block_time,
			b.chain_work,
			bc.depth + 1
		FROM
			blocks b
		INNER JOIN
			block_chain bc ON b.id = bc.parent_id
		WHERE
			bc.depth < 3
	)
	SELECT
		id,
		hash,
		parent_id,
		n_bits,
		height,
		block_time,
		chain_work
	FROM
		block_chain
	ORDER BY
		depth DESC
	`

	rows, err := s.db.QueryContext(ctx, q, hash[:])
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil // TODO should this be an ErrNotFound error?
		}

		return nil, errors.NewStorageError("failed to get suitableBlock", err)
	}
	defer rows.Close()

	var suitableBlockCandidates []*model.SuitableBlock

	for rows.Next() {
		suitableBlock := &model.SuitableBlock{}

		if err = rows.Scan(
			&id,
			&suitableBlock.Hash,
			&parentID,
			&suitableBlock.NBits,
			&suitableBlock.Height,
			&suitableBlock.Time,
			&suitableBlock.ChainWork,
		); err != nil {
			return nil, errors.NewStorageError("failed to scan row", err)
		}

		suitableBlockCandidates = append(suitableBlockCandidates, suitableBlock)
	}

	// we have 3 candidates - now sort them by time and choose the median
	b, err := getMedianBlock(suitableBlockCandidates)
	if err != nil {
		return nil, errors.NewProcessingError("failed to get median block", err)
	}

	// Cache the result in response cache
	s.responseCache.Set(cacheID, b, s.cacheTTL)

	return b, nil
}

// getMedianBlock determines the median block from a set of blocks based on timestamp.
// This is a helper function used by GetSuitableBlock to implement Bitcoin consensus rules.
//
// In Bitcoin-based blockchains, the median time of recent blocks is used for timestamp
// validation and difficulty adjustment calculations. This function sorts the provided blocks
// by timestamp and returns the median block, which is used to determine the minimum timestamp
// allowed for a new block according to consensus rules.
//
// Parameters:
//   - blocks: An array of SuitableBlock structures containing block information
//     (expected to contain exactly 3 blocks for Bitcoin consensus rules)
//
// Returns:
//   - *model.SuitableBlock: The median block based on timestamp sorting
//   - error: ProcessingError if there aren't exactly 3 blocks provided
func getMedianBlock(blocks []*model.SuitableBlock) (*model.SuitableBlock, error) {
	if len(blocks) != 3 {
		return nil, errors.NewProcessingError("not enough candidates for suitable block. have %d, need 3", len(blocks))
	}

	util.SortForDifficultyAdjustment(blocks)

	// Return the middle block (out of 3)
	return blocks[1], nil
}
