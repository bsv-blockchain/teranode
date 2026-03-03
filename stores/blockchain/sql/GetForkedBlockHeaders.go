// Package sql implements the blockchain.Store interface using SQL database backends.
// It provides concrete SQL-based implementations for all blockchain operations
// defined in the interface, with support for different SQL engines.
//
// This file implements the GetForkedBlockHeaders method, which retrieves block headers
// that are not part of a specific chain starting from a given block. This functionality
// is critical for handling blockchain forks, where multiple competing chains exist
// simultaneously. The implementation uses a recursive Common Table Expression (CTE) in SQL
// to efficiently identify blocks that are not part of the specified chain. This method is
// particularly important for Teranode's fork resolution mechanisms, chain reorganization
// processes, and maintaining consensus across the network by identifying and tracking
// competing chains.
package sql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/model/time"
	"github.com/bsv-blockchain/teranode/util/tracing"
)

// GetForkedBlockHeaders retrieves block headers that are not part of a specific chain.
// This implements the blockchain.Store.GetForkedBlockHeaders interface method.
//
// The method identifies and retrieves block headers that are not part of the chain that
// includes the specified starting block. This is essential for detecting and analyzing
// blockchain forks, where multiple competing chains exist simultaneously. Fork detection
// and analysis are critical components of Teranode's consensus mechanism and chain
// reorganization processes.
//
// The implementation first checks the blockchainCache for cached headers to optimize performance.
// If not found in cache, it executes a complex SQL query that uses a recursive Common Table
// Expression (CTE) to identify the blocks in the specified chain, then retrieves headers
// for blocks that are not part of this chain. This approach efficiently identifies fork blocks
// without requiring multiple database queries.
//
// Parameters:
//   - ctx: Context for the database operation, allowing for cancellation and timeouts
//   - blockHashFrom: The hash of the starting block that defines the chain of interest
//   - numberOfHeaders: The maximum number of fork headers to retrieve
//
// Returns:
//   - []*model.BlockHeader: An array of block headers that are not part of the specified chain
//   - []*model.BlockHeaderMeta: Corresponding metadata for each header including height and chainwork
//   - error: Any error encountered during retrieval, specifically:
//   - BlockNotFoundError if the starting block does not exist
//   - StorageError for database errors or data processing failures
func (s *SQL) GetForkedBlockHeaders(ctx context.Context, blockHashFrom *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "sql:GetForkedBlockHeaders")
	defer deferFn()

	// Try to get from response cache using derived cache key
	// Use operation-prefixed key to be consistent with other operations
	cacheID := chainhash.HashH([]byte(fmt.Sprintf("GetForkedBlockHeaders-%s-%d", blockHashFrom.String(), numberOfHeaders)))
	cacheOp := s.responseCache.Begin(cacheID)

	cached := cacheOp.Get()
	if cached != nil {
		if result, ok := cached.Value().([2]interface{}); ok {
			if headers, ok := result[0].([]*model.BlockHeader); ok {
				if metas, ok := result[1].([]*model.BlockHeaderMeta); ok {
					return headers, metas, nil
				}
			}
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	blockHeaders := make([]*model.BlockHeader, 0, numberOfHeaders)
	blockHeaderMetas := make([]*model.BlockHeaderMeta, 0, numberOfHeaders)

	q := `
		SELECT
			 b.version
			,b.block_time
			,b.nonce
			,b.previous_hash
			,b.merkle_root
			,b.n_bits
			,b.id
			,b.height
			,b.tx_count
			,b.size_in_bytes
			,b.peer_id
			,b.block_time
			,b.inserted_at
			,b.median_time_past
		FROM blocks b
		WHERE id NOT IN (
			SELECT id FROM blocks
			WHERE id IN (
				WITH RECURSIVE ChainBlocks AS (
					SELECT id, parent_id, height
					FROM blocks
					WHERE hash = $1
					UNION ALL
					SELECT bb.id, bb.parent_id, bb.height
					FROM blocks bb
					JOIN ChainBlocks cb ON bb.id = cb.parent_id
					WHERE bb.id != cb.id
				)
				SELECT id FROM ChainBlocks
			)
		)
		ORDER BY height DESC
		LIMIT $2
	`
	rows, err := s.db.QueryContext(ctx, q, blockHashFrom[:], numberOfHeaders)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return blockHeaders, blockHeaderMetas, nil
		}

		return nil, nil, errors.NewStorageError("failed to get headers", err)
	}

	defer rows.Close()

	var (
		hashPrevBlock  []byte
		hashMerkleRoot []byte
		nBits          []byte
		insertedAt     time.CustomTime
	)

	for rows.Next() {
		blockHeader := &model.BlockHeader{}
		blockHeaderMeta := &model.BlockHeaderMeta{}

		if err = rows.Scan(
			&blockHeader.Version,
			&blockHeader.Timestamp,
			&blockHeader.Nonce,
			&hashPrevBlock,
			&hashMerkleRoot,
			&nBits,
			&blockHeaderMeta.ID,
			&blockHeaderMeta.Height,
			&blockHeaderMeta.TxCount,
			&blockHeaderMeta.SizeInBytes,
			&blockHeaderMeta.Miner,
			&blockHeaderMeta.BlockTime,
			&insertedAt,
			&blockHeaderMeta.MedianTimePast,
		); err != nil {
			return nil, nil, errors.NewStorageError("failed to scan row", err)
		}

		bits, _ := model.NewNBitFromSlice(nBits)
		blockHeader.Bits = *bits

		blockHeader.HashPrevBlock, err = chainhash.NewHash(hashPrevBlock)
		if err != nil {
			return nil, nil, errors.NewProcessingError("failed to convert hashPrevBlock", err)
		}

		blockHeader.HashMerkleRoot, err = chainhash.NewHash(hashMerkleRoot)
		if err != nil {
			return nil, nil, errors.NewProcessingError("failed to convert hashMerkleRoot", err)
		}

		blockHeaders = append(blockHeaders, blockHeader)
		blockHeaderMetas = append(blockHeaderMetas, blockHeaderMeta)
	}

	// Cache the result in response cache
	cacheOp.Set([2]interface{}{blockHeaders, blockHeaderMetas}, s.cacheTTL)

	return blockHeaders, blockHeaderMetas, nil
}
