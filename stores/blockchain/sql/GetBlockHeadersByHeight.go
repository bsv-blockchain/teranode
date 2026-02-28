// Package sql implements the blockchain.Store interface using SQL database backends.
// It provides concrete SQL-based implementations for all blockchain operations
// defined in the interface, with support for different SQL engines.
//
// This file implements the GetBlockHeadersByHeight method, which retrieves block headers
// within a specified height range. This functionality is essential for blockchain
// synchronization, chain analysis, and block explorer tools. The implementation efficiently
// retrieves multiple block headers in a single database query, reconstructs the header
// objects with their metadata, and handles edge cases such as empty results or invalid blocks.
// It's optimized for Teranode's high-throughput architecture where rapid access to
// consecutive blocks is often required for validation and synchronization processes.
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

// GetBlockHeadersByHeight retrieves block headers within a specified height range.
// This implements the blockchain.Store.GetBlockHeadersByHeight interface method.
//
// This method is designed to efficiently retrieve multiple block headers based on their
// height in the blockchain. It's particularly useful for blockchain synchronization,
// where a node needs to request blocks it's missing within a specific height range.
// Other common use cases include block explorer functionality, chain analysis tools,
// and validation of continuous chain segments.
//
// The implementation executes a single optimized SQL query to retrieve all headers
// within the specified range, filtering out invalid blocks to ensure only valid chain
// data is returned. The results are ordered by ascending height, providing a chronological
// view of the blockchain within the requested range.
//
// For each block, the method constructs both a BlockHeader object containing the core
// consensus fields and a BlockHeaderMeta object containing additional metadata such as
// height, transaction count, and chainwork. This separation allows clients to access
// either the consensus-critical data or the extended metadata as needed.
//
// Parameters:
//   - ctx: Context for the database operation, allowing for cancellation and timeouts
//   - startHeight: The lower bound of the height range (inclusive)
//   - endHeight: The upper bound of the height range (inclusive)
//
// Returns:
//   - []*model.BlockHeader: Slice of block headers within the specified height range
//   - []*model.BlockHeaderMeta: Slice of metadata for the corresponding block headers
//   - error: Any error encountered during retrieval, specifically:
//   - StorageError for database access or query execution errors
//   - nil if the operation was successful (even if no headers were found)
func (s *SQL) GetBlockHeadersByHeight(ctx context.Context, startHeight, endHeight uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "sql:GetBlockHeadersByHeight")
	defer deferFn()

	// Try to get from response cache using derived cache key
	// Use operation-prefixed key to be consistent with other operations
	cacheID := chainhash.HashH([]byte(fmt.Sprintf("GetBlockHeadersByHeight-%d-%d", startHeight, endHeight)))
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

	// Calculate capacity safely, avoiding uint32 underflow when startHeight > endHeight
	var capacity uint32 = 1
	if endHeight >= startHeight {
		capacity = max(1, endHeight-startHeight+1)
	}

	blockHeaders := make([]*model.BlockHeader, 0, capacity)
	blockMetas := make([]*model.BlockHeaderMeta, 0, capacity)

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
		WHERE id IN (
			SELECT id FROM blocks
			WHERE id IN (
				WITH RECURSIVE ChainBlocks AS (
					SELECT id, parent_id, height
					FROM blocks
					WHERE invalid = false
					AND hash = (
						SELECT b.hash
						FROM blocks b
						WHERE b.invalid = false
						ORDER BY chain_work DESC, peer_id ASC, id ASC
						LIMIT 1
					)
					UNION ALL
					SELECT bb.id, bb.parent_id, bb.height
					FROM blocks bb
					JOIN ChainBlocks cb ON bb.id = cb.parent_id
					WHERE bb.id != cb.id
					  AND bb.invalid = false
				)
				SELECT id FROM ChainBlocks
				WHERE height >= $1 AND height <= $2
				  AND invalid = FALSE
				ORDER BY height ASC
			)
		)
	`
	rows, err := s.db.QueryContext(ctx, q, startHeight, endHeight)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return blockHeaders, blockMetas, nil
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
			&blockHeaderMeta.PeerID,
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
		blockMetas = append(blockMetas, blockHeaderMeta)
	}

	// Cache the result in response cache
	cacheOp.Set([2]interface{}{blockHeaders, blockMetas}, s.cacheTTL)

	return blockHeaders, blockMetas, nil
}
