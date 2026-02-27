// Package sql implements the blockchain.Store interface using SQL database backends.
// It provides concrete SQL-based implementations for all blockchain operations
// defined in the interface, with support for different SQL engines.
//
// This file implements the GetBlockHeadersFromTill method, which retrieves a sequence
// of block headers between two specified blocks in the blockchain. This functionality
// is essential for blockchain synchronization, fork analysis, and chain validation
// processes that require examining a specific segment of the blockchain. The implementation
// uses a recursive Common Table Expression (CTE) in SQL to efficiently traverse the
// blockchain structure between the specified blocks, following parent-child relationships.
// This approach is particularly important in Teranode's high-throughput architecture,
// where efficient retrieval of blockchain segments is critical for maintaining consensus
// and handling chain reorganizations.
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

// GetBlockHeadersFromTill retrieves a sequence of block headers between two specified blocks.
// This implements the blockchain.Store.GetBlockHeadersFromTill interface method.
//
// This method is designed to retrieve a continuous chain of block headers between two
// specified blocks, which is particularly useful for blockchain synchronization, fork
// analysis, and chain validation processes. In Teranode's high-throughput architecture,
// efficient retrieval of blockchain segments is critical for maintaining consensus and
// handling chain reorganizations.
//
// The implementation follows these key steps:
//  1. First retrieves metadata for both the starting and ending blocks to determine
//     the expected number of headers in the sequence
//  2. Uses a recursive SQL Common Table Expression (CTE) to efficiently traverse the
//     blockchain structure between the specified blocks
//  3. The query follows parent-child relationships between blocks, starting from the
//     ending block and traversing backward until reaching the starting block
//  4. For each block in the sequence, constructs both a BlockHeader object containing
//     the core consensus fields and a BlockHeaderMeta object with additional metadata
//  5. Validates that the sequence forms a continuous chain by checking that the last
//     header in the result matches the expected starting block
//
// The headers are returned in descending height order (newest to oldest), which is
// consistent with Bitcoin's blockchain traversal convention where chains are typically
// explored from tip to genesis.
//
// Parameters:
//   - ctx: Context for the database operation, allowing for cancellation and timeouts
//   - blockHashFrom: The hash of the starting block (chronologically earlier)
//   - blockHashTill: The hash of the ending block (chronologically later)
//
// Returns:
//   - []*model.BlockHeader: Slice of block headers forming a continuous chain between
//     the specified blocks, ordered from newest to oldest
//   - []*model.BlockHeaderMeta: Slice of metadata for the corresponding block headers
//   - error: Any error encountered during retrieval, specifically:
//   - BlockNotFoundError if either the starting or ending block doesn't exist
//   - StorageError for database access or query execution errors
//   - ProcessingError if the headers don't form a continuous chain or during header reconstruction
func (s *SQL) GetBlockHeadersFromTill(ctx context.Context, blockHashFrom *chainhash.Hash, blockHashTill *chainhash.Hash) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "sql:GetBlockHeaders",
		tracing.WithLogMessage(s.logger, "[GetBlockHeadersFromTill] called for %s -> %s", blockHashFrom.String(), blockHashTill.String()),
	)
	defer deferFn()

	// Try to get from response cache using derived cache key
	// Use operation-prefixed key to be consistent with other operations
	cacheID := chainhash.HashH([]byte(fmt.Sprintf("GetBlockHeadersFromTill-%s-%s", blockHashFrom.String(), blockHashTill.String())))
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

	_, blockHeaderFromMeta, err := s.GetBlockHeader(ctx, blockHashFrom)
	if err != nil {
		return nil, nil, err
	}

	_, blockHeaderTillMeta, err := s.GetBlockHeader(ctx, blockHashTill)
	if err != nil {
		return nil, nil, err
	}

	numberOfHeaders := blockHeaderTillMeta.Height - blockHeaderFromMeta.Height + 1

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
		WHERE id IN (
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
				LIMIT $2
			)
		)
		ORDER BY height DESC
	`

	rows, err := s.db.QueryContext(ctx, q, blockHashTill[:], numberOfHeaders)
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
		blockHeaderMetas = append(blockHeaderMetas, blockHeaderMeta)
	}

	// make sure the last block header is the blockHashTill
	if blockHeaders[len(blockHeaders)-1].Hash().String() != blockHashFrom.String() {
		return nil, nil, errors.NewProcessingError("last block header is not the blockHashFrom", nil)
	}

	// Cache the result in response cache
	cacheOp.Set([2]interface{}{blockHeaders, blockHeaderMetas}, s.cacheTTL)

	return blockHeaders, blockHeaderMetas, nil
}
