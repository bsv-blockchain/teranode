// Package sql implements the blockchain.Store interface using SQL database backends.
// It provides concrete SQL-based implementations for all blockchain operations
// defined in the interface, with support for different SQL engines.
//
// This file implements the GetBlockHeadersFromHeight method, which retrieves a sequence
// of block headers starting from a specified height in the blockchain. Block headers are
// lightweight representations of blocks containing only metadata without the full transaction
// data. This implementation uses the blockchainCache for optimized lookups and includes
// all headers at the specified heights, including those from fork chains. This method is
// particularly useful for blockchain synchronization, chain validation, and fork detection
// processes that require examining sequences of headers without the overhead of retrieving
// complete blocks.
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

// GetBlockHeadersFromHeight retrieves a sequence of block headers starting from a specified height.
// This implements the blockchain.Store.GetBlockHeadersFromHeight interface method.
//
// This method is critical for blockchain synchronization, fork detection, and chain analysis
// operations that need to examine blocks at specific heights. In Teranode's high-throughput
// architecture, efficient header retrieval by height is essential for maintaining consensus
// and handling chain reorganizations.
//
// Key features of this implementation include:
//
//  1. Multi-fork support: Unlike methods that follow a single chain, this method returns ALL
//     headers at each height, including those from competing fork chains. This comprehensive
//     view is essential for fork detection and resolution.
//
//  2. Descending order: Headers are returned in descending height order (newest to oldest),
//     which optimizes for the common case of needing the most recent blocks first.
//
// 3. Tiered performance optimization:
//
//   - First checks the blockchainCache for cached headers
//
//   - If not found, executes an optimized SQL query with height-based filtering
//
//   - Uses database-specific optimizations for both PostgreSQL and SQLite
//
//     4. Complete header reconstruction: For each block, constructs both a BlockHeader object
//     containing the core consensus fields and a BlockHeaderMeta object with additional metadata
//     such as height, transaction count, and chainwork.
//
// This method is particularly valuable during initial block download, when validating
// competing chains during reorganizations, and when serving block explorer requests that
// need to display all blocks at specific heights, including those on minority chains.
//
// Parameters:
//   - ctx: Context for the database operation, allowing for cancellation and timeouts
//   - height: The starting blockchain height from which to retrieve headers
//   - limit: The maximum number of heights to include in the retrieval
//
// Returns:
//   - []*model.BlockHeader: An array of block headers within the specified height range,
//     including headers from all fork chains at each height
//   - []*model.BlockHeaderMeta: Corresponding metadata for each header including height,
//     transaction count, size, and chainwork
//   - error: Any error encountered during retrieval, specifically:
//   - StorageError for database access or query execution errors
//   - ProcessingError for errors during header reconstruction
//   - nil if the operation was successful (even if no headers were found)
func (s *SQL) GetBlockHeadersFromHeight(ctx context.Context, height, limit uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "sql:GetBlockHeadersFromHeight")
	defer deferFn()

	// Try to get from response cache using derived cache key
	// Use operation-prefixed key to be consistent with other operations
	cacheID := chainhash.HashH([]byte(fmt.Sprintf("GetBlockHeadersFromHeight-%d-%d", height, limit)))
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

	// we are getting all forks, so we need a bit more than the limit
	blockHeaders := make([]*model.BlockHeader, 0, 2*limit)
	blockMetas := make([]*model.BlockHeaderMeta, 0, 2*limit)

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
		WHERE height >= $1 AND height < $2
		ORDER BY height DESC
	`

	rows, err := s.db.QueryContext(ctx, q, height, height+limit)
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
