// Package sql implements the blockchain.Store interface using SQL database backends.
// It provides concrete SQL-based implementations for all blockchain operations
// defined in the interface, with support for different SQL engines.
//
// This file implements the GetBlockHeaders method, which retrieves a sequence of consecutive
// block headers starting from a specified block hash. This functionality is essential for
// blockchain synchronization, where nodes need to efficiently retrieve chains of headers
// to validate and update their local blockchain state. The implementation uses a recursive
// Common Table Expression (CTE) in SQL to efficiently traverse the blockchain graph structure,
// following the parent-child relationships between blocks. It also includes caching mechanisms
// to optimize performance for frequently requested header sequences and handles special cases
// like chain reorganizations and invalid blocks.
//
// In Teranode's high-throughput architecture, efficient header retrieval is critical for
// maintaining synchronization with the network, especially during initial block download
// or when recovering from network partitions. The method is designed to work efficiently
// with both PostgreSQL and SQLite database backends.
package sql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/util/tracing"
)

// GetBlockHeadersFromOldest retrieves a sequence of consecutive block headers starting from a specified block hash.
// This implements the blockchain.Store.GetBlockHeadersFromOldest interface method.
//
// Unlike the GetBlockHeaders method, which retrieves headers in reverse order,
// this method retrieves headers starting from the oldest block in the sequence and returns them in ascending order
// of height.
//
// This method is a cornerstone of blockchain synchronization, enabling nodes to efficiently
// retrieve chains of headers to validate and update their local blockchain state. In Bitcoin's
// headers-first synchronization model, nodes first download and validate headers before
// requesting the corresponding block data, making this method critical for maintaining
// network consensus.
//
// The implementation follows a tiered approach to optimize performance:
//  1. First checks the blocks cache for the requested headers sequence
//  2. If not found in cache, executes a SQL query to recursively traverse the blockchain graph structure
//  3. The query follows parent-child relationships between blocks, starting from the
//     specified block and retrieving the requested number of headers
//  4. For each block, constructs both a BlockHeader object containing the core consensus
//     fields and a BlockHeaderMeta object containing additional metadata
//
// The SQL implementation uses database-specific optimizations for both PostgreSQL and
// SQLite to ensure efficient execution of the recursive query. The method also handles
// special cases such as chain reorganizations and invalid blocks, ensuring that only
// valid headers are returned.
//
// Parameters:
//   - ctx: Context for the database operation, allowing for cancellation and timeouts
//   - chainTipHash: The hash of the current chain tip, used to determine the valid chain
//   - targetHash: The hash of the starting block for header retrieval
//   - numberOfHeaders: Maximum number of consecutive headers to retrieve
//
// Returns:
//   - []*model.BlockHeader: Slice of consecutive block headers starting from the specified block
//   - []*model.BlockHeaderMeta: Slice of metadata for the corresponding block headers
//   - error: Any error encountered during retrieval, specifically:
//   - StorageError for database access or query execution errors
//   - ProcessingError for errors during header reconstruction
//   - nil if the operation was successful (even if fewer headers than requested were found)
func (s *SQL) GetBlockHeadersFromOldest(ctx context.Context, chainTipHash, targetHash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "sql:GetBlockHeaders",
		tracing.WithLogMessage(s.logger, "[GetBlockHeaders][%s] called for %d headers", targetHash.String(), numberOfHeaders),
	)
	defer deferFn()

	// Try to get from response cache using derived cache key
	// Use operation-prefixed key to be consistent with other operations
	cacheID := chainhash.HashH([]byte(fmt.Sprintf("GetBlockHeadersFromOldest-%s-%s-%d", chainTipHash.String(), targetHash.String(), numberOfHeaders)))
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

	const q = `
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
			,b.chain_work
			,b.mined_set
			,b.subtrees_set
			,b.invalid
			,b.processed_at
			,b.median_time_past
			,b.coinbase_tx
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
			)
		)				        
		  AND id >= (SELECT id from blocks WHERE hash = $2)
		ORDER BY height ASC
		LIMIT $3
	`

	rows, err := s.db.QueryContext(ctx, q, chainTipHash[:], targetHash[:], numberOfHeaders)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return []*model.BlockHeader{}, []*model.BlockHeaderMeta{}, nil
		}

		return nil, nil, errors.NewStorageError("failed to get headers", err)
	}

	defer rows.Close()

	blockHeaders, blockMetas, err := s.processBlockHeadersRows(rows, numberOfHeaders, true)
	if err != nil {
		return nil, nil, err
	}

	// Cache the result in response cache
	cacheOp.Set([2]interface{}{blockHeaders, blockMetas}, s.cacheTTL)

	return blockHeaders, blockMetas, nil
}
