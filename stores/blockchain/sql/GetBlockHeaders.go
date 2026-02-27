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

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/model/time"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/tracing"
)

// GetBlockHeaders retrieves a sequence of consecutive block headers starting from a specified block hash.
// This implements the blockchain.Store.GetBlockHeaders interface method.
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
//   - blockHashFrom: The hash of the starting block for header retrieval
//   - numberOfHeaders: Maximum number of consecutive headers to retrieve
//
// Returns:
//   - []*model.BlockHeader: Slice of consecutive block headers starting from the specified block
//   - []*model.BlockHeaderMeta: Slice of metadata for the corresponding block headers
//   - error: Any error encountered during retrieval, specifically:
//   - StorageError for database access or query execution errors
//   - ProcessingError for errors during header reconstruction
//   - nil if the operation was successful (even if fewer headers than requested were found)
func (s *SQL) GetBlockHeaders(ctx context.Context, blockHashFrom *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "sql:GetBlockHeaders",
		tracing.WithDebugLogMessage(s.logger, "[GetBlockHeaders][%s] called for %d headers", blockHashFrom.String(), numberOfHeaders),
	)
	defer deferFn()

	// Try to get from response cache using derived cache key
	// Use operation-prefixed key to avoid conflicts with other cached data
	cacheID := chainhash.HashH([]byte(fmt.Sprintf("GetBlockHeaders-%s-%d", blockHashFrom.String(), numberOfHeaders)))
	cacheOp := s.responseCache.Begin(cacheID)

	cached := cacheOp.Get()
	if cached != nil {
		if result, ok := cached.Value().([2]interface{}); ok {
			if h, ok := result[0].([]*model.BlockHeader); ok {
				if m, ok := result[1].([]*model.BlockHeaderMeta); ok {
					return h, m, nil
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

	rows, err := s.db.QueryContext(ctx, q, blockHashFrom[:], numberOfHeaders)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return []*model.BlockHeader{}, []*model.BlockHeaderMeta{}, nil
		}

		return nil, nil, errors.NewStorageError("failed to get headers", err)
	}

	defer rows.Close()

	h, m, err := s.processBlockHeadersRows(rows, numberOfHeaders, false)
	if err != nil {
		return nil, nil, err
	}

	// Cache the result in response cache
	cacheOp.Set([2]interface{}{h, m}, s.cacheTTL)

	return h, m, nil
}

func (s *SQL) processBlockHeadersRows(rows *sql.Rows, numberOfHeaders uint64, hasCoinbaseColumn bool) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	var (
		hashPrevBlock  []byte
		hashMerkleRoot []byte
		nBits          []byte
		insertedAt     time.CustomTime
		coinbaseBytes  []byte
		processedAt    *time.CustomTime
	)

	blockHeaders := make([]*model.BlockHeader, 0, numberOfHeaders)
	blockHeaderMetas := make([]*model.BlockHeaderMeta, 0, numberOfHeaders)

	for rows.Next() {
		blockHeader := &model.BlockHeader{}
		blockHeaderMeta := &model.BlockHeaderMeta{}

		// Create scan targets
		scanTargets := []interface{}{
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
			&blockHeaderMeta.ChainWork,
			&blockHeaderMeta.MinedSet,
			&blockHeaderMeta.SubtreesSet,
			&blockHeaderMeta.Invalid,
			&processedAt,
			&blockHeaderMeta.MedianTimePast,
		}

		// Add coinbase_tx if it's in the query
		if hasCoinbaseColumn {
			scanTargets = append(scanTargets, &coinbaseBytes)
		}

		if err := rows.Scan(scanTargets...); err != nil {
			return nil, nil, errors.NewStorageError("failed to scan row", err)
		}

		// If miner is empty but we have coinbase_tx, extract miner from it
		if blockHeaderMeta.Miner == "" && len(coinbaseBytes) > 0 {
			coinbaseTx, err := bt.NewTxFromBytes(coinbaseBytes)
			if err == nil {
				extractedMiner, err := util.ExtractCoinbaseMiner(coinbaseTx)
				if err == nil && extractedMiner != "" {
					blockHeaderMeta.Miner = extractedMiner
				}
			}
		}

		bits, _ := model.NewNBitFromSlice(nBits)
		blockHeader.Bits = *bits

		var err error

		blockHeader.HashPrevBlock, err = chainhash.NewHash(hashPrevBlock)
		if err != nil {
			return nil, nil, errors.NewProcessingError("failed to convert hashPrevBlock", err)
		}

		blockHeader.HashMerkleRoot, err = chainhash.NewHash(hashMerkleRoot)
		if err != nil {
			return nil, nil, errors.NewProcessingError("failed to convert hashMerkleRoot", err)
		}

		insertedAtUint32, err := safeconversion.Int64ToUint32(insertedAt.Unix())
		if err != nil {
			return nil, nil, errors.NewProcessingError("failed to convert insertedAt", err)
		}

		blockHeaderMeta.Timestamp = insertedAtUint32

		// Set the block time to the timestamp in the meta
		blockHeaderMeta.BlockTime = blockHeader.Timestamp

		if processedAt != nil {
			blockHeaderMeta.ProcessedAt = &processedAt.Time
		}

		blockHeaders = append(blockHeaders, blockHeader)
		blockHeaderMetas = append(blockHeaderMetas, blockHeaderMeta)
	}

	return blockHeaders, blockHeaderMetas, nil
}
