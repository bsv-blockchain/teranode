// Package sql implements the blockchain.Store interface using SQL database backends.
// It provides concrete SQL-based implementations for all blockchain operations
// defined in the interface, with support for different SQL engines.
//
// This file implements the GetBlockByID method, which retrieves a complete block from
// the blockchain database using its internal database ID. Unlike methods that retrieve
// blocks by hash or height, this method provides direct access to blocks using their
// database-specific identifier. The implementation uses a response caching strategy to
// optimize performance for repeated queries and includes comprehensive block reconstruction
// logic to convert raw database values into structured blockchain objects. This method
// supports Teranode's high-throughput architecture by providing efficient access to block
// data for internal operations that track blocks by their database IDs rather than by
// their cryptographic hashes or blockchain heights.
package sql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/tracing"
)

// GetBlockByID retrieves a complete block from the database using its internal database ID.
// This implements a specialized blockchain query method not directly defined in the Store interface.
//
// Unlike methods that retrieve blocks by hash or height, this method provides direct access
// to blocks using their database-specific identifier. This is particularly useful for internal
// operations where blocks are tracked by their database IDs rather than by their cryptographic
// hashes or blockchain heights. In Teranode's high-throughput architecture, this method
// supports efficient block retrieval for various internal processes including mining status
// updates, subtree processing, and database maintenance operations.
//
// The implementation employs a multi-tiered approach for optimal performance:
//
// 1. Response Cache Layer: First checks a dedicated response cache for recently accessed blocks
//   - Uses a hash of the query parameters as the cache key for consistent lookups
//   - Provides immediate response for frequently accessed blocks without database queries
//   - Cache entries are automatically invalidated when new blocks are added or after the TTL expires
//
// 2. Database Layer: If not found in cache, executes an optimized SQL query
//   - Retrieves all block fields in a single query using the database ID as the primary key
//   - Leverages database indexing for efficient retrieval by primary key
//   - Includes all necessary fields to reconstruct a complete block object
//
// 3. Block Reconstruction: Converts raw database values to structured blockchain types
//   - Handles binary-to-structured data conversions (hashes, difficulty bits, transactions)
//   - Processes subtree data specific to Teranode's transaction management architecture
//   - Constructs a complete block object with header, metadata, and transaction information
//
// 4. Cache Update: Stores the reconstructed block in the response cache for future queries
//   - Applies the configured cache TTL to balance freshness with performance
//   - Ensures consistent responses for repeated queries within the cache window
//
// Parameters:
//   - ctx: Context for the database operation, allowing for cancellation and timeouts
//   - id: The internal database ID of the block to retrieve
//
// Returns:
//   - *model.Block: A complete block object with header, metadata, and transaction information
//   - error: Any error encountered during retrieval, specifically:
//   - BlockNotFoundError if no block with the specified ID exists
//   - StorageError for database connection or query execution errors
//   - InvalidArgumentError for data conversion failures
func (s *SQL) GetBlockByID(ctx context.Context, id uint64) (*model.Block, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "sql:GetBlockByID")
	defer deferFn()

	// the cache will be invalidated by the StoreBlock function when a new block is added, or after cacheTTL seconds
	cacheID := chainhash.HashH([]byte(fmt.Sprintf("GetBlockByID-%d", id)))

	cacheOp := s.responseCache.Begin(cacheID)
	cached := cacheOp.Get()
	if cached != nil && cached.Value() != nil {
		if cacheData, ok := cached.Value().(*model.Block); ok && cacheData != nil {
			s.logger.Debugf("GetBlockByID cache hit")
			return cacheData, nil
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	q := `
		SELECT
		 b.ID
		,b.height
	    ,b.version
		,b.block_time
		,b.n_bits
        ,b.nonce
		,b.previous_hash
		,b.merkle_root
	    ,b.tx_count
		,b.size_in_bytes
		,b.coinbase_tx
		,b.subtree_count
		,b.subtrees
		FROM blocks b
		WHERE id = $1
	`

	block := &model.Block{
		Header: &model.BlockHeader{},
	}

	var (
		subtreeCount     uint64
		transactionCount uint64
		sizeInBytes      uint64
		subtreeBytes     []byte
		hashPrevBlock    []byte
		hashMerkleRoot   []byte
		coinbaseTx       []byte
		nBits            []byte
		err              error
	)

	if err = s.db.QueryRowContext(ctx, q, id).Scan(
		&block.ID,
		&block.Height,
		&block.Header.Version,
		&block.Header.Timestamp,
		&nBits,
		&block.Header.Nonce,
		&hashPrevBlock,
		&hashMerkleRoot,
		&transactionCount,
		&sizeInBytes,
		&coinbaseTx,
		&subtreeCount,
		&subtreeBytes,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errors.NewBlockNotFoundError("failed to get block by ID", err)
		}

		return nil, errors.NewStorageError("failed to get block by ID", err)
	}

	bits, _ := model.NewNBitFromSlice(nBits)
	block.Header.Bits = *bits

	block.Header.HashPrevBlock, err = chainhash.NewHash(hashPrevBlock)
	if err != nil {
		return nil, errors.NewInvalidArgumentError("failed to convert hashPrevBlock: %s", util.ReverseAndHexEncodeSlice(hashPrevBlock), err)
	}

	block.Header.HashMerkleRoot, err = chainhash.NewHash(hashMerkleRoot)
	if err != nil {
		return nil, errors.NewInvalidArgumentError("failed to convert hashMerkleRoot: %s", util.ReverseAndHexEncodeSlice(hashMerkleRoot), err)
	}

	block.TransactionCount = transactionCount
	block.SizeInBytes = sizeInBytes

	if len(coinbaseTx) > 0 {
		block.CoinbaseTx, err = bt.NewTxFromBytes(coinbaseTx)
		if err != nil {
			return nil, errors.NewInvalidArgumentError("failed to convert coinbaseTx", err)
		}
	}

	err = block.SubTreesFromBytes(subtreeBytes)
	if err != nil {
		return nil, errors.NewInvalidArgumentError("failed to convert subtrees", err)
	}

	cacheOp.Set(block, s.cacheTTL)

	return block, nil
}
