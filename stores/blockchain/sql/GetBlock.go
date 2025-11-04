// Package sql implements the blockchain.Store interface using SQL database backends.
// It provides concrete SQL-based implementations for all blockchain operations
// defined in the interface, with support for different SQL engines.
//
// This file implements the GetBlock method, which retrieves complete block data
// from the database by hash. This functionality is essential for blockchain validation,
// transaction verification, and block exploration tools. The implementation includes
// sophisticated caching mechanisms to optimize performance for frequently accessed blocks,
// particularly important in Teranode's high-throughput architecture where rapid access
// to recent blocks is critical. It handles the reconstruction of complete block objects
// from their database representation, including header information, transaction data,
// and metadata such as height and chainwork. The method also includes special handling
// for database-specific errors and edge cases, ensuring consistent behavior across
// different SQL backends.
package sql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/util/tracing"
)

// getBlockCache is an internal struct used for caching block data.
// It stores both the block object and its height to avoid additional database queries.
// This caching mechanism is particularly important for Teranode's high-throughput
// architecture, where frequently accessed blocks (such as recent blocks in the main chain)
// can be served from memory rather than requiring repeated database access.
// The cache is invalidated when new blocks are added or during chain reorganizations.
type getBlockCache struct {
	block  *model.Block // Complete block data including header and transaction information
	height uint32       // Block height in the blockchain
}

// GetBlock retrieves a complete block from the database by its hash.
// This implements the blockchain.Store.GetBlock interface method.
//
// The method retrieves the full block data including header, transactions, and metadata.
// This is a fundamental operation in blockchain systems, used for block validation,
// transaction verification, chain analysis, and block explorer functionality. In Teranode's
// high-throughput architecture, efficient block retrieval is critical for maintaining
// performance during synchronization, validation, and serving API requests.
//
// The implementation uses a two-tier caching strategy to optimize performance:
// 1. First checks a dedicated response cache using a hash-based cache ID
// 2. If not found, executes optimized SQL queries to retrieve the block data
//
// The method handles reconstruction of the complete block object from its database
// representation, including header information, transaction data, and metadata such as
// height and chainwork. This reconstruction process involves:
// - Retrieving the block header and basic metadata
// - Fetching transaction data if available
// - Assembling the complete block structure
// - Caching the result for future requests
//
// Parameters:
//   - ctx: Context for the database operation, allowing for cancellation and timeouts
//   - blockHash: The unique hash identifier of the block to retrieve
//
// Returns:
//   - *model.Block: The complete block data if found, including header, transactions,
//     and metadata such as transaction count and size
//   - uint32: The height of the block in the blockchain
//   - error: Any error encountered during retrieval, specifically:
//   - BlockNotFoundError if the block does not exist in the database
//   - StorageError for database access or query execution errors
//   - ProcessingError for errors during block reconstruction
func (s *SQL) GetBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.Block, uint32, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "sql:GetBlock")
	defer deferFn()

	// header, meta, er := cache.GetBlock(*blockHash)
	// if er != nil {
	// 	return nil, 0, errors.NewStorageError("error in GetBlock", er)
	// }
	// if header != nil {
	// 	block := &model.Block{
	// 		Header:           header,
	// 		TransactionCount: meta.TxCount,
	// 		SizeInBytes:      meta.SizeInBytes,
	// 		Height:           meta.Height,
	// 	}
	// 	return block, meta.Height, nil
	// }

	// the cache will be invalidated by the StoreBlock function when a new block is added, or after cacheTTL seconds
	cacheID := chainhash.HashH([]byte(fmt.Sprintf("GetBlock-%s", blockHash.String())))

	cached := s.responseCache.Get(cacheID)
	if cached != nil && cached.Value() != nil {
		if cacheData, ok := cached.Value().(*getBlockCache); ok && cacheData != nil {
			s.logger.Debugf("GetBlock cache hit")
			return cacheData.block, cacheData.height, nil
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	q := `
		SELECT
		 b.id
	  ,b.version
		,b.block_time
		,b.n_bits
	  ,b.nonce
		,b.previous_hash
		,b.merkle_root
		,b.size_in_bytes
		,b.coinbase_tx
		,b.height
	  ,b.tx_count
		,b.subtree_count
		,b.subtrees
		FROM blocks b
		WHERE b.hash = $1
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
		height           uint32
		nBits            []byte
		err              error
	)

	if err = s.db.QueryRowContext(ctx, q, blockHash[:]).Scan(
		&block.ID,
		&block.Header.Version,
		&block.Header.Timestamp,
		&nBits,
		&block.Header.Nonce,
		&hashPrevBlock,
		&hashMerkleRoot,
		&sizeInBytes,
		&coinbaseTx,
		&height,
		&transactionCount,
		&subtreeCount,
		&subtreeBytes,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, 0, errors.NewBlockNotFoundError("error in GetBlock", err)
		}

		return nil, 0, errors.NewStorageError("error in GetBlock", err)
	}

	bits, _ := model.NewNBitFromSlice(nBits)
	block.Header.Bits = *bits

	block.Header.HashPrevBlock, err = chainhash.NewHash(hashPrevBlock)
	if err != nil {
		return nil, 0, errors.NewStorageError("failed to convert hashPrevBlock", err)
	}

	block.Header.HashMerkleRoot, err = chainhash.NewHash(hashMerkleRoot)
	if err != nil {
		return nil, 0, errors.NewStorageError("failed to convert hashMerkleRoot", err)
	}

	block.TransactionCount = transactionCount
	block.SizeInBytes = sizeInBytes

	if len(coinbaseTx) > 0 {
		block.CoinbaseTx, err = bt.NewTxFromBytes(coinbaseTx)
		if err != nil {
			return nil, 0, errors.NewStorageError("failed to convert coinbaseTx", err)
		}
	}

	err = block.SubTreesFromBytes(subtreeBytes)
	if err != nil {
		return nil, 0, errors.NewStorageError("failed to convert subtrees", err)
	}

	// set the block height on the block
	block.Height = height

	s.responseCache.Set(cacheID, &getBlockCache{
		block:  block,
		height: height,
	}, s.cacheTTL)

	return block, height, nil
}
