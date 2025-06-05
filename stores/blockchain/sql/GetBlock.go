// Package sql implements the blockchain.Store interface using SQL database backends.
// It provides concrete SQL-based implementations for all blockchain operations
// defined in the interface, with support for different SQL engines.
package sql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/util/tracing"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

// getBlockCache is an internal struct used for caching block data.
// It stores both the block object and its height to avoid additional database queries.
type getBlockCache struct {
	block  *model.Block // Complete block data including header and transaction information
	height uint32       // Block height in the blockchain
}

// GetBlock retrieves a complete block from the database by its hash.
// It implements the blockchain.Store.GetBlock interface method.
//
// The method first checks an in-memory cache for the block to avoid database queries.
// If not found in cache, it executes a SQL query to retrieve the block data,
// reconstructs the full block object with header information, transaction count,
// subtrees, and other metadata, then adds it to the cache before returning.
//
// Parameters:
//   - ctx: Context for the database operation, allows for cancellation and timeouts
//   - blockHash: The unique hash identifier of the block to retrieve
//
// Returns:
//   - *model.Block: The complete block data if found
//   - uint32: The height of the block in the blockchain
//   - error: Any error encountered during retrieval, specifically:
//   - BlockNotFoundError if the block does not exist
//   - StorageError for other database or processing errors
func (s *SQL) GetBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.Block, uint32, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "sql:GetBlock")
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
	cacheID := chainhash.HashH([]byte(fmt.Sprintf("getBlock-%s", blockHash.String())))

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
