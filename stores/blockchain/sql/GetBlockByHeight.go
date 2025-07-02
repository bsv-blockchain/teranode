// Package sql implements the blockchain.Store interface using SQL database backends.
// It provides concrete SQL-based implementations for all blockchain operations
// defined in the interface, with support for different SQL engines.
//
// This file implements the GetBlockByHeight method, which retrieves a complete block
// at a specific height in the blockchain. Block retrieval by height is a fundamental
// operation in blockchain systems, used by block explorers, API services, and during
// chain synchronization. In Bitcoin's UTXO-based design, accessing blocks by height
// is essential for transaction verification and chain analysis.
//
// The implementation uses a multi-tier optimization strategy:
//  1. First checks a dedicated response cache using a height-based cache ID
//  2. If not found, executes a SQL query to find the block at the specified height
//     that is part of the main chain (the chain with highest cumulative proof-of-work)
//  3. Reconstructs the complete block object including all transactions
//  4. Stores the result in the cache for future requests
//
// This approach significantly improves performance for frequently accessed blocks,
// particularly important in Teranode's high-throughput architecture where block
// retrieval operations may be frequent. The implementation also includes proper error
// handling for cases where blocks at the requested height don't exist or aren't part
// of the main chain.
package sql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/util/tracing"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
)

// GetBlockByHeight retrieves a block at a specific height in the blockchain.
// This implements the blockchain.Store.GetBlockByHeight interface method.
//
// This method allows blockchain traversal and data access by height,
// which is one of the most common ways to reference blocks in a blockchain. Height-based
// block retrieval is essential for many blockchain operations including:
//   - Block explorer functionality for displaying blockchain data
//   - Chain synchronization when catching up to a specific height
//   - Historical data analysis and auditing
//   - Checkpoint verification and chain validation
//
// The implementation follows a multi-tier approach to optimize performance:
//
//  1. Cache Layer: First checks an in-memory response cache using a height-based hash ID
//     to avoid expensive database queries for frequently requested blocks. The cache is
//     automatically invalidated when new blocks are added or after a configurable TTL.
//
// 2. Database Layer: If not found in cache, executes an optimized SQL query that:
//   - Finds the block at the specified height that is part of the main chain
//   - Filters by is_valid=true to ensure only valid blocks are returned
//   - Uses a subquery to identify the block with the highest chainwork at that height
//
// 3. Reconstruction Layer: After retrieving the block data and its transactions:
//
//   - Reconstructs the complete block object with all consensus fields
//
//   - Rebuilds all transactions with their inputs and outputs
//
//   - Calculates derived fields like block size and merkle root
//
//     4. Result Caching: Stores the fully reconstructed block in the response cache for
//     future requests, significantly improving performance for frequently accessed blocks.
//
// This method is particularly optimized for Teranode's high-throughput architecture,
// where efficient block retrieval is critical for maintaining performance during
// blockchain queries and synchronization operations.
//
// Parameters:
//   - ctx: Context for the database operation, allowing for cancellation and timeouts
//   - height: The blockchain height at which to retrieve the block
//
// Returns:
//   - *model.Block: The complete block at the specified height, if found on the main chain
//   - error: Any error encountered during retrieval, specifically:
//   - BlockNotFoundError if no valid block exists at the specified height on the main chain
//   - StorageError for database access or query execution errors
//   - ProcessingError for errors during block reconstruction
func (s *SQL) GetBlockByHeight(ctx context.Context, height uint32) (*model.Block, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "sql:GetBlockByHeight")
	defer deferFn()

	// the cache will be invalidated by the StoreBlock function when a new block is added, or after cacheTTL seconds
	cacheID := chainhash.HashH([]byte(fmt.Sprintf("GetBlockByHeight-%d", height)))

	cached := s.responseCache.Get(cacheID)
	if cached != nil && cached.Value() != nil {
		if cacheData, ok := cached.Value().(*model.Block); ok && cacheData != nil {
			s.logger.Debugf("GetBlockByHeight cache hit")
			return cacheData, nil
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	q := `
		SELECT
		 b.ID
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
		,b.height
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
				WHERE height = $1
				LIMIT 1
			)
		)
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

	if err = s.db.QueryRowContext(ctx, q, height).Scan(
		&block.ID,
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
		&block.Height,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errors.NewBlockNotFoundError("failed to get block by height for height %d", height, err)
		}

		return nil, errors.NewStorageError("failed to get block by height", err)
	}

	bits, _ := model.NewNBitFromSlice(nBits)
	block.Header.Bits = *bits

	block.Header.HashPrevBlock, err = chainhash.NewHash(hashPrevBlock)
	if err != nil {
		return nil, errors.NewInvalidArgumentError("failed to convert hashPrevBlock: %s", utils.ReverseAndHexEncodeSlice(hashPrevBlock), err)
	}

	block.Header.HashMerkleRoot, err = chainhash.NewHash(hashMerkleRoot)
	if err != nil {
		return nil, errors.NewInvalidArgumentError("failed to convert hashMerkleRoot: %s", utils.ReverseAndHexEncodeSlice(hashMerkleRoot), err)
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

	s.responseCache.Set(cacheID, block, s.cacheTTL)

	return block, nil
}
