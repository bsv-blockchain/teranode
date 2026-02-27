// Package sql implements the blockchain.Store interface using SQL database backends.
// It provides concrete SQL-based implementations for all blockchain operations
// defined in the interface, with support for different SQL engines.
//
// This file implements the GetBlockInChainByHeightHash method, which retrieves a block
// at a specific height in a particular chain identified by a starting block hash. This
// functionality is particularly important in blockchain systems like Teranode that support
// multiple competing chains (forks). The implementation uses a recursive Common Table
// Expression (CTE) in SQL to efficiently traverse a specific chain backward from the
// starting block, allowing retrieval of blocks from non-main chains. This is essential
// for fork resolution, chain comparison, and blockchain reorganization processes.
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

// GetBlockInChainByHeightHash retrieves a block at a specific height in a particular chain.
// This implements a specialized blockchain retrieval method not directly defined in the Store interface.
//
// This method allows retrieving a block at a specific height from a particular chain identified
// by a starting block hash, which may not be the main chain with the highest cumulative proof-of-work.
// This is particularly useful during blockchain reorganizations, fork analysis, or when validating
// alternative chains that may eventually become the main chain.
//
// The implementation uses a recursive SQL query to efficiently traverse the blockchain structure
// backward from the specified starting block, following parent-child relationships until it finds
// a block at the requested height. It also includes response caching to optimize performance for
// frequently requested blocks in specific chains.
//
// Parameters:
//   - ctx: Context for the database operation, allowing for cancellation and timeouts
//   - height: The blockchain height at which to retrieve the block
//   - startHash: The hash of a block in the chain of interest, typically the tip of that chain
//
// Returns:
//   - *model.Block: The complete block at the specified height in the specified chain, if found
//   - bool: Whether the block is marked as invalid in the database
//   - error: Any error encountered during retrieval, specifically:
//   - BlockNotFoundError if no block exists at the specified height in the specified chain
//   - StorageError for database or processing errors
func (s *SQL) GetBlockInChainByHeightHash(ctx context.Context, height uint32, startHash *chainhash.Hash) (block *model.Block, invalid bool, err error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "sql:GetBlockInChainByHeightHash",
		tracing.WithDebugLogMessage(s.logger, "[GetBlockInChainByHeightHash][%s:%d] called", startHash.String(), height),
	)
	defer deferFn()

	// the cache will be invalidated by the StoreBlock function when a new block is added, or after cacheTTL seconds
	cacheID := chainhash.HashH([]byte(fmt.Sprintf("GetBlockInChainByHeightHash-%d-%s", height, startHash.String())))

	cacheOp := s.responseCache.Begin(cacheID)
	cached := cacheOp.Get()
	if cached != nil && cached.Value() != nil {
		if cacheData, ok := cached.Value().(*model.Block); ok && cacheData != nil {
			s.logger.Debugf("[GetBlockInChainByHeightHash][%s:%d] cache hit", startHash.String(), height)
			return cacheData, false, nil
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	q := `
		WITH RECURSIVE ChainBlocks AS (
			SELECT id, parent_id, height
			FROM blocks
			WHERE hash = $2

			UNION ALL

			SELECT bb.id, bb.parent_id, bb.height
			FROM blocks bb
			JOIN ChainBlocks cb ON bb.id = cb.parent_id
			WHERE bb.id != cb.id
		)
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
		,b.invalid
		FROM blocks b
		WHERE b.id IN (
			SELECT id FROM ChainBlocks
			WHERE height = $1
			LIMIT 1
		)
	`

	block = &model.Block{
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
	)

	if err = s.db.QueryRowContext(ctx, q, height, startHash.CloneBytes()).Scan(
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
		&invalid,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, false, errors.NewBlockNotFoundError("[GetBlockInChainByHeightHash][%s:%d] failed to get block in-chain by height", startHash.String(), height, err)
		}

		return nil, false, errors.NewStorageError("[GetBlockInChainByHeightHash][%s:%d] failed to get block in-chain by height", startHash.String(), height, err)
	}

	bits, _ := model.NewNBitFromSlice(nBits)
	block.Header.Bits = *bits

	block.Header.HashPrevBlock, err = chainhash.NewHash(hashPrevBlock)
	if err != nil {
		return nil, false, errors.NewInvalidArgumentError("[GetBlockInChainByHeightHash][%s:%d] failed to convert hashPrevBlock: %s", startHash.String(), height, util.ReverseAndHexEncodeSlice(hashPrevBlock), err)
	}

	block.Header.HashMerkleRoot, err = chainhash.NewHash(hashMerkleRoot)
	if err != nil {
		return nil, false, errors.NewInvalidArgumentError("[GetBlockInChainByHeightHash][%s:%d] failed to convert hashMerkleRoot: %s", startHash.String(), height, util.ReverseAndHexEncodeSlice(hashMerkleRoot), err)
	}

	block.TransactionCount = transactionCount
	block.SizeInBytes = sizeInBytes

	if len(coinbaseTx) > 0 {
		block.CoinbaseTx, err = bt.NewTxFromBytes(coinbaseTx)
		if err != nil {
			return nil, false, errors.NewInvalidArgumentError("[GetBlockInChainByHeightHash][%s:%d] failed to convert coinbaseTx", startHash.String(), height, err)
		}
	}

	err = block.SubTreesFromBytes(subtreeBytes)
	if err != nil {
		return nil, false, errors.NewInvalidArgumentError("[GetBlockInChainByHeightHash][%s:%d] failed to convert subtrees", startHash.String(), height, err)
	}

	cacheOp.Set(block, s.cacheTTL)

	return block, invalid, nil
}
