// Package sql implements the blockchain.Store interface using SQL database backends.
// It provides concrete SQL-based implementations for all blockchain operations
// defined in the interface, with support for different SQL engines.
//
// This file implements the GetBlockHeader method, which retrieves block headers by their
// hash. Block headers are lightweight representations of blocks containing only the metadata
// without the full transaction data. In Bitcoin's design, block headers serve as cryptographic
// links in the blockchain, containing the previous block's hash to form the chain structure.
//
// The implementation employs a multi-tiered approach to optimize performance:
//
//  1. An in-memory cache layer for frequently accessed headers, dramatically reducing
//     database load for common lookup patterns such as recent blocks and chain tips
//
//  2. Efficient SQL queries optimized for header retrieval by hash, which is a fundamental
//     operation in blockchain synchronization and validation
//
//  3. Comprehensive metadata extraction including height, transaction count, and cumulative
//     proof-of-work (chainwork) to support consensus operations
//
// 4. Miner identification through coinbase transaction parsing when available
//
// This implementation is critical for Teranode's high-throughput architecture where header
// lookups occur frequently during block validation, chain reorganization, transaction
// verification, and peer synchronization processes. The performance of this method directly
// impacts the node's ability to maintain consensus and process transactions efficiently.
package sql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/lib/pq"
)

// GetLatestBlockHeaderFromBlockLocator retrieves the latest block header from the database by a block locator.
//
// Parameters:
//   - ctx: Context for the database operation, allows for cancellation and timeouts
//   - bestBlockHash: The best block hash to start the search from, backwards through the blockchain
//
// Returns:
//   - *model.BlockHeader: The complete block header data including version, previous block hash,
//     merkle root, timestamp, difficulty target (nBits), and nonce
//   - *model.BlockHeaderMeta: Extended metadata about the block including:
//   - Height: The block's position in the blockchain
//   - TxCount: Number of transactions in the block
//   - ChainWork: Cumulative proof-of-work up to this block (critical for consensus)
//   - SizeInBytes: Total size of the block
//   - Miner: Identification of the miner who produced the block (when available)
//   - error: Any error encountered during retrieval, specifically:
//   - BlockNotFoundError if the block does not exist in the database
//   - StorageError for database connection or query execution errors
//   - ProcessingError for data conversion or parsing errors
func (s *SQL) GetLatestBlockHeaderFromBlockLocator(ctx context.Context, bestBlockHash *chainhash.Hash, blockLocator []chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "sql:GetLatestBlockHeaderFromBlockLocator")
	defer deferFn()

	if len(blockLocator) == 0 {
		return nil, nil, errors.NewProcessingError("blockLocator cannot be empty")
	}

	// Try to get from response cache using derived cache key
	// Use operation-prefixed key to be consistent with other operations
	locatorStrs := make([]string, len(blockLocator))
	for i, hash := range blockLocator {
		locatorStrs[i] = hash.String()
	}
	cacheID := chainhash.HashH([]byte(fmt.Sprintf("GetLatestHeaderFromBlockLocator-%s-%s", bestBlockHash.String(), strings.Join(locatorStrs, ","))))
	cacheOp := s.responseCache.Begin(cacheID)

	cached := cacheOp.Get()
	if cached != nil {
		if result, ok := cached.Value().([2]interface{}); ok {
			if header, ok := result[0].(*model.BlockHeader); ok {
				if meta, ok := result[1].(*model.BlockHeaderMeta); ok {
					return header, meta, nil
				}
			}
		}
	}

	var q string
	var args []interface{}

	baseQuery := `
		SELECT
	   	 b.version
		,b.block_time
		,b.n_bits
		,b.nonce
		,b.previous_hash
		,b.merkle_root
		,b.size_in_bytes
		,b.coinbase_tx
		,b.peer_id
		,b.height
		,b.tx_count
		,b.chain_work
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
			)
		)`

	if s.engine == util.Postgres {
		// Convert []chainhash.Hash to [][]byte
		hashBytes := make([][]byte, len(blockLocator))
		for i, hash := range blockLocator {
			hashBytes[i] = hash[:]
		}

		q = baseQuery + `
			AND b.hash = ANY($2)
			ORDER BY height DESC
			LIMIT 1`
		args = []interface{}{bestBlockHash[:], pq.Array(hashBytes)}
	} else {
		// SQLite: Generate dynamic placeholders for IN clause
		placeholders := make([]string, len(blockLocator))
		args = make([]interface{}, len(blockLocator)+1)
		args[0] = bestBlockHash[:]

		for i, hash := range blockLocator {
			placeholders[i] = fmt.Sprintf("$%d", i+2)
			args[i+1] = hash[:]
		}

		q = baseQuery + fmt.Sprintf(`
			AND b.hash IN (%s)
			ORDER BY height DESC
			LIMIT 1`, strings.Join(placeholders, ","))
	}

	blockHeader := &model.BlockHeader{}
	blockHeaderMeta := &model.BlockHeaderMeta{}

	var (
		hashPrevBlock  []byte
		hashMerkleRoot []byte
		nBits          []byte
		coinbaseBytes  []byte
		err            error
	)

	if err = s.db.QueryRowContext(ctx, q, args...).Scan(
		&blockHeader.Version,
		&blockHeader.Timestamp,
		&nBits,
		&blockHeader.Nonce,
		&hashPrevBlock,
		&hashMerkleRoot,
		&blockHeaderMeta.SizeInBytes,
		&coinbaseBytes,
		&blockHeaderMeta.PeerID,
		&blockHeaderMeta.Height,
		&blockHeaderMeta.TxCount,
		&blockHeaderMeta.ChainWork,
		&blockHeaderMeta.MedianTimePast,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil, errors.NewBlockNotFoundError("error in GetBlockHeader", errors.ErrNotFound)
		}

		return nil, nil, errors.NewStorageError("error in GetBlockHeader", err)
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

	if len(coinbaseBytes) > 0 {
		coinbaseTx, err := bt.NewTxFromBytes(coinbaseBytes)
		if err != nil {
			return nil, nil, errors.NewProcessingError("failed to convert coinbaseTx", err)
		}

		miner, err := util.ExtractCoinbaseMiner(coinbaseTx)
		if err != nil {
			return nil, nil, errors.NewProcessingError("failed to extract miner", err)
		}

		blockHeaderMeta.Miner = miner
	}

	// Cache the result in response cache
	cacheOp.Set([2]interface{}{blockHeader, blockHeaderMeta}, s.cacheTTL)

	return blockHeader, blockHeaderMeta, nil
}
