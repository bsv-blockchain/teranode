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

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/model/time"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/tracing"
)

// GetBlockHeader retrieves a block header from the database by its hash.
// This implements the blockchain.Store.GetBlockHeader interface method.
//
// Block headers are fundamental components in Bitcoin's blockchain architecture, containing
// critical metadata that forms the cryptographic chain of blocks. Each header is 80 bytes and
// contains six fields: version, previous block hash, merkle root, timestamp, difficulty target,
// and nonce. This method retrieves both the header and extended metadata about the block.
//
// The implementation follows a tiered retrieval strategy for optimal performance:
//
// 1. Cache Layer: First checks the in-memory response cache for the requested header
//   - This cache is populated during previous retrievals and related operations
//   - Provides O(1) access time for frequently accessed headers
//   - Particularly effective for recent blocks and chain tips
//   - Cache entries have a TTL and are automatically invalidated when blocks are added
//
// 2. Database Layer: If not found in cache, executes an optimized SQL query
//   - Retrieves all header fields plus additional metadata in a single query
//   - Uses parameterized queries to prevent SQL injection
//   - Employs indexed lookups by block hash for efficient retrieval
//
// 3. Reconstruction Phase: Converts raw database values to appropriate types
//   - Handles binary-to-structured data conversions (hashes, difficulty bits)
//   - Extracts miner information from coinbase transaction when available
//   - Populates both header and metadata objects
//
// This method is critical for multiple blockchain operations:
//   - Block validation: Verifying the integrity of new blocks
//   - Chain reorganization: Determining the valid chain during forks
//   - Transaction verification: Confirming transaction inclusion
//   - Peer synchronization: Responding to header requests from peers
//   - Mining: Providing template data for new block creation
//
// Parameters:
//   - ctx: Context for the database operation, allows for cancellation and timeouts
//   - blockHash: The unique hash identifier of the block header to retrieve
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
func (s *SQL) GetBlockHeader(ctx context.Context, blockHash *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "sql:GetBlockHeader")
	defer deferFn()

	// Try to get from response cache using derived cache key
	// Use operation-prefixed key to avoid conflicts with other cached data
	cacheID := chainhash.HashH([]byte(fmt.Sprintf("GetBlockHeader-%s", blockHash.String())))
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
		,b.peer_id
		,b.height
		,b.tx_count
		,b.chain_work
		,b.mined_set
		,b.subtrees_set
		,b.invalid
		,b.inserted_at
		,b.processed_at
		,b.median_time_past
		FROM blocks b
		WHERE b.hash = $1
	`

	blockHeader := &model.BlockHeader{}
	blockHeaderMeta := &model.BlockHeaderMeta{}

	var (
		hashPrevBlock  []byte
		hashMerkleRoot []byte
		nBits          []byte
		coinbaseBytes  []byte
		insertedAt     time.CustomTime
		processedAt    *time.CustomTime
		err            error
	)

	if err = s.db.QueryRowContext(ctx, q, blockHash[:]).Scan(
		&blockHeaderMeta.ID,
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
		&blockHeaderMeta.MinedSet,
		&blockHeaderMeta.SubtreesSet,
		&blockHeaderMeta.Invalid,
		&insertedAt,
		&processedAt,
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

	if processedAt != nil {
		blockHeaderMeta.ProcessedAt = &processedAt.Time
	}

	blockHeaderMeta.Timestamp = uint32(insertedAt.Unix())

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
