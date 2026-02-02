// Package sql implements the blockchain.Store interface using SQL database backends.
// It provides concrete SQL-based implementations for all blockchain operations
// defined in the interface, with support for different SQL engines.
//
// This file implements the GetBestBlockHeader method, which is critical for blockchain
// consensus as it determines the current tip of the best chain. In Bitcoin's Nakamoto
// consensus model, the best chain is identified by the highest cumulative proof-of-work
// (chainwork), representing the chain that required the most computational effort to create.
//
// The GetBestBlockHeader method is one of the most frequently called operations in a
// blockchain node as it's essential for:
//   - Determining where to mine the next block
//   - Validating incoming blocks and transactions against the current chain state
//   - Synchronizing with peers by identifying the local chain tip
//   - Providing accurate chain tip information to API clients and block explorers
//
// The implementation uses a sophisticated multi-tier approach to optimize performance:
//  1. First checks a dedicated blocks cache to avoid database queries
//  2. If not in cache, executes an optimized SQL query with careful indexing
//  3. Uses deterministic tie-breaking logic for the rare case of equal chainwork
//     values between competing chain tips
//  4. Reconstructs the header and metadata with all required fields
//
// In Teranode's high-throughput architecture, this method's efficiency is particularly
// critical as it may be called thousands of times per second during peak operation,
// especially during block propagation and transaction validation processes.
package sql

import (
	"context"
	"database/sql"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/model/time"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/tracing"
)

// GetBestBlockHeader retrieves the header of the block at the tip of the best chain.
// This implements the blockchain.Store.GetBestBlockHeader interface method.
//
// This method is a cornerstone of blockchain consensus and synchronization, providing
// the current chain tip that represents the node's view of the canonical blockchain.
// In Bitcoin's Nakamoto consensus model, the best chain is determined by the highest
// cumulative proof-of-work (chainwork), not simply by block height. This ensures that
// the selected chain represents the one that required the most computational effort
// to create, making it the most difficult to attack or forge.
//
// The implementation follows a sophisticated multi-tier approach to optimize performance:
//
//  1. Cache Layer: First checks the response cache for the best block header to avoid
//     expensive database queries. This cache is carefully maintained with TTL-based expiration
//     and is automatically invalidated when the blockchain state changes (e.g., when new blocks
//     are added or during chain reorganizations).
//
// 2. Database Layer: If not found in cache, executes an optimized SQL query that:
//   - Filters for valid blocks only (is_valid = true)
//   - Orders by chainwork in descending order to find the highest
//   - Implements a deterministic tie-breaker for the rare case of equal chainwork
//     values between competing chain tips, using peer_id and block id
//   - Limits to a single result (the best block)
//
// 3. Reconstruction Layer: After retrieving the block data:
//   - Reconstructs the BlockHeader object with all consensus fields
//   - Builds the BlockHeaderMeta object with additional metadata
//   - Converts binary data to appropriate types (e.g., chainhash.Hash)
//
// 4. Cache Update: Updates the response cache with the best block header for future requests
//
// This method is particularly critical in Teranode's high-throughput architecture as it
// serves as the foundation for transaction validation, block assembly, mining operations,
// and peer synchronization. Its efficiency directly impacts the node's ability to maintain
// consensus with the network and process transactions at scale.
//
// Parameters:
//   - ctx: Context for the database operation, allowing for cancellation and timeouts
//
// Returns:
//   - *model.BlockHeader: The header of the best block in the chain, containing all
//     consensus-critical fields (version, timestamp, nonce, etc.)
//   - *model.BlockHeaderMeta: Metadata about the best block including height, transaction
//     count, size, chainwork, and other non-consensus fields
//   - error: Any error encountered during retrieval, specifically:
//   - BlockNotFoundError if no valid blocks exist in the database
//   - StorageError for database access or query execution errors
//   - ProcessingError for errors during header reconstruction
func (s *SQL) GetBestBlockHeader(ctx context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "sql:GetBestBlockHeader")
	defer deferFn()

	// Begin cache-safe query - captures generation to prevent stale writes
	cacheID := chainhash.HashH([]byte("GetBestBlockHeader"))
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

	var insertedAt time.CustomTime

	q := `
		SELECT
		 b.id
	    ,b.version
		,b.block_time
	    ,b.nonce
		,b.previous_hash
		,b.merkle_root
		,b.n_bits
		,b.height
		,b.tx_count
		,b.size_in_bytes
		,b.coinbase_tx
	    ,b.peer_id
		,b.chain_work
		,b.inserted_at
		,b.invalid
		,b.processed_at
		,b.median_time_past
		FROM blocks b
		WHERE invalid = false
		ORDER BY chain_work DESC, peer_id ASC, id ASC
		LIMIT 1
	`

	blockHeader := &model.BlockHeader{}
	blockHeaderMeta := &model.BlockHeaderMeta{}

	var (
		hashPrevBlock  []byte
		hashMerkleRoot []byte
		nBits          []byte
		coinbaseBytes  []byte
		processedAt    *time.CustomTime
		err            error
	)

	if err = s.db.QueryRowContext(ctx, q).Scan(
		&blockHeaderMeta.ID,
		&blockHeader.Version,
		&blockHeader.Timestamp,
		&blockHeader.Nonce,
		&hashPrevBlock,
		&hashMerkleRoot,
		&nBits,
		&blockHeaderMeta.Height,
		&blockHeaderMeta.TxCount,
		&blockHeaderMeta.SizeInBytes,
		&coinbaseBytes,
		&blockHeaderMeta.PeerID,
		&blockHeaderMeta.ChainWork,
		&insertedAt,
		&blockHeaderMeta.Invalid,
		&processedAt,
		&blockHeaderMeta.MedianTimePast,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil, errors.NewBlockNotFoundError("error in GetBestBlockHeader", err)
		}

		return nil, nil, errors.NewStorageError("error in GetBestBlockHeader", err)
	}

	bits, _ := model.NewNBitFromSlice(nBits)
	blockHeader.Bits = *bits

	blockHeader.HashPrevBlock, err = chainhash.NewHash(hashPrevBlock)
	if err != nil {
		return nil, nil, errors.NewStorageError("failed to convert hashPrevBlock", err)
	}

	blockHeader.HashMerkleRoot, err = chainhash.NewHash(hashMerkleRoot)
	if err != nil {
		return nil, nil, errors.NewStorageError("failed to convert hashMerkleRoot", err)
	}

	if len(coinbaseBytes) > 0 {
		coinbaseTx, err := bt.NewTxFromBytes(coinbaseBytes)
		if err != nil {
			return nil, nil, errors.NewStorageError("failed to convert coinbaseTx", err)
		}

		miner, err := util.ExtractCoinbaseMiner(coinbaseTx)
		if err != nil {
			return nil, nil, errors.NewStorageError("failed to extract miner", err)
		}

		blockHeaderMeta.Miner = miner
	}

	if processedAt != nil {
		blockHeaderMeta.ProcessedAt = &processedAt.Time
	}

	blockHeaderMeta.Timestamp = uint32(insertedAt.Unix())

	// Set the block time to the timestamp in the meta
	blockHeaderMeta.BlockTime = blockHeader.Timestamp

	// Cache result - CacheQuery.Set() checks if cache was invalidated during query
	result := [2]interface{}{blockHeader, blockHeaderMeta}
	cacheOp.Set(result, s.cacheTTL)

	return blockHeader, blockHeaderMeta, nil
}
