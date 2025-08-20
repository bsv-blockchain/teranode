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

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/tracing"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
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
// 1. Cache Layer: First checks the in-memory blocksCache for the requested header
//   - This cache is populated during block storage and previous retrievals
//   - Provides O(1) access time for frequently accessed headers
//   - Particularly effective for recent blocks and chain tips
//   - No cache expiration policy is applied as header data is immutable
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

	header, meta := s.blocksCache.GetBlockHeader(*blockHash)
	if header != nil {
		return header, meta, nil
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

	return blockHeader, blockHeaderMeta, nil
}
