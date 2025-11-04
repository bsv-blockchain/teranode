// Package sql implements the blockchain.Store interface using SQL database backends.
// It provides concrete SQL-based implementations for all blockchain operations
// defined in the interface, with support for different SQL engines.
//
// This file implements the GetHeader method, which provides a lightweight alternative
// to GetBlockHeader by retrieving only the essential header fields without additional
// metadata. This implementation follows the same caching strategy and database access
// pattern as GetBlockHeader but is optimized for scenarios where only the core header
// data is needed, reducing both memory usage and processing overhead.
package sql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/util/tracing"
)

// GetHeader retrieves only the essential block header data from the database by its hash.
// This method provides a lightweight alternative to GetBlockHeader when only the core
// header fields are needed without additional metadata.
//
// The implementation follows the same tiered retrieval strategy as GetBlockHeader:
//
// 1. Cache Layer: First checks the response cache for the requested header
//   - Provides immediate response for recently accessed headers
//   - Cache entries are automatically invalidated when new blocks are added or after TTL expires
//
// 2. Database Layer: If not found in cache, executes a streamlined SQL query
//   - Retrieves only the six essential header fields (version, timestamp, nonce,
//     previous hash, merkle root, and difficulty bits)
//   - Omits additional metadata fields like height, transaction count, and chainwork
//   - Uses the same indexed lookup by block hash for efficient retrieval
//
// 3. Reconstruction Phase: Converts raw database values to appropriate types
//   - Handles the same binary-to-structured data conversions as GetBlockHeader
//   - Does not extract or process any coinbase transaction data
//
// 4. Cache Update: Stores the reconstructed header in the response cache for future queries
//
// This method is particularly useful for performance-critical operations where:
//   - Only the cryptographic chain verification is needed
//   - Header validation must be performed without block positioning context
//   - Memory usage needs to be minimized for large-scale header processing
//   - Network bandwidth must be conserved when transmitting headers
//
// Parameters:
//   - ctx: Context for the database operation, allowing for cancellation and timeouts
//   - blockHash: The unique hash identifier of the block header to retrieve
//
// Returns:
//   - *model.BlockHeader: The block header data including only the six essential fields
//     (version, previous block hash, merkle root, timestamp, difficulty target, and nonce)
//   - error: Any error encountered during retrieval, specifically:
//   - BlockNotFoundError if the block does not exist in the database
//   - StorageError for database connection or query execution errors
//   - ProcessingError for data conversion or parsing errors
func (s *SQL) GetHeader(ctx context.Context, blockHash *chainhash.Hash) (*model.BlockHeader, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "sql:GetHeader")
	defer deferFn()

	// the cache will be invalidated by the StoreBlock function when a new block is added, or after cacheTTL seconds
	cacheID := chainhash.HashH([]byte(fmt.Sprintf("GetHeader-%s", blockHash.String())))

	cached := s.responseCache.Get(cacheID)
	if cached != nil && cached.Value() != nil {
		if cacheData, ok := cached.Value().(*model.BlockHeader); ok && cacheData != nil {
			s.logger.Debugf("GetHeader cache hit")
			return cacheData, nil
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	q := `
		SELECT
	     b.version
		,b.block_time
	    ,b.nonce
		,b.previous_hash
		,b.merkle_root
		,b.n_bits
		FROM blocks b
		WHERE b.hash = $1
	`

	blockHeader := &model.BlockHeader{}

	var (
		hashPrevBlock  []byte
		hashMerkleRoot []byte
		nBits          []byte
	)

	if err := s.db.QueryRowContext(ctx, q, blockHash[:]).Scan(
		&blockHeader.Version,
		&blockHeader.Timestamp,
		&blockHeader.Nonce,
		&hashPrevBlock,
		&hashMerkleRoot,
		&nBits,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// add not found error in error chain
			return nil, errors.NewBlockNotFoundError("error in GetHeader", errors.ErrNotFound)
		}

		return nil, errors.NewStorageError("error in GetHeader", err)
	}

	var err error

	blockHeader.HashPrevBlock, err = chainhash.NewHash(hashPrevBlock)
	if err != nil {
		return nil, errors.NewProcessingError("failed to convert hashPrevBlock", err)
	}

	blockHeader.HashMerkleRoot, err = chainhash.NewHash(hashMerkleRoot)
	if err != nil {
		return nil, errors.NewProcessingError("failed to convert hashMerkleRoot", err)
	}

	bits, _ := model.NewNBitFromSlice(nBits)
	blockHeader.Bits = *bits

	// Cache the result in response cache
	s.responseCache.Set(cacheID, blockHeader, s.cacheTTL)

	return blockHeader, nil
}
