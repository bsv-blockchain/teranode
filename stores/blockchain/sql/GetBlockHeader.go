// Package sql implements the blockchain.Store interface using SQL database backends.
// It provides concrete SQL-based implementations for all blockchain operations
// defined in the interface, with support for different SQL engines.
package sql

import (
	"context"
	"database/sql"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/tracing"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

// GetBlockHeader retrieves a block header from the database by its hash.
// This implements the blockchain.Store.GetBlockHeader interface method.
//
// The method first checks an in-memory cache for the block header to avoid database queries.
// If not found in cache, it executes a SQL query to retrieve the header data,
// then reconstructs the header object with all required fields converted to appropriate types.
// Additionally, if a coinbase transaction is available, it extracts the miner information.
//
// Parameters:
//   - ctx: Context for the database operation, allows for cancellation and timeouts
//   - blockHash: The unique hash identifier of the block header to retrieve
//
// Returns:
//   - *model.BlockHeader: The complete block header data if found
//   - *model.BlockHeaderMeta: Metadata about the block including height, transaction count, and chainwork
//   - error: Any error encountered during retrieval, specifically:
//     * BlockNotFoundError if the block does not exist
//     * StorageError for database errors
//     * ProcessingError for data conversion errors
func (s *SQL) GetBlockHeader(ctx context.Context, blockHash *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "sql:GetBlockHeader")
	defer deferFn()

	header, meta := s.blocksCache.GetBlockHeader(*blockHash)
	if header != nil {
		return header, meta, nil
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	q := `
		SELECT
	   	 b.version
		,b.block_time
		,b.n_bits
		,b.nonce
		,b.previous_hash
		,b.merkle_root
		,b.size_in_bytes
		,b.coinbase_tx
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
		&blockHeader.Version,
		&blockHeader.Timestamp,
		&nBits,
		&blockHeader.Nonce,
		&hashPrevBlock,
		&hashMerkleRoot,
		&blockHeaderMeta.SizeInBytes,
		&coinbaseBytes,
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
