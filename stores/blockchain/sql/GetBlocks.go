// Package sql implements the blockchain.Store interface using SQL database backends.
// It provides concrete SQL-based implementations for all blockchain operations
// defined in the interface, with support for different SQL engines.
//
// This file implements the GetBlocks method, which retrieves a sequence of consecutive
// blocks from the blockchain starting from a specified block hash. The implementation
// uses a recursive Common Table Expression (CTE) in SQL to efficiently traverse the
// blockchain structure by following the parent-child relationships between blocks.
// This method is particularly useful for blockchain synchronization, chain analysis,
// and block validation processes that require examining sequences of blocks.
package sql

import (
	"context"
	"database/sql"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/util/tracing"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

// GetBlocks retrieves a sequence of consecutive blocks from the blockchain.
// This implements the blockchain.Store.GetBlocks interface method.
//
// The method retrieves a specified number of blocks starting from a given block hash
// and traversing backward through the blockchain (from newer to older blocks). It uses
// a recursive SQL query to efficiently navigate the blockchain structure by following
// the parent-child relationships between blocks.
//
// For each block, the method retrieves the complete block data including header information,
// transaction count, size, and other metadata. This comprehensive retrieval is particularly
// useful for blockchain synchronization processes, chain analysis tools, and validation
// operations that require examining sequences of blocks.
//
// Parameters:
//   - ctx: Context for the database operation, allowing for cancellation and timeouts
//   - blockHashFrom: The hash of the starting block from which to retrieve the sequence
//   - numberOfHeaders: The maximum number of blocks to retrieve in the sequence
//
// Returns:
//   - []*model.Block: An array of complete block objects in the sequence
//   - error: Any error encountered during retrieval, specifically:
//   - BlockNotFoundError if the starting block does not exist
//   - StorageError for database errors or data processing failures
func (s *SQL) GetBlocks(ctx context.Context, blockHashFrom *chainhash.Hash, numberOfHeaders uint32) ([]*model.Block, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "sql:GetBlocks")
	defer deferFn()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	blocks := make([]*model.Block, 0, numberOfHeaders)

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
					WHERE hash = $1
					UNION ALL
					SELECT bb.id, bb.parent_id, bb.height
					FROM blocks bb
					JOIN ChainBlocks cb ON bb.id = cb.parent_id
					WHERE bb.id != cb.id
				)
				SELECT id FROM ChainBlocks
				LIMIT $2
			)
		)
		ORDER BY height DESC
	`

	rows, err := s.db.QueryContext(ctx, q, blockHashFrom[:], numberOfHeaders)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return blocks, nil
		}

		return nil, errors.NewStorageError("failed to get blocks", err)
	}

	defer rows.Close()

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
	)

	for rows.Next() {
		block := &model.Block{
			Header: &model.BlockHeader{},
		}

		if err = rows.Scan(
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
			&height,
		); err != nil {
			return nil, errors.NewStorageError("failed to scan row", err)
		}

		bits, _ := model.NewNBitFromSlice(nBits)
		block.Header.Bits = *bits

		block.Header.HashPrevBlock, err = chainhash.NewHash(hashPrevBlock)
		if err != nil {
			return nil, errors.NewStorageError("failed to convert hashPrevBlock", err)
		}

		block.Header.HashMerkleRoot, err = chainhash.NewHash(hashMerkleRoot)
		if err != nil {
			return nil, errors.NewStorageError("failed to convert hashMerkleRoot", err)
		}

		block.TransactionCount = transactionCount
		block.SizeInBytes = sizeInBytes

		if len(coinbaseTx) > 0 {
			block.CoinbaseTx, err = bt.NewTxFromBytes(coinbaseTx)
			if err != nil {
				return nil, errors.NewProcessingError("failed to convert coinbaseTx", err)
			}
		}

		err = block.SubTreesFromBytes(subtreeBytes)
		if err != nil {
			return nil, errors.NewProcessingError("failed to convert subtrees", err)
		}

		block.Height = height

		blocks = append(blocks, block)
	}

	return blocks, nil
}
