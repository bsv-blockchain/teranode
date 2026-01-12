package sql

import (
	"context"
	"database/sql"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/util/tracing"
)

// GetBlocksNotPersisted retrieves blocks that haven't been persisted yet.
// It returns blocks where persisted_at IS NULL and invalid = false, ordered by height ascending.
// The limit parameter controls the maximum number of blocks returned per call.
func (s *SQL) GetBlocksNotPersisted(ctx context.Context, limit int) ([]*model.Block, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "sql:GetBlocksNotPersisted")
	defer deferFn()

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
		WHERE persisted_at IS NULL
		AND invalid = false
		ORDER BY height ASC
		LIMIT $1
	`

	rows, err := s.db.QueryContext(ctx, q, limit)
	if err != nil {
		return nil, errors.NewStorageError("error querying blocks not persisted", err)
	}
	defer rows.Close()

	blocks := make([]*model.Block, 0)

	for rows.Next() {
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
			if errors.Is(err, sql.ErrNoRows) {
				return blocks, nil
			}
			return nil, errors.NewStorageError("error scanning block row", err)
		}

		bits, _ := model.NewNBitFromSlice(nBits)
		block.Header.Bits = *bits

		block.Header.HashPrevBlock, err = chainhash.NewHash(hashPrevBlock)
		if err != nil {
			return nil, errors.NewProcessingError("failed to convert hashPrevBlock", err)
		}

		block.Header.HashMerkleRoot, err = chainhash.NewHash(hashMerkleRoot)
		if err != nil {
			return nil, errors.NewProcessingError("failed to convert hashMerkleRoot", err)
		}

		block.TransactionCount = transactionCount
		block.SizeInBytes = sizeInBytes
		block.Height = height

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

		// Note: subtreeCount is read from DB but not assigned to block since
		// the Block struct uses len(Subtrees) to represent the count

		blocks = append(blocks, block)
	}

	if err = rows.Err(); err != nil {
		return nil, errors.NewStorageError("error iterating block rows", err)
	}

	return blocks, nil
}
