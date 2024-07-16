package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

func (s *SQL) GetBlocks(ctx context.Context, blockHashFrom *chainhash.Hash, numberOfHeaders uint32) ([]*model.Block, error) {
	start, stat, ctx := tracing.StartStatFromContext(ctx, "GetBlockHeaders")
	defer func() {
		stat.AddTime(start)
	}()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	blocks := make([]*model.Block, 0, numberOfHeaders)

	q := `
		SELECT
	     b.version
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
		return nil, fmt.Errorf("failed to get blocks: %w", err)
	}
	defer rows.Close()

	var subtreeCount uint64
	var transactionCount uint64
	var sizeInBytes uint64
	var subtreeBytes []byte
	var hashPrevBlock []byte
	var hashMerkleRoot []byte
	var coinbaseTx []byte
	var height uint32
	var nBits []byte
	for rows.Next() {
		block := &model.Block{
			Header: &model.BlockHeader{},
		}

		if err = rows.Scan(
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
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		block.Header.Bits = model.NewNBitFromSlice(nBits)

		block.Header.HashPrevBlock, err = chainhash.NewHash(hashPrevBlock)
		if err != nil {
			return nil, fmt.Errorf("failed to convert hashPrevBlock: %w", err)
		}
		block.Header.HashMerkleRoot, err = chainhash.NewHash(hashMerkleRoot)
		if err != nil {
			return nil, fmt.Errorf("failed to convert hashMerkleRoot: %w", err)
		}
		block.TransactionCount = transactionCount
		block.SizeInBytes = sizeInBytes

		block.CoinbaseTx, err = bt.NewTxFromBytes(coinbaseTx)
		if err != nil {
			return nil, fmt.Errorf("failed to convert coinbaseTx: %w", err)
		}

		err = block.SubTreesFromBytes(subtreeBytes)
		if err != nil {
			return nil, fmt.Errorf("failed to convert subtrees: %w", err)
		}

		block.Height = height

		blocks = append(blocks, block)
	}

	return blocks, nil
}
