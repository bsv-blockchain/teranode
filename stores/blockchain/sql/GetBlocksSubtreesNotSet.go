package sql

import (
	"context"
	"database/sql"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

func (s *SQL) GetBlocksSubtreesNotSet(ctx context.Context) ([]*model.Block, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "sql:GetBlocksSubtreesNotSet")
	defer deferFn()

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
	    ,b.tx_count
		,b.size_in_bytes
		,b.coinbase_tx
		,b.subtree_count
		,b.subtrees
		,b.height
		FROM blocks b
		WHERE subtrees_set = false
		ORDER BY height ASC
	`

	return s.getBlocksWithQuery(ctx, q)
}

func (s *SQL) getBlocksWithQuery(ctx context.Context, q string) ([]*model.Block, error) {
	rows, err := s.db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
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
				return nil, errors.NewStorageError("error in GetBlock", err)
			}

			return nil, err
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

		blocks = append(blocks, block)
	}

	return blocks, nil
}
