package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/TAAL-GmbH/arc/blocktx/store"
	"github.com/TAAL-GmbH/ubsv/model"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
	"github.com/ordishs/gocore"
)

func (s *SQL) GetBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.Block, uint64, error) {
	start := gocore.CurrentNanos()
	defer func() {
		gocore.NewStat("blockchain").NewStat("GetBlock").AddTime(start)
	}()

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
		,b.subtree_count
		,b.subtrees
		,b.height
		FROM blocks b
		WHERE b.hash = $1
	`

	block := &model.Block{
		Header: &model.BlockHeader{},
	}

	var subtreeCount uint64
	var transactionCount uint64
	var subtreeBytes []byte
	var hashPrevBlock []byte
	var hashMerkleRoot []byte
	var height uint64
	var nBits []byte
	var err error

	if err = s.db.QueryRowContext(ctx, q, blockHash[:]).Scan(
		&block.Header.Version,
		&block.Header.Timestamp,
		&nBits,
		&block.Header.Nonce,
		&hashPrevBlock,
		&hashMerkleRoot,
		&transactionCount,
		&subtreeCount,
		&subtreeBytes,
		&height,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, 0, store.ErrBlockNotFound
		}
		return nil, 0, err
	}

	block.Header.Bits = model.NewNBitFromSlice(nBits)

	block.Header.HashPrevBlock, err = chainhash.NewHash(hashPrevBlock)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to convert hashPrevBlock: %w", err)
	}
	block.Header.HashMerkleRoot, err = chainhash.NewHash(hashMerkleRoot)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to convert hashMerkleRoot: %w", err)
	}
	block.TransactionCount = transactionCount

	err = block.SubTreesFromBytes(subtreeBytes)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to convert subtrees: %w", err)
	}

	return block, height, nil
}
