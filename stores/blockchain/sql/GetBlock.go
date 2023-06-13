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

func (s *SQL) GetBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.Block, error) {
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
		,b.subtree_count
		,b.subtrees
		FROM blocks b
		WHERE b.hash = $1
	`

	block := &model.Block{
		Header: &model.BlockHeader{},
	}

	var subtreeCount uint64
	var subtreeBytes []byte
	var hashPrevBlock []byte
	var hashMerkleRoot []byte
	var err error

	if err = s.db.QueryRowContext(ctx, q, blockHash[:]).Scan(
		&block.Header.Version,
		&block.Header.Timestamp,
		&block.Header.Bits,
		&block.Header.Nonce,
		&hashPrevBlock,
		&hashMerkleRoot,
		&subtreeCount,
		&subtreeBytes,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, store.ErrBlockNotFound
		}
		return nil, err
	}

	block.Header.HashPrevBlock, err = chainhash.NewHash(hashPrevBlock)
	if err != nil {
		return nil, fmt.Errorf("failed to convert hashPrevBlock: %w", err)
	}
	block.Header.HashMerkleRoot, err = chainhash.NewHash(hashMerkleRoot)
	if err != nil {
		return nil, fmt.Errorf("failed to convert hashMerkleRoot: %w", err)
	}

	block.Subtrees = make([]*chainhash.Hash, subtreeCount)
	for i := uint64(0); i < subtreeCount; i++ {
		bytes := subtreeBytes[i*32 : (i+1)*32]
		block.Subtrees[i], err = chainhash.NewHash(bytes)
		if err != nil {
			return nil, fmt.Errorf("failed to create hash from bytes: %w", err)
		}
	}

	return block, nil
}
