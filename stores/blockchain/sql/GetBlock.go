package sql

import (
	"context"
	"database/sql"
	"errors"

	"github.com/TAAL-GmbH/arc/blocktx/store"
	"github.com/TAAL-GmbH/ubsv/model"
	"github.com/TAAL-GmbH/ubsv/util"
	"github.com/libsv/go-bc"
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
	    ,b.version
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
		Header: &bc.BlockHeader{},
	}

	var subtreeCount uint64
	var subtreeBytes []byte

	if err := s.db.QueryRowContext(ctx, q, blockHash[:]).Scan(
		&block.Header.Version,
		&block.Header.Time,
		&block.Header.Bits,
		&block.Header.Nonce,
		&block.Header.HashPrevBlock,
		&block.Header.HashMerkleRoot,
		&subtreeCount,
		&subtreeBytes,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, store.ErrBlockNotFound
		}
		return nil, err
	}

	block.SubTrees = make([]*util.SubTree, subtreeCount)
	for i := uint64(0); i < subtreeCount; i++ {
		// TODO - read subtree from store ???
		block.SubTrees[i] = &util.SubTree{}
	}

	return block, nil
}
