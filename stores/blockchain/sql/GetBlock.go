package sql

import (
	"context"
	"database/sql"
	"errors"

	"github.com/TAAL-GmbH/arc/blocktx/store"
	"github.com/libsv/go-bc"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
	"github.com/ordishs/gocore"
)

func (s *SQL) GetBlock(ctx context.Context, blockHash *chainhash.Hash) (*bc.Block, error) {
	start := gocore.CurrentNanos()
	defer func() {
		gocore.NewStat("blockchain").NewStat("GetBlock").AddTime(start)
	}()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	q := `
		SELECT
		 b.hash
		,b.prevhash
		,b.merkleroot
		//,b.height
		,b.processed_at
		//,b.orphanedyn
		FROM blocks b
		WHERE b.hash = $1
	`

	block := &bc.Block{
		BlockHeader: &bc.BlockHeader{},
	}

	var processed_at sql.NullString

	if err := s.db.QueryRowContext(ctx, q, blockHash[:]).Scan(
		&block.BlockHeader.HashPrevBlock,
		&block.BlockHeader.HashPrevBlock,
		&block.BlockHeader.HashMerkleRoot,
		//&block.BlockHeader.Height,
		&processed_at,
		//&block.Orphaned,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, store.ErrBlockNotFound
		}
		return nil, err
	}

	//block.Processed = processed_at.Valid

	return block, nil
}
