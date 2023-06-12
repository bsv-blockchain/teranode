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

func (s *SQL) GetHeader(ctx context.Context, blockHash *chainhash.Hash) (*bc.BlockHeader, error) {
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
	    ,b.nonce
		,b.previous_hash
		,b.merkle_root
		,b.n_bits
		FROM blocks b
		WHERE b.hash = $1
	`

	blockHeader := &bc.BlockHeader{}

	var err error
	if err = s.db.QueryRowContext(ctx, q, blockHash[:]).Scan(
		&blockHeader.Version,
		&blockHeader.Time,
		&blockHeader.Nonce,
		&blockHeader.HashPrevBlock,
		&blockHeader.HashMerkleRoot,
		&blockHeader.Bits,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, store.ErrBlockNotFound
		}
		return nil, err
	}

	return blockHeader, nil
}
