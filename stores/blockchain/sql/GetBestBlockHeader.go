package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/TAAL-GmbH/arc/blocktx/store"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

type getBestBlockHeaderCache struct {
	blockHeader *model.BlockHeader
	height      uint32
}

func (s *SQL) GetBestBlockHeader(ctx context.Context) (*model.BlockHeader, uint32, error) {
	start := gocore.CurrentNanos()
	defer func() {
		gocore.NewStat("blockchain").NewStat("GetBlock").AddTime(start)
	}()

	cached, ok := cache.Load("GetBestBlockHeader")
	if ok {
		s.logger.Debugf("GetBestBlockHeader cache hit")
		return cached.(*getBestBlockHeaderCache).blockHeader, cached.(*getBestBlockHeaderCache).height, nil
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	q := `
		SELECT
	     b.version
		,b.block_time
	    ,b.nonce
		,b.previous_hash
		,b.merkle_root
		,b.n_bits
		,b.height
		FROM blocks b
		ORDER BY chain_work DESC, id ASC
		LIMIT 1
	`

	blockHeader := &model.BlockHeader{}

	var hashPrevBlock []byte
	var hashMerkleRoot []byte
	var height uint32
	var nBits []byte

	var err error
	if err = s.db.QueryRowContext(ctx, q).Scan(
		&blockHeader.Version,
		&blockHeader.Timestamp,
		&blockHeader.Nonce,
		&hashPrevBlock,
		&hashMerkleRoot,
		&nBits,
		&height,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, 0, store.ErrBlockNotFound
		}
		return nil, 0, err
	}

	blockHeader.Bits = model.NewNBitFromSlice(nBits)

	blockHeader.HashPrevBlock, err = chainhash.NewHash(hashPrevBlock)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to convert hashPrevBlock: %w", err)
	}
	blockHeader.HashMerkleRoot, err = chainhash.NewHash(hashMerkleRoot)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to convert hashMerkleRoot: %w", err)
	}

	// set cache
	cache.Store("GetBestBlockHeader", &getBestBlockHeaderCache{
		blockHeader: blockHeader,
		height:      height,
	})

	return blockHeader, height, nil
}
