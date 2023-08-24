package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

type getBlockHeadersCache struct {
	blockHeaders []*model.BlockHeader
	heights      []uint32
}

func (s *SQL) GetBlockHeaders(ctx context.Context, blockHashFrom *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []uint32, error) {
	start := gocore.CurrentNanos()
	defer func() {
		gocore.NewStat("blockchain").NewStat("GetBlock").AddTime(start)
	}()

	cacheId := fmt.Sprintf("GetBlockHeaders_%s_%d", blockHashFrom.String(), numberOfHeaders)
	cached, ok := cache.Load(cacheId)
	if ok {
		s.logger.Debugf("GetBlockHeaders cache hit")
		return cached.(*getBlockHeadersCache).blockHeaders, cached.(*getBlockHeadersCache).heights, nil
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	blockHeaders := make([]*model.BlockHeader, 0, numberOfHeaders)
	heights := make([]uint32, 0, numberOfHeaders)

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
			return blockHeaders, heights, nil
		}
		return nil, nil, fmt.Errorf("failed to get headers: %w", err)
	}
	defer rows.Close()

	var hashPrevBlock []byte
	var hashMerkleRoot []byte
	var nBits []byte
	var height uint32
	for rows.Next() {
		blockHeader := &model.BlockHeader{}

		if err = rows.Scan(
			&blockHeader.Version,
			&blockHeader.Timestamp,
			&blockHeader.Nonce,
			&hashPrevBlock,
			&hashMerkleRoot,
			&nBits,
			&height,
		); err != nil {
			return nil, nil, fmt.Errorf("failed to scan row: %w", err)
		}

		blockHeader.Bits = model.NewNBitFromSlice(nBits)

		blockHeader.HashPrevBlock, err = chainhash.NewHash(hashPrevBlock)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to convert hashPrevBlock: %w", err)
		}
		blockHeader.HashMerkleRoot, err = chainhash.NewHash(hashMerkleRoot)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to convert hashMerkleRoot: %w", err)
		}

		blockHeaders = append(blockHeaders, blockHeader)
		heights = append(heights, height)
	}

	cache.Store(cacheId, &getBlockHeadersCache{
		blockHeaders: blockHeaders,
		heights:      heights,
	})

	return blockHeaders, heights, nil
}
