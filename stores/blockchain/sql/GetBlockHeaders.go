package sql

import (
	"context"
	"fmt"

	"github.com/TAAL-GmbH/ubsv/model"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

func (s *SQL) GetBlockHeaders(ctx context.Context, blockHashFrom *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, error) {
	start := gocore.CurrentNanos()
	defer func() {
		gocore.NewStat("blockchain").NewStat("GetBlock").AddTime(start)
	}()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	height, err := s.GetBlockHeight(ctx, blockHashFrom)
	if err != nil {
		return nil, err
	}

	q := `
		SELECT
	     b.version
		,b.block_time
	    ,b.nonce
		,b.previous_hash
		,b.merkle_root
		,b.n_bits
		FROM blocks b
		WHERE b.height <= $1
		  AND b.orphaned = false
		ORDER BY b.height DESC
		LIMIT $2
	`
	rows, err := s.db.QueryContext(ctx, q, height, numberOfHeaders)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	blockHeaders := make([]*model.BlockHeader, 0, numberOfHeaders)

	var hashPrevBlock []byte
	var hashMerkleRoot []byte
	var nBits []byte
	for rows.Next() {
		var blockHeader model.BlockHeader
		err = rows.Scan(
			&blockHeader.Version,
			&blockHeader.Timestamp,
			&blockHeader.Nonce,
			&hashPrevBlock,
			&hashMerkleRoot,
			&nBits,
		)
		if err != nil {
			return nil, err
		}

		blockHeader.HashPrevBlock, err = chainhash.NewHash(hashPrevBlock)
		if err != nil {
			return nil, fmt.Errorf("failed to convert hashPrevBlock: %w", err)
		}
		blockHeader.HashMerkleRoot, err = chainhash.NewHash(hashMerkleRoot)
		if err != nil {
			return nil, fmt.Errorf("failed to convert hashMerkleRoot: %w", err)
		}

		blockHeader.Bits = model.NewNBitFromSlice(nBits)

		blockHeaders = append(blockHeaders, &blockHeader)
	}

	return blockHeaders, nil
}
