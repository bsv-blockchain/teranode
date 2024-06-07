package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
)

func (s *SQL) GetHeader(ctx context.Context, blockHash *chainhash.Hash) (*model.BlockHeader, error) {
	start, stat, ctx := util.StartStatFromContext(ctx, "GetHeader")
	defer func() {
		stat.AddTime(start)
	}()

	header, _, err := s.blocksCache.GetBlockHeader(*blockHash)
	if err != nil {
		return nil, fmt.Errorf("error in GetHeader: %w", err)
	}
	if header != nil {
		return header, nil
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
		FROM blocks b
		WHERE b.hash = $1
	`

	blockHeader := &model.BlockHeader{}

	var hashPrevBlock []byte
	var hashMerkleRoot []byte
	var nBits []byte

	if err := s.db.QueryRowContext(ctx, q, blockHash[:]).Scan(
		&blockHeader.Version,
		&blockHeader.Timestamp,
		&blockHeader.Nonce,
		&hashPrevBlock,
		&hashMerkleRoot,
		&nBits,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("error in Header: %w", err)
		}
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

	return blockHeader, nil
}
