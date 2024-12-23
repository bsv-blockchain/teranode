package sql

import (
	"context"
	"database/sql"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/tracing"
	"github.com/libsv/go-bt/v2/chainhash"
)

func (s *SQL) GetHeader(ctx context.Context, blockHash *chainhash.Hash) (*model.BlockHeader, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "sql:GetHeader")
	defer deferFn()

	header, _ := s.blocksCache.GetBlockHeader(*blockHash)
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

	var (
		hashPrevBlock  []byte
		hashMerkleRoot []byte
		nBits          []byte
	)

	if err := s.db.QueryRowContext(ctx, q, blockHash[:]).Scan(
		&blockHeader.Version,
		&blockHeader.Timestamp,
		&blockHeader.Nonce,
		&hashPrevBlock,
		&hashMerkleRoot,
		&nBits,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// add not found error in error chain
			return nil, errors.NewBlockNotFoundError("error in GetHeader", errors.ErrNotFound)
		}

		return nil, errors.NewStorageError("error in GetHeader", err)
	}

	var err error

	blockHeader.HashPrevBlock, err = chainhash.NewHash(hashPrevBlock)
	if err != nil {
		return nil, errors.NewProcessingError("failed to convert hashPrevBlock", err)
	}

	blockHeader.HashMerkleRoot, err = chainhash.NewHash(hashMerkleRoot)
	if err != nil {
		return nil, errors.NewProcessingError("failed to convert hashMerkleRoot", err)
	}

	bits, _ := model.NewNBitFromSlice(nBits)
	blockHeader.Bits = *bits

	return blockHeader, nil
}
