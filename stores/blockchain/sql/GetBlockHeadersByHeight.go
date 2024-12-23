package sql

import (
	"context"
	"database/sql"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/tracing"
	"github.com/libsv/go-bt/v2/chainhash"
	"golang.org/x/exp/constraints"
)

func (s *SQL) GetBlockHeadersByHeight(ctx context.Context, startHeight, endHeight uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "sql:GetBlockHeadersByHeight")
	defer deferFn()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	capacity := max(1, endHeight-startHeight+1)

	blockHeaders := make([]*model.BlockHeader, 0, capacity)
	blockMetas := make([]*model.BlockHeaderMeta, 0, capacity)

	q := `
		SELECT
		 b.version
		,b.block_time
		,b.nonce
		,b.previous_hash
		,b.merkle_root
		,b.n_bits
		,b.id
		,b.height
		,b.tx_count
		,b.size_in_bytes
		,b.peer_id
		,b.block_time
		,b.inserted_at
		FROM blocks b
		WHERE height >= $1 AND height <= $2
		AND invalid = FALSE
		ORDER BY height ASC
	`

	rows, err := s.db.QueryContext(ctx, q, startHeight, endHeight)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return blockHeaders, blockMetas, nil
		}

		return nil, nil, errors.NewStorageError("failed to get headers", err)
	}

	defer rows.Close()

	var (
		hashPrevBlock  []byte
		hashMerkleRoot []byte
		nBits          []byte
		insertedAt     CustomTime
	)

	for rows.Next() {
		blockHeader := &model.BlockHeader{}
		blockHeaderMeta := &model.BlockHeaderMeta{}

		if err = rows.Scan(
			&blockHeader.Version,
			&blockHeader.Timestamp,
			&blockHeader.Nonce,
			&hashPrevBlock,
			&hashMerkleRoot,
			&nBits,
			&blockHeaderMeta.ID,
			&blockHeaderMeta.Height,
			&blockHeaderMeta.TxCount,
			&blockHeaderMeta.SizeInBytes,
			&blockHeaderMeta.Miner,
			&blockHeaderMeta.BlockTime,
			&insertedAt,
		); err != nil {
			return nil, nil, errors.NewStorageError("failed to scan row", err)
		}

		bits, _ := model.NewNBitFromSlice(nBits)
		blockHeader.Bits = *bits

		blockHeader.HashPrevBlock, err = chainhash.NewHash(hashPrevBlock)
		if err != nil {
			return nil, nil, errors.NewProcessingError("failed to convert hashPrevBlock", err)
		}

		blockHeader.HashMerkleRoot, err = chainhash.NewHash(hashMerkleRoot)
		if err != nil {
			return nil, nil, errors.NewProcessingError("failed to convert hashMerkleRoot", err)
		}

		blockHeaders = append(blockHeaders, blockHeader)
		blockMetas = append(blockMetas, blockHeaderMeta)
	}

	return blockHeaders, blockMetas, nil
}

func max[T constraints.Ordered](a, b T) T {
	if a > b {
		return a
	}

	return b
}
