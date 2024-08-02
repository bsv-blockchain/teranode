package sql

import (
	"context"
	"database/sql"
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/libsv/go-bt/v2/chainhash"
)

func (s *SQL) GetBlockHeadersFromHeight(ctx context.Context, height, limit uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "sql:GetBlockHeadersFromHeight")
	defer deferFn()

	headers, metas, err := s.blocksCache.GetBlockHeadersFromHeight(height, int(limit))
	if err != nil {
		return nil, nil, errors.NewStorageError("error in GetBlockHeadersFromHeight", err)
	}
	if headers != nil {
		return headers, metas, nil
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// we are getting all forks, so we need a bit more than the limit
	blockHeaders := make([]*model.BlockHeader, 0, 2*limit)
	blockMetas := make([]*model.BlockHeaderMeta, 0, 2*limit)

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
		WHERE height >= $1 AND height < $2
		ORDER BY height DESC
	`
	rows, err := s.db.QueryContext(ctx, q, height, height+limit)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return blockHeaders, blockMetas, nil
		}
		return nil, nil, errors.NewStorageError("failed to get headers", err)
	}
	defer rows.Close()

	var hashPrevBlock []byte
	var hashMerkleRoot []byte
	var nBits []byte
	var insertedAt CustomTime
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

		blockHeaderMeta.Timestamp = uint32(insertedAt.Unix())
		blockHeader.Bits = model.NewNBitFromSlice(nBits)

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
