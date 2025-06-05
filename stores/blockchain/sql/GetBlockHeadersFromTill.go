package sql

import (
	"context"
	"database/sql"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/util/tracing"
	"github.com/libsv/go-bt/v2/chainhash"
)

func (s *SQL) GetBlockHeadersFromTill(ctx context.Context, blockHashFrom *chainhash.Hash, blockHashTill *chainhash.Hash) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "sql:GetBlockHeaders",
		tracing.WithLogMessage(s.logger, "[GetBlockHeadersFromTill] called for %s -> %s", blockHashFrom.String(), blockHashTill.String()),
	)
	defer deferFn()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	_, blockHeaderFromMeta, err := s.GetBlockHeader(ctx, blockHashFrom)
	if err != nil {
		return nil, nil, err
	}

	_, blockHeaderTillMeta, err := s.GetBlockHeader(ctx, blockHashTill)
	if err != nil {
		return nil, nil, err
	}

	numberOfHeaders := blockHeaderTillMeta.Height - blockHeaderFromMeta.Height + 1

	blockHeaders := make([]*model.BlockHeader, 0, numberOfHeaders)
	blockHeaderMetas := make([]*model.BlockHeaderMeta, 0, numberOfHeaders)

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

	rows, err := s.db.QueryContext(ctx, q, blockHashTill[:], numberOfHeaders)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return blockHeaders, blockHeaderMetas, nil
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
		blockHeaderMetas = append(blockHeaderMetas, blockHeaderMeta)
	}

	// make sure the last block header is the blockHashTill
	if blockHeaders[len(blockHeaders)-1].Hash().String() != blockHashFrom.String() {
		return nil, nil, errors.NewProcessingError("last block header is not the blockHashFrom", nil)
	}

	return blockHeaders, blockHeaderMetas, nil
}
