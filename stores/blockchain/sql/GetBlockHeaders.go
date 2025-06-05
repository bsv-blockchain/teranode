package sql

import (
	"context"
	"database/sql"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/tracing"
	"github.com/libsv/go-bt/v2/chainhash"
)

func (s *SQL) GetBlockHeaders(ctx context.Context, blockHashFrom *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "sql:GetBlockHeaders",
		tracing.WithLogMessage(s.logger, "[GetBlockHeaders][%s] called for %d headers", blockHashFrom.String(), numberOfHeaders),
	)
	defer deferFn()

	headers, metas := s.blocksCache.GetBlockHeaders(blockHashFrom, numberOfHeaders)
	if headers != nil {
		return headers, metas, nil
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	const q = `
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
			,b.chain_work
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

	blockHeaders := make([]*model.BlockHeader, 0, numberOfHeaders)
	blockHeaderMetas := make([]*model.BlockHeaderMeta, 0, numberOfHeaders)

	rows, err := s.db.QueryContext(ctx, q, blockHashFrom[:], numberOfHeaders)
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
			&blockHeaderMeta.ChainWork,
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

		insertedAtUint32, err := util.SafeInt64ToUint32(insertedAt.Unix())
		if err != nil {
			return nil, nil, errors.NewProcessingError("failed to convert insertedAt", err)
		}

		blockHeaderMeta.Timestamp = insertedAtUint32

		// Set the block time to the timestamp in the meta
		blockHeaderMeta.BlockTime = blockHeader.Timestamp

		blockHeaders = append(blockHeaders, blockHeader)
		blockHeaderMetas = append(blockHeaderMetas, blockHeaderMeta)
	}

	return blockHeaders, blockHeaderMetas, nil
}
