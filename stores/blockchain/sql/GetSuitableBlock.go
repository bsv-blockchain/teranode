package sql

import (
	"context"
	"database/sql"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
)

func (s *SQL) GetSuitableBlock(ctx context.Context, hash *chainhash.Hash) (*model.SuitableBlock, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "sql:GetSuitableBlock")
	defer deferFn()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		id       int
		parentID int
	)

	q := `WITH RECURSIVE block_chain AS (
		SELECT
			id,
			hash,
			parent_id,
			n_bits,
			height,
			block_time,
			chain_work,
			1 as depth
		FROM
			blocks
		WHERE
			hash = $1

		UNION ALL

		SELECT
			b.id,
			b.hash,
			b.parent_id,
			b.n_bits,
			b.height,
			b.block_time,
			b.chain_work,
			bc.depth + 1
		FROM
			blocks b
		INNER JOIN
			block_chain bc ON b.id = bc.parent_id
		WHERE
			bc.depth < 3
	)
	SELECT
		id,
		hash,
		parent_id,
		n_bits,
		height,
		block_time,
		chain_work
	FROM
		block_chain`

	rows, err := s.db.QueryContext(ctx, q, hash[:])
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil // TODO should this be an ErrNotFound error?
		}

		return nil, errors.NewStorageError("failed to get suitableBlock", err)
	}
	defer rows.Close()

	var suitableBlockCandidates []*model.SuitableBlock

	for rows.Next() {
		suitableBlock := &model.SuitableBlock{}

		if err = rows.Scan(
			&id,
			&suitableBlock.Hash,
			&parentID,
			&suitableBlock.NBits,
			&suitableBlock.Height,
			&suitableBlock.Time,
			&suitableBlock.ChainWork,
		); err != nil {
			return nil, errors.NewStorageError("failed to scan row", err)
		}

		suitableBlockCandidates = append(suitableBlockCandidates, suitableBlock)
	}

	// we have 3 candidates - now sort them by time and choose the median
	b, err := getMedianBlock(suitableBlockCandidates)
	if err != nil {
		return nil, errors.NewProcessingError("failed to get median block", err)
	}

	return b, nil
}

func getMedianBlock(blocks []*model.SuitableBlock) (*model.SuitableBlock, error) {
	if len(blocks) != 3 {
		return nil, errors.NewProcessingError("not enough candidates for suitable block. have %d, need 3", len(blocks))
	}

	util.SortForDifficultyAdjustment(blocks)

	// Return the middle block (out of 3)
	return blocks[1], nil
}
