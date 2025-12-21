package sql

import (
	"context"
	"database/sql"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/tracing"
)

func (s *SQL) FindBlocksContainingSubtree(ctx context.Context, subtreeHash *chainhash.Hash, maxBlocks uint32) ([]*model.Block, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "sql:FindBlocksContainingSubtree")
	defer deferFn()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if subtreeHash == nil {
		return nil, errors.NewInvalidArgumentError("subtree hash cannot be nil")
	}

	limitClause := ""
	args := []interface{}{subtreeHash[:]}

	if maxBlocks > 0 {
		limitClause = "LIMIT $2"
		args = append(args, maxBlocks)
	}

	subtreeSearchClause := "instr(b.subtrees, $1) > 0"
	if s.engine == util.Postgres {
		subtreeSearchClause = "position($1 in b.subtrees) > 0"
	}

	q := `
		SELECT
		 b.ID
		,b.version
		,b.block_time
		,b.n_bits
		,b.nonce
		,b.previous_hash
		,b.merkle_root
		,b.tx_count
		,b.size_in_bytes
		,b.coinbase_tx
		,b.subtree_count
		,b.subtrees
		,b.height
		FROM blocks b
		WHERE ` + subtreeSearchClause + `
		AND id IN (
			SELECT id FROM blocks
			WHERE id IN (
				WITH RECURSIVE ChainBlocks AS (
					SELECT id, parent_id, height
					FROM blocks
					WHERE invalid = false
					AND hash = (
						SELECT b.hash
						FROM blocks b
						WHERE b.invalid = false
						ORDER BY chain_work DESC, peer_id ASC, id ASC
						LIMIT 1
					)
					UNION ALL
					SELECT bb.id, bb.parent_id, bb.height
					FROM blocks bb
					JOIN ChainBlocks cb ON bb.id = cb.parent_id
					WHERE bb.id != cb.id
					  AND bb.invalid = false
				)
				SELECT id FROM ChainBlocks
				WHERE invalid = FALSE
				ORDER BY height DESC
				` + limitClause + `
			)
		)
		ORDER BY height ASC
	`

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return make([]*model.Block, 0), nil
		}

		return nil, errors.NewStorageError("failed to query blocks", err)
	}

	defer rows.Close()

	return s.processFullBlockRows(rows)
}
