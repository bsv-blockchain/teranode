package sql

import (
	"context"
	"database/sql"
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/lib/pq"
)

func (s *SQL) CheckIfBlockIsInCurrentChain(ctx context.Context, blockIDs []uint32) (bool, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "sql:CheckIfBlockIsInCurrentChain",
		tracing.WithLogMessage(s.logger, "[CheckIfBlockIsInCurrentChain] checking if blocks (%v) are in current chain", blockIDs),
	)
	defer deferFn()

	// SQL query to recursively check if any of the provided block IDs is part of the current chain
	q := `
		WITH RECURSIVE ChainBlocks AS (
			SELECT id, parent_id, is_current
			FROM blocks
			WHERE id = ANY($1)
			UNION ALL
			SELECT b.id, b.parent_id, b.is_current
			FROM blocks b
			JOIN ChainBlocks cb ON b.id = cb.parent_id
		)
		SELECT is_current FROM ChainBlocks WHERE is_current = true LIMIT 1;
	`

	var isInCurrentChain bool
	// Convert blockIDs from uint32 to int slices, since PostgreSQL handles integer types.
	intBlockIDs := make([]int, len(blockIDs))
	for i, id := range blockIDs {
		intBlockIDs[i] = int(id)
	}

	// Execute the query using intBlockIDs as input
	err := s.db.QueryRowContext(ctx, q, pq.Array(intBlockIDs)).Scan(&isInCurrentChain)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// None of the blocks are in the current chain
			return false, nil
		}
		return false, errors.NewStorageError("failed to check block chain status", err)
	}

	// Return true if any block is part of the current chain, false otherwise
	return isInCurrentChain, nil
}
