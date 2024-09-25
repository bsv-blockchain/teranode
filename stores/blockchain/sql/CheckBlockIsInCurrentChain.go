package sql

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/lib/pq"
)

func (s *SQL) CheckIfBlockIsInCurrentChain(ctx context.Context, blockIDs []uint32) (bool, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "sql:CheckIfBlockIsInCurrentChain",
		tracing.WithLogMessage(s.logger, "[CheckIfBlockIsInCurrentChain] checking if blocks (%v) are in current chain", blockIDs),
	)
	defer deferFn()

	// get current best block Header
	bestBlockHeader, _, err := s.GetBestBlockHeader(ctx)
	if err != nil {
		return false, errors.NewStorageError("failed to get best block header", err)
	}

	// SQL query to recursively check if any of the provided block IDs is part of the current chain
	// start from the block with the given hash and recursively check its parent until a match is found
	// 1- check if the start block is in the list of block IDs
	// 2- initializes the depth to 1
	// 3- recursive query traverses the parent block of the current block
	// 4- increases depth by 1
	// 5- checks if the parent block is in the list of block IDs
	// 6- found_match is set to true if a match is found, if so the recursion stops
	// 7- or recursion stops if the depth exceeds the specified limit
	// 8- after recursion ends, the result is set to true if a match was found, false otherwise
	q := `
		WITH RECURSIVE ChainBlocks AS (
			SELECT
				id,
				parent_id,
				1 AS depth,
				(id IN ($2)) AS found_match
			FROM blocks
			WHERE hash = $1
			UNION ALL
			SELECT
				bb.id,
				bb.parent_id,
				cb.depth + 1 AS depth,
				(bb.id IN ($2)) AS found_match
			FROM blocks bb
			INNER JOIN ChainBlocks cb ON bb.id = cb.parent_id
			WHERE
				NOT cb.found_match -- Stop recursion if a match has been found
				AND cb.depth < $3   -- Limit recursion to the specified depth
		)
		SELECT CASE
			WHEN EXISTS (SELECT 1 FROM ChainBlocks WHERE found_match)
			THEN TRUE
			ELSE FALSE
		END AS result;
	`

	var isInCurrentChain bool
	// Convert blockIDs from uint32 to int slices, since PostgreSQL handles integer types.
	intBlockIDs := make([]int, len(blockIDs))
	for i, id := range blockIDs {
		intBlockIDs[i] = int(id)
	}

	recursionDepth := 10000

	// Execute the query using intBlockIDs as input
	err = s.db.QueryRowContext(ctx, q, bestBlockHeader.Hash()[:], pq.Array(intBlockIDs), recursionDepth).Scan(&isInCurrentChain)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// None of the blocks are in the current chain
			return false, nil
		}
		return false, errors.NewStorageError("failed to check if given blocks are part of the current chain", err)
	} else {
		fmt.Println("Result isInCurrentChain: ", isInCurrentChain)
	}

	// Return true if any block is part of the current chain, false otherwise
	return isInCurrentChain, nil
}
