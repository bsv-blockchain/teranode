package sql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util/tracing"
)

// CheckBlockIsInCurrentChain determines if any of the specified blocks are on the current
// main chain. When useInMemoryChainCheck is true, uses a pure in-memory O(1) lookup via
// the off-chain set. When false, falls back to the original SQL recursive CTE.
//
// Returns true as soon as any block ID passes all checks (ANY-of semantics).
func (s *SQL) CheckBlockIsInCurrentChain(ctx context.Context, blockIDs []uint32) (bool, error) {
	ctx, _, deferFn := tracing.Tracer("SyncManager").Start(ctx, "sql:CheckIfBlockIsInCurrentChain",
		tracing.WithDebugLogMessage(s.logger, "[CheckIfBlockIsInCurrentChain] checking if blocks (%v) are in current chain", blockIDs),
	)
	defer deferFn()

	if len(blockIDs) == 0 {
		return false, nil
	}

	if !s.useInMemoryChainCheck {
		return s.checkBlockIsInCurrentChainSQL(ctx, blockIDs)
	}

	maxID := uint32(s.maxBlockID.Load())

	s.offChainBlockIDsMu.RLock()
	offChain := s.offChainBlockIDs
	s.offChainBlockIDsMu.RUnlock()

	// ANY-of semantics: return true if at least one block is on the main chain.
	// This matches the old CTE behavior and is required by callers like
	// BlockValidation.checkOldBlockIDs which passes candidate block IDs for a
	// transaction across forks and needs true if any candidate is on-chain.
	for _, id := range blockIDs {
		// IDs above the highest known block cannot exist in the database.
		if id > maxID {
			continue
		}
		if _, isOffChain := offChain[id]; !isOffChain {
			return true, nil
		}
	}

	return false, nil
}

// checkBlockIsInCurrentChainSQL is the original SQL recursive CTE implementation.
// Used when useInMemoryChainCheck is false.
func (s *SQL) checkBlockIsInCurrentChainSQL(ctx context.Context, blockIDs []uint32) (bool, error) {
	_, bestBlockMeta, err := s.GetBestBlockHeader(ctx)
	if err != nil {
		return false, errors.NewStorageError("failed to get best block header", err)
	}

	args := make([]interface{}, 0, len(blockIDs)+2)

	blockIDPlaceholders := make([]string, len(blockIDs))
	for i, id := range blockIDs {
		placeholder := fmt.Sprintf("$%d", i+1)
		if s.engine == "sqlite" || s.engine == "sqlitememory" {
			blockIDPlaceholders[i] = fmt.Sprintf("SELECT CAST(%s as int) AS id", placeholder)
		} else {
			blockIDPlaceholders[i] = fmt.Sprintf("SELECT %s::INTEGER AS id", placeholder)
		}
		args = append(args, id)
	}

	blockIDsCTE := strings.Join(blockIDPlaceholders, " UNION ALL ")

	bestBlockID := bestBlockMeta.ID

	lowestBlockID := blockIDs[0] //nolint:gosec // length is checked above
	for _, id := range blockIDs {
		if id < lowestBlockID {
			lowestBlockID = id
		}
	}

	recursionDepthBlockID := bestBlockID - lowestBlockID
	if lowestBlockID > bestBlockID {
		recursionDepthBlockID = 0
	}

	args = append(args, bestBlockID, recursionDepthBlockID)

	bestBlockIDPlaceholder := fmt.Sprintf("$%d", len(blockIDs)+1)
	recursionDepthPlaceholder := fmt.Sprintf("$%d", len(blockIDs)+2)

	q := fmt.Sprintf(`
        WITH RECURSIVE
        block_ids(id) AS (
            %s
        ),
        ChainBlocks AS (
            SELECT id, parent_id, 1 AS depth, EXISTS (SELECT 1 FROM block_ids WHERE id = blocks.id) AS found_match
            FROM blocks
            WHERE id = %s
            UNION ALL
            SELECT
                bb.id,
                bb.parent_id,
                cb.depth + 1 AS depth,
                EXISTS (SELECT 1 FROM block_ids WHERE id = bb.id) AS found_match
            FROM blocks bb
            INNER JOIN ChainBlocks cb ON bb.id = cb.parent_id
            WHERE
                NOT cb.found_match
                AND cb.depth <= %s
        )
        SELECT CASE
            WHEN EXISTS (SELECT 1 FROM ChainBlocks WHERE found_match)
            THEN TRUE
            ELSE FALSE
        END AS is_in_current_chain;
    `, blockIDsCTE, bestBlockIDPlaceholder, recursionDepthPlaceholder)

	var result bool
	err = s.db.QueryRowContext(ctx, q, args...).Scan(&result)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, errors.NewStorageError("failed to check if given blocks are part of the current chain", err)
	}

	return result, nil
}
