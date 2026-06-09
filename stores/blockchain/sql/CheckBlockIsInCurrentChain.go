package sql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util/tracing"
)

// maxIDsPerCheckBatch caps the number of placeholders per IN() query in the
// on_main_chain fast path. Postgres has a 32767 bind-parameter limit; 1000 is
// far below that and keeps plan-cache pressure low while still amortising
// round-trip cost across many IDs.
const maxIDsPerCheckBatch = 1000

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

	// Fall back to SQL when:
	//   - in-memory mode is disabled, OR
	//   - a rebuild is in progress: offChainBlockIDs may be empty (startup) or stale
	//     (ongoing reorg/invalidation) and the SQL path has its own CTE fallback.
	if !s.useInMemoryChainCheck || s.mainChainRebuilding.Load() > 0 {
		return s.checkBlockIsInCurrentChainSQL(ctx, blockIDs)
	}

	maxID := uint32(s.maxBlockID.Load())

	s.offChainBlockIDsMu.RLock()
	offChain := s.offChainBlockIDs
	s.offChainBlockIDsMu.RUnlock()

	// ANY-of semantics: return true if at least one block is on the main chain.
	//
	// The off-chain set + maxID give a fast NEGATIVE only: an id above the highest
	// known id, or one in the off-chain set, definitely is NOT on the main chain.
	// A surviving candidate is NOT positively on-chain, though — the off-chain set
	// tracks only blocks that EXIST off-chain, so a *non-existent* (phantom /
	// id-sequence gap) id <= maxID survives this filter despite having no row.
	// Treating a survivor as on-chain (the previous behaviour) disagreed with the
	// authoritative SQL route (checkBlockIsInCurrentChainSQL requires a real
	// on_main_chain=true row) and could split a useInMemoryChainCheck=on node from
	// an off node on a dangling id. So confirm survivors against the authoritative
	// on_main_chain flag instead of assuming membership.
	survivors := make([]uint32, 0, len(blockIDs))
	for _, id := range blockIDs {
		if id > maxID {
			continue
		}
		if _, isOffChain := offChain[id]; isOffChain {
			continue
		}
		survivors = append(survivors, id)
	}

	if len(survivors) == 0 {
		return false, nil
	}

	// We reach here only when mainChainRebuilding == 0, so checkBlockIsInCurrentChainSQL
	// takes its indexed fast path (SELECT 1 ... WHERE id IN (...) AND on_main_chain=true),
	// never the recursive CTE. A non-existent id matches no row and is rejected,
	// identically to the always-SQL route — closing the toggled/untoggled split.
	return s.checkBlockIsInCurrentChainSQL(ctx, survivors)
}

// checkBlockIsInCurrentChainSQL is the SQL fallback implementation used when
// useInMemoryChainCheck is false. Uses the on_main_chain column when flags are
// consistent; falls back to the recursive CTE when a rebuild is in progress.
func (s *SQL) checkBlockIsInCurrentChainSQL(ctx context.Context, blockIDs []uint32) (bool, error) {
	// Defense in depth: the public wrapper already rejects empty input, but
	// direct callers (tests, benchmarks) may bypass that. The CTE fallback
	// below indexes blockIDs[0], so an empty slice must not reach it.
	if len(blockIDs) == 0 {
		return false, nil
	}

	if s.mainChainRebuilding.Load() == 0 {
		// Fast path: on_main_chain flags are reliable. Resolve ANY-of semantics
		// in a single round-trip per batch, rather than one query per ID. Cap
		// each batch at maxIDsPerCheckBatch so we never approach Postgres's
		// parameter limit (32767) even if a future caller passes a huge slice.
		for start := 0; start < len(blockIDs); start += maxIDsPerCheckBatch {
			end := start + maxIDsPerCheckBatch
			if end > len(blockIDs) {
				end = len(blockIDs)
			}
			batch := blockIDs[start:end]

			placeholders := make([]string, len(batch))
			args := make([]interface{}, len(batch))
			for i, id := range batch {
				placeholders[i] = fmt.Sprintf("$%d", i+1)
				args[i] = id
			}
			q := fmt.Sprintf(`SELECT 1 FROM blocks WHERE id IN (%s) AND on_main_chain = true LIMIT 1`, strings.Join(placeholders, ","))
			var found int // sentinel — we only care whether a row is returned
			err := s.db.QueryRowContext(ctx, q, args...).Scan(&found)
			if err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					continue // this batch had no match; try the next
				}
				return false, errors.NewStorageError("failed to check on_main_chain for blocks", err)
			}
			return true, nil // ANY-of short-circuit
		}
		return false, nil
	}

	// CTE fallback when on_main_chain is being rebuilt.
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
