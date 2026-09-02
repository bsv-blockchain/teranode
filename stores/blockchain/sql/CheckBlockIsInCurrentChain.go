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

	// Fail safe when maxBlockID is uninitialised. It is loaded synchronously at
	// New() and refreshed by rebuildOffChainSet, but is held in an atomic that
	// starts at 0. Genesis is committed with id 1 before either runs, so a real
	// chain never has a committed MAX(id) of 0 — maxID==0 unambiguously means
	// "not yet initialised" (e.g. the synchronous load errored and the first
	// async rebuild has not completed). With maxID==0 the id<=maxID filter below
	// would drop EVERY committed candidate as "dangling" and return a (false, nil)
	// false negative — which checkOldBlockIDs escalates into a PERMANENT block
	// invalidation. Defer to the authoritative, flag-free parent_id CTE instead;
	// it needs no maxBlockID and cannot produce that false negative.
	if maxID == 0 {
		return s.checkBlockIsInCurrentChainSQL(ctx, blockIDs)
	}

	// Drop ids above the highest committed id. These are allocated-but-uncommitted
	// (GetNextBlockID writes a tx's BlockIDs before AddBlock bumps maxBlockID), so
	// they have no committed row and are definitively not on the main chain. This
	// is a pure in-memory reject with no DB round-trip and is consensus-critical:
	// it keeps useInMemoryChainCheck on/off nodes agreeing on dangling ids.
	candidates := make([]uint32, 0, len(blockIDs))
	for _, id := range blockIDs {
		if id <= maxID {
			candidates = append(candidates, id)
		}
	}

	if len(candidates) == 0 {
		return false, nil
	}

	// Confirm the committed candidates against the authoritative SQL route.
	//
	// We deliberately do NOT use the in-memory off-chain set as a negative
	// short-circuit. That set is rebuilt FROM the on_main_chain flags
	// (rebuildOffChainSet), so a transiently-false flag on a block that IS on the
	// best chain — a raced slow-path StoreBlock whose reconcileOnMainChain failed,
	// or an exhausted startup rebuild — lands the block in the off-chain set and
	// would make this a FALSE NEGATIVE. Because checkOldBlockIDs escalates a
	// negative into a PERMANENT ValidateBlock invalidation, a transient flag must
	// never be allowed to reject here. checkBlockIsInCurrentChainSQL confirms
	// positives via the indexed on_main_chain flag and confirms negatives via the
	// flag-free parent_id CTE, so it stays correct even while flags are mid-flux.
	// This path is only reached when every parent block id of a tx is off-chain or
	// uncommitted per the in-memory prefetch, which is rare, so the round-trip is
	// not on the hot path.
	return s.checkBlockIsInCurrentChainSQL(ctx, candidates)
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
		// No on_main_chain=true row matched in any batch. Do NOT return false here:
		// on_main_chain can be transiently false on a block that IS on the best
		// chain — a slow-path StoreBlock whose reconcileOnMainChain failed
		// (log-and-continue), or a startup rebuildOnMainChainFlag that exhausted
		// its retries/timed out. A false negative is not cosmetic here: the caller
		// (checkOldBlockIDs) escalates it into a PERMANENT ValidateBlock
		// invalidation, which the transient flag never gets a chance to undo. Fall
		// through to the authoritative, flag-free parent_id CTE walk below to
		// confirm the block really is off the chain before rejecting. Positives
		// already short-circuited above, so the CTE only runs on the rare
		// about-to-reject path.
	}

	// Authoritative, flag-free confirmation via the parent_id CTE walk. Reached
	// both when a rebuild is in progress (on_main_chain unreliable) and when the
	// flag fast-path above found no match (the flag may be transiently false on an
	// on-chain block). Batched so the block_ids UNION ALL never exceeds sqlite's
	// compound-SELECT limit; ANY-of short-circuits on the first on-chain hit.
	for start := 0; start < len(blockIDs); start += cteBlockIDBatch {
		end := start + cteBlockIDBatch
		if end > len(blockIDs) {
			end = len(blockIDs)
		}
		onChain, err := s.checkBlockIsInCurrentChainCTE(ctx, blockIDs[start:end])
		if err != nil {
			return false, err
		}
		if onChain {
			return true, nil
		}
	}
	return false, nil
}

// cteBlockIDBatch caps how many ids go into one block_ids CTE. The CTE materialises
// the ids as a UNION ALL of single-row SELECTs, and sqlite limits a compound SELECT
// to 500 terms (SQLITE_MAX_COMPOUND_SELECT), so stay safely under it. Postgres has
// no comparable limit. In practice CheckBlockIsInCurrentChain is called with one
// tx's handful of parent block ids, so batching almost never splits.
const cteBlockIDBatch = 400

// checkBlockIsInCurrentChainCTE answers ANY-of "is one of blockIDs on the main
// chain?" by walking parent_id backward from the chain_work-best block. It does
// NOT read on_main_chain, so it stays correct even while those flags are being
// rebuilt or are transiently wrong. Callers must keep len(blockIDs) within
// cteBlockIDBatch (the sqlite compound-SELECT cap).
func (s *SQL) checkBlockIsInCurrentChainCTE(ctx context.Context, blockIDs []uint32) (bool, error) {
	if len(blockIDs) == 0 {
		return false, nil
	}

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
