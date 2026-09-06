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
	// starts at 0. Genesis is committed at id 0, so a real chain holding nothing but
	// genesis has a committed MAX(id) of 0 too, and this check cannot tell that apart
	// from "not yet initialised" (the synchronous load errored and the first async
	// rebuild has not completed). Reading both as uninitialised is deliberate and safe:
	// both go to SQL, which is correct either way and costs one query on a one-block
	// chain. Do not "fix" this by treating 0 as initialised. With maxID==0 the id<=maxID filter below
	// would drop EVERY committed candidate as "dangling" and return a (false, nil)
	// false negative — which checkOldBlockIDs escalates into a PERMANENT block
	// invalidation. Defer to the authoritative, flag-free parent_id CTE instead;
	// it needs no maxBlockID and cannot produce that false negative.
	if maxID == 0 {
		return s.checkBlockIsInCurrentChainSQL(ctx, blockIDs)
	}

	// Fail safe when the forked set has never been built. The startup rebuild runs
	// asynchronously and mainChainRebuilding is held for it, but that guard is
	// released whether the rebuild succeeded or failed, and rebuildOffChainSet is
	// explicitly expected to time out on a cold cache during catchup. maxBlockID
	// survives such a failure, because refreshMaxBlockID runs first and is a cheap
	// index-only scan, so the two checks above both pass while offChainBlockIDs is
	// still the empty map New() made.
	//
	// An empty forked set means every committed id is absent from it, and absence is
	// what this route reads as proof of main-chain membership. Every fork block and
	// every invalidated block on the node would come back on-chain until the next
	// successful rebuild. That is a FALSE POSITIVE, the direction this route added:
	// checkOldBlockIDs would accept a parent that lives only on a fork, and the asset
	// server would serve a merkle proof against a block that is not on the chain.
	//
	// lastSuccessfulRebuild is set only by a rebuild that completed, so zero means
	// "never built" and is unambiguous. It cannot race the startup path: the goroutine
	// that releases the guard above is the same one that runs the startup rebuild, so
	// a cleared guard together with a zero timestamp is precisely the failed-rebuild
	// case and nothing else.
	//
	// A rebuild that fails AFTER an earlier success is not covered here, and does not
	// need to be: the previous set still holds every fork the node knew about, only
	// blocks that forked since the last success are missing, and backgroundRefreshLoop
	// retries every two minutes.
	if s.lastSuccessfulRebuild.Load() == 0 {
		return s.checkBlockIsInCurrentChainSQL(ctx, blockIDs)
	}

	result, fromMemory, err := s.checkBlockIsInCurrentChainInMemory(ctx, blockIDs, maxID)
	if err != nil {
		return false, err
	}

	// While the forked-set route is being soaked, compute the same answer the
	// authoritative way and compare. See shadowCompareChainCheck.
	if fromMemory && s.chainCheckShadowCompare {
		s.shadowCompareChainCheck(ctx, blockIDs, result)
	}

	return result, nil
}

// checkBlockIsInCurrentChainInMemory answers ANY-of "is one of blockIDs on the main
// chain?" from the forked set and maxBlockID, with no query. fromMemory reports
// whether it managed that: it is false only on the about-to-reject path, where every
// candidate is in the forked set and the answer is confirmed against SQL first.
//
// Callers must have established that the in-memory route applies: the setting is on,
// no main-chain rebuild is in flight, and maxID is initialised.
func (s *SQL) checkBlockIsInCurrentChainInMemory(ctx context.Context, blockIDs []uint32, maxID uint32) (bool, bool, error) {
	s.offChainBlockIDsMu.RLock()
	offChain := s.offChainBlockIDs
	s.offChainBlockIDsMu.RUnlock()

	candidates := make([]uint32, 0, len(blockIDs))

	for _, id := range blockIDs {
		// Drop ids above the highest committed id. These are allocated-but-uncommitted
		// (AssignBlockID hands out an id, and the below-checkpoint sync paths stamp a
		// tx's BlockIDs with it, before AddBlock bumps maxBlockID), so they have no
		// committed row and are definitively not on the main chain. Pure in-memory
		// reject, no round trip, and consensus-critical: it keeps useInMemoryChainCheck
		// on/off nodes agreeing on ids that have been reserved but not yet stored.
		if id > maxID {
			continue
		}

		// Committed, and not in the forked set, so it is on the main chain. Answered
		// from memory with no query. See the contract note on rebuildOffChainSet for
		// why absence from the forked set is a positive proof here rather than the
		// unsound guess it was before PR 1043 closed phantom-id creation.
		if _, forked := offChain[id]; !forked {
			return true, true, nil
		}

		candidates = append(candidates, id)
	}

	// Every id was above maxBlockID, so this is the allocated-but-uncommitted reject and
	// not the forked-set accept. Report fromMemory=false so the shadow comparison skips it.
	// Both routes answer false here by construction, so comparing them cannot ever disagree:
	// counting it would pad the denominator with answers that prove nothing, and below the
	// checkpoint (where every node currently sits) this input is the dominant traffic. It
	// also costs a round trip per call to learn nothing.
	if len(candidates) == 0 {
		return false, false, nil
	}

	// Every candidate is in the forked set, so this call is about to reject. Confirm
	// against the authoritative SQL before doing so.
	//
	// The forked set is rebuilt FROM the on_main_chain flags (rebuildOffChainSet), so
	// a transiently-false flag on a block that IS on the best chain, a raced slow-path
	// StoreBlock whose reconcileOnMainChain failed or an exhausted startup rebuild,
	// lands that block in the forked set. Rejecting on it would be a FALSE NEGATIVE,
	// and checkOldBlockIDs escalates a negative into a PERMANENT ValidateBlock
	// invalidation that the self-healing flag never gets to undo. That is the incident
	// PR 1086 fixed and this path preserves.
	//
	// The asymmetry is deliberate. Membership of the forked set is enough to stop the
	// in-memory accept, never enough to reject on its own. Positives short-circuit
	// above and never reach here, so the confirmation only costs anything on the rare
	// about-to-reject call.
	result, err := s.checkBlockIsInCurrentChainSQL(ctx, candidates)

	return result, false, err
}

// shadowCompareChainCheck recomputes an in-memory answer the authoritative way and
// records any disagreement. It is the instrument for the forked-set soak: the whole
// question is whether the two routes ever differ on a live node, and the only honest
// way to find out is to run both and count.
//
// Three properties matter more than the measurement. It never changes the answer, a
// shadow query that cannot run is not an error, and a disagreement that repeats does
// not drown the node in log. Otherwise turning the instrument on would make the node
// worse than leaving it off, and nobody would leave it on long enough to learn
// anything.
func (s *SQL) shadowCompareChainCheck(ctx context.Context, blockIDs []uint32, inMemoryResult bool) {
	authoritative, err := s.checkBlockIsInCurrentChainSQL(ctx, blockIDs)
	if err != nil {
		// Count the failure as well as logging it. Without this a node whose shadow query
		// fails on every call is indistinguishable from one that never reached this route:
		// both report nothing, and "no mismatches" reads as a clean bill of health on a
		// node that never compared anything. The soak's whole value is being able to tell
		// those two apart.
		s.chainCheckShadowFailures.Add(1)
		s.logger.Debugf("[CheckBlockIsInCurrentChain] shadow comparison could not run for blocks (%v): %v", blockIDs, err)

		return
	}

	s.chainCheckShadowChecks.Add(1)

	if authoritative == inMemoryResult {
		return
	}

	total := s.chainCheckShadowMismatches.Add(1)

	if !shouldLogShadowMismatch(total) {
		return
	}

	s.logger.Errorf("[CheckBlockIsInCurrentChain] shadow mismatch #%d: forked-set route said %v, authoritative route said %v for blocks (%v). The forked-set route is not safe to run without the shadow comparison on this node.",
		total, inMemoryResult, authoritative, blockIDs)
}

// shouldLogShadowMismatch samples the per-mismatch error line.
//
// A disagreement that is systematic rather than freak, a gap id that several
// transactions all point at, say, does not happen once. It happens on every call that
// touches that id, which on a node serving asset traffic is an unbounded error line
// per request into a disk that has filled and taken a node down on this fleet before.
// An instrument that can do that is one operators turn off, and this one defaults on.
//
// The first ten carry the diagnostic value: the ids involved, and enough repetition to
// see whether it is one id or many. After that the running total is what matters, and
// the two-minute totals line from logShadowCompareTotals already carries it, so sample
// hard. The mismatch counter itself is never sampled, so the number in that line and
// the number in this function's message stay exact.
func shouldLogShadowMismatch(total uint64) bool {
	return total <= 10 || total%1000 == 0
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
