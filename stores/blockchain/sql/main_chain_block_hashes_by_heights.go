package sql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/jackc/pgx/v5/pgtype"
)

// MainChainBlockHashesByHeights returns the hash of the main-chain block at each
// requested height in a single indexed query, but only when startHash is itself
// on the main chain. When startHash is a fork tip, the store is mid-rebuild, or
// any requested height is absent, it returns ok=false (nil error) so the caller
// falls back to the per-height recursive-CTE walk.
//
// Correctness: the main chain is linear, so the on_main_chain=true block at a
// given height (<= startHeight) is the unique ancestor of startHash at that
// height — identical to what the recursive walk would return. This mirrors the
// preflight + fallback used by GetLatestBlockHeaderFromBlockLocator.
func (s *SQL) MainChainBlockHashesByHeights(ctx context.Context, startHash *chainhash.Hash, heights []uint32) (map[uint32]*chainhash.Hash, bool, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "sql:MainChainBlockHashesByHeights")
	defer deferFn()

	if startHash == nil || len(heights) == 0 {
		return nil, false, nil
	}

	// Reorg guard: during a main-chain rebuild the on_main_chain flags are in
	// flux, so the fast path cannot be trusted. Fall back.
	if s.mainChainRebuilding.Load() > 0 {
		return nil, false, nil
	}

	// Preflight: only safe when startHash is on the main chain. Fetch the hash's
	// own height and use it as the ceiling so results are constrained to
	// ancestors-of-or-equal-to startHash — matching the recursive walk exactly
	// and avoiding any trust in a caller-supplied height. Treat DB errors or an
	// unknown hash (ErrNoRows) as "not on main chain" so the CTE walk stays
	// authoritative. Mirrors GetLatestBlockHeaderFromBlockLocator.
	var (
		startOnMain bool
		startHeight int64
	)
	if err := s.db.QueryRowContext(ctx,
		`SELECT on_main_chain, height FROM blocks WHERE hash = $1 LIMIT 1`,
		startHash[:],
	).Scan(&startOnMain, &startHeight); err != nil {
		return nil, false, nil
	}

	if !startOnMain {
		return nil, false, nil
	}

	q, args := s.mainChainHeightsQuery(startHeight, heights)

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, false, errors.NewStorageError("[MainChainBlockHashesByHeights] query failed", err)
	}
	defer rows.Close()

	result, err := scanHeightHashes(rows, len(heights))
	if err != nil {
		return nil, false, err
	}

	// Defensive: if the result is incomplete — any requested height missing, or
	// duplicate input heights collapsed by the map — we cannot guarantee a
	// correct locator, so fall back rather than emit a short or wrong one. This
	// method takes arbitrary heights and does not assume the caller's schedule.
	if len(result) != len(heights) {
		return nil, false, nil
	}

	return result, true, nil
}

// mainChainHeightsQuery builds the engine-specific SELECT (and its bound args)
// returning (height, hash) for the given on-main-chain heights, capped at
// startHeight. Postgres binds a single array via ANY($2); SQLite expands
// positional IN placeholders. Heights are always passed as bound parameters —
// never interpolated — so the dynamic SQLite string carries no injection risk.
func (s *SQL) mainChainHeightsQuery(startHeight int64, heights []uint32) (string, []interface{}) {
	if s.engine == util.Postgres {
		hs := make(pgtype.FlatArray[int64], len(heights))
		for i, h := range heights {
			hs[i] = int64(h)
		}

		return `
			SELECT height, hash
			FROM blocks
			WHERE on_main_chain = true
			  AND height <= $1
			  AND height = ANY($2)`, []interface{}{startHeight, hs}
	}

	placeholders := make([]string, len(heights))
	args := make([]interface{}, len(heights)+1)
	args[0] = startHeight

	for i, h := range heights {
		placeholders[i] = fmt.Sprintf("$%d", i+2)
		args[i+1] = int64(h)
	}

	return fmt.Sprintf(`
		SELECT height, hash
		FROM blocks
		WHERE on_main_chain = true
		  AND height <= $1
		  AND height IN (%s)`, strings.Join(placeholders, ",")), args
}

// scanHeightHashes reads (height, hash) rows into a map keyed by height. The
// caller compares len against the number of requested heights to detect an
// incomplete result; this helper only surfaces real DB/decode errors.
func scanHeightHashes(rows *sql.Rows, expectedLen int) (map[uint32]*chainhash.Hash, error) {
	result := make(map[uint32]*chainhash.Hash, expectedLen)

	for rows.Next() {
		var (
			height    uint32
			hashBytes []byte
		)

		if err := rows.Scan(&height, &hashBytes); err != nil {
			return nil, errors.NewStorageError("[MainChainBlockHashesByHeights] scan failed", err)
		}

		hash, err := chainhash.NewHash(hashBytes)
		if err != nil {
			return nil, errors.NewProcessingError("[MainChainBlockHashesByHeights] failed to convert hash", err)
		}

		result[height] = hash
	}

	if err := rows.Err(); err != nil {
		return nil, errors.NewStorageError("[MainChainBlockHashesByHeights] rows error", err)
	}

	return result, nil
}
