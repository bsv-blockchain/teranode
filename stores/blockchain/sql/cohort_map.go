// Package sql implements the blockchain.Store interface using SQL database backends.
//
// This file implements the cohort map: the two tables behind the cohort-based
// mined state design (issue 556). cohort_map says which cohorts a block
// contains, and cohort_split_allocations hands out the synthetic cohort number
// used when a cohort has to be split around a block boundary.
//
// The map is insert-only. No row is ever updated or deleted by this code, so
// replaying a block's map writes after a crash changes nothing, and a reorg is a
// pure flag flip on the blocks table rather than a rewrite here.
package sql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/cohort"
	"github.com/bsv-blockchain/teranode/util/tracing"
)

// cohortMapInsertBatch caps how many rows go into one INSERT. Each row carries
// four placeholders, so this keeps a statement at 800 parameters, well under
// both sqlite's default SQLITE_MAX_VARIABLE_NUMBER and postgres's 65535.
const cohortMapInsertBatch = 200

// cohortLookupBatch caps how many cohorts go into one IN() list, for the same
// reason maxIDsPerCheckBatch does on the block-id side.
const cohortLookupBatch = 1000

// splitAllocationAttempts is how many times AllocateSplitCohort re-reads and
// retries after losing a race for a synthetic number. Each retry recomputes the
// next free number, so a loser only has to go round once per concurrent winner.
const splitAllocationAttempts = 5

// RecordCohortMap inserts cohort->block rows. The map is insert-only: an
// existing row is left exactly as it is, which is what makes replay after a
// crash a no-op. Passing no rows is a no-op and not an error.
//
// Parameters:
//   - ctx: Context for managing request lifecycle and cancellation
//   - rows: Cohort/block pairs to record
//
// Returns:
//   - error: Error if any batch fails to insert
func (s *SQL) RecordCohortMap(ctx context.Context, rows []model.CohortMapRow) error {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "sql:RecordCohortMap")
	defer deferFn()

	if len(rows) == 0 {
		return nil
	}

	for _, row := range rows {
		if row.Cohort.IsUnset() {
			return errors.NewInvalidArgumentError("cannot map the unset cohort to block %d", row.BlockID)
		}
	}

	for start := 0; start < len(rows); start += cohortMapInsertBatch {
		end := start + cohortMapInsertBatch
		if end > len(rows) {
			end = len(rows)
		}

		if err := s.recordCohortMapBatch(ctx, rows[start:end]); err != nil {
			return err
		}
	}

	return nil
}

// recordCohortMapBatch inserts one batch of rows in a single statement,
// ignoring any row that is already there.
func (s *SQL) recordCohortMapBatch(ctx context.Context, rows []model.CohortMapRow) error {
	tuples := make([]string, len(rows))
	args := make([]interface{}, 0, len(rows)*4)

	for i, row := range rows {
		base := i * 4
		tuples[i] = fmt.Sprintf("($%d,$%d,$%d,$%d)", base+1, base+2, base+3, base+4)
		args = append(args, uint64(row.Cohort), row.BlockID, row.MemberCount, row.Verified)
	}

	var q string

	if s.engine == util.Postgres {
		//#nosec G201 -- the interpolated text is generated placeholders, never user input
		q = fmt.Sprintf(`
			INSERT INTO cohort_map (cohort, block_id, member_count, verified)
			VALUES %s
			ON CONFLICT (cohort, block_id) DO NOTHING
		`, strings.Join(tuples, ","))
	} else {
		//#nosec G201 -- the interpolated text is generated placeholders, never user input
		q = fmt.Sprintf(`
			INSERT OR IGNORE INTO cohort_map (cohort, block_id, member_count, verified)
			VALUES %s
		`, strings.Join(tuples, ","))
	}

	if _, err := s.db.ExecContext(ctx, q, args...); err != nil {
		return errors.NewStorageError("failed to record cohort map rows", err)
	}

	return nil
}

// CohortBlocks returns, for each requested cohort, the blocks it maps to
// together with their chain state. A cohort that maps to nothing is simply
// absent from the returned map: that is the ordinary "not mined" answer and is
// never an error.
//
// The chain state comes back as data. blocks.on_main_chain can be transiently
// false for a block that IS on the best chain, which is why
// CheckBlockIsInCurrentChain refuses to answer false from the flag alone and
// falls through to a flag-free parent_id walk before rejecting anything. This
// method does not make that judgement; deciding whether a cohort is mined is
// left to the caller.
//
// Parameters:
//   - ctx: Context for managing request lifecycle and cancellation
//   - cohorts: Cohorts to look up
//
// Returns:
//   - map[cohort.ID][]model.CohortBlock: The blocks each mapped cohort belongs to
//   - error: Error if the lookup fails
func (s *SQL) CohortBlocks(ctx context.Context, cohorts []cohort.ID) (map[cohort.ID][]model.CohortBlock, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "sql:CohortBlocks")
	defer deferFn()

	result := make(map[cohort.ID][]model.CohortBlock)

	if len(cohorts) == 0 {
		return result, nil
	}

	for start := 0; start < len(cohorts); start += cohortLookupBatch {
		end := start + cohortLookupBatch
		if end > len(cohorts) {
			end = len(cohorts)
		}

		if err := s.cohortBlocksBatch(ctx, cohorts[start:end], result); err != nil {
			return nil, err
		}
	}

	return result, nil
}

// cohortBlocksBatch reads one batch of cohorts and merges the rows it finds into
// result.
func (s *SQL) cohortBlocksBatch(ctx context.Context, cohorts []cohort.ID, result map[cohort.ID][]model.CohortBlock) error {
	placeholders := make([]string, len(cohorts))
	args := make([]interface{}, len(cohorts))

	for i, id := range cohorts {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = uint64(id)
	}

	//#nosec G201 -- the interpolated text is generated placeholders, never user input
	q := fmt.Sprintf(`
		SELECT
		 cm.cohort
		,cm.block_id
		,cm.member_count
		,cm.verified
		,b.height
		,b.hash
		,b.on_main_chain
		,b.invalid
		FROM cohort_map cm
		JOIN blocks b ON b.id = cm.block_id
		WHERE cm.cohort IN (%s)
		ORDER BY cm.cohort ASC, b.height ASC, cm.block_id ASC
	`, strings.Join(placeholders, ","))

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return errors.NewStorageError("failed to read cohort map rows", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			cohortID  uint64
			hashBytes []byte
			block     model.CohortBlock
		)

		if err := rows.Scan(
			&cohortID,
			&block.BlockID,
			&block.MemberCount,
			&block.Verified,
			&block.Height,
			&hashBytes,
			&block.OnMainChain,
			&block.Invalid,
		); err != nil {
			return errors.NewStorageError("failed to scan cohort map row", err)
		}

		block.Hash, err = chainhash.NewHash(hashBytes)
		if err != nil {
			return errors.NewStorageError("failed to read block hash for cohort %d", cohortID, err)
		}

		id := cohort.ID(cohortID)
		result[id] = append(result[id], block)
	}

	if err := rows.Err(); err != nil {
		return errors.NewStorageError("failed to iterate cohort map rows", err)
	}

	return nil
}

// AllocateSplitCohort returns the synthetic cohort number reserved for splitting
// sourceCohort against blockHash, allocating one on first call and returning the
// same number on every subsequent call for the same pair. That is what makes a
// split replayable: a node that crashes part-way through and comes back reuses
// the number it started with rather than minting a second one.
//
// Numbers come from the synthetic range only, [cohort.FirstSynthetic,
// cohort.LastSynthetic], and the call fails with a clear error once that range
// is exhausted.
//
// Parameters:
//   - ctx: Context for managing request lifecycle and cancellation
//   - sourceCohort: The straddling cohort being split
//   - blockHash: Hash of the block the split is being made against
//
// Returns:
//   - cohort.ID: The synthetic cohort reserved for this (source, block) pair
//   - error: Error if the allocation fails
func (s *SQL) AllocateSplitCohort(ctx context.Context, sourceCohort cohort.ID, blockHash *chainhash.Hash) (cohort.ID, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "sql:AllocateSplitCohort")
	defer deferFn()

	if blockHash == nil {
		return cohort.Unset, errors.NewInvalidArgumentError("block hash cannot be nil")
	}

	if !sourceCohort.IsClock() && !sourceCohort.IsSynthetic() {
		return cohort.Unset, errors.NewInvalidArgumentError("cannot split %s: only clock and synthetic cohorts hold splittable transactions", sourceCohort)
	}

	hashBytes := blockHash.CloneBytes()

	for attempt := 0; attempt < splitAllocationAttempts; attempt++ {
		allocated, ok, err := s.allocateSplitCohortOnce(ctx, sourceCohort, hashBytes)
		if err != nil {
			return cohort.Unset, err
		}

		if ok {
			return allocated, nil
		}
	}

	return cohort.Unset, errors.NewStorageError("gave up allocating a split cohort for %s against %s after %d attempts", sourceCohort, blockHash.String(), splitAllocationAttempts)
}

// allocateSplitCohortOnce runs one allocation attempt in its own transaction. It
// returns ok=false when the insert was swallowed because another allocation took
// the number first, in which case the caller retries and recomputes.
//
// The transaction runs through RetryTx, so a lock error (SQLite BUSY or LOCKED,
// a Postgres serialization failure or deadlock) rolls back and re-runs the whole
// attempt instead of surfacing as a failed insert. The closure returns driver
// errors unwrapped so RetryTx can recognise them, and the outcome is reset at the
// top of every run because a retried run starts from scratch.
func (s *SQL) allocateSplitCohortOnce(ctx context.Context, sourceCohort cohort.ID, hashBytes []byte) (cohort.ID, bool, error) {
	var (
		allocated cohort.ID
		ok        bool
		step      string
	)

	var insertQ string
	if s.engine == util.Postgres {
		// No conflict target: the row can collide either on the (source, block)
		// primary key or on the unique new_cohort index, and both mean somebody
		// else got there first.
		insertQ = `INSERT INTO cohort_split_allocations (source_cohort, block_hash, new_cohort) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING`
	} else {
		insertQ = `INSERT OR IGNORE INTO cohort_split_allocations (source_cohort, block_hash, new_cohort) VALUES ($1,$2,$3)`
	}

	const selectQ = `SELECT new_cohort FROM cohort_split_allocations WHERE source_cohort = $1 AND block_hash = $2`

	err := s.db.RetryTx(ctx, nil, func(tx *sql.Tx) error {
		allocated, ok, step = cohort.Unset, false, ""

		// On Postgres, take a lock that conflicts with itself before reading
		// MAX(new_cohort). Without it every concurrent allocation reads the same
		// MAX, picks the same number and all but one lose, so N racers need N
		// rounds and run out of attempts. Readers are not blocked. SQLite already
		// runs one writer at a time, and a lock error there is retried by RetryTx.
		if s.engine == util.Postgres {
			if _, err := tx.ExecContext(ctx, `LOCK TABLE cohort_split_allocations IN SHARE ROW EXCLUSIVE MODE`); err != nil {
				step = "lock cohort_split_allocations"
				return err
			}
		}

		// Already allocated for this exact pair? Then that number is the answer,
		// however many times we are asked.
		var existing uint64

		err := tx.QueryRowContext(ctx, selectQ, uint64(sourceCohort), hashBytes).Scan(&existing)

		switch {
		case err == nil:
			allocated, ok = cohort.ID(existing), true
			return nil
		case errors.Is(err, sql.ErrNoRows):
			// Fall through and allocate a fresh number.
		default:
			step = "read split cohort allocation"
			return err
		}

		next, err := nextSyntheticCohort(ctx, tx)
		if err != nil {
			step = "pick the next synthetic cohort"
			return err
		}

		if _, err = tx.ExecContext(ctx, insertQ, uint64(sourceCohort), hashBytes, uint64(next)); err != nil {
			step = "insert split cohort allocation"
			return err
		}

		// Re-read rather than trusting the insert: if it was swallowed because a
		// concurrent allocation took our number, the row is not ours and the
		// caller has to try again with a recomputed number.
		var stored uint64

		err = tx.QueryRowContext(ctx, selectQ, uint64(sourceCohort), hashBytes).Scan(&stored)

		switch {
		case err == nil:
			allocated, ok = cohort.ID(stored), true
			return nil
		case errors.Is(err, sql.ErrNoRows):
			return nil
		default:
			step = "confirm split cohort allocation"
			return err
		}
	})
	if err != nil {
		if step == "" {
			step = "run split cohort allocation"
		}

		return cohort.Unset, false, errors.NewStorageError("failed to %s", step, err)
	}

	return allocated, ok, nil
}

// nextSyntheticCohort returns the lowest unused synthetic cohort number, which
// is one above the highest allocated so far, or cohort.FirstSynthetic when
// nothing has been allocated yet. It fails once the synthetic range is used up.
func nextSyntheticCohort(ctx context.Context, tx *sql.Tx) (cohort.ID, error) {
	var highest sql.NullInt64

	if err := tx.QueryRowContext(ctx, `SELECT MAX(new_cohort) FROM cohort_split_allocations`).Scan(&highest); err != nil {
		return cohort.Unset, errors.NewStorageError("failed to read the highest allocated split cohort", err)
	}

	if !highest.Valid {
		return cohort.FirstSynthetic, nil
	}

	if highest.Int64 >= int64(cohort.LastSynthetic) {
		return cohort.Unset, errors.NewStorageError("synthetic cohort range is exhausted: highest allocated is %d and the range ends at %d", highest.Int64, int64(cohort.LastSynthetic))
	}

	next := cohort.ID(highest.Int64 + 1)
	if next < cohort.FirstSynthetic {
		return cohort.FirstSynthetic, nil
	}

	return next, nil
}
