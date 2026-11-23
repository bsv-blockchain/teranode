package sql

import (
	"context"
	"database/sql"
	"math"
	"time"

	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/util/lane"
)

// The Phase 1 block gate's store half, at utxostore_laneMigration shadow: StoreBlock inserts the
// rows its caller built (options.WithLaneMapRows) in the block's own database transaction, so rows
// are only ever inserted with their block, and commit or roll back with it. Rows are kept for ever:
// DeleteBlock, which the rewind tool uses, removes the block and leaves its rows. Block IDs come
// from a sequence that never hands one out again (BIGSERIAL, SQLite AUTOINCREMENT), so such rows
// name a block ID no block has, which no chain reaches, and a reader has to resolve rows through
// their blocks. Readers load the rows at shadow and do not act on them: rows that fail their
// check, or whose insert fails, are rolled back to a savepoint, and the block commits as it would
// without lanes. What is measured is counted once the block is stored, from the attempt that
// stored it, so a store that is retried, or that fails, adds nothing for the attempts it gave up.

// laneMapRowsSavepoint brackets a block's rows inside its transaction. Postgres aborts the whole
// transaction on a failed statement; rolling back to the savepoint keeps the block's own writes.
const (
	laneMapRowsSavepoint         = `SAVEPOINT lane_map_rows`
	laneMapRowsRollbackSavepoint = `ROLLBACK TO SAVEPOINT lane_map_rows`
	laneMapRowsReleaseSavepoint  = `RELEASE SAVEPOINT lane_map_rows`
)

// laneRowsToStore is the rows StoreBlock is to insert with its block, already checked, and what
// the last attempt to insert them did. insertLaneMapRowsWithBlock sets insert, stored and refused
// afresh on each attempt, so after the attempt that commits they describe that attempt alone.
type laneRowsToStore struct {
	rows []lane.MapRow
	// check is what the in-memory check took, before the block's transaction began.
	check time.Duration
	// insert is what the insert took in the last attempt.
	insert time.Duration
	// stored is whether the last attempt stored the rows.
	stored bool
	// refused is whether the last attempt left the rows out: a block ID too wide for the map, or
	// an insert rolled back to its savepoint.
	refused bool
}

// observe records what the committed attempt did with the rows: their gate time when it stored
// them, a failure when it left them out.
func (r *laneRowsToStore) observe() {
	switch {
	case r.stored:
		lane.ObserveGate(lane.GateStageInsert, r.check+r.insert)
	case r.refused:
		lane.CountRowInsertFailure()
	}
}

// checkedLaneMapRows returns the rows StoreBlock is to insert with block, checked with
// lane.CheckMapRows, or nil when there are none or the block is asked to be stored as invalid. A
// block stored as invalid because its parent is invalid is caught later: StoreBlock skips its
// insert and counts nothing for it. refused is whether the rows failed their check, which is
// logged here; StoreBlock counts it once the block is stored valid.
func (s *SQL) checkedLaneMapRows(block *model.Block, o options.StoreBlockOptions) (rows *laneRowsToStore, refused bool) {
	if len(o.LaneMapRows) == 0 || o.Invalid {
		return nil, false
	}

	start := time.Now()

	if err := lane.CheckMapRows(o.LaneMapRows); err != nil {
		s.logger.Warnf("[StoreBlock][%s] storing the block without its lane map rows, which fail their check: %v", block.Hash().String(), err)

		return nil, true
	}

	return &laneRowsToStore{rows: o.LaneMapRows, check: time.Since(start)}, false
}

// insertLaneMapRowsWithBlock inserts rows for the block just written as blockID, inside tx, under
// a savepoint. A failed insert is rolled back to the savepoint and logged, and returns nil, so tx
// commits the block alone; only a failure of the savepoint itself, which leaves tx unusable, is
// returned. It records in rows what this attempt did, for StoreBlock to report once the block
// commits (laneRowsToStore.observe).
func (s *SQL) insertLaneMapRowsWithBlock(ctx context.Context, tx *sql.Tx, block *model.Block, blockID uint64, rows *laneRowsToStore) error {
	start := time.Now()

	rows.stored, rows.refused = false, false

	defer func() { rows.insert = time.Since(start) }()

	if blockID > math.MaxUint32 {
		rows.refused = true
		s.logger.Warnf("[StoreBlock][%s] storing the block without its lane map rows: block ID %d is wider than the map's 32 bits", block.Hash().String(), blockID)

		return nil
	}

	if _, err := tx.ExecContext(ctx, laneMapRowsSavepoint); err != nil {
		return err
	}

	if err := s.insertLaneMapRowsTx(ctx, tx, uint32(blockID), rows.rows); err != nil {
		if _, rbErr := tx.ExecContext(ctx, laneMapRowsRollbackSavepoint); rbErr != nil {
			return rbErr
		}

		rows.refused = true
		s.logger.Warnf("[StoreBlock][%s] storing the block without its lane map rows, whose insert failed: %v", block.Hash().String(), err)
	}

	if _, err := tx.ExecContext(ctx, laneMapRowsReleaseSavepoint); err != nil {
		return err
	}

	rows.stored = !rows.refused

	return nil
}
