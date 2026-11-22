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
// their blocks. Nothing reads the rows yet, so they are measured, never acted on: rows that fail
// their check, or whose insert fails, are rolled back to a savepoint and counted, and the block
// commits as it would without lanes.

// laneMapRowsSavepoint brackets a block's rows inside its transaction. Postgres aborts the whole
// transaction on a failed statement; rolling back to the savepoint keeps the block's own writes.
const (
	laneMapRowsSavepoint         = `SAVEPOINT lane_map_rows`
	laneMapRowsRollbackSavepoint = `ROLLBACK TO SAVEPOINT lane_map_rows`
	laneMapRowsReleaseSavepoint  = `RELEASE SAVEPOINT lane_map_rows`
)

// laneRowsToStore is the rows StoreBlock is to insert with its block, already checked, and what
// the check and the insert took.
type laneRowsToStore struct {
	rows    []lane.MapRow
	elapsed time.Duration
	// block is the lane metrics' block label, lane.BlockPeer or lane.BlockOwn.
	block string
}

// checkedLaneMapRows returns the rows StoreBlock is to insert with block, checked with
// lane.CheckMapRows, or nil when there are none, the block is stored as invalid, or the check
// fails, which is counted and logged.
func (s *SQL) checkedLaneMapRows(block *model.Block, o options.StoreBlockOptions) *laneRowsToStore {
	if len(o.LaneMapRows) == 0 || o.Invalid {
		return nil
	}

	start := time.Now()

	if err := lane.CheckMapRows(o.LaneMapRows); err != nil {
		lane.CountRowInsertFailure(o.LaneMetricsBlock())
		s.logger.Warnf("[StoreBlock][%s] storing the block without its lane map rows, which fail their check: %v", block.Hash().String(), err)

		return nil
	}

	return &laneRowsToStore{rows: o.LaneMapRows, elapsed: time.Since(start), block: o.LaneMetricsBlock()}
}

// insertLaneMapRowsWithBlock inserts rows for the block just written as blockID, inside tx, under
// a savepoint. A failed insert is rolled back to the savepoint, counted and logged, and returns
// nil, so tx commits the block alone; only a failure of the savepoint itself, which leaves tx
// unusable, is returned. The time taken is added to rows.elapsed, which StoreBlock reports once
// the block commits.
func (s *SQL) insertLaneMapRowsWithBlock(ctx context.Context, tx *sql.Tx, block *model.Block, blockID uint64, rows *laneRowsToStore) error {
	start := time.Now()

	defer func() { rows.elapsed += time.Since(start) }()

	if blockID > math.MaxUint32 {
		lane.CountRowInsertFailure(rows.block)
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

		lane.CountRowInsertFailure(rows.block)
		s.logger.Warnf("[StoreBlock][%s] storing the block without its lane map rows, whose insert failed: %v", block.Hash().String(), err)
	}

	_, err := tx.ExecContext(ctx, laneMapRowsReleaseSavepoint)

	return err
}
