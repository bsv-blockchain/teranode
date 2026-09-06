package postgres

import (
	"context"

	"github.com/bsv-blockchain/teranode/errors"
)

// dahSafeTip returns the highest height Worker 2 may scan: the persisted block
// height minus a safety lag, so no spend tagged in range can still be mid-commit.
func (s *Store) dahSafeTip(lag int64) int64 {
	h := int64(s.GetBlockHeight())
	if h <= lag {
		return 0
	}

	return h - lag
}

// runDAHCursor is the Worker 2 background entry point: it drives the server-side
// dah_sweep_batch() procedure (runDAHCursorProc) and owns the cursorWg lifetime.
// The procedure is bootstrapped during schema creation (store.New fails if it
// cannot be installed), so the cursor can always assume it is present.
//
// It also launches the stagnation monitor alongside the cursor proc, sharing
// the same ctx, and waits for the monitor to fully exit before returning so
// stop()'s cursorWg.Wait() cannot race a monitor goroutine still reading
// s.store.pool into a subsequent pool.Close().
func (s *postgresPrunerService) runDAHCursor(ctx context.Context) {
	defer s.cursorWg.Done() // let stop() wait for this goroutine to fully exit

	monitorDone := make(chan struct{})

	go func() {
		defer close(monitorDone)
		s.runDAHStagnationMonitor(ctx)
	}()

	s.runDAHCursorProc(ctx)
	<-monitorDone
}

// RewindDAHWatermark moves the sweep watermark BACK to forkHeight so that a reorg
// causes (forkHeight, tip] to be re-swept: the new chain's spends are tagged at
// heights the watermark may already have passed, and must be re-evaluated. The
// guard last_swept_height > forkHeight ensures the watermark is never advanced
// forward by this call (it only ever rewinds).
//
// A re-sweep after a rewind re-folds spends that survived the reorg, but the fold is
// idempotent: it OR-s each spend's bit into txs.spent_bits, so folding the same spend
// twice is a no-op. There is no counter to inflate and no ground-truth recount to
// protect — the DAH stamp gate is the single-row check bit_count(spent_bits) =
// spendable_count, which a duplicate fold cannot move. The reconcile backstop
// recomputes spent_bits from spends over bounded slices and un-stamps any wrong stamp.
func (s *Store) RewindDAHWatermark(ctx context.Context, forkHeight int64) error {
	if _, err := s.pool.Exec(ctx,
		`UPDATE dah_part_watermark SET last_swept_height = $1 WHERE last_swept_height > $1`,
		forkHeight,
	); err != nil {
		return errors.NewStorageError("[dahSweep] rewind watermark to %d", forkHeight, err)
	}

	return nil
}
