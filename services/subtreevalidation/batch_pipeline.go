package subtreevalidation

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2"
)

// runLoadProcessPipeline drives CheckBlockSubtrees' per-batch work as a
// producer/consumer pipeline: it loads batch N+1 while batch N is being
// processed, but calls process strictly serially and in batch order.
//
// This shape exists because the LOAD phase (loadSubtreeBatch) is read-only with
// respect to the UTXO set — it only fetches and decodes subtree transactions —
// whereas PROCESS (processTransactionsInLevels) performs the Spend/Create
// mutations and MUST stay in-order: a child in batch N+1 may spend a parent
// created in batch N. Overlapping the next batch's load with the current
// batch's process reclaims the otherwise-idle gap between batches without
// changing validation ordering.
//
// Contract:
//   - process is never called concurrently with itself and is called for
//     batches 0..numBatches-1 in ascending order, stopping at the first error.
//   - load runs at most one batch ahead of process (unbuffered hand-off), so at
//     most two batches' transactions are resident at once.
//   - On any load or process error the run is aborted: the producer is
//     cancelled and release is called exactly once for every batch whose arenas
//     were loaded but whose normal post-process release did not run (i.e. the
//     batches the producer loaded ahead). A batch that fails to load is expected
//     to have already released its own arenas (loadSubtreeBatch does this), so
//     it must return nil arenas.
//
// release is always invoked for a successfully processed batch's arenas
// (success or process-error) and for any loaded-ahead batch left in flight on
// abort.
func runLoadProcessPipeline(
	ctx context.Context,
	numBatches int,
	load func(ctx context.Context, batchIdx int) ([]*bt.Tx, []*bt.Arena, error),
	process func(batchIdx int, txs []*bt.Tx, arenas []*bt.Arena) error,
	release func(arenas []*bt.Arena),
) error {
	if numBatches <= 0 {
		return nil
	}

	type loadedBatch struct {
		idx    int
		txs    []*bt.Tx
		arenas []*bt.Arena
		err    error
	}

	pctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Unbuffered: the producer blocks on send until the consumer takes the
	// previous batch, so it runs exactly one batch ahead — bounding resident
	// memory to two batches.
	ch := make(chan loadedBatch)

	go func() {
		defer close(ch)

		for i := 0; i < numBatches; i++ {
			txs, arenas, err := load(pctx, i)

			select {
			case ch <- loadedBatch{idx: i, txs: txs, arenas: arenas, err: err}:
			case <-pctx.Done():
				// Consumer aborted before taking this batch; it never reaches
				// the drain loop, so release here to avoid a leak.
				if err == nil {
					release(arenas)
				}

				return
			}

			if err != nil {
				return
			}
		}
	}()

	var retErr error

	for b := range ch {
		if retErr != nil {
			// Draining loaded-ahead batches after an abort: release their
			// arenas (a failed-load batch carries none).
			if b.err == nil {
				release(b.arenas)
			}

			continue
		}

		if b.err != nil {
			retErr = b.err

			cancel()

			continue
		}

		// Release via defer so the batch's arenas return to the pool even if
		// process panics (the panic unwinds through here to the recover in
		// CheckBlockSubtrees); a plain post-call release would be skipped.
		perr := func() error {
			defer release(b.arenas)
			return process(b.idx, b.txs, b.arenas)
		}()

		if perr != nil {
			retErr = perr

			cancel()

			continue
		}
	}

	return retErr
}
