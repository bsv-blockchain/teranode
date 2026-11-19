package pruner

import (
	"context"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"golang.org/x/sync/errgroup"
)

// A short, bounded window combines workers that finish together. Waiting for
// every partition would delay deletes behind the slowest worker; sending each
// tail immediately would retain one final batch per worker.
const drainCoalesceWindow = 250 * time.Microsecond

// drainReadyCarry splits acknowledged deletes into bounded parallel batches.
// Chunk processing still pipelines within each worker; only completed workers'
// tails reach this method. Their markers have already landed. A blob stays with
// its master's batch, and each batch retains the marker information needed to
// withdraw a refused master's markers.
func (s *Service) drainReadyCarry(ctx context.Context, carry *deleteCarry) (deleted, heldBack int, err error) {
	pending := carry.take()
	limit := max(s.chunkSize, pending.batchSize)
	if limit <= 0 {
		limit = 1000
	}

	var batches []*deleteCarry
	newBatch := func() *deleteCarry {
		batch := &deleteCarry{blockHeight: pending.blockHeight, updates: pending.updates}
		batches = append(batches, batch)
		return batch
	}
	masters := make(map[chainhash.Hash]*deleteCarry, len(pending.deletions))
	var batch *deleteCarry
	batchRecords := 0
	for _, deletion := range pending.deletions {
		// Include a master's later page cleanup in its allowance as well.
		if batch == nil || batchRecords+len(deletion.keys) > limit {
			batch = newBatch()
			batchRecords = 0
		}
		batch.deletions = append(batch.deletions, deletion)
		batchRecords += len(deletion.keys)
		if !deletion.page {
			masters[*deletion.txHash] = batch
		}
	}
	for _, page := range pending.pages {
		if batch == nil || batchRecords >= limit {
			batch = newBatch()
			batchRecords = 0
		}
		batch.pages = append(batch.pages, page)
		batchRecords++
	}
	for _, file := range pending.files {
		owner := masters[*file.txHash]
		if owner == nil {
			carry.add(pending)
			return 0, 0, errors.NewProcessingError("pruner blob has no carried master: %s", file.txHash)
		}
		owner.files = append(owner.files, file)
	}

	var group errgroup.Group
	// Each contributing worker previously drained one batch concurrently.
	// Reuse that allowance without competing with workers still processing.
	group.SetLimit(max(1, pending.workers))
	var mu sync.Mutex
	for _, batch := range batches {
		group.Go(func() error {
			d, h, drainErr := s.drainCarryDetached(ctx, batch)
			mu.Lock()
			deleted += d
			heldBack += h
			mu.Unlock()
			return drainErr
		})
	}
	err = group.Wait()
	return deleted, heldBack, err
}

// addWorkerTail publishes a stopped worker's pending deletes. Retaining its
// original allowance prevents a large tail becoming many additional batches.
func (c *deleteCarry) addWorkerTail(worker *deleteCarry) {
	tail := worker.take()
	tail.workers = 1
	tail.batchSize = len(tail.pages)
	for _, deletion := range tail.deletions {
		tail.batchSize += len(deletion.keys)
	}
	c.add(tail)
}
