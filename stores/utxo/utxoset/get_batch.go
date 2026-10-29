package utxoset

import (
	"context"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-batcher/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
)

// getResult is what one queued read gets back.
type getResult struct {
	data *meta.Data
	err  error
}

// getItem is a single read waiting for its batch to flush.
//
// wantChildren travels with the item because two reads in one batch can disagree about it: the
// validator resolving a parent never wants the contest, and a conflict walk always does. The
// batch runs the extra statement if ANY item asked, which is one statement for the batch
// rather than one per asking caller, and gives an item that did not ask a field it can ignore.
type getItem struct {
	hash         chainhash.Hash
	wantChildren bool
	// wantBody does NOT work the same way. A batch is split by it and each half is its own
	// lookup, so an item that did not ask never pays for a body. An unwanted contest is one
	// small statement; an unwanted body is the whole transaction read, scanned and decoded,
	// and on Hetzner mainnet on 2026-10-19 fee-only existence checks from the legacy
	// inventory handler, sharing batches with body reads, held tens of gigabytes of them.
	wantBody bool
	done     chan getResult
}

// newGetBatcher funnels single reads into one BatchDecorate call, which is what the sql
// store's get batcher does and for the same reason: the validator resolves parents one at a
// time from many goroutines, and each of those would otherwise be its own round trip.
//
// background is true. A read takes no locks, so two batches running at once cannot deadlock
// or interfere.
//
// Drain mode and greedy accumulate come from GetBatcherDrainMode and
// GetBatcherGreedyAccumulate, the same keys the aerospike store's get batcher reads.
func newGetBatcher(s *Store, size int, duration time.Duration, drainMode, greedyAccumulate bool) *batcher.Batcher[getItem] {
	b := batcher.NewWithPool(size, duration, s.sendGetBatch, true,
		batcher.WithGreedyAccumulate(greedyAccumulate))

	if drainMode {
		b.SetDrainMode(true)
	}

	return b
}

// sendGetBatch resolves a batch of reads through the shared read order, then hands each caller
// its own answer.
//
// It calls lookupMany rather than going the long way round through BatchDecorate, so all three
// entry points sit directly on the one function that owns the order.
func (s *Store) sendGetBatch(batch []*getItem) {
	s.getInFlight.Add(1)
	defer s.getInFlight.Done()

	// Split by body: the reads that named a body field and the reads that did not each get
	// their own lookup. See getItem.wantBody.
	withBody := make([]*getItem, 0, len(batch))
	withoutBody := make([]*getItem, 0, len(batch))

	for _, it := range batch {
		if it.wantBody {
			withBody = append(withBody, it)
		} else {
			withoutBody = append(withoutBody, it)
		}
	}

	// The two lookups are independent, so a mixed batch runs them at the same time when the pool
	// has a spare connection for the second. Run one after the other, the body readers, validator
	// parent resolution among them, wait for the whole no-body lookup first. But the second
	// lookup holds a connection of its own, and nothing caps how many batches run at once
	// (background is true), so without a spare it would queue in front of the spend and create
	// writers on the same pool. It then runs in turn on this goroutine's connection, as a batch
	// of one kind does. Each half answers only its own items.
	switch {
	case len(withoutBody) > 0 && len(withBody) > 0 && !poolSpare(s.pool):
		getBatchHalves.WithLabelValues("in_turn").Inc()
		s.resolveGetItems(withBody, true)
		s.resolveGetItems(withoutBody, false)
	case len(withoutBody) > 0 && len(withBody) > 0:
		getBatchHalves.WithLabelValues("together").Inc()

		var wg sync.WaitGroup

		wg.Add(1)

		go func() {
			defer wg.Done()

			s.resolveGetItems(withoutBody, false)
		}()

		s.resolveGetItems(withBody, true)

		wg.Wait()
	case len(withoutBody) > 0:
		s.resolveGetItems(withoutBody, false)
	case len(withBody) > 0:
		s.resolveGetItems(withBody, true)
	}
}

// resolveGetItems resolves one body-alike group of a get batch through the shared read order,
// then hands each caller its own answer.
func (s *Store) resolveGetItems(batch []*getItem, wantBody bool) {
	ctx := context.Background()

	hashes := make([]chainhash.Hash, len(batch))
	wantChildren := false

	for i, it := range batch {
		hashes[i] = it.hash

		if it.wantChildren {
			wantChildren = true
		}
	}

	res, err := s.lookupMany(ctx, hashes, wantChildren, !wantBody)
	if err != nil {
		for _, it := range batch {
			it.done <- getResult{err: err}
		}

		return
	}

	// A miss is reported per entry rather than as a call failure, so each caller gets its own
	// verdict. One absent parent must not fail the reads it happened to travel with.
	//
	// Each caller gets its OWN record, because two reads in one batch can name the same
	// transaction and lookupMany resolves it once; a shared pointer would let one caller's
	// spending-data decoration land in the other's answer.
	given := make(map[chainhash.Hash]struct{}, len(batch))

	for _, it := range batch {
		// A row that would not decode fails its OWN read, not the batch's, exactly as a miss
		// does. See lookupResult.
		if derr, bad := res.failed[it.hash]; bad {
			it.done <- getResult{err: derr}
			continue
		}

		data, ok := res.found[it.hash]
		if !ok {
			it.done <- getResult{err: errors.NewTxNotFoundError("[utxoset][Get] %s", it.hash.String())}
			continue
		}

		if _, dup := given[it.hash]; dup {
			copied := *data
			data = &copied
		} else {
			given[it.hash] = struct{}{}
		}

		it.done <- getResult{data: data}
	}
}

// lockItem is a single lock change waiting for its batch to flush.
type lockItem struct {
	hash  chainhash.Hash
	value bool
	errCh chan error
}

// newLockBatcher collects single-hash lock changes.
//
// The release is the two-phase commit path: every mempool transaction is created locked and
// unlocked when it commits, one call per transaction. The sql store batches exactly this.
//
// Batches run concurrently, up to maxConcurrent, as the spend-and-create batcher's do. Two
// batches can name the same transaction, and each update touches that transaction's rows, so
// two batches can lock the same rows in different orders. PostgreSQL then stops one of them as
// a deadlock, and setLockedDirect runs it again (withDeadlockRetry). This batcher used to
// dispatch one batch at a time to avoid that, and that queue was where the validator's write
// path waited: in the throughput harness at 10,000 workers, 78% of the workers' blocked time
// was the release.
//
// Drain mode and greedy accumulate come from LockedBatcherDrainMode and
// LockedBatcherGreedyAccumulate, the same keys the aerospike store's locked batcher reads.
func newLockBatcher(s *Store, size int, duration time.Duration, background bool, maxConcurrent int,
	drainMode, greedyAccumulate bool) *batcher.Batcher[lockItem] {
	b := batcher.NewWithPool(size, duration, s.sendLockBatch, background,
		batcher.WithGreedyAccumulate(greedyAccumulate))

	if background && maxConcurrent > 0 {
		b.SetMaxConcurrent(maxConcurrent)
	}

	if drainMode {
		b.SetDrainMode(true)
	}

	return b
}

// sendLockBatch splits the batch by the value being set and issues at most two statements.
//
// Splitting rather than assuming: a batch can mix setting and clearing, and one statement
// cannot do both. The sql store sidesteps this by batching only the release direction; doing
// it properly here costs one extra statement in the rare mixed case and nothing otherwise.
func (s *Store) sendLockBatch(batch []*lockItem) {
	s.lockInFlight.Add(1)
	defer s.lockInFlight.Done()

	ctx := context.Background()

	var setTrue, setFalse []chainhash.Hash

	for _, it := range batch {
		if it.value {
			setTrue = append(setTrue, it.hash)
		} else {
			setFalse = append(setFalse, it.hash)
		}
	}

	var errTrue, errFalse error

	if len(setTrue) > 0 {
		errTrue = s.setLockedDirect(ctx, setTrue, true)
	}

	if len(setFalse) > 0 {
		errFalse = s.setLockedDirect(ctx, setFalse, false)
	}

	for _, it := range batch {
		if it.value {
			it.errCh <- errTrue
		} else {
			it.errCh <- errFalse
		}
	}
}

// getBatched routes one read through the batcher.
func (s *Store) getBatched(ctx context.Context, hash *chainhash.Hash,
	wantChildren, wantBody bool) (*meta.Data, error) {
	done := make(chan getResult, 1)

	s.getBatcher.PutCtx(ctx, &getItem{hash: *hash, wantChildren: wantChildren, wantBody: wantBody, done: done})

	select {
	case res := <-done:
		if res.err != nil {
			return nil, res.err
		}

		if res.data == nil {
			return nil, errors.NewTxNotFoundError("[utxoset][Get] %s", hash.String())
		}

		return res.data, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
