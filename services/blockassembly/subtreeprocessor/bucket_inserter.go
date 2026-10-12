package subtreeprocessor

import (
	"sync"
	"sync/atomic"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
)

// bucketBatch is one subtree's hashes for a single bucket, queued for the
// bucket's owning worker.
type bucketBatch struct {
	bucket uint16
	hashes []chainhash.Hash
}

// bucketInserter inserts pre-bucketed hash batches into a SplitSwissMap with
// exactly one writer per bucket. Worker w exclusively owns the bucket set
// {b : b % numWorkers == w} and is fed by its own channel, so the per-bucket
// mutex inside PutMultiBucket is taken but never contended — unlike the
// previous shape where every concurrent subtree goroutine spawned a goroutine
// per bucket and all of them queued on the same 1024 bucket mutexes.
//
// Usage contract (mirrors errgroup-then-join in CreateTransactionMap):
// concurrent submit() calls are allowed; all submitters must have returned
// before closeAndWait(), which closes the channels, joins the workers and
// returns the first insert error.
type bucketInserter struct {
	m *SplitSwissMap
	// Each channel carries one message per submit() call holding ALL of that
	// submit's batches for the worker — one send per (submitter, worker)
	// instead of one per (submitter, bucket). The per-bucket variant produced
	// ~256k channel handoffs per block and showed up as ~39% of profile
	// samples in runtime.runqgrab (scheduler work-stealing churn).
	channels   []chan []bucketBatch
	numWorkers int

	wg     sync.WaitGroup
	closed atomic.Bool

	errMu    sync.Mutex
	firstErr error
}

// newBucketInserter starts numWorkers insert workers for m. numWorkers is
// clamped to [1, m.Buckets()].
func newBucketInserter(m *SplitSwissMap, numWorkers int) *bucketInserter {
	if numWorkers < 1 {
		numWorkers = 1
	}

	if numWorkers > int(m.Buckets()) {
		numWorkers = int(m.Buckets())
	}

	ins := &bucketInserter{
		m:          m,
		channels:   make([]chan []bucketBatch, numWorkers),
		numWorkers: numWorkers,
	}

	for w := 0; w < numWorkers; w++ {
		// Small buffer: enough to overlap deserialization with inserts
		// without staging meaningful amounts of the block in channels.
		ins.channels[w] = make(chan []bucketBatch, 8)

		ins.wg.Add(1)

		go func(ch chan []bucketBatch) {
			defer ins.wg.Done()

			for batches := range ch {
				for _, batch := range batches {
					if err := ins.m.PutMultiBucket(batch.bucket, batch.hashes); err != nil {
						ins.recordErr(err)
						// Keep draining so submitters never block on a full
						// channel after an error.
					}
				}
			}
		}(ins.channels[w])
	}

	return ins
}

// submit routes one subtree's per-bucket hash slices to their owning workers.
// Safe for concurrent use. Returns an error after closeAndWait has been called.
func (ins *bucketInserter) submit(buckets map[uint16][]chainhash.Hash) error {
	if ins.closed.Load() {
		return errors.NewProcessingError("bucketInserter is closed, submit rejected")
	}

	perWorker := make([][]bucketBatch, ins.numWorkers)

	for bucket, hashes := range buckets {
		if len(hashes) == 0 {
			continue
		}

		w := int(bucket) % ins.numWorkers
		perWorker[w] = append(perWorker[w], bucketBatch{bucket: bucket, hashes: hashes})
	}

	for w, batches := range perWorker {
		if len(batches) > 0 {
			ins.channels[w] <- batches
		}
	}

	return nil
}

// closeAndWait closes the worker channels, joins the workers and returns the
// first insert error. All submit() callers must have returned before this is
// called.
func (ins *bucketInserter) closeAndWait() error {
	if ins.closed.CompareAndSwap(false, true) {
		for _, ch := range ins.channels {
			close(ch)
		}
	}

	ins.wg.Wait()

	ins.errMu.Lock()
	defer ins.errMu.Unlock()

	return ins.firstErr
}

func (ins *bucketInserter) recordErr(err error) {
	ins.errMu.Lock()
	defer ins.errMu.Unlock()

	if ins.firstErr == nil {
		ins.firstErr = err
	}
}
