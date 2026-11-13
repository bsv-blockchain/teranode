// Package subtreevalidation provides functionality for validating subtrees in a blockchain context.
// It handles the validation of transaction subtrees, manages transaction metadata caching,
// and interfaces with blockchain and validation services.
package subtreevalidation

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/txmetacache"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"golang.org/x/sync/errgroup"
)

// probeTaskMinKeys is the smallest number of lookups a probe task is given:
// adjacent bucket ranges are merged until they reach it, so a small subtree is
// one task rather than one goroutine and pool hand-off per range. A full-size
// subtree (~1M txs, ~4k per range) still gets one task per range.
const probeTaskMinKeys = 2048

// probeSpanKeys is the span of txMetaSlice one probe task covers when the
// cache has no BucketLocality: as many lookups as a bucket range holds in a
// full-size subtree, so both layouts split a subtree into about as many tasks.
const probeSpanKeys = 4096

// probePool is a server-wide pool of cache-probe worker slots, sized by
// subtreevalidation_processTxMetaUsingCache_Concurrency and created on first
// use so struct-literal Servers (tests) get it too.
type probePool struct {
	once     sync.Once
	slots    chan struct{}
	inFlight atomic.Int32
	peak     atomic.Int32 // highest inFlight seen; reported by tests
}

func (p *probePool) slotsFor(u *Server) chan struct{} {
	p.once.Do(func() {
		n := u.settings.SubtreeValidation.ProcessTxMetaUsingCacheConcurrency
		if n < 1 {
			// Same fallback as util.SafeSetLimit for the per-call limit this replaced.
			u.logger.Warnf("[processTxMetaUsingCache] subtreevalidation_processTxMetaUsingCache_Concurrency=%d, using %d (NumCPU)", n, runtime.NumCPU())
			n = runtime.NumCPU()
		}

		p.slots = make(chan struct{}, n)
	})

	return p.slots
}

func (p *probePool) enter() {
	n := p.inFlight.Add(1)

	for {
		peak := p.peak.Load()
		if n <= peak || p.peak.CompareAndSwap(peak, n) {
			return
		}
	}
}

func (p *probePool) leave() {
	p.inFlight.Add(-1)
}

// processTxMetaUsingCache looks up a peer-announced subtree's transactions in
// the txmeta cache. See probeTxMetaCache; peer subtrees share one pool of
// probe workers.
func (u *Server) processTxMetaUsingCache(ctx context.Context, txHashes []chainhash.Hash, txMetaSlice []metaSliceItem, failFast bool) (int, error) {
	return u.probeTxMetaCache(ctx, &u.peerProbePool, txHashes, txMetaSlice, failFast, 0)
}

// processBlockTxMetaUsingCache is processTxMetaUsingCache for block validation's
// cache pre-check. Block validation, here and in ValidateSubtreeInternal for a
// block's subtrees (ValidateSubtree.FromBlock), uses a separate pool of the same
// size, so a burst of peer subtrees cannot delay a block, while the total number
// of probe workers stays bounded.
func (u *Server) processBlockTxMetaUsingCache(ctx context.Context, txHashes []chainhash.Hash, txMetaSlice []metaSliceItem) (int, error) {
	return u.probeTxMetaCache(ctx, &u.blockProbePool, txHashes, txMetaSlice, false, 0)
}

// probeTxMetaCache attempts to retrieve transaction metadata from the cache for a batch
// of transactions.
//
// On a bucketed cache the lookups are grouped by bucket range (txmetacache.ProbeRangeOf) and
// each task (one or more adjacent ranges) is probed with one batched read, so every bucket's
// read lock is taken once per task rather than once per transaction. A cache without bucket
// locks (TxMetaCache.BucketLocality false) gets contiguous spans of txHashes instead, so each
// task writes its part of txMetaSlice sequentially. Tasks run on probe workers
// whose number is limited across ALL concurrent calls drawing from the same pool
// (subtreevalidation_processTxMetaUsingCache_Concurrency): when hundreds of subtrees arrive at
// once they share the workers instead of each starting its own, which kept thousands of
// goroutines contending with the Kafka txmeta handler for the cache's bucket locks and the CPU.
//
// Parameters:
//   - ctx: Context for cancellation and tracing
//   - pool: The server-wide worker pool to draw from
//   - txHashes: Slice of transaction hashes to process
//   - txMetaSlice: Pre-allocated slice to store retrieved metadata
//   - failFast: If true, fails quickly when missing transaction threshold is exceeded
//
// Returns:
//   - int: Number of transactions missing from cache
//   - error: Any error encountered during processing
//
// The function will return a ThresholdExceededError if failFast is true and
// the number of missing transactions exceeds the configured threshold.
//
// missAllowance raises the fail-fast miss threshold by misses already explained, such as
// txs the validator rejected as conflicting, which will never be in the cache.
func (u *Server) probeTxMetaCache(ctx context.Context, pool *probePool, txHashes []chainhash.Hash, txMetaSlice []metaSliceItem, failFast bool, missAllowance uint32) (int, error) {
	if len(txHashes) != len(txMetaSlice) {
		return 0, errors.NewProcessingError("txHashes and txMetaSlice must be the same length")
	}

	ctx, _, deferFn := tracing.Tracer("subtreevalidation").Start(ctx, "processTxMetaUsingCache")
	defer deferFn()

	cache, ok := u.utxoStore.(*txmetacache.TxMetaCache)
	if !ok {
		u.logger.Warnf("[processTxMetaUsingCache] txMetaStore is not a cached implementation")
		return len(txHashes), nil // As there was no cache, we "missed" all the txHashes
	}

	missingTxThreshold := uint32(u.settings.SubtreeValidation.ProcessTxMetaUsingCacheMissingTxThreshold) // nolint: gosec // G115: configured threshold is non-negative
	if missingTxThreshold > 0 {
		missingTxThreshold += missAllowance
	}

	var (
		missed atomic.Uint32
		tasks  int
		probe  func(ctx context.Context, task int) error
	)

	if cache.BucketLocality() {
		keyHashes, order, ranges := groupByProbeRange(txHashes, txMetaSlice)
		tasks = len(ranges)
		probe = func(ctx context.Context, task int) error {
			return probeCacheRange(ctx, cache, txHashes, keyHashes, txMetaSlice, order[ranges[task][0]:ranges[task][1]], &missed, failFast, missingTxThreshold)
		}
	} else {
		// No bucket locks to save: give each task a contiguous span, so its
		// result writes into txMetaSlice are sequential rather than scattered
		// across the whole slice.
		spans := probeSpans(len(txHashes), probeSpanKeys)
		tasks = len(spans)
		probe = func(ctx context.Context, task int) error {
			return probeCacheSpan(ctx, cache, txHashes, txMetaSlice, spans[task][0], spans[task][1], &missed, failFast, missingTxThreshold)
		}
	}

	if err := pool.run(ctx, u, tasks, probe); err != nil {
		return int(missed.Load()), errors.NewProcessingError("error processing txMeta using cache", err)
	}

	return int(missed.Load()), nil
}

// groupByProbeRange hashes the lookups still to do once (the cache reuses the
// hash) and groups their indices by bucket range with a counting sort. It
// returns the hashes, the grouped indices, and per task the [start, end) of
// its indices in order.
func groupByProbeRange(txHashes []chainhash.Hash, txMetaSlice []metaSliceItem) ([]uint64, []int32, [][2]int) {
	keyHashes := make([]uint64, len(txHashes))
	pending := make([]bool, len(txHashes))

	var counts [txmetacache.ProbeRanges]int

	for i := range txHashes {
		if !probePending(txHashes, txMetaSlice, i) {
			continue
		}

		pending[i] = true
		keyHashes[i] = txmetacache.KeyHash(&txHashes[i])
		counts[txmetacache.ProbeRangeOf(keyHashes[i])]++
	}

	var next [txmetacache.ProbeRanges]int

	offset := 0
	for r, n := range counts {
		next[r] = offset
		offset += n
	}

	starts := next
	order := make([]int32, offset)

	for i := range txHashes {
		if !pending[i] {
			continue
		}

		r := txmetacache.ProbeRangeOf(keyHashes[i])
		order[next[r]] = int32(i) // nolint: gosec // G115: a subtree batch is far below 2^31 transactions
		next[r]++
	}

	tasks := probeTasks(counts[:], probeTaskMinKeys)
	ranges := make([][2]int, len(tasks))

	for k, task := range tasks {
		// Ranges task[0]..task[1]-1 are contiguous in order.
		ranges[k] = [2]int{starts[task[0]], next[task[1]-1]}
	}

	return keyHashes, order, ranges
}

// probePending reports whether txMetaSlice[i] still has to be looked up: it is
// not filled in yet and is not the coinbase placeholder.
func probePending(txHashes []chainhash.Hash, txMetaSlice []metaSliceItem, i int) bool {
	return !txMetaSlice[i].isSet && !txHashes[i].Equal(*subtree.CoinbasePlaceholderHash)
}

// run starts one goroutine per task, each holding one of the pool's slots, and
// waits for them. Slots are taken before a goroutine starts, so the number of
// probe goroutines is bounded across every concurrent call.
func (p *probePool) run(ctx context.Context, u *Server, tasks int, probe func(ctx context.Context, task int) error) error {
	g, gCtx := errgroup.WithContext(ctx)
	slots := p.slotsFor(u)

	aborted := false // not every task was started

dispatch:
	for task := 0; task < tasks; task++ {
		select {
		case slots <- struct{}{}:
		case <-gCtx.Done():
			aborted = true
			break dispatch
		}

		g.Go(func() error {
			p.enter()

			defer func() {
				p.leave()
				<-slots
			}()

			return probe(gCtx, task)
		})
	}

	err := g.Wait()
	if err == nil && aborted {
		// Cancelled while waiting for a slot: the unstarted tasks were neither found
		// nor counted as missed.
		err = errors.NewContextCanceledError("[processTxMetaUsingCache context cancelled]", ctx.Err())
	}

	return err
}

// probeTasks splits the per-range lookup counts into tasks of adjacent ranges,
// [first, last+1), each with at least minKeys lookups where possible (the last
// task takes whatever is left). Empty ranges are absorbed into a neighbour.
func probeTasks(counts []int, minKeys int) [][2]int {
	var (
		tasks [][2]int
		first = -1
		keys  int
	)

	for r, n := range counts {
		if first < 0 {
			first = r
		}

		keys += n

		if keys >= minKeys {
			tasks = append(tasks, [2]int{first, r + 1})
			first, keys = -1, 0
		}
	}

	if first >= 0 && keys > 0 {
		tasks = append(tasks, [2]int{first, len(counts)})
	}

	return tasks
}

// probeSpans splits [0, n) into contiguous spans of at most size indices.
func probeSpans(n, size int) [][2]int {
	spans := make([][2]int, 0, (n+size-1)/size)

	for lo := 0; lo < n; lo += size {
		spans = append(spans, [2]int{lo, min(lo+size, n)})
	}

	return spans
}

// probeScratch is probeCacheRange's per-task working memory. A task covers
// thousands of lookups, so these slices are hundreds of KB; allocating them
// per task would take the heap's global lock each time.
type probeScratch struct {
	indices   []int32
	hashes    []chainhash.Hash
	keyHashes []uint64
	found     []bool
	metas     []txmetacache.CachedMeta
}

var probeScratchPool = sync.Pool{
	New: func() any { return &probeScratch{} },
}

// sized returns the scratch slices with length n, growing them if needed.
func (sc *probeScratch) sized(n int) ([]chainhash.Hash, []uint64, []bool, []txmetacache.CachedMeta) {
	if cap(sc.hashes) < n {
		sc.hashes = make([]chainhash.Hash, n)
		sc.keyHashes = make([]uint64, n)
		sc.found = make([]bool, n)
		sc.metas = make([]txmetacache.CachedMeta, n)
	}

	return sc.hashes[:n], sc.keyHashes[:n], sc.found[:n], sc.metas[:n]
}

// probeCacheRange looks up the given indices, which fall into one task's
// bucket ranges, with a single batched cache read and fills txMetaSlice.
func probeCacheRange(ctx context.Context, cache *txmetacache.TxMetaCache, txHashes []chainhash.Hash, keyHashes []uint64,
	txMetaSlice []metaSliceItem, indices []int32, missed *atomic.Uint32, failFast bool, missingTxThreshold uint32) error {
	sc := probeScratchPool.Get().(*probeScratch)
	defer probeScratchPool.Put(sc)

	return probeIndices(ctx, cache, sc, txHashes, keyHashes, txMetaSlice, indices, missed, failFast, missingTxThreshold)
}

// probeCacheSpan looks up the pending indices in [lo, hi) with a single
// batched cache read and fills txMetaSlice. It passes no key hashes, so it is
// only for a backend without BucketLocality, which does not use them.
func probeCacheSpan(ctx context.Context, cache *txmetacache.TxMetaCache, txHashes []chainhash.Hash, txMetaSlice []metaSliceItem,
	lo, hi int, missed *atomic.Uint32, failFast bool, missingTxThreshold uint32) error {
	sc := probeScratchPool.Get().(*probeScratch)
	defer probeScratchPool.Put(sc)

	indices := sc.indices[:0]

	for i := lo; i < hi; i++ {
		if probePending(txHashes, txMetaSlice, i) {
			indices = append(indices, int32(i)) // nolint: gosec // G115: a subtree batch is far below 2^31 transactions
		}
	}

	sc.indices = indices

	return probeIndices(ctx, cache, sc, txHashes, nil, txMetaSlice, indices, missed, failFast, missingTxThreshold)
}

// probeIndices looks up txHashes at the given indices with a single batched
// cache read, using sc as working memory, and fills txMetaSlice. keyHashes may
// be nil, for a backend that does not use them.
func probeIndices(ctx context.Context, cache *txmetacache.TxMetaCache, sc *probeScratch, txHashes []chainhash.Hash, keyHashes []uint64,
	txMetaSlice []metaSliceItem, indices []int32, missed *atomic.Uint32, failFast bool, missingTxThreshold uint32) error {
	if ctx.Err() != nil {
		return errors.NewContextCanceledError("[processTxMetaUsingCache context cancelled]", ctx.Err())
	}

	if len(indices) == 0 {
		return nil
	}

	hashes, hashKeys, found, metas := sc.sized(len(indices))
	if keyHashes == nil {
		hashKeys = nil
	}

	for j, i := range indices {
		hashes[j] = txHashes[i]
	}

	for j := range hashKeys {
		hashKeys[j] = keyHashes[indices[j]]
	}

	// Decode straight into caller-owned values: no *meta.Data per hit. The
	// TxInpoints reference chunks that are never reused, so txMetaSlice may
	// keep them after metas is recycled.
	if _, err := cache.GetMetaCachedInto(ctx, hashes, hashKeys, metas, found); err != nil {
		return err
	}

	for j, i := range indices {
		txMeta := &metas[j]

		// Defensively reject a partial cache entry: a non-coinbase tx with no
		// parent tx hashes is incomplete (TxInpoints was never populated) and
		// would later fail subtree-meta serialization and wedge the block. Treat
		// it as a miss so the store fallback — which can reconstruct inpoints from
		// the tx inputs — resolves it and self-heals the cache.
		if found[j] && (txMeta.IsCoinbase || len(txMeta.TxInpoints.ParentTxHashes) > 0) {
			txMetaSlice[i] = metaSliceItem{
				fee:         txMeta.Fee,
				sizeInBytes: txMeta.SizeInBytes,
				coinbase:    txMeta.IsCoinbase,
				conflicting: txMeta.Conflicting,
				// The cached encoding has no Creating flag; a cached entry was
				// always reported as not creating.
				creating:   false,
				isSet:      true,
				txInpoints: txMeta.TxInpoints,
			}

			continue
		}

		newMissed := missed.Add(1)

		if failFast && missingTxThreshold > 0 && newMissed > missingTxThreshold {
			return errors.NewThresholdExceededError("threshold exceeded for missing txs: %d > %d", newMissed, missingTxThreshold)
		}
	}

	return nil
}
