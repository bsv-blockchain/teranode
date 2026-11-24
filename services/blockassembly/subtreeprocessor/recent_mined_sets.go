package subtreeprocessor

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
)

// recentMinedSet is the transaction set of a block that block assembly moved forward onto. It is
// kept for a short time after the move (blockassembly_minedTxSetRetention), so a transaction the
// block mined stays out of assembly when its hand-off reaches the queue after the block's drain.
//
// Such a late hand-off is normal, not a fault. The validator creates a transaction's record
// before it hands the transaction to block assembly, and a block from a peer can mine the
// transaction in between. A hand-off waits up to about 0.6 s per attempt and is retried for up to
// 2 s, and an abandoned hand-off can still be sent after that. Without the kept set, the steady
// state dequeue loop placed such a transaction with no block check, and in a fork reorg a later
// forward block's drain checked it against that block alone.
//
// Meant to be replaced: if lanes becomes the way teranode records mined state, a block's lane
// map rows answer the same question exactly, from a position the hand-off carries, with no copy
// of the block's transactions and no time limit. That change should delete this file, its
// calls in SubtreeProcessor.go, blockassembly_minedTxSetRetention and the late-mined metric.
type recentMinedSet struct {
	blockHash chainhash.Hash
	txs       *SortedHashSet
	// seq orders the sets by when they were kept, so a failed reorg can drop its own.
	seq uint64
	// expires is when the set is dropped. It is zero while the reorg that kept it runs, so
	// every forward block of a long reorg stays checked until the reorg ends.
	expires time.Time
}

// recentMinedSets holds the kept sets. Readers load the slice without a lock; writers replace
// it under mu, so a slice once loaded never changes.
type recentMinedSets struct {
	mu      sync.Mutex
	sets    atomic.Pointer[[]recentMinedSet]
	nextSeq uint64 // guarded by mu
	pinned  int    // reorgs running; guarded by mu
}

func (r *recentMinedSets) load() []recentMinedSet {
	if p := r.sets.Load(); p != nil {
		return *p
	}

	return nil
}

// storeLocked replaces the kept sets. The caller holds mu.
func (r *recentMinedSets) storeLocked(sets []recentMinedSet) {
	if len(sets) == 0 {
		r.sets.Store(nil)
		return
	}

	r.sets.Store(&sets)
}

// keepMinedSet keeps the transaction set of a block just moved forward onto. Outside a reorg it
// is dropped once the retention has passed; inside one, when the reorg ends (unpin). A nil or
// empty set, or a retention of 0, keeps nothing.
func (stp *SubtreeProcessor) keepMinedSet(block *model.Block, txs *SortedHashSet) {
	retention := stp.settings.BlockAssembly.MinedTxSetRetention
	if retention <= 0 || block == nil || txs == nil || txs.Length() == 0 {
		return
	}

	now := stp.clock.Now()

	r := &stp.recentMined
	r.mu.Lock()
	defer r.mu.Unlock()

	r.nextSeq++

	entry := recentMinedSet{blockHash: *block.Hash(), txs: txs, seq: r.nextSeq}
	if r.pinned == 0 {
		entry.expires = now.Add(retention)
	}

	old := r.load()
	sets := make([]recentMinedSet, 0, len(old)+1)

	for _, s := range old {
		if s.blockHash != entry.blockHash && !s.expired(now) {
			sets = append(sets, s)
		}
	}

	r.storeLocked(append(sets, entry))
}

func (s *recentMinedSet) expired(now time.Time) bool {
	return !s.expires.IsZero() && !now.Before(s.expires)
}

// pinMinedSets holds every set kept from now on until the returned finish runs, as reorgBlocks
// does for its forward blocks. finish(false) starts their retention; finish(true) drops them,
// because the blocks they belong to were not applied (a failed catch-up rolls back, and a failed
// fork reorg is followed by a reset).
func (stp *SubtreeProcessor) pinMinedSets() (finish func(failed bool)) {
	r := &stp.recentMined
	r.mu.Lock()
	r.pinned++
	fromSeq := r.nextSeq
	r.mu.Unlock()

	return func(failed bool) {
		now := stp.clock.Now()
		retention := stp.settings.BlockAssembly.MinedTxSetRetention

		r.mu.Lock()
		defer r.mu.Unlock()

		r.pinned--

		old := r.load()
		sets := make([]recentMinedSet, 0, len(old))

		for _, s := range old {
			if s.seq > fromSeq && s.expires.IsZero() {
				if failed {
					continue
				}

				if r.pinned == 0 {
					s.expires = now.Add(retention)
				}
			}

			sets = append(sets, s)
		}

		r.storeLocked(sets)
	}
}

// forgetMinedSets drops the sets of blocks that are moved back: their transactions are no longer
// mined on the chain block assembly follows, so a late hand-off of one must be placed.
func (stp *SubtreeProcessor) forgetMinedSets(blocks []*model.Block) {
	r := &stp.recentMined
	if len(blocks) == 0 || len(r.load()) == 0 {
		return
	}

	movedBack := make(map[chainhash.Hash]struct{}, len(blocks))
	for _, block := range blocks {
		if block != nil {
			movedBack[*block.Hash()] = struct{}{}
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	old := r.load()
	sets := make([]recentMinedSet, 0, len(old))

	for _, s := range old {
		if _, gone := movedBack[s.blockHash]; !gone {
			sets = append(sets, s)
		}
	}

	r.storeLocked(sets)
}

// clearMinedSets drops every kept set, for a reset, which rebuilds block assembly from the UTXO
// store and may move back any block.
func (stp *SubtreeProcessor) clearMinedSets() {
	r := &stp.recentMined
	r.mu.Lock()
	r.storeLocked(nil)
	r.mu.Unlock()
}

// pruneMinedSets drops the sets whose retention has passed. It only reads when nothing has
// expired, so the dequeue loop can call it every iteration.
func (stp *SubtreeProcessor) pruneMinedSets() {
	r := &stp.recentMined

	sets := r.load()
	if len(sets) == 0 {
		return
	}

	now := stp.clock.Now()

	anyExpired := false

	for i := range sets {
		if sets[i].expired(now) {
			anyExpired = true
			break
		}
	}

	if !anyExpired {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	old := r.load()
	kept := make([]recentMinedSet, 0, len(old))

	for _, s := range old {
		if !s.expired(now) {
			kept = append(kept, s)
		}
	}

	r.storeLocked(kept)
}

// keptMinedSetCount reports how many block sets are kept, for tests and logs.
func (stp *SubtreeProcessor) keptMinedSetCount() int {
	return len(stp.recentMined.load())
}

// filterRecentlyMined removes from the dequeued batches, in place and keeping queue order, every
// transaction a kept block set holds. It runs before any of them is added to the tx map, on
// every path that places queued transactions: the dequeue loop and each drain (runDrainChunk).
// A set past its retention but not yet pruned still filters, which only keeps out a transaction
// that block mined. The lookups run in parallel; with no set kept it returns at once.
func (stp *SubtreeProcessor) filterRecentlyMined(batches []*TxBatch) {
	sets := stp.recentMined.load()
	if len(sets) == 0 || len(batches) == 0 {
		return
	}

	offsets := make([]int, len(batches)+1)
	for i, b := range batches {
		offsets[i+1] = offsets[i] + len(b.nodes)
	}

	total := offsets[len(batches)]
	if total == 0 {
		return
	}

	mined := make([]bool, total)

	forEachChunk(total, func(start, end int) {
		// the batch holding flat index start: the last offset at or below it
		bi := sort.Search(len(batches), func(i int) bool { return offsets[i+1] > start })

		for idx := start; idx < end; idx++ {
			for idx >= offsets[bi+1] {
				bi++
			}

			h := batches[bi].nodes[idx-offsets[bi]].Hash

			for k := range sets {
				if sets[k].txs.Exists(h) {
					mined[idx] = true
					break
				}
			}
		}
	})

	dropped := 0

	for bi, b := range batches {
		base := offsets[bi]
		kept := 0

		for i := range b.nodes {
			if mined[base+i] {
				dropped++
				continue
			}

			b.nodes[kept] = b.nodes[i]
			b.txInpoints[kept] = b.txInpoints[i]

			if len(b.validatedHeights) == len(b.nodes) {
				b.validatedHeights[kept] = b.validatedHeights[i]
			}

			kept++
		}

		if kept == len(b.nodes) {
			continue
		}

		if len(b.validatedHeights) == len(b.nodes) {
			b.validatedHeights = b.validatedHeights[:kept]
		} else {
			b.validatedHeights = nil
		}

		b.nodes = b.nodes[:kept]
		b.txInpoints = b.txInpoints[:kept]
	}

	if dropped > 0 && prometheusSubtreeProcessorLateMinedDropped != nil {
		prometheusSubtreeProcessorLateMinedDropped.Add(float64(dropped))
	}
}
