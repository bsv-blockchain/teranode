package subtreeprocessor

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// recentMinedSet is the transaction set of a block that block assembly moved forward onto. It is
// kept for a short time after the move (blockassembly_minedTxSetRetention), so a transaction the
// block mined stays out of assembly when its hand-off reaches block assembly after the block's
// drain.
//
// Such a late hand-off is normal, not a fault. The validator creates a transaction's record
// before it hands the transaction to block assembly, and a block from a peer can mine the
// transaction in between. A hand-off waits up to about 0.6 s per attempt and is retried for up to
// 2 s. Without the kept set, the steady-state dequeue loop placed such a transaction with no
// block check, and in a fork reorg a later forward block's drain checked it against that block
// alone.
//
// A hand-off is checked when it arrives (AddBatch, AddBatchBounded), against the sets kept at that
// moment, and when it is placed against the sets kept after it arrived. Checking on arrival is what lets a set go after its
// retention even when a long reorg leaves the hand-off queued for longer than that.
//
// Meant to be replaced: if lanes becomes the way teranode records mined state, a block's lane
// map rows answer the same question exactly, from a position the hand-off carries, with no copy
// of the block's transactions and no time limit. That change should delete this file, its
// calls in SubtreeProcessor.go, blockassembly_minedTxSetRetention and the late-mined metric.
type recentMinedSet struct {
	blockHash chainhash.Hash
	txs       *SortedHashSet
	// seq numbers the sets in the order they were kept, so a batch checked on arrival is checked
	// at placement only against sets kept after that (TxBatch.checkedSeq).
	seq uint64
	// expires is when the set is dropped.
	expires time.Time
}

// recentMinedSets holds the kept sets. Readers load the slice without a lock; writers replace
// it under mu, so a slice once loaded never changes.
type recentMinedSets struct {
	mu      sync.Mutex
	sets    atomic.Pointer[[]recentMinedSet]
	nextSeq uint64 // guarded by mu
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

// keepMinedSet keeps the transaction set of a block just moved forward onto until the retention
// has passed, and drops the sets whose retention has passed, so a reorg or catch-up holds only the
// sets of the blocks it moved in the last retention plus the newest. A nil or empty set, or a
// retention of 0, keeps nothing, but still drops the expired sets: a catch-up of empty blocks
// must not hold an earlier block's set past its retention.
//
// The retention is blockassembly_minedTxSetRetention plus the double-spend window. The block's
// drain leaves a batch younger than the window queued, and the dequeue loop places it when the
// window has passed, so the set must outlive the window or a tx the block mined goes back into
// block assembly.
func (stp *SubtreeProcessor) keepMinedSet(block *model.Block, txs *SortedHashSet) {
	retention := stp.minedSetRetention()
	if retention <= 0 || block == nil || txs == nil || txs.Length() == 0 {
		stp.pruneMinedSets()
		return
	}

	now := stp.clock.Now()

	r := &stp.recentMined
	r.mu.Lock()
	defer r.mu.Unlock()

	r.nextSeq++

	entry := recentMinedSet{blockHash: *block.Hash(), txs: txs, seq: r.nextSeq, expires: now.Add(retention)}

	old := r.load()
	sets := make([]recentMinedSet, 0, len(old)+1)

	for _, s := range old {
		if s.blockHash != entry.blockHash && !s.expired(now) {
			sets = append(sets, s)
		}
	}

	r.storeLocked(append(sets, entry))
}

// minedSetRetention is how long keepMinedSet keeps a block's set: blockassembly_minedTxSetRetention
// plus the double-spend window. A retention of 0 keeps nothing even with a window: the window
// alone leaves no margin, since a batch stamped as the block moves is let out as the set expires.
func (stp *SubtreeProcessor) minedSetRetention() time.Duration {
	retention := stp.settings.BlockAssembly.MinedTxSetRetention
	if retention <= 0 {
		return 0
	}

	return retention + max(stp.settings.BlockAssembly.DoubleSpendWindow, 0)
}

// warnMinedSetRetentionOff warns when blockassembly_minedTxSetRetention is 0 or less while the
// double-spend window is above 0: no set is kept, so a transaction a block mined that the window
// held back in the queue can go back into block assembly when the window lets it out.
func warnMinedSetRetentionOff(logger ulogger.Logger, tSettings *settings.Settings) {
	if tSettings.BlockAssembly.MinedTxSetRetention > 0 || tSettings.BlockAssembly.DoubleSpendWindow <= 0 {
		return
	}

	logger.Warnf("BlockAssembly.MinedTxSetRetention=%s keeps no mined transaction set while DoubleSpendWindow=%s; a transaction a block mined, held back by the window, can re-enter block assembly", tSettings.BlockAssembly.MinedTxSetRetention, tSettings.BlockAssembly.DoubleSpendWindow)
}

func (s *recentMinedSet) expired(now time.Time) bool {
	return !now.Before(s.expires)
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
// transaction a kept block set other than skip holds. A drain passes its own block's set as skip,
// because the drain checks that set itself. It runs before any of them is added to the tx map, on
// every path that places queued transactions: the dequeue loop and each drain (runDrainChunk).
// A set past its retention but not yet pruned still filters, which only keeps out a transaction
// that block mined. With no set kept it returns at once.
func (stp *SubtreeProcessor) filterRecentlyMined(batches []*TxBatch, skip *SortedHashSet) {
	mined, offsets := stp.lookupRecentlyMined(batches, skip)
	if mined == nil {
		return
	}

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

			if len(b.positions) == len(b.nodes) {
				b.positions[kept] = b.positions[i]
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

		if len(b.positions) == len(b.nodes) {
			b.positions = b.positions[:kept]
		} else {
			b.positions = nil
		}

		b.nodes = b.nodes[:kept]
		b.txInpoints = b.txInpoints[:kept]
	}

	countLateMinedDropped(dropped)
}

// lookupRecentlyMined looks up every transaction of the batches in the kept sets other than skip,
// in parallel. It returns, by flat position, whether each is mined, with each batch's first flat
// position, or a nil slice when none is.
func (stp *SubtreeProcessor) lookupRecentlyMined(batches []*TxBatch, skip *SortedHashSet) (mined []bool, offsets []int) {
	all := stp.recentMined.load()
	if len(all) == 0 || len(batches) == 0 {
		return nil, nil
	}

	sets := make([]recentMinedSet, 0, len(all))

	var newest uint64

	for i := range all {
		if all[i].txs != skip {
			sets = append(sets, all[i])
			newest = max(newest, all[i].seq)
		}
	}

	if len(sets) == 0 {
		return nil, nil
	}

	// A batch checked on arrival against every set here needs no lookup at all.
	unchecked := false

	for _, b := range batches {
		if b.checkedSeq < newest {
			unchecked = true
			break
		}
	}

	if !unchecked {
		return nil, nil
	}

	offsets = make([]int, len(batches)+1)
	for i, b := range batches {
		offsets[i+1] = offsets[i] + len(b.nodes)
	}

	total := offsets[len(batches)]
	if total == 0 {
		return nil, nil
	}

	mined = make([]bool, total)

	var anyMined atomic.Bool

	forEachChunk(total, func(start, end int) {
		// the batch holding flat index start: the last offset at or below it
		bi := sort.Search(len(batches), func(i int) bool { return offsets[i+1] > start })

		for idx := start; idx < end; idx++ {
			for idx >= offsets[bi+1] {
				bi++
			}

			b := batches[bi]
			h := b.nodes[idx-offsets[bi]].Hash

			for _, set := range sets {
				if set.seq > b.checkedSeq && set.txs.Exists(h) {
					mined[idx] = true
					anyMined.Store(true)

					break
				}
			}
		}
	})

	if !anyMined.Load() {
		return nil, nil
	}

	return mined, offsets
}

func countLateMinedDropped(dropped int) {
	if dropped > 0 && prometheusSubtreeProcessorLateMinedDropped != nil {
		prometheusSubtreeProcessorLateMinedDropped.Add(float64(dropped))
	}
}

// filterHandOff returns a batch arriving at the queue without the transactions a kept block set
// holds, so a hand-off is checked against the sets kept when it arrives, and the newest set it
// was checked against, for TxBatch.checkedSeq. It never changes the caller's slices, which a
// refused hand-off sends again: when it drops anything it returns new ones. validatedHeights and
// positions may be nil; a positions of another length than nodes is returned as nil.
func (stp *SubtreeProcessor) filterHandOff(nodes []subtreepkg.Node, txInpoints []*subtreepkg.TxInpoints, validatedHeights []uint32, positions []uint64) ([]subtreepkg.Node, []*subtreepkg.TxInpoints, []uint32, []uint64, uint64) {
	if len(positions) != len(nodes) {
		positions = nil
	}

	kept := stp.recentMined.load()
	if len(kept) == 0 || len(nodes) == 0 || len(nodes) != len(txInpoints) {
		return nodes, txInpoints, validatedHeights, positions, 0
	}

	var checkedSeq uint64
	for i := range kept {
		checkedSeq = max(checkedSeq, kept[i].seq)
	}

	// A hand-off batch is small (blockassembly_sendBatchSize), and many arrive at once on their
	// own goroutines, so it is looked up in place, with no allocation until a transaction is found.
	first := -1

	for i := range nodes {
		if inKeptSet(kept, nodes[i].Hash) {
			first = i
			break
		}
	}

	if first < 0 {
		return nodes, txInpoints, validatedHeights, positions, checkedSeq
	}

	keptNodes := make([]subtreepkg.Node, first, len(nodes))
	copy(keptNodes, nodes[:first])
	keptInpoints := make([]*subtreepkg.TxInpoints, first, len(nodes))
	copy(keptInpoints, txInpoints[:first])

	var keptHeights []uint32
	if len(validatedHeights) == len(nodes) {
		keptHeights = make([]uint32, first, len(nodes))
		copy(keptHeights, validatedHeights[:first])
	}

	var keptPositions []uint64
	if positions != nil {
		keptPositions = make([]uint64, first, len(nodes))
		copy(keptPositions, positions[:first])
	}

	for i := first + 1; i < len(nodes); i++ {
		if inKeptSet(kept, nodes[i].Hash) {
			continue
		}

		keptNodes = append(keptNodes, nodes[i])
		keptInpoints = append(keptInpoints, txInpoints[i])

		if keptHeights != nil {
			keptHeights = append(keptHeights, validatedHeights[i])
		}

		if keptPositions != nil {
			keptPositions = append(keptPositions, positions[i])
		}
	}

	countLateMinedDropped(len(nodes) - len(keptNodes))

	return keptNodes, keptInpoints, keptHeights, keptPositions, checkedSeq
}

func inKeptSet(sets []recentMinedSet, h chainhash.Hash) bool {
	for i := range sets {
		if sets[i].txs.Exists(h) {
			return true
		}
	}

	return false
}
