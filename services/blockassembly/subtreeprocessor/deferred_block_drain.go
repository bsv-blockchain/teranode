package subtreeprocessor

import (
	"context"
	"runtime"
	"sync"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/model"
)

// deferredBlockDrain is the end of a moveForwardBlock that the caller runs
// after it has reported the block as applied: the drain of the queue that
// built up while the block was applied (filtered against the block, as
// dequeueDuringBlockMovement does) and the clear of the retired tx map half.
// Neither changes what the block itself did, and both used to sit between the
// block being applied and MoveForwardBlock returning - seconds on a large
// block, during which block assembly kept serving empty mining candidates.
//
// The drain's time cutoff is sampled when the drain starts, after the
// response, not when the block was applied. It therefore also takes the
// batches queued while the block was being answered, and filters them against
// the block the same way, which only drops more txs that the block already
// holds.
type deferredBlockDrain struct {
	drainQueue        bool // false on the own-block path, which never drains
	transactionMap    *SplitSwissMap
	losingTxHashesMap txmap.TxMap
	conflictingHashes map[chainhash.Hash]struct{}
}

// runDeferredBlockDrain runs the end of a moveForwardBlock that
// handleMoveForwardRequest deferred past its response. The block is already
// applied and reported, so nothing here can be rolled back: a failure is
// logged and requests a block assembly reset (TakeDrainResetRequested), which
// reloads the unmined transactions from the UTXO store. Storage errors seen by
// the flush keep the disk tx map's own reset path.
func (stp *SubtreeProcessor) runDeferredBlockDrain(ctx context.Context, block *model.Block, drain *deferredBlockDrain) {
	if drain.drainQueue {
		if err := stp.runHandlerWithRecover("deferredBlockDrain", func() error {
			return stp.drainQueueAfterBlock(ctx, drain)
		}); err != nil {
			stp.logger.Errorf("[SubtreeProcessor][%s] error draining the queue after moveForwardBlock, requesting a block assembly reset: %v", block.String(), err)
			stp.drainResetRequested.Store(true)
		}

		// addNode does not count what it adds; finalizeBlockProcessing's
		// recount ran before this drain, so count again.
		stp.setTxCountFromSubtrees()

		// The drain's own SetIfNotExists calls may still be below
		// logBufferSize; flush so a failure among them is seen here.
		stp.flushDiskTxMapWriters()
		stp.drainAndLogDiskTxMapErr("moveForwardBlock_postDequeue")
	}

	if err := stp.runHandlerWithRecover("clearCurrentTxMapShadow", func() error {
		stp.clearCurrentTxMapShadow()
		return nil
	}); err != nil {
		stp.logger.Errorf("[SubtreeProcessor][%s] error clearing the retired tx map after moveForwardBlock, requesting a block assembly reset: %v", block.String(), err)
		stp.drainResetRequested.Store(true)
	}

	// The clear is moveForwardBlock's commit point, so a storage error from it
	// keeps the label it had when the clear ran inside moveForwardBlock.
	stp.drainAndLogDiskTxMapErr("moveForwardBlock_commit")
}

// drainQueueAfterBlock drains the queue as dequeueDuringBlockMovement does -
// the same batches, the same txs skipped and the same txs added in the same
// order - but filters the txs, inserts them into the tx map and builds their
// subtrees on every core instead of one tx at a time. A block queues seconds of
// ingest while it is applied, millions of txs on a busy node.
//
// It falls back to the sequential drain when the block left conflicting
// hashes, because then a tx is skipped when an earlier queued tx was, which is
// an order the parallel steps do not keep, and when a queued tx has no
// inpoints, which addNode treats specially.
func (stp *SubtreeProcessor) drainQueueAfterBlock(ctx context.Context, drain *deferredBlockDrain) error {
	if len(drain.conflictingHashes) > 0 {
		return stp.dequeueDuringBlockMovement(drain.transactionMap, drain.losingTxHashesMap, drain.conflictingHashes, false)
	}

	return stp.forEachDrainChunk(drainChunkItems, func(batches []*TxBatch) error {
		return stp.addDrainedBatches(ctx, batches, drain)
	})
}

// drainChunkItems is about how many txs drainQueueAfterBlock takes off the
// queue at a time: enough to keep every core busy, few enough that the queue
// length stays close to the truth while a large backlog drains.
var drainChunkItems = 1 << 20 // a var so tests can make the drain cross chunks

// addDrainedBatches adds one chunk of drained batches, in parallel unless a
// tx in it has no inpoints.
func (stp *SubtreeProcessor) addDrainedBatches(ctx context.Context, batches []*TxBatch, drain *deferredBlockDrain) error {
	total := 0
	for _, batch := range batches {
		total += len(batch.nodes)
	}

	nodes := make([]subtreepkg.Node, 0, total)
	inpoints := make([]*subtreepkg.TxInpoints, 0, total)

	for _, batch := range batches {
		nodes = append(nodes, batch.nodes...)
		inpoints = append(inpoints, batch.txInpoints...)
	}

	for _, ip := range inpoints {
		if ip == nil {
			return stp.addDrainedNodesSequentially(nodes, inpoints, drain.transactionMap, drain.losingTxHashesMap)
		}
	}

	keep := filterDrainedNodes(nodes, drain.transactionMap, drain.losingTxHashesMap)

	setIfNotExistsInOrder(stp.currentTxMap, nodes, inpoints, keep)

	// Compact in place: nodes is not read again, and each write lands at or
	// before the position being read.
	kept := nodes[:0]

	for i := range nodes {
		if keep[i] {
			kept = append(kept, nodes[i])
		}
	}

	prometheusSubtreeProcessorDequeuedTxs.Add(float64(total))

	return stp.parallelBuildRemainderSubtrees(ctx, kept, false)
}

// addDrainedNodesSequentially is dequeueDuringBlockMovement's per-tx loop
// for nodes already taken from the queue, without conflicting hashes.
func (stp *SubtreeProcessor) addDrainedNodesSequentially(nodes []subtreepkg.Node, inpoints []*subtreepkg.TxInpoints,
	transactionMap *SplitSwissMap, losingTxHashesMap txmap.TxMap) error {
	for i, node := range nodes {
		if transactionMap != nil && transactionMap.Exists(node.Hash) {
			continue
		}

		if losingTxHashesMap != nil && losingTxHashesMap.Exists(node.Hash) {
			continue
		}

		if err := stp.addNode(node, inpoints[i], false); err != nil {
			stp.logger.Errorf("[SubtreeProcessor] error adding node %s during sequential remainder processing: %v", node.Hash.String(), err)
		}
	}

	prometheusSubtreeProcessorDequeuedTxs.Add(float64(len(nodes)))

	return nil
}

// filterDrainedNodes reports, per node, whether it is neither in the block
// nor on the losing side of a conflict. Both maps are read-only here.
func filterDrainedNodes(nodes []subtreepkg.Node, transactionMap *SplitSwissMap, losingTxHashesMap txmap.TxMap) []bool {
	keep := make([]bool, len(nodes))

	forEachChunk(len(nodes), func(start, end int) {
		for i := start; i < end; i++ {
			h := nodes[i].Hash

			keep[i] = !(transactionMap != nil && transactionMap.Exists(h)) &&
				!(losingTxHashesMap != nil && losingTxHashesMap.Exists(h))
		}
	})

	return keep
}

// setIfNotExistsInOrder inserts every kept node into m, as addNode does one
// node at a time, and clears keep for the nodes m already held. Copies of the
// same hash always go to the same worker, which walks its nodes in input
// order, so the first copy is the one that is inserted and kept, as it is
// when the nodes are added one by one.
func setIfNotExistsInOrder(m TxInpointsMap, nodes []subtreepkg.Node, inpoints []*subtreepkg.TxInpoints, keep []bool) {
	workers := runtime.GOMAXPROCS(0)

	owned := make([][]int, workers)
	for i := range nodes {
		if keep[i] {
			w := int(uint16(nodes[i].Hash[0])<<8|uint16(nodes[i].Hash[1])) % workers
			owned[w] = append(owned[w], i)
		}
	}

	var wg sync.WaitGroup

	for _, idxs := range owned {
		if len(idxs) == 0 {
			continue
		}

		wg.Go(func() {
			for _, i := range idxs {
				if _, wasSet := m.SetIfNotExists(nodes[i].Hash, inpoints[i]); !wasSet {
					keep[i] = false
				}
			}
		})
	}

	wg.Wait()
}

// forEachChunk splits [0, n) into one contiguous chunk per GOMAXPROCS worker
// and runs fn on each in parallel.
func forEachChunk(n int, fn func(start, end int)) {
	workers := runtime.GOMAXPROCS(0)
	chunk := max(1024, (n+workers-1)/workers)

	var wg sync.WaitGroup

	for start := 0; start < n; start += chunk {
		end := min(start+chunk, n)

		wg.Go(func() { fn(start, end) })
	}

	wg.Wait()
}
