package subtreeprocessor

import (
	"context"
	"runtime/debug"
	"runtime/metrics"
	"slices"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"golang.org/x/sync/errgroup"
)

// The in-place fork reorg (issue 2269, behind blockassembly_reorgFastPath). The slow path moves the lost
// block back and each winning block forward, and every forward step copies the pending set and inserts
// all of it into a fresh tx map. This path keeps the tx map and the order the slow path produces:
//
//   - effective D1 (the lost block's transactions no winner has, minus the ones already pending), in the
//     lost block's order, then the pending transactions no winner mined, in their order;
//   - the map holds exactly those, so only effective D1 is inserted and only the mined entries deleted.
//
// Every check that can fail is made before anything changes (list A, planFastReorg); a failed check
// declines and reorgBlocks runs the slow path. Once the coinbase of the lost block is removed there is
// no way back: a failure returns an error and the reset that follows sweeps the installed subtrees, and
// reorgPendingD1 for the lost transactions the subtrees do not hold yet.

// reorgMemoryStats reports the heap the last GC found live and the soft memory limit, which is
// math.MaxInt64 when GOMEMLIMIT is not set. A variable so a test can inject both.
var reorgMemoryStats = func() (liveHeap uint64, limit int64) {
	sample := []metrics.Sample{{Name: "/gc/heap/live:bytes"}}
	metrics.Read(sample)

	if sample[0].Value.Kind() == metrics.KindUint64 {
		liveHeap = sample[0].Value.Uint64()
	}

	return liveHeap, debug.SetMemoryLimit(-1)
}

const (
	// per hash of the frozen winners set, and per pending transaction (design section 4.3)
	fastPathSetBytesPerHash = 33
	fastPathBytesPerPending = 48

	// the flags the pass over the pending set gives a node. A node stays when its flag is zero.
	fastFlagHit       = 1 << 0
	fastFlagRemoveMap = 1 << 1
	fastFlagCoinbase  = 1 << 2

	// fastFlagChunkNodes is how many nodes one task of the flag pass covers
	fastFlagChunkNodes = 16 << 10
)

// reorgFastResult is what an applied fast path hands to the rest of reorgBlocks.
type reorgFastResult struct {
	// movedBack is the lost block's transactions the winners lack (raw D1), each false: none was mined
	// forward. movedBackStillInAssembly reads it.
	movedBack map[chainhash.Hash]bool
	// markOnLongestChain is D2, the winners' transactions the lost block lacks.
	markOnLongestChain []chainhash.Hash
}

// fastReorgPlan is everything list A decided, read-only until the plan is applied.
type fastReorgPlan struct {
	winners *SortedHashSet
	delta   *reorgDelta

	// d1Nodes and d1Inpoints are effective D1: raw D1 without the transactions the tx map holds, in
	// the lost block's order.
	d1Nodes    []subtreepkg.Node
	d1Inpoints []subtreepkg.TxInpoints

	subtrees  []*subtreepkg.Subtree // the installed subtrees: chained, then current
	flags     [][]byte              // per installed node, zero when it stays
	survivors int
}

// reorgInPlace applies a fork reorg to the pending set in place. It returns nil, nil when it declined
// before changing anything, and reorgBlocks then runs the slow path. An error means the coinbase of the
// lost block is gone and only a reset can follow.
func (stp *SubtreeProcessor) reorgInPlace(ctx context.Context, moveBackBlocks, moveForwardBlocks []*model.Block, moveBackConflicting [][]chainhash.Hash) (*reorgFastResult, error) {
	plan, reason := stp.planFastReorg(ctx, moveBackBlocks, moveForwardBlocks, moveBackConflicting)
	if plan == nil {
		// the sets prepared for the winners are no use to the slow path, which allocates next
		stp.releaseForkPrepared()
		stp.declineFastPath(reason)

		return nil, nil
	}

	return stp.applyFastReorg(ctx, plan, moveBackBlocks[0], moveForwardBlocks)
}

func (stp *SubtreeProcessor) declineFastPath(reason string) {
	prometheusSubtreeProcessorReorgFastPathDeclined.WithLabelValues(reason).Inc()
}

// installedSubtrees are the chained subtrees and the open one, in order.
func (stp *SubtreeProcessor) installedSubtrees() []*subtreepkg.Subtree {
	return append(slices.Clone(stp.chainedSubtrees), stp.currentSubtree.Load())
}

// installedNodeCount is how many transactions the installed subtrees hold: every node but the coinbase
// placeholder, which is the first node of the first subtree.
func installedNodeCount(subtrees []*subtreepkg.Subtree) int {
	total := 0
	for _, st := range subtrees {
		total += len(st.Nodes)
	}

	if len(subtrees[0].Nodes) > 0 && subtrees[0].Nodes[0].Hash.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
		total--
	}

	return total
}

// planFastReorg runs list A: every check, cheapest first, with the frozen set and the reads last. It
// changes nothing. It returns the reason for declining when a check fails.
func (stp *SubtreeProcessor) planFastReorg(ctx context.Context, moveBackBlocks, moveForwardBlocks []*model.Block, moveBackConflicting [][]chainhash.Hash) (*fastReorgPlan, string) {
	if len(moveBackBlocks) != 1 || len(moveForwardBlocks) == 0 {
		return nil, "back_blocks"
	}

	lost := moveBackBlocks[0]

	for _, block := range moveForwardBlocks {
		if len(block.Subtrees) == 0 {
			return nil, "empty_forward_block"
		}
	}

	if lost.CoinbaseTx == nil {
		return nil, "read_failed"
	}

	if _, meta, err := stp.blockchainClient.GetBlockHeader(ctx, lost.Header.Hash()); err != nil || meta == nil {
		stp.logger.Warnf("[reorgInPlace] cannot read the lost block's header meta, declining: %v", err)

		return nil, "read_failed"
	} else if meta.Invalid {
		return nil, "invalid_block"
	}

	for _, conflicting := range moveBackConflicting {
		if len(conflicting) > 0 {
			return nil, "conflicting_back"
		}
	}

	subtrees := stp.installedSubtrees()
	nodeCount := installedNodeCount(subtrees)

	// An entry no subtree holds, or a node the map lacks, cannot be told from the real thing by a
	// count of one; the equality is the necessary half, the suppressed-hash check below the rest.
	if stp.currentTxMap.Length() != nodeCount {
		return nil, "tx_map_length"
	}

	// A set prepared while the winners were validated is not read again below. The gate charges it all
	// the same: the live figure is the heap the last GC found, which need not include a set built since.
	prepared := stp.takePreparedFork(moveForwardBlocks)

	var setBytes uint64

	for _, block := range moveForwardBlocks {
		setBytes += block.TransactionCount * fastPathSetBytesPerHash
	}

	// The pending set is charged twice: the flat copy of the survivors (copySurvivors) is alive while the
	// build copies it into the new subtrees, and the old subtrees are alive until the install.
	if live, limit := reorgMemoryStats(); limit > 0 && limit < 1<<62 &&
		live+setBytes+2*uint64(nodeCount)*fastPathBytesPerPending > uint64(limit)/4*3 { //nolint:gosec // limit is positive
		return nil, "memory"
	}

	for _, block := range slices.Concat(moveBackBlocks, moveForwardBlocks) {
		saved, err := stp.readReorgDeletedTxs(ctx, block.Hash())
		if err != nil || len(saved) > 0 {
			return nil, "journal"
		}
	}

	switch hasChildren, err := stp.coinbaseHasChildren(ctx, lost); {
	case err != nil:
		stp.logger.Warnf("[reorgInPlace] cannot read the lost block's coinbase, declining: %v", err)

		return nil, "read_failed"
	case hasChildren:
		return nil, "coinbase_children"
	}

	plan := &fastReorgPlan{subtrees: subtrees}

	var (
		conflicting []chainhash.Hash
		err         error
	)

	if prepared != nil {
		plan.winners, conflicting = prepared.txMap, prepared.conflicting
	} else if plan.winners, conflicting, err = stp.buildUnionSet(ctx, moveForwardBlocks); err != nil {
		stp.logger.Warnf("[reorgInPlace] cannot build the set of the winning blocks, declining: %v", err)

		return nil, "read_failed"
	}

	if len(conflicting) > 0 {
		return nil, "conflicting_forward"
	}

	workers := remainderWorkers(stp.settings.BlockAssembly.ProcessRemainderTxHashesConcurrency)

	// the lost block was probed against the first winner's set while it was the tip, if it still is
	if plan.delta = prepared.lostDelta(lost, plan.winners, workers); plan.delta == nil {
		if plan.delta, err = stp.computeReorgDelta(ctx, moveBackBlocks, plan.winners, workers); err != nil {
			stp.logger.Warnf("[reorgInPlace] cannot read the lost block, declining: %v", err)

			return nil, "read_failed"
		}
	}

	rawD1 := make(map[chainhash.Hash]struct{}, len(plan.delta.D1))
	for _, node := range plan.delta.D1 {
		rawD1[node.Hash] = struct{}{}
	}

	if stp.removeMap.Length() > 0 {
		overlap := false

		stp.removeMap.Iter(func(hash chainhash.Hash, _ uint64) bool {
			if _, ok := rawD1[hash]; ok || plan.winners.Exists(hash) {
				overlap = true
			}

			return overlap
		})

		if overlap {
			return nil, "remove_map_overlap"
		}
	}

	// Effective D1 is raw D1 minus what the tx map holds, membership only, as the move-back's dedup.
	suppressed := make(map[chainhash.Hash]struct{})
	effective := make([]reorgDeltaNode, 0, len(plan.delta.D1))

	for _, node := range plan.delta.D1 {
		if stp.currentTxMap.Exists(node.Hash) {
			suppressed[node.Hash] = struct{}{}
		} else {
			effective = append(effective, node)
		}
	}

	found, survivors, deletes := stp.flagPending(plan, suppressed, workers)

	stp.phases.done("fast_flags")

	// A suppressed hash with no pending position is a stale map entry that would silently drop that
	// transaction.
	if len(found) != len(suppressed) {
		return nil, "suppressed_not_pending"
	}

	// The in-place delete of more than is kept costs more than the rebuild it replaces.
	if deletes > survivors {
		return nil, "hits_exceed_survivors"
	}

	plan.survivors = survivors

	if reason := stp.readEffectiveD1(ctx, plan, lost, effective); reason != "" {
		return nil, reason
	}

	stp.phases.count("fast_d1", len(plan.d1Nodes))
	stp.phases.count("fast_deletes", deletes)
	stp.phases.done("fast_gates")

	return plan, ""
}

// coinbaseHasChildren reports whether the lost block's coinbase has a recorded spender. It only reads:
// the walk removeCoinbaseUtxos does locks the records.
func (stp *SubtreeProcessor) coinbaseHasChildren(ctx context.Context, block *model.Block) (bool, error) {
	data, err := stp.utxoStore.Get(ctx, block.CoinbaseTx.TxIDChainHash(), fields.Utxos)
	if err != nil {
		if errors.Is(err, errors.ErrTxNotFound) {
			return false, nil
		}

		return false, err
	}

	if data == nil {
		return false, nil
	}

	for _, spending := range data.SpendingDatas {
		if spending != nil {
			return true, nil
		}
	}

	return false, nil
}

// flagPending flags every installed node against the winners set and the removeMap, in parallel by chunk,
// as the first phase of the remainder pass does. It returns the suppressed hashes it saw at a pending
// position, the number of nodes that stay and the number that are deleted from the tx map.
func (stp *SubtreeProcessor) flagPending(plan *fastReorgPlan, suppressed map[chainhash.Hash]struct{}, workers int) (found map[chainhash.Hash]struct{}, survivors, deletes int) {
	removeMapLength := stp.removeMap.Length()

	plan.flags = make([][]byte, len(plan.subtrees))

	type chunkResult struct {
		survivors, deletes int
		found              []chainhash.Hash
	}

	var (
		results []*chunkResult
		g       errgroup.Group
	)

	g.SetLimit(workers)

	for idx, st := range plan.subtrees {
		nodes := st.Nodes
		flags := make([]byte, len(nodes))
		plan.flags[idx] = flags

		for start := 0; start < len(nodes); start += fastFlagChunkNodes {
			end := min(start+fastFlagChunkNodes, len(nodes))
			res := &chunkResult{}
			results = append(results, res)

			g.Go(func() error {
				for i := start; i < end; i++ {
					hash := nodes[i].Hash

					switch {
					case hash.Equal(subtreepkg.CoinbasePlaceholderHashValue):
						flags[i] = fastFlagCoinbase
						continue
					case removeMapLength > 0 && stp.removeMap.Exists(hash):
						flags[i] = fastFlagRemoveMap
						res.deletes++
					case plan.winners.Exists(hash):
						flags[i] = fastFlagHit
						res.deletes++
					default:
						res.survivors++
					}

					if _, ok := suppressed[hash]; ok {
						res.found = append(res.found, hash)
					}
				}

				return nil
			})
		}
	}

	_ = g.Wait()

	found = make(map[chainhash.Hash]struct{}, len(suppressed))

	for _, res := range results {
		survivors += res.survivors
		deletes += res.deletes

		for _, hash := range res.found {
			found[hash] = struct{}{}
		}
	}

	return found, survivors, deletes
}

// readEffectiveD1 reads the nodes and the inpoints of effective D1 from the lost block's subtrees that
// hold any, and refuses a lost transaction that spends a pending one: the move-back holds such a
// child behind its pending parent, which this path does not reproduce. It returns the reason to decline.
func (stp *SubtreeProcessor) readEffectiveD1(ctx context.Context, plan *fastReorgPlan, lost *model.Block, effective []reorgDeltaNode) string {
	if len(effective) == 0 {
		return ""
	}

	needed := make(map[int]int)

	var order []int

	for _, node := range effective {
		if _, ok := needed[node.Subtree]; !ok {
			needed[node.Subtree] = -1

			order = append(order, node.Subtree)
		}
	}

	slices.Sort(order)

	part := &model.Block{Header: lost.Header, Height: lost.Height, ID: lost.ID, TransactionCount: lost.TransactionCount}

	for i, subtree := range order {
		needed[subtree] = i

		part.Subtrees = append(part.Subtrees, lost.Subtrees[subtree])
	}

	nodes, inpoints, _, err := stp.moveBackBlockGetSubtrees(ctx, part)
	if err != nil {
		stp.logger.Warnf("[reorgInPlace] cannot read the lost block's transactions, declining: %v", err)

		return "read_failed"
	}

	plan.d1Nodes = make([]subtreepkg.Node, 0, len(effective))
	plan.d1Inpoints = make([]subtreepkg.TxInpoints, 0, len(effective))

	for _, d := range effective {
		i := needed[d.Subtree]

		if d.Node >= len(nodes[i]) || !nodes[i][d.Node].Hash.Equal(d.Hash) {
			stp.logger.Warnf("[reorgInPlace] the lost block's subtree %d changed between two reads, declining", d.Subtree)

			return "read_failed"
		}

		for _, parent := range inpoints[i][d.Node].ParentTxHashes {
			if stp.currentTxMap.Exists(parent) {
				return "d1_spends_pending"
			}
		}

		plan.d1Nodes = append(plan.d1Nodes, nodes[i][d.Node])
		plan.d1Inpoints = append(plan.d1Inpoints, inpoints[i][d.Node])
	}

	return ""
}

// applyFastReorg changes the pending set in place. From the coinbase removal on, a failure ends in a reset.
func (stp *SubtreeProcessor) applyFastReorg(ctx context.Context, plan *fastReorgPlan, lost *model.Block, moveForwardBlocks []*model.Block) (result *reorgFastResult, err error) {
	// Effective D1 is flagged mined in the store from the coinbase removal until it is in the installed
	// subtrees, which is all a reset sweeps: record it first. It is cleared once installed, never in a
	// failure path, and by the reset that read it.
	d1Hashes := make([]chainhash.Hash, len(plan.d1Nodes))
	for i := range plan.d1Nodes {
		d1Hashes[i] = plan.d1Nodes[i].Hash
	}

	stp.reorgPendingD1 = d1Hashes

	txMapLength := stp.currentTxMap.Length()

	if err = stp.removeCoinbaseUtxos(ctx, lost); err != nil && !errors.Is(err, errors.ErrTxNotFound) {
		return nil, errors.NewProcessingError("[reorgInPlace][%s] error removing coinbase utxo", lost.String(), err)
	}

	// No children, so nothing should have left the assembly; if something did, the flags are stale. The
	// removeMap is not checked: Remove writes it from any goroutine, and a removal accepted after the flag
	// pass is applied by the processor's removal channel once it resumes, as after any other block move.
	if stp.currentTxMap.Length() != txMapLength {
		return nil, errors.NewProcessingError("[reorgInPlace][%s] removing the coinbase changed the assembly, the flags are stale", lost.String())
	}

	if err = stp.blockchainClient.SetBlockProcessedAt(ctx, lost.Header.Hash(), true); err != nil {
		// not critical, as in the move-back
		stp.logger.Errorf("[reorgInPlace][%s] error clearing block processed_at timestamp: %v", lost.String(), err)
	}

	stp.phases.done("fast_remove_coinbase")
	stp.reportProgressUnlessDone(ctx)

	// Effective D1 goes into the map before the subtrees are built and sent: the meta of a subtree
	// that holds one reads its inpoints from the map.
	// At utxostore_laneMigration shadow each goes in with the lane position the UTXO store holds for it,
	// as in a move-back, so the subtrees built from it keep complete lane bitmaps.
	d1Positions := stp.lanePositionsFromUtxoStore(ctx, lost, d1Hashes)

	for i := range plan.d1Nodes {
		var d1Position uint64
		if d1Positions != nil {
			d1Position = d1Positions[i]
		}

		if _, inserted := stp.currentTxMap.SetIfNotExistsAt(plan.d1Nodes[i].Hash, &plan.d1Inpoints[i], d1Position); !inserted {
			return nil, errors.NewProcessingError("[reorgInPlace][%s] effective D1 transaction %s is already in the tx map", lost.String(), plan.d1Nodes[i].Hash.String())
		}
	}

	stp.flushDiskTxMapWriters()

	if mapErr := stp.diskTxMapErr(); mapErr != nil {
		return nil, errors.NewStorageError("[reorgInPlace][%s] disk tx map storage error after inserting D1", lost.String(), mapErr)
	}

	stp.phases.done("fast_d1_insert")
	stp.reportProgressUnlessDone(ctx)

	workers := remainderWorkers(stp.settings.BlockAssembly.ProcessRemainderTxHashesConcurrency)

	kept := stp.copySurvivors(plan, workers)

	stp.phases.done("fast_copy")

	// The new chain: the open subtree and the chained ones are replaced, retired and not closed so that
	// a failed build can put them back; the reset sweeps whatever is installed.
	oldChained, oldCurrent := stp.chainedSubtrees, stp.currentSubtree.Load()
	built := false

	defer func() {
		if !built {
			stp.restoreSubtrees(oldChained, oldCurrent)
		}
	}()

	newSubtree, err := stp.newSubtree(int(stp.currentItemsPerFile.Load()))
	if err != nil {
		return nil, errors.NewProcessingError("[reorgInPlace][%s] error creating the new current subtree", lost.String(), err)
	}

	_ = newSubtree.AddCoinbaseNode()

	// The store requests the old subtrees still have in flight read the entries deleted below.
	stp.supersedeStoreRequests()

	stp.retireSubtrees(oldCurrent)
	stp.currentSubtree.Store(newSubtree)
	stp.retireChainedSubtrees()

	// The remainder pass of the slow path reads each kept node's position from the map; so does this,
	// on the same workers, since kept is the whole pending set.
	var positions []uint64

	if stp.keepsLanePositions() {
		positions = make([]uint64, len(kept))

		var g errgroup.Group

		chunk := max(1, (len(kept)+workers-1)/workers)
		for lo := 0; lo < len(kept); lo += chunk {
			hi := min(lo+chunk, len(kept))

			g.Go(func() error {
				for i := lo; i < hi; i++ {
					positions[i] = stp.currentTxMap.Position(kept[i].Hash)
				}

				return nil
			})
		}

		_ = g.Wait()
	}

	if err = stp.parallelBuildRemainderSubtrees(ctx, kept, positions, true); err != nil {
		return nil, errors.NewProcessingError("[reorgInPlace][%s] error building the subtrees", lost.String(), err)
	}

	built = true
	stp.reorgPendingD1 = nil
	kept = nil //nolint:ineffassign,wastedassign // let the copy go

	stp.setTxCountFromSubtrees()

	stp.phases.done("fast_build")
	stp.reportProgressUnlessDone(ctx)

	stp.deleteFlaggedEntries(plan, workers)

	stp.flushDiskTxMapWriters()

	if mapErr := stp.diskTxMapErr(); mapErr != nil {
		return nil, errors.NewStorageError("[reorgInPlace][%s] disk tx map storage error after the build", lost.String(), mapErr)
	}

	stp.phases.done("fast_delete")

	for _, block := range moveForwardBlocks {
		stp.reportProgressUnlessDone(ctx)

		if err = stp.applyCoinbaseUtxos(ctx, block, true, true); err != nil {
			return nil, errors.NewProcessingError("[reorgInPlace][%s] error processing coinbase utxos", block.String(), err)
		}
	}

	stp.phases.done("fast_apply_coinbase")

	// A slow-path step forgets the drops its block mines before its drain, and puts them back if the
	// move fails (issue 2269). The fast path does the same once for the winners.
	if forgotten := stp.forgetMinedDroppedTxs(plan.winners); len(forgotten) != 0 {
		defer func() {
			if err != nil {
				stp.restoreForgottenDroppedTxs(forgotten)
			}
		}()
	}

	// One drain against both blocks. The slow path drains after each step against that step's block.
	stp.phases.count("fast_drain_queue", int(stp.QueueLength()))

	if err = stp.dequeueDuringBlockMovement(plan.winners, nil, nil, true); err != nil {
		return nil, errors.NewProcessingError("[reorgInPlace][%s] error dequeueing transactions", lost.String(), err)
	}

	stp.phases.done("fast_drain")

	stp.flushDiskTxMapWriters()
	stp.drainAndLogDiskTxMapErr("reorgBlocks_inplace_commit")

	// Kept for late hand-offs of what the winners mined (recent_mined_sets.go), after the drain
	// like the end of a slow-path step, so a failed drain leaves no set. The fast path has one set,
	// the winners' union, kept once under the tip: a later reorg that moves back any forward block
	// moves back the tip too, so the union is forgotten whenever it must be. A reorg that moves back
	// only the tip forgets the earlier blocks' protection with it, which only falls back to the
	// behaviour without kept sets and never drops a transaction wrongly. The earlier blocks get no
	// entry: this call also prunes the expired sets, as each step's does.
	stp.keepMinedSet(moveForwardBlocks[len(moveForwardBlocks)-1], plan.winners)

	stp.currentBlockHeader.Store(moveForwardBlocks[len(moveForwardBlocks)-1].Header)
	stp.updatePrecomputedMiningData()
	stp.closeRetiredSubtrees()

	movedBack := make(map[chainhash.Hash]bool, len(plan.delta.D1))
	for _, node := range plan.delta.D1 {
		movedBack[node.Hash] = false
	}

	stp.phases.count("d2", len(plan.delta.D2))
	stp.phases.count("hits", int(plan.delta.Hits))
	stp.phases.count("remainder", plan.survivors)
	prometheusSubtreeProcessorReorgFastPathApplied.Inc()

	return &reorgFastResult{movedBack: movedBack, markOnLongestChain: plan.delta.D2}, nil
}

// copySurvivors returns effective D1, then every node that stays, in pending order, in one slice sized
// exactly. It deletes the removeMap entry of each node the removeMap dropped.
func (stp *SubtreeProcessor) copySurvivors(plan *fastReorgPlan, workers int) []subtreepkg.Node {
	offsets := make([]int, len(plan.subtrees)+1)
	offsets[0] = len(plan.d1Nodes)

	for idx, flags := range plan.flags {
		stays := 0

		for _, f := range flags {
			if f == 0 {
				stays++
			}
		}

		offsets[idx+1] = offsets[idx] + stays
	}

	kept := make([]subtreepkg.Node, offsets[len(plan.subtrees)])
	copy(kept, plan.d1Nodes)

	var g errgroup.Group

	g.SetLimit(workers)

	for idx, st := range plan.subtrees {
		g.Go(func() error {
			pos := offsets[idx]

			for i, node := range st.Nodes {
				switch f := plan.flags[idx][i]; {
				case f == 0:
					kept[pos] = node
					pos++
				case f&fastFlagRemoveMap != 0:
					_ = stp.removeMap.Delete(node.Hash)
				}
			}

			return nil
		})
	}

	_ = g.Wait()

	return kept
}

// deleteFlaggedEntries deletes from the tx map every pending transaction the winners mined or the
// removeMap dropped.
func (stp *SubtreeProcessor) deleteFlaggedEntries(plan *fastReorgPlan, workers int) {
	var g errgroup.Group

	g.SetLimit(workers)

	for idx, st := range plan.subtrees {
		g.Go(func() error {
			for i, node := range st.Nodes {
				if f := plan.flags[idx][i]; f != 0 && f&fastFlagCoinbase == 0 {
					stp.currentTxMap.Delete(node.Hash)
				}
			}

			return nil
		})
	}

	_ = g.Wait()
}
