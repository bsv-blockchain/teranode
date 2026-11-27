package subtreeprocessor

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/stretchr/testify/require"
)

// These tests pin the scenario builder that the issue 2269 reorg tests and benchmarks share, and
// through it the order today's slow path produces for a fork reorg of one block back and two
// forward (design section 1): the transactions only the lost block had, in its order, then the
// pending transactions the winning blocks do not contain, in their order.

func smallScenarioConfig() reorgScenarioConfig {
	return reorgScenarioConfig{
		seed:              1,
		pending:           2000,
		lost:              1000,
		lostInWinners:     0.9,
		pendingInWinners:  0.3,
		winnerOnly:        50,
		forwardBlocks:     2,
		lostAlsoPending:   5,
		pendingChains:     40,
		pendingSpendsLost: 20,
		subtreeSize:       64,
	}
}

func TestReorgScenario_SetsMatchTheConfig(t *testing.T) {
	sc := newReorgScenario(t, smallScenarioConfig())

	require.Len(t, sc.pendingOrder(), 2000)
	require.Len(t, sc.lostOrder(), 1000)
	require.Len(t, sc.forward, 2)
	require.Len(t, sc.rawD1(), 100, "10% of the lost block is not in the winners")
	require.Len(t, sc.hits(), 600, "30% of the pending set is in the winners")

	inPending := 0
	for _, h := range sc.rawD1() {
		if sc.inPending(h) {
			inPending++
		}
	}

	require.Equal(t, 5, inPending, "lostAlsoPending transactions are in the lost block and pending, not in the winners")
}

func TestReorgScenario_StartsValid(t *testing.T) {
	sc := newReorgScenario(t, smallScenarioConfig())

	requireAssemblyValid(t, sc.stp, sc.onChainBefore)
	require.Equal(t, sc.lost.Header.Hash(), sc.stp.GetCurrentBlockHeader().Hash())
	require.Equal(t, sc.pendingOrder(), txHashes(assemblyNodes(sc.stp)))
}

// The order, the marks and the maps of a fork reorg do not depend on the tx map backend, how many
// blocks the winner has, where the hits sit, or which remainder branch runs (node by node below the
// parallel threshold, parallel insert and compaction at or above it).
func TestReorgScenario_SlowPathKeepsTheOrder(t *testing.T) {
	type shape struct {
		name          string
		diskTxMap     bool
		forwardBlocks int
		hitsAtFront   bool
		parallel      bool
	}

	shapes := []shape{
		{name: "in-memory map", forwardBlocks: 2},
		{name: "disk map", diskTxMap: true, forwardBlocks: 2},
	}

	for _, forwardBlocks := range []int{1, 2, 3} {
		for _, hitsAtFront := range []bool{false, true} {
			for _, parallel := range []bool{false, true} {
				shapes = append(shapes, shape{
					name:          fmt.Sprintf("forward blocks %d, hits at front %t, parallel remainder %t", forwardBlocks, hitsAtFront, parallel),
					forwardBlocks: forwardBlocks,
					hitsAtFront:   hitsAtFront,
					parallel:      parallel,
				})
			}
		}
	}

	for _, tc := range shapes {
		t.Run(tc.name, func(t *testing.T) {
			cfg := smallScenarioConfig()
			cfg.diskTxMap = tc.diskTxMap
			cfg.forwardBlocks = tc.forwardBlocks
			cfg.hitsAtFront = tc.hitsAtFront

			if tc.parallel {
				cfg.parallelThreshold = 1
			}

			sc := newReorgScenario(t, cfg)

			logger := &phaseLogger{}
			sc.stp.logger = logger

			require.NoError(t, sc.run(t))

			branch, other := "remainder_add_nodes", "remainder_map_insert"
			if tc.parallel {
				branch, other = other, branch
			}

			require.Contains(t, logger.phases("reorgBlocks"), branch, "the remainder pass ran the branch under test")
			require.NotContains(t, logger.phases("reorgBlocks"), other)

			require.Equal(t, sc.forward[len(sc.forward)-1].Header.Hash(), sc.stp.GetCurrentBlockHeader().Hash())
			requireAssemblyValid(t, sc.stp, sc.onChainAfter)
			require.Equal(t, sc.expectedOrderAfter(), txHashes(assemblyNodes(sc.stp)),
				"effective D1 in the lost block's order, then the pending set minus the winners in its order")
			require.Equal(t, len(sc.expectedOrderAfter()), sc.stp.GetCurrentTxMap().Length(), "the tx map holds exactly what the subtrees hold")
			require.Zero(t, sc.stp.GetRemoveMapLength())
			require.ElementsMatch(t, sc.rawD1(), sc.utxo.marked(false))
			require.ElementsMatch(t, sc.winnerNotLost(), sc.utxo.marked(true))
		})
	}
}

func TestReorgScenario_SlowPathMarks(t *testing.T) {
	sc := newReorgScenario(t, smallScenarioConfig())

	require.NoError(t, sc.run(t))

	require.ElementsMatch(t, sc.rawD1(), sc.utxo.marked(false), "raw D1 is marked not on the longest chain")
	require.ElementsMatch(t, sc.winnerNotLost(), sc.utxo.marked(true), "D2, the winners minus the lost block, is marked on the longest chain")
}

func TestReorgScenario_IsDeterministic(t *testing.T) {
	a := newReorgScenario(t, smallScenarioConfig())
	b := newReorgScenario(t, smallScenarioConfig())

	require.Equal(t, a.pendingOrder(), b.pendingOrder())
	require.Equal(t, a.lostOrder(), b.lostOrder())
	require.Equal(t, a.lost.Header.Hash(), b.lost.Header.Hash())
}

func txHashes(nodes []subtreepkg.Node) []chainhash.Hash {
	out := make([]chainhash.Hash, 0, len(nodes))

	for _, n := range nodes {
		if n.Hash.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
			continue
		}

		out = append(out, n.Hash)
	}

	return out
}

func TestReorgScenario_IntakeFeedsTheQueueAtItsRate(t *testing.T) {
	sc := newReorgScenario(t, smallScenarioConfig())

	stop := sc.startIntake(100_000)
	time.Sleep(200 * time.Millisecond)
	sent := stop()

	require.Positive(t, sent, "intake sends while it runs")
	require.LessOrEqual(t, sent, 40_000, "and not much faster than its rate")
	require.Equal(t, int64(sent), sc.stp.QueueLength(), "nothing dequeues while the processor is not started")
}

func TestReorgScenario_SkipStoreMarksCountsThem(t *testing.T) {
	cfg := smallScenarioConfig()
	cfg.skipStoreMarks = true
	sc := newReorgScenario(t, cfg)

	require.NoError(t, sc.run(t))

	require.Equal(t, len(sc.rawD1()), sc.utxo.markCount(false))
	require.Equal(t, len(sc.winnerNotLost()), sc.utxo.markCount(true))
	require.Empty(t, sc.utxo.marked(true), "counted, not recorded")
}

// inAssemblyParentEdges counts the transactions in assembly with a parent that is also in assembly.
func inAssemblyParentEdges(stp *SubtreeProcessor) int {
	nodes := txHashes(assemblyNodes(stp))
	inAssembly := make(map[chainhash.Hash]struct{}, len(nodes))

	for _, h := range nodes {
		inAssembly[h] = struct{}{}
	}

	edges := 0

	for _, h := range nodes {
		p, ok := stp.GetCurrentTxMap().Get(h)
		if !ok {
			continue
		}

		for _, parent := range p.ParentTxHashes {
			if _, ok := inAssembly[parent]; ok {
				edges++
			}
		}
	}

	return edges
}

// The order oracle can only catch an ordering bug if some transactions in assembly have parents in
// assembly: pending chains, and pending children of lost-block transactions (D1 must come first).
func TestReorgScenario_HasParentEdgesInsideTheAssembly(t *testing.T) {
	sc := newReorgScenario(t, smallScenarioConfig())
	require.Positive(t, inAssemblyParentEdges(sc.stp), "before the reorg")

	require.NoError(t, sc.run(t))
	require.Greater(t, inAssemblyParentEdges(sc.stp), 20, "after the reorg the D1 parents are in assembly too")
}

func TestReorgScenario_OracleCatchesAParentMovedAfterItsChild(t *testing.T) {
	sc := newReorgScenario(t, smallScenarioConfig())
	require.NoError(t, sc.run(t))

	nodes := assemblyNodes(sc.stp)
	txMap := sc.stp.GetCurrentTxMap()
	position := make(map[chainhash.Hash]int, len(nodes))

	for i, n := range nodes {
		position[n.Hash] = i
	}

	for i, n := range nodes {
		p, ok := txMap.Get(n.Hash)
		if !ok {
			continue
		}

		for _, parent := range p.ParentTxHashes {
			at, inAssembly := position[parent]
			if !inAssembly {
				continue
			}

			moved := slices.Clone(nodes)
			parentNode := moved[at]
			moved = slices.Delete(moved, at, at+1)
			moved = slices.Insert(moved, i, parentNode) // the child is now at i-1, the parent at i

			require.ErrorContains(t, checkAssemblyOrder(moved, txMap.Get, sc.onChainAfter), "before its parent")

			return
		}
	}

	t.Fatal("no transaction in assembly has a parent in assembly")
}

// The cases below are the ones plan PR 6 (the in-place fast path) must match today's slow path on,
// each pinned here on the slow path (design section 5, equivalence items 1 to 10). They run through
// reorgBlocks, so the fast path's differential test reuses the scenario configs unchanged.

func removeMapScenarioConfig() reorgScenarioConfig {
	cfg := smallScenarioConfig()
	cfg.removeMapPending = 20
	cfg.removeMapHits = 10
	cfg.removeMapLost = 3
	cfg.removeMapD1 = 5

	return cfg
}

// A transaction in the removeMap (pending, a hit, X or lost-block only) is absent from the assembly
// and the tx map afterwards, and its removeMap entry is consumed.
func TestReorgScenario_SlowPathConsumesRemoveMapEntries(t *testing.T) {
	for _, parallel := range []bool{false, true} {
		name := map[bool]string{false: "sequential remainder", true: "parallel remainder"}[parallel]

		t.Run(name, func(t *testing.T) {
			cfg := removeMapScenarioConfig()
			if parallel {
				cfg.parallelThreshold = 1
			}

			sc := newReorgScenario(t, cfg)
			require.Len(t, sc.removeIDs, 38)
			require.Equal(t, 38, sc.stp.GetRemoveMapLength(), "every chosen transaction is in the removeMap before the reorg")

			require.NoError(t, sc.run(t))

			requireAssemblyValid(t, sc.stp, sc.onChainAfter)
			require.Equal(t, sc.expectedOrderAfter(), txHashes(assemblyNodes(sc.stp)))
			require.Equal(t, sc.expectedRemoveMapLength(), sc.stp.GetRemoveMapLength(), "every entry is consumed")

			txMap := sc.stp.GetCurrentTxMap()
			require.Equal(t, len(sc.expectedOrderAfter()), txMap.Length())

			for _, id := range sc.removeIDs {
				require.False(t, txMap.Exists(sc.hash(id)), "removed transaction %x is out of the tx map", id)
				require.NotContains(t, txHashes(assemblyNodes(sc.stp)), sc.hash(id))
			}

			require.ElementsMatch(t, sc.expectedMarkFalse(), sc.utxo.marked(false),
				"a removed lost-block transaction is not marked: the marks cover what the assembly still holds")
			require.Less(t, len(sc.expectedMarkFalse()), len(sc.rawD1()))
			require.ElementsMatch(t, sc.winnerNotLost(), sc.utxo.marked(true))
		})
	}
}

// The slow path publishes a lost-block child C before its pending parent X when a subtree boundary
// separates them; block validation rejects that order, so this pins the order oracle's rejection.
// PR 9a (bsv-blockchain/teranode#2268) turns it green.
func TestReorgScenario_SlowPathPublishesLostChildBeforePendingParent(t *testing.T) {
	cfg := smallScenarioConfig()
	cfg.lostChildOfPending = 3
	sc := newReorgScenario(t, cfg)

	require.NoError(t, sc.run(t))

	nodes := assemblyNodes(sc.stp)
	require.Equal(t, sc.expectedOrderAfter(), txHashes(nodes), "today's order: effective D1 (with C) first, then the pending set (with X)")

	position := make(map[chainhash.Hash]int, len(nodes))
	for i, n := range nodes {
		position[n.Hash] = i
	}

	for j := range cfg.lostChildOfPending {
		x := sc.hash(kindRawD1 | scenarioID(j))
		c := sc.hash(kindRawD1 | scenarioID(cfg.lostAlsoPending+cfg.pendingSpendsLost+j))

		require.Less(t, position[c], position[x], "C %d is published before its parent X", j)
		require.NotEqual(t, position[c]/cfg.subtreeSize, position[x]/cfg.subtreeSize, "a subtree boundary lies between C and X")
	}

	require.NoError(t, checkSubtreeShape(append(sc.stp.GetChainedSubtrees(), sc.stp.GetCurrentSubtree())))
	require.ErrorContains(t, checkAssemblyOrder(nodes, sc.stp.GetCurrentTxMap().Get, sc.onChainAfter), "before its parent",
		"the order oracle rejects the inversion; PR 9a makes this pass")

	require.ElementsMatch(t, sc.expectedMarkFalse(), sc.utxo.marked(false), "C and X are both marked off the longest chain")
}

// Pending children of the lost block's coinbase are deleted with it and every other pending
// transaction keeps its order. Their removeMap entries stay: removeCoinbaseUtxos adds them and then
// takes the children out of the subtrees itself, so no remainder pass meets them.
func TestReorgScenario_SlowPathRemovesCoinbaseChildren(t *testing.T) {
	cfg := smallScenarioConfig()
	cfg.coinbaseChildren = 4
	sc := newReorgScenario(t, cfg)

	require.Equal(t, sc.pendingOrder(), txHashes(assemblyNodes(sc.stp)), "the children are pending before the reorg")
	requireAssemblyValid(t, sc.stp, sc.onChainBefore)

	require.NoError(t, sc.run(t))

	requireAssemblyValid(t, sc.stp, sc.onChainAfter)
	require.Equal(t, sc.expectedOrderAfter(), txHashes(assemblyNodes(sc.stp)))
	require.Equal(t, len(sc.expectedOrderAfter()), sc.stp.GetCurrentTxMap().Length())
	// This pins a quirk, not a rule: the entries are left behind. A fast path may legitimately clean
	// them up, and this assertion, which is all that kills the coinbase_children_stay_in_subtrees
	// mutant, then moves with it.
	require.Equal(t, 4, sc.stp.GetRemoveMapLength(), "one leftover removeMap entry per coinbase child")

	for _, tx := range sc.cbChildTxs {
		h := *tx.TxIDChainHash()

		require.False(t, sc.stp.GetCurrentTxMap().Exists(h))
		require.NotContains(t, txHashes(assemblyNodes(sc.stp)), h)
		require.True(t, sc.stp.removeMap.Exists(h))

		_, err := sc.utxo.Get(t.Context(), &h)
		require.Error(t, err, "the child is deleted from the UTXO store")
	}

	_, err := sc.utxo.Get(t.Context(), sc.lost.CoinbaseTx.TxIDChainHash())
	require.Error(t, err, "and so is B's coinbase")

	require.ElementsMatch(t, sc.winnerNotLost(), sc.utxo.marked(true))
}

// A pending hash sitting twice in the subtrees is built once, at its first position, and both
// copies of a hit are dropped, below and above the parallel threshold alike.
func TestReorgScenario_SlowPathLeavesADuplicatePendingHashOnce(t *testing.T) {
	for _, parallel := range []bool{false, true} {
		name := map[bool]string{false: "sequential remainder", true: "parallel remainder"}[parallel]

		t.Run(name, func(t *testing.T) {
			cfg := smallScenarioConfig()
			cfg.duplicateInPending = 6

			if parallel {
				cfg.parallelThreshold = 1
			}

			sc := newReorgScenario(t, cfg)
			require.Len(t, sc.dupIDs, 6)

			logger := &phaseLogger{}
			sc.stp.logger = logger

			before := txHashes(assemblyNodes(sc.stp))
			require.Len(t, before, len(sc.pendingOrder())+6, "the duplicates are in the subtrees before the reorg")

			require.NoError(t, sc.run(t))

			branch := map[bool]string{false: "remainder_add_nodes", true: "remainder_map_insert"}[parallel]
			require.Contains(t, logger.phases("reorgBlocks"), branch)

			after := txHashes(assemblyNodes(sc.stp))
			requireAssemblyValid(t, sc.stp, sc.onChainAfter)
			require.Equal(t, sc.expectedOrderAfter(), after, "each duplicate is left once, at its first position, or not at all when it is a hit")
			require.Equal(t, len(after), sc.stp.GetCurrentTxMap().Length())

			survivors := 0

			for _, id := range sc.dupIDs {
				if !sc.inWinners(sc.hash(id)) {
					survivors++
					require.Equal(t, 1, countHash(after, sc.hash(id)))
				} else {
					require.Zero(t, countHash(after, sc.hash(id)))
				}
			}

			require.Equal(t, 3, survivors)
		})
	}
}

func countHash(hashes []chainhash.Hash, h chainhash.Hash) int {
	n := 0

	for _, x := range hashes {
		if x == h {
			n++
		}
	}

	return n
}

// An empty first and last forward block create the coinbase and return before resetSubtreeState,
// so with an empty last block the reorg ends on one 1,048,576-leaf open subtree. Pinned as today.
func TestReorgScenario_SlowPathWithEmptyForwardBlocks(t *testing.T) {
	const moveBackSize = 1024 * 1024

	for _, tc := range []struct {
		name          string
		first, last   bool
		forwardBlocks int
		wantSize      int
	}{
		{"empty first", true, false, 2, 64},
		{"empty first, one block with winners", true, false, 1, 64},
		{"empty last", false, true, 2, moveBackSize},
		{"empty last, one block with winners", false, true, 1, moveBackSize},
		{"empty first and last", true, true, 2, moveBackSize},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := smallScenarioConfig()
			cfg.forwardBlocks = tc.forwardBlocks
			cfg.emptyForwardFirst, cfg.emptyForwardLast = tc.first, tc.last
			sc := newReorgScenario(t, cfg)

			require.Len(t, sc.forward, tc.forwardBlocks+btoi(tc.first)+btoi(tc.last))

			if tc.first {
				require.Empty(t, sc.forward[0].Subtrees)
			}

			if tc.last {
				require.Empty(t, sc.forward[len(sc.forward)-1].Subtrees)
			}

			require.NoError(t, sc.run(t))

			require.Equal(t, sc.forward[len(sc.forward)-1].Header.Hash(), sc.stp.GetCurrentBlockHeader().Hash())
			requireAssemblyValid(t, sc.stp, sc.onChainAfter)
			require.Equal(t, sc.expectedOrderAfter(), txHashes(assemblyNodes(sc.stp)))
			require.Equal(t, len(sc.expectedOrderAfter()), sc.stp.GetCurrentTxMap().Length())
			require.Zero(t, sc.stp.GetRemoveMapLength())
			require.ElementsMatch(t, sc.expectedMarkFalse(), sc.utxo.marked(false))
			require.ElementsMatch(t, sc.winnerNotLost(), sc.utxo.marked(true))

			for _, block := range sc.forward {
				_, err := sc.utxo.Get(t.Context(), block.CoinbaseTx.TxIDChainHash())
				require.NoError(t, err, "every forward block's coinbase is created, an empty block's too")
			}

			// Fixing the empty-last-block finding makes the forward_steps_always_properly_sized mutant
			// survive: this size is the only thing that kills it, so drop or rewrite that patch then.
			require.Equal(t, tc.wantSize, sc.stp.GetCurrentSubtree().Size(), "the subtree size the reorg leaves behind")
		})
	}
}

func btoi(b bool) int {
	if b {
		return 1
	}

	return 0
}

// The head drain places a batch queued before the reorg after the pending set, and each forward step
// drains the queue after its remainder pass, filtering what its block mined. The mid-reorg arrival
// is injected at the progress beat before the second forward block, so it is deterministic.
func TestReorgScenario_SlowPathDrainsTheQueueAroundTheReorg(t *testing.T) {
	sc := newReorgScenario(t, smallScenarioConfig())
	require.Zero(t, sc.stp.settings.BlockAssembly.DoubleSpendWindow, "with a window the drains would hold recent batches back")

	// Every batch is stamped a millisecond before any drain, so each drain is free to take it.
	enqueueAt := time.Now()
	sc.stp.queue.clock = fixedClock{t: enqueueAt}
	sc.stp.clock = fixedClock{t: enqueueAt.Add(time.Millisecond)}

	var (
		nodes    []subtreepkg.Node
		inpoints []*subtreepkg.TxInpoints
	)

	add := func(node subtreepkg.Node, p subtreepkg.TxInpoints) {
		nodes = append(nodes, node)
		inpoints = append(inpoints, &p)
	}

	newTx := func(k int) (scenarioID, chainhash.Hash) {
		id := kindIntake | scenarioID(1<<30+k)
		add(subtreepkg.Node{Hash: sc.hash(id), Fee: 200, SizeInBytes: 250}, voutZeroInpoints(sc.confirmedParent(id)))

		return id, sc.hash(id)
	}

	// Queued before the reorg: a lost-block transaction no one else has, new transactions and one
	// that is also in the removeMap.
	d1Queued := sc.rawD1()[0]
	require.False(t, sc.inPending(d1Queued))

	for _, id := range sc.lostIDs {
		if sc.hash(id) == d1Queued {
			tx := sc.tx(t, id)
			add(tx.node, tx.inpoints)

			break
		}
	}

	wantTail := []chainhash.Hash{d1Queued}

	for k := range 40 {
		_, h := newTx(k)
		wantTail = append(wantTail, h)
	}

	_, removed := newTx(40)
	require.NoError(t, sc.stp.removeMap.Put(removed, 1))

	sc.stp.AddBatch(nodes, inpoints)

	// Arriving between the two forward steps: new transactions, each next to one the second forward
	// block mined.
	secondBlock := sc.winnerIDs[len(sc.winnerIDs)/2:]
	minedInSecond := make([]scenarioID, 0, 5)

	for _, id := range secondBlock {
		if id.kind() == kindWinnerOnly && len(minedInSecond) < 5 {
			minedInSecond = append(minedInSecond, id)
		}
	}

	require.Len(t, minedInSecond, 5)

	nodes, inpoints = nil, nil

	var arrivals []chainhash.Hash

	for k := range 5 {
		_, h := newTx(100 + k)
		arrivals = append(arrivals, h)

		tx := sc.tx(t, minedInSecond[k])
		add(tx.node, tx.inpoints)
	}

	fired := beforeForwardBlockOn(sc.stp, sc.forward[0].Header, func() { sc.stp.AddBatch(nodes, inpoints) })

	require.NoError(t, sc.run(t))
	require.True(t, *fired, "the batch arrived between the forward steps")

	after := txHashes(assemblyNodes(sc.stp))
	requireAssemblyValid(t, sc.stp, sc.onChainAfter)

	effectiveD1 := filter(sc.lostOrder(), func(h chainhash.Hash) bool {
		return !sc.inWinners(h) && !sc.inPending(h) && h != d1Queued
	})
	want := append(effectiveD1, filter(sc.pendingOrder(), func(h chainhash.Hash) bool { return !sc.inWinners(h) })...)
	want = append(want, wantTail...)
	want = append(want, arrivals...)

	require.Equal(t, want, after, "effective D1, the pending set, the head-drained batch in queue order (the lost-block "+
		"transaction keeps its queue position), then the batch that arrived between the steps, in order")

	for _, id := range minedInSecond {
		require.NotContains(t, after, sc.hash(id), "a queued transaction the second forward block mined is filtered out")
	}

	require.NotContains(t, after, removed)
	require.Zero(t, sc.stp.QueueLength(), "the last forward step drained everything")
	require.Equal(t, len(after), sc.stp.GetCurrentTxMap().Length())
	require.Zero(t, sc.stp.GetRemoveMapLength(), "the queued transaction in the removeMap was consumed")
	require.ElementsMatch(t, sc.rawD1(), sc.utxo.marked(false), "the queued lost-block transaction is in the assembly, so it is marked like the rest of raw D1")
}

// Transactions that arrive during the reorg are never lost or placed twice: what the drains took
// follows the remainder in send order, and the rest is still queued.
func TestReorgScenario_SlowPathKeepsEveryTransactionSentDuringTheReorg(t *testing.T) {
	sc := newReorgScenario(t, smallScenarioConfig())
	require.Zero(t, sc.stp.settings.BlockAssembly.DoubleSpendWindow)

	stop := sc.startIntake(50_000)
	require.Eventually(t, func() bool { return sc.stp.QueueLength() > 0 }, 5*time.Second, time.Millisecond)

	require.NoError(t, sc.run(t))

	sent := stop()
	after := txHashes(assemblyNodes(sc.stp))
	requireAssemblyValid(t, sc.stp, sc.onChainAfter)

	prefix := sc.expectedOrderAfter()
	require.Equal(t, prefix, after[:len(prefix)])

	arrived := after[len(prefix):]
	require.NotEmpty(t, arrived)

	for i, h := range arrived {
		require.Equal(t, sc.hash(kindIntake|scenarioID(i)), h, "arrivals follow in the order they were sent")
	}

	require.Equal(t, int64(sent-len(arrived)), sc.stp.QueueLength(), "everything else is still queued")
	require.Equal(t, len(after), sc.stp.GetCurrentTxMap().Length())
}

// A winning block's conflicting node demotes the pending transaction that spends the same output,
// and the assembly's own parent links drop its pending child (dropConflictingDescendants), not the
// store's cascade. The fast path declines these, so the slow path is the reference.
func TestReorgScenario_SlowPathDemotesPendingLosersOfAWinnerConflict(t *testing.T) {
	for _, parallel := range []bool{false, true} {
		name := map[bool]string{false: "sequential remainder", true: "parallel remainder"}[parallel]

		t.Run(name, func(t *testing.T) {
			cfg := smallScenarioConfig()
			cfg.conflictsInWinner = 3

			if parallel {
				cfg.parallelThreshold = 1
			}

			sc := newReorgScenario(t, cfg)
			require.Len(t, sc.loserIDs, 3)
			require.Equal(t, sc.pendingOrder(), txHashes(assemblyNodes(sc.stp)))
			requireAssemblyValid(t, sc.stp, sc.onChainBefore)

			require.NoError(t, sc.run(t))

			after := txHashes(assemblyNodes(sc.stp))
			requireAssemblyValid(t, sc.stp, sc.onChainAfter)
			require.Equal(t, sc.expectedOrderAfter(), after)
			require.Equal(t, len(after), sc.stp.GetCurrentTxMap().Length())
			require.Zero(t, sc.stp.GetRemoveMapLength())

			for _, id := range sc.droppedIDs {
				require.NotContains(t, after, sc.hash(id), "L' and D' are gone")
				require.False(t, sc.stp.GetCurrentTxMap().Exists(sc.hash(id)))
			}

			require.ElementsMatch(t, sc.expectedMarkFalse(), sc.utxo.marked(false), "L' is marked off the longest chain with raw D1; D' has no record and is not")
			require.ElementsMatch(t, sc.winnerNotLost(), sc.utxo.marked(true), "W' is marked with the rest of D2")

			for _, pair := range sc.winnerPairs {
				loser, err := sc.utxo.Get(t.Context(), pair.loser.TxIDChainHash(), fields.Conflicting)
				require.NoError(t, err)
				require.True(t, loser.Conflicting)

				winner, err := sc.utxo.Get(t.Context(), pair.winner.TxIDChainHash(), fields.Conflicting)
				require.NoError(t, err)
				require.False(t, winner.Conflicting)
			}
		})
	}
}

// The move-back reverses a conflict the lost block won: the winner is evicted through the removeMap
// (entry consumed, not marked) and the loser it displaced is not re-added to the assembly, the
// documented gap in reorgBlocks.
func TestReorgScenario_SlowPathReversesAConflictWonInTheLostBlock(t *testing.T) {
	cfg := smallScenarioConfig()
	cfg.conflictsInLost = 2
	sc := newReorgScenario(t, cfg)

	for _, pair := range sc.lostPairs {
		w, err := sc.utxo.Get(t.Context(), pair.winner.TxIDChainHash(), fields.Conflicting)
		require.NoError(t, err)
		require.False(t, w.Conflicting, "before the reorg W won")
	}

	require.NoError(t, sc.run(t))

	after := txHashes(assemblyNodes(sc.stp))
	requireAssemblyValid(t, sc.stp, sc.onChainAfter)
	require.Equal(t, sc.expectedOrderAfter(), after)
	require.Equal(t, len(after), sc.stp.GetCurrentTxMap().Length())
	require.Zero(t, sc.stp.GetRemoveMapLength())

	require.ElementsMatch(t, sc.expectedMarkFalse(), sc.utxo.marked(false))
	require.ElementsMatch(t, sc.winnerNotLost(), sc.utxo.marked(true))

	for _, pair := range sc.lostPairs {
		w := *pair.winner.TxIDChainHash()
		l := *pair.loser.TxIDChainHash()

		require.NotContains(t, after, w)
		require.NotContains(t, after, l)

		wMeta, err := sc.utxo.Get(t.Context(), &w, fields.Conflicting)
		require.NoError(t, err)
		require.True(t, wMeta.Conflicting, "W is conflicting again")

		lMeta, err := sc.utxo.Get(t.Context(), &l, fields.Conflicting)
		require.NoError(t, err)
		require.False(t, lMeta.Conflicting, "L is restored")
	}
}
