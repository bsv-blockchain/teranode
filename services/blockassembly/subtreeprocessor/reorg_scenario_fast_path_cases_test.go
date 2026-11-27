package subtreeprocessor

import (
	"fmt"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/stretchr/testify/require"
)

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

// Design 3.2 step 7 and section 5 item 2b: a transaction in the removeMap, whether pending, a hit,
// a lost-block transaction that is also pending (X) or one only the lost block has, is absent from
// the assembly and the tx map afterwards and its removeMap entry is consumed.
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

// Design section 5 item 2c, plan PR 9a (bsv-blockchain/teranode#2268): a lost-block child C of a
// pending X is emitted after its last pending parent, not into the prefix ahead of X, so no subtree
// boundary gives a miner C without its parent.
func TestReorgScenario_SlowPathPublishesLostChildAfterItsPendingParents(t *testing.T) {
	// childID is the scenario id of lost-block child j.
	childID := func(cfg reorgScenarioConfig, j int) scenarioID {
		return scenarioID(cfg.lostAlsoPending + cfg.pendingSpendsLost + j)
	}

	for _, tt := range []struct {
		name string
		cfg  func(cfg *reorgScenarioConfig)
		// before runs on the assembly before the reorg.
		before func(t *testing.T, sc *reorgScenario)
		// check pins where the children land, given every node's position.
		check func(t *testing.T, sc *reorgScenario, position map[chainhash.Hash]int)
	}{
		{
			name: "C comes right after its parent X",
			cfg:  func(cfg *reorgScenarioConfig) { cfg.lostChildOfPending = 3 },
			check: func(t *testing.T, sc *reorgScenario, position map[chainhash.Hash]int) {
				for j := range sc.cfg.lostChildOfPending {
					x := sc.hash(kindRawD1 | scenarioID(j))
					c := sc.hash(kindRawD1 | childID(sc.cfg, j))

					require.Equal(t, position[x]+1, position[c], "C %d is right after its parent X", j)
				}
			},
		},
		{
			name: "a chain of children follows X in order",
			cfg: func(cfg *reorgScenarioConfig) {
				cfg.lostChildOfPending = 3
				cfg.lostChildChain = true
			},
			check: func(t *testing.T, sc *reorgScenario, position map[chainhash.Hash]int) {
				x := position[sc.hash(kindRawD1)]

				for j := range sc.cfg.lostChildOfPending {
					require.Equal(t, x+1+j, position[sc.hash(kindRawD1|childID(sc.cfg, j))], "link %d follows the one before it", j)
				}
			},
		},
		{
			name: "C follows the later of its two parents",
			cfg: func(cfg *reorgScenarioConfig) {
				cfg.lostChildOfPending = 2
				cfg.lostChildTwoParents = true
				cfg.lostAlsoPending = 6
			},
			check: func(t *testing.T, sc *reorgScenario, position map[chainhash.Hash]int) {
				for j := range sc.cfg.lostChildOfPending {
					first := position[sc.hash(kindRawD1|scenarioID(j))]
					last := position[sc.hash(kindRawD1|scenarioID(sc.cfg.lostChildOfPending+j))]

					require.Less(t, first+1, last, "the two parents of child %d are apart", j)
					require.Equal(t, last+1, position[sc.hash(kindRawD1|childID(sc.cfg, j))], "child %d follows its last parent", j)
				}
			},
		},
		{
			// X is in the open current subtree, so C is released from the second pending loop.
			name: "X is in the open subtree",
			cfg: func(cfg *reorgScenarioConfig) {
				cfg.pending = 100
				cfg.pendingChains = 5
				cfg.pendingSpendsLost = 5
				cfg.lostChildOfPending = 5
				cfg.pendingSpendsLostChild = true
			},
			before: func(t *testing.T, sc *reorgScenario) {
				x := sc.hash(kindRawD1 | scenarioID(sc.cfg.lostChildOfPending-1))
				inOpen := false

				for _, n := range sc.stp.GetCurrentSubtree().Nodes {
					inOpen = inOpen || n.Hash == x
				}

				require.True(t, inOpen, "the last X starts in the open subtree")
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := smallScenarioConfig()
			tt.cfg(&cfg)
			sc := newReorgScenario(t, cfg)

			if tt.before != nil {
				tt.before(t, sc)
			}

			require.NoError(t, sc.run(t))

			nodes := assemblyNodes(sc.stp)
			requireEveryPrefixResolves(t, sc, nodes)
			require.Equal(t, sc.expectedOrderAfter(), txHashes(nodes), "effective D1 without the children, then the pending set with each child after its parents")

			if tt.check != nil {
				position := make(map[chainhash.Hash]int, len(nodes))
				for i, n := range nodes {
					position[n.Hash] = i
				}

				tt.check(t, sc, position)
			}

			requireAssemblyValid(t, sc.stp, sc.onChainAfter)
			require.ElementsMatch(t, sc.expectedMarkFalse(), sc.utxo.marked(false), "C and X are both marked off the longest chain")
		})
	}
}

// A pending Q that spends the lost-block child C sits ahead of C's parent X, so C waits for X and Q
// for C. With X also in the winning blocks, X leaves the assembly and C and Q keep X's place.
func TestReorgScenario_SlowPathKeepsPendingSpenderBehindLostChild(t *testing.T) {
	for _, xInWinners := range []bool{false, true} {
		t.Run(fmt.Sprintf("xInWinners=%v", xInWinners), func(t *testing.T) {
			cfg := smallScenarioConfig()
			cfg.lostChildOfPending = 3
			cfg.pendingSpendsLostChild = true
			cfg.lostChildParentsInWinners = xInWinners
			sc := newReorgScenario(t, cfg)

			require.NoError(t, sc.run(t))

			nodes := assemblyNodes(sc.stp)
			requireEveryPrefixResolves(t, sc, nodes)
			require.Equal(t, sc.expectedOrderAfter(), txHashes(nodes))
			requireAssemblyValid(t, sc.stp, sc.onChainAfter)

			position := make(map[chainhash.Hash]int, len(nodes))
			for i, n := range nodes {
				position[n.Hash] = i
			}

			c := sc.hash(kindRawD1 | scenarioID(cfg.lostAlsoPending+cfg.pendingSpendsLost+cfg.lostChildOfPending-1))

			spenders := 0

			for id, e := range sc.extraParent {
				if sc.hash(e.parent) != c {
					continue
				}

				spenders++

				if at, ok := position[sc.hash(id)]; ok {
					require.Greater(t, at, position[c], "Q is behind C")
				}
			}

			require.Equal(t, 1, spenders)
		})
	}
}

// C is in the moved-back block and spends X, which the move-back keeps at its pending position, but
// X never comes out of the pending loops: it is in no subtree, or keepPrevious drops it. The held-back
// C must not be lost, so flush appends it.
func TestReorgScenario_SlowPathFlushKeepsLostChildOfAParentThatIsNotEmitted(t *testing.T) {
	for _, tt := range []struct {
		name string
		// setup puts X in the tx map and returns the previous current subtree, if any.
		setup func(t *testing.T, stp *SubtreeProcessor, x chainhash.Hash, onChain chainhash.Hash) *subtreepkg.Subtree
		// blockHasDroppedTx adds a block transaction that spends a dropped parent.
		blockHasDroppedTx bool
	}{
		{
			name: "X is in the tx map and in no subtree",
			setup: func(t *testing.T, stp *SubtreeProcessor, x, onChain chainhash.Hash) *subtreepkg.Subtree {
				_, wasSet := stp.currentTxMap.SetIfNotExists(x, inpointsWithParent(onChain))
				require.True(t, wasSet)

				return nil
			},
		},
		{
			name: "keepPrevious drops X",
			setup: func(t *testing.T, stp *SubtreeProcessor, x, _ chainhash.Hash) *subtreepkg.Subtree {
				dropped := chainhash.HashH([]byte("flush-dropped-parent"))

				stp.droppedMu.Lock()
				stp.recordDroppedLocked(dropped)
				stp.droppedMu.Unlock()

				_, wasSet := stp.currentTxMap.SetIfNotExists(x, inpointsWithParent(dropped))
				require.True(t, wasSet)

				st, err := subtreepkg.NewTreeByLeafCount(4)
				require.NoError(t, err)
				require.NoError(t, st.AddNode(x, 1, 1))

				return st
			},
			blockHasDroppedTx: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			stp, _ := lanePoolProcessor(t, false, false)

			x := chainhash.HashH([]byte("flush-x"))
			c := chainhash.HashH([]byte("flush-c"))
			onChain := chainhash.HashH([]byte("flush-on-chain"))
			previous := tt.setup(t, stp, x, onChain)

			st, err := subtreepkg.NewTreeByLeafCount(4)
			require.NoError(t, err)
			require.NoError(t, st.AddCoinbaseNode())
			require.NoError(t, st.AddNode(x, 1, 1))
			require.NoError(t, st.AddNode(c, 1, 1))

			inpoints := []subtreepkg.TxInpoints{{}, *inpointsWithParent(onChain), *inpointsWithParent(x)}

			if tt.blockHasDroppedTx {
				e := chainhash.HashH([]byte("flush-e"))
				require.NoError(t, st.AddNode(e, 1, 1))

				inpoints = append(inpoints, *inpointsWithParent(chainhash.HashH([]byte("flush-dropped-parent"))))
			}

			block := &model.Block{Header: prevBlockHeader, Subtrees: []*chainhash.Hash{st.RootHash()}}

			_, _, err = stp.moveBackBlockBulkBuild(t.Context(), block, true, nil, previous, [][]subtreepkg.Node{st.Nodes}, [][]subtreepkg.TxInpoints{inpoints}, nil)
			require.NoError(t, err)

			require.Equal(t, []chainhash.Hash{c}, txHashes(assemblyNodes(stp)), "C is kept, and X, in no subtree, is not emitted")
		})
	}
}

// requireEveryPrefixResolves checks the order rule on each prefix a miner could be given, one
// ending at every subtree boundary: each transaction's parents are on chain or inside the prefix.
func requireEveryPrefixResolves(t *testing.T, sc *reorgScenario, nodes []subtreepkg.Node) {
	t.Helper()

	size := sc.cfg.subtreeSize
	require.Greater(t, len(nodes), size, "the assembly spans more than one subtree")

	for end := size; ; end += size {
		end = min(end, len(nodes))
		require.NoError(t, checkAssemblyOrder(nodes[:end], sc.stp.GetCurrentTxMap().Get, sc.onChainAfter), "prefix of %d nodes", end)

		if end == len(nodes) {
			return
		}
	}
}

// Design section 5, gate on B's coinbase children and on the removeMap "including what
// removeCoinbaseUtxos will add". Pending transactions that spend B's coinbase are deleted with it:
// out of the UTXO store (saved to the reorg journal), out of the assembly and out of the tx map,
// and every other pending transaction keeps its order. Their removeMap entries are not consumed:
// the entries were made after the children left the subtrees, so no remainder pass meets them, and
// they stay to keep the same transactions out of the queue.
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

// Plan A1 row 6: a pending hash that sits twice in the chained subtrees, as a failed path can leave
// it. The remainder pass inserts into the fresh tx map and, above the parallel threshold, compacts
// to what was inserted (wasInserted), so a surviving duplicate is built once, at its first position,
// and both copies of a hit are dropped. Below the threshold addNode skips the second copy through the
// map. Both branches give the same result.
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

// Plan A1 row 6: an empty forward block, first and last. Its step creates the block's coinbase and
// returns before resetSubtreeState, so it neither drains nor resizes the subtrees.
//
// Finding (design item: empty and empty-last forward block): the subtree size is set by the LAST
// forward step, which resets with the configured size; every earlier step, and the move-back, build
// 1,048,576-leaf subtrees. With an empty last block no step does, so the reorg ends with a single
// 1,048,576-leaf open subtree where the configured size is smaller. Pinned as it is today; the fast
// path must match it or the finding must be fixed first.
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

// Design section 5 items 6 and 10: the queue during a reorg. The head drain takes a batch queued
// before the reorg ahead of the move-back, so it sits in the pending set when the move-back and the
// forward steps run: a queued lost-block transaction keeps its queue position after the pending set
// instead of joining the D1 prefix, and one in the removeMap is flagged and consumed by the first
// forward step. A forward step drains the queue after its remainder pass: a batch that arrives
// between the steps is placed after the remainder, in queue order, and what the step's block mined is
// filtered out. The arrival is injected at the progress beat before the second forward block, so it
// is deterministic.
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

// Transactions that arrive while the reorg runs are never lost or placed twice: what the drains took
// follows the remainder in the order they were sent, and the rest is still queued. A batch is queued
// before the reorg, so the head drain takes at least that.
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

// Design section 5 gate "no conflicting nodes in B or F": the fast path declines these, so the slow
// path's result is the reference. A winning block's conflicting node W' demotes the pending
// transaction L' that spends the same output: L' leaves the assembly and the tx map and is marked off
// the longest chain, and its pending child D', which the UTXO store has no record of, is dropped by
// the assembly's own parent links (dropConflictingDescendants), not by the store's cascade.
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

// A lost block's conflicting node W (it won a conflict when B was applied) is reversed by the
// move-back: W is conflicting again, the loser L it displaced owns the output again, and W is evicted
// through the removeMap, so it is absent from the assembly, not marked (marks cover what the assembly
// holds) and the removeMap entry is consumed by the first forward step. L is in no set, so it is not
// re-added to the assembly (the documented gap in reorgBlocks).
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
