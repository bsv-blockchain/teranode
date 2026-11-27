package subtreeprocessor

import (
	"slices"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
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

func TestReorgScenario_SlowPathKeepsTheOrder(t *testing.T) {
	for _, disk := range []bool{false, true} {
		t.Run(map[bool]string{false: "in-memory map", true: "disk map"}[disk], func(t *testing.T) {
			cfg := smallScenarioConfig()
			cfg.diskTxMap = disk
			sc := newReorgScenario(t, cfg)

			require.NoError(t, sc.run(t))

			require.Equal(t, sc.forward[len(sc.forward)-1].Header.Hash(), sc.stp.GetCurrentBlockHeader().Hash())
			requireAssemblyValid(t, sc.stp, sc.onChainAfter)
			require.Equal(t, sc.expectedOrderAfter(), txHashes(assemblyNodes(sc.stp)),
				"effective D1 in the lost block's order, then the pending set minus the winners in its order")
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

	const tps = 100_000

	start := time.Now()
	stop := sc.startIntake(tps)

	require.Eventually(t, func() bool { return sc.stp.QueueLength() > 0 }, 10*time.Second, 5*time.Millisecond)

	sent := stop()
	elapsed := time.Since(start)

	require.Positive(t, sent, "intake sends while it runs")
	// One batch of slack: the ticker sends a batch per 1000/tps seconds and drops ticks when late.
	require.LessOrEqual(t, sent, int(tps*elapsed.Seconds())+1000, "and not faster than its rate")
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

// The winning blocks are what the network accepted, so a chain child is never before its parent,
// in the same block or in an earlier one. The blocks are consecutive slices of the winners.
func TestReorgScenario_WinningBlocksPutEachChainChildAfterItsParent(t *testing.T) {
	cfg := smallScenarioConfig()
	cfg.hitsAtFront = true
	cfg.forwardBlocks = 4

	for seed := uint64(1); seed <= 20; seed++ {
		cfg.seed = seed
		sc := newReorgScenario(t, cfg)

		position := make(map[scenarioID]int, len(sc.winnerIDs))
		for i, id := range sc.winnerIDs {
			position[id] = i
		}

		chained := 0

		for i, id := range sc.winnerIDs {
			extra, ok := sc.extraParent[id]
			if !ok {
				continue
			}

			parentPos, inWinners := position[extra.parent]
			if !inWinners {
				continue
			}

			chained++

			require.Less(t, parentPos, i, "seed %d: a chain child comes after its parent", seed)
		}

		require.Positive(t, chained, "seed %d: the winners hold chains", seed)
	}
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
