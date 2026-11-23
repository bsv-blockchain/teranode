package subtreeprocessor

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
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

// Design section 5 item 2c, plan PR 9a: C is in B and not in F or P, and spends X, which is in B and
// P and not in F. Today's move-back skips X (it is in the tx map), builds C into the prefix and
// leaves X at its pending position, so with a subtree boundary between them the slow path publishes
// C before its parent X. Block validation rejects that order.
//
// This test pins the inversion as the order oracle's rejection. It is the red test PR 9a turns
// green (bsv-blockchain/teranode#2268): when the slow path emits a recovered child only after its
// last unconfirmed parent, the final assertion below flips and expectedOrderAfter gains the rule.
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

	require.ElementsMatch(t, sc.expectedMarkFalse(), sc.utxo.marked(false), "the children are not marked: they are gone")
	require.ElementsMatch(t, sc.winnerNotLost(), sc.utxo.marked(true))
}
