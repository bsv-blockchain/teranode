package subtreeprocessor

import (
	"testing"

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
