package subtreeprocessor

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// The differential harness compares two reorg paths on the same scenario. These tests pin the
// harness itself: the slow path matches itself, and a changed outcome is reported unless a named
// divergence says the difference is intended.

func TestReorgDifferential_SlowPathMatchesItself(t *testing.T) {
	for _, disk := range []bool{false, true} {
		cfg := smallScenarioConfig()
		cfg.diskTxMap = disk

		requireReorgEquivalent(t, cfg, slowReorgPath, slowReorgPath)
	}
}

func TestReorgDifferential_ReportsEachDifference(t *testing.T) {
	sc := newReorgScenario(t, smallScenarioConfig())
	require.NoError(t, sc.run(t))

	base := captureReorgOutcome(sc, nil)

	changes := map[string]func(o *reorgOutcome){
		"tip":        func(o *reorgOutcome) { o.tip = chainhash.Hash{1} },
		"order":      func(o *reorgOutcome) { o.order[0], o.order[1] = o.order[1], o.order[0] },
		"tx map":     func(o *reorgOutcome) { o.txMapLen++ },
		"inpoints":   func(o *reorgOutcome) { o.inpoints[o.order[0]] = "changed" },
		"removeMap":  func(o *reorgOutcome) { o.removeMapLen++ },
		"mark true":  func(o *reorgOutcome) { o.markTrue = o.markTrue[1:] },
		"mark false": func(o *reorgOutcome) { o.markFalse = append(o.markFalse, chainhash.Hash{2}) },
		"error":      func(o *reorgOutcome) { o.err = "boom" },
	}

	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			other := base.clone()
			change(&other)

			require.NotEmpty(t, compareReorgOutcomes(base, other, nil), "a changed %s must be reported", name)
		})
	}
}

func TestReorgDifferential_NamedDivergenceIsApplied(t *testing.T) {
	sc := newReorgScenario(t, smallScenarioConfig())
	require.NoError(t, sc.run(t))

	base := captureReorgOutcome(sc, nil)
	other := base.clone()
	other.removeMapLen += 3

	ignoreRemoveMap := reorgDivergence{
		name:      "removeMap left over",
		normalize: func(o *reorgOutcome) { o.removeMapLen = 0 },
	}

	require.Empty(t, compareReorgOutcomes(base, other, []reorgDivergence{ignoreRemoveMap}))
}
