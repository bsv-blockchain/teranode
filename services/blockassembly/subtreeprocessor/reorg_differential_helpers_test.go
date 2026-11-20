package subtreeprocessor

import (
	"bytes"
	"fmt"
	"maps"
	"slices"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// reorgPath runs a reorg on a scenario. The slow path is today's reorgBlocks; a fast path added by
// issue 2269 is another reorgPath over the same scenario.
type reorgPath func(t testing.TB, sc *reorgScenario) error

func slowReorgPath(t testing.TB, sc *reorgScenario) error { return sc.run(t) }

// reorgOutcome is what a reorg leaves behind that the two paths must agree on.
type reorgOutcome struct {
	err          string
	tip          chainhash.Hash
	order        []chainhash.Hash
	inpoints     map[chainhash.Hash]string
	txMapLen     int
	removeMapLen int
	markTrue     []chainhash.Hash
	markFalse    []chainhash.Hash
}

func (o reorgOutcome) clone() reorgOutcome {
	o.order = slices.Clone(o.order)
	o.inpoints = maps.Clone(o.inpoints)
	o.markTrue = slices.Clone(o.markTrue)
	o.markFalse = slices.Clone(o.markFalse)

	return o
}

// reorgDivergence is a difference between two paths that is intended, with its reason as its name.
// normalize is applied to both outcomes before they are compared.
type reorgDivergence struct {
	name      string
	normalize func(o *reorgOutcome)
}

// sortedHashes sorts a copy of hashes and keeps duplicates, so a path that marks a hash twice
// differs from one that marks it once.
func sortedHashes(hashes []chainhash.Hash) []chainhash.Hash {
	out := slices.Clone(hashes)
	slices.SortFunc(out, func(a, b chainhash.Hash) int { return bytes.Compare(a[:], b[:]) })

	return out
}

// captureReorgOutcome reads the outcome from the scenario's processor and stores after a run that
// returned runErr.
func captureReorgOutcome(sc *reorgScenario, runErr error) reorgOutcome {
	o := reorgOutcome{
		tip:          *sc.stp.GetCurrentBlockHeader().Hash(),
		order:        txHashes(assemblyNodes(sc.stp)),
		txMapLen:     sc.stp.GetCurrentTxMap().Length(),
		removeMapLen: sc.stp.GetRemoveMapLength(),
		markTrue:     sortedHashes(sc.utxo.marked(true)),
		markFalse:    sortedHashes(sc.utxo.marked(false)),
	}

	if runErr != nil {
		o.err = runErr.Error()
	}

	txMap := sc.stp.GetCurrentTxMap()
	o.inpoints = make(map[chainhash.Hash]string, len(o.order))

	for _, h := range o.order {
		if p, ok := txMap.Get(h); ok && p != nil {
			o.inpoints[h] = fmt.Sprint(p.GetTxInpoints())
		} else {
			o.inpoints[h] = "missing"
		}
	}

	return o
}

// compareReorgOutcomes lists every difference between a and b after the divergences are applied.
func compareReorgOutcomes(a, b reorgOutcome, divergences []reorgDivergence) []string {
	a, b = a.clone(), b.clone()

	for _, d := range divergences {
		d.normalize(&a)
		d.normalize(&b)
	}

	var diffs []string

	if a.err != b.err {
		diffs = append(diffs, fmt.Sprintf("error: %q vs %q", a.err, b.err))
	}

	if a.tip != b.tip {
		diffs = append(diffs, fmt.Sprintf("tip: %s vs %s", a.tip, b.tip))
	}

	if !slices.Equal(a.order, b.order) {
		diffs = append(diffs, fmt.Sprintf("order: %d vs %d transactions, first difference at %d", len(a.order), len(b.order), firstDifference(a.order, b.order)))
	}

	if !maps.Equal(a.inpoints, b.inpoints) {
		diffs = append(diffs, "inpoints of the transactions in assembly differ")
	}

	if a.txMapLen != b.txMapLen {
		diffs = append(diffs, fmt.Sprintf("tx map length: %d vs %d", a.txMapLen, b.txMapLen))
	}

	if a.removeMapLen != b.removeMapLen {
		diffs = append(diffs, fmt.Sprintf("removeMap length: %d vs %d", a.removeMapLen, b.removeMapLen))
	}

	if !slices.Equal(a.markTrue, b.markTrue) {
		diffs = append(diffs, fmt.Sprintf("marked on the longest chain: %d vs %d", len(a.markTrue), len(b.markTrue)))
	}

	if !slices.Equal(a.markFalse, b.markFalse) {
		diffs = append(diffs, fmt.Sprintf("marked not on the longest chain: %d vs %d", len(a.markFalse), len(b.markFalse)))
	}

	return diffs
}

func firstDifference(a, b []chainhash.Hash) int {
	for i := range min(len(a), len(b)) {
		if a[i] != b[i] {
			return i
		}
	}

	return min(len(a), len(b))
}

// runReorgDifferential builds the scenario of cfg twice, runs pathA on one and pathB on the other,
// checks each successful result with the order and shape oracles, and returns the differences
// between the two outcomes after the divergences are applied.
func runReorgDifferential(t testing.TB, cfg reorgScenarioConfig, pathA, pathB reorgPath, divergences ...reorgDivergence) []string {
	t.Helper()

	outcomes := make([]reorgOutcome, 2)

	for i, path := range []reorgPath{pathA, pathB} {
		sc := newReorgScenario(t, cfg)
		err := path(t, sc)

		if err == nil {
			requireAssemblyValid(t, sc.stp, sc.onChainAfter)
		}

		outcomes[i] = captureReorgOutcome(sc, err)
	}

	for _, d := range divergences {
		t.Logf("named divergence applied: %s", d.name)
	}

	return compareReorgOutcomes(outcomes[0], outcomes[1], divergences)
}

// requireReorgEquivalent fails the test when pathA and pathB differ on cfg beyond the divergences.
func requireReorgEquivalent(t testing.TB, cfg reorgScenarioConfig, pathA, pathB reorgPath, divergences ...reorgDivergence) {
	t.Helper()

	require.Empty(t, runReorgDifferential(t, cfg, pathA, pathB, divergences...))
}
