package utxoset

import (
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// TestPrunerServiceContract covers what the pruner service must satisfy for the daemon to
// boot at all, separately from what it reclaims.
//
// services/pruner/server.go type-asserts the UTXO store to pruner.PrunerServiceProvider
// and refuses to start without it, which crash-loops the daemon. Switching the service off
// in settings is not an option: a feature disabled on a node to dodge a gap is not a fix.
//
// What it reclaims is the spend journal, and that is covered by
// TestSpendJournalReclaimIsDrivenByThePruner. There is no DAH sweep to run: the DELETE that
// spends an output frees its space and its index entry in the same statement, so the
// per-row reclaim that dominated the previous store does not exist here.
func TestPrunerServiceContract(t *testing.T) {
	s, ctx := newTestStore(t)

	svc, err := s.GetPrunerService()
	require.NoError(t, err, "the pruner service refuses to start without this")
	require.NotNil(t, svc, "a nil service panics the caller, which only checks err")

	svc.Start(ctx) // must not block: the service calls Prune per block, there is no loop here

	svc.AddObserver(nil) // must not panic

	// Below retention there is nothing aged out yet, and asking must not be an error.
	n, err := svc.Prune(ctx, 100, "deadbeef")
	require.NoError(t, err)
	require.Zero(t, n)

	// Nor at a height with no journal at all. Reclaim has to tolerate being called on a
	// store that has never spent anything, because the pruner service starts with the node.
	n, err = svc.Prune(ctx, DefaultSpendJournalRetentionBlocks*3, "deadbeef")
	require.NoError(t, err)
	require.Zero(t, n, "no transaction records are deleted yet, and journal rows do not belong in that counter")
}

// TestPrunerDropsMembershipWindowsOnTheJournalCutoff: identity reclaim in this design is a
// catalog drop. A window whose upper bound is 1440 blocks below the pruner's height goes,
// a younger one stays, and the floor advances.
func TestPrunerDropsMembershipWindowsOnTheJournalCutoff(t *testing.T) {
	s, ctx := newTestStore(t)
	s.journalRetention = 96

	old := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, old, 100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 100, OnLongestChain: true}))
	require.NoError(t, err)

	young := mkTx(t, 1, 5_001)
	_, err = s.Create(ctx, young, 900, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 2, BlockHeight: 900, OnLongestChain: true}))
	require.NoError(t, err)

	svc, err := s.GetPrunerService()
	require.NoError(t, err)

	_, err = svc.Prune(ctx, 1_000, "deadbeef")
	require.NoError(t, err)

	require.Equal(t, 0, minedRows(t, s, ctx, old), "window 0 retired at 1000 - 96")
	require.Equal(t, 1, minedRows(t, s, ctx, young), "window 3 is inside retention")

	floor, err := s.txMinedFloor(ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(1), floor)
}

// TestPrunerRebuildsCoinIndexBelowJournalRetention pins a review finding: the coin-index
// rebuild step must not sit inside the "height > journalRetention" gate that guards the
// window and spend-journal drops. That gate exists because there is nothing aged out to
// drop below it; it has no bearing on the coin index, which can already be churned on a
// chain three blocks deep. Every dev/test net and every from-scratch sync spends most of
// its life below DefaultSpendJournalRetentionBlocks (1440), so a rebuild gated on it would
// never run there.
//
// s.coinIndexDecider is the injection point: New sets it to the real coinIndexNeedsRebuild,
// and this test swaps in a stub that just counts calls, so the assertion is "the pruner
// consulted the decider exactly once" rather than depending on a real index actually being
// bloated. Whether the decider says yes and a REINDEX CONCURRENTLY then runs is already
// covered by TestRebuildCoinIndexRunsConcurrentlyAndOnce; returning false here keeps this
// test to the one thing it is pinning, and fast.
func TestPrunerRebuildsCoinIndexBelowJournalRetention(t *testing.T) {
	s, ctx := newTestStore(t)

	calls := 0
	s.coinIndexDecider = func(_, _ int64) bool {
		calls++
		return false
	}

	svc, err := s.GetPrunerService()
	require.NoError(t, err)

	_, err = svc.Prune(ctx, 100, "deadbeef")
	require.NoError(t, err)
	require.Equal(t, 1, calls,
		"the coin-index rebuild must run on every pruner session, not only past journal retention")
}
