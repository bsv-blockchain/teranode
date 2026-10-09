package postgres

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// runEarlyStampCase drives one below-checkpoint early-DAH scenario end to end: it
// creates a fresh 2-output tx, mines it at minedHeight, spends its two outputs at
// spendA and spendB, arms the early-DAH feature (setting + published boundary),
// runs one full sweep pass with a safe tip above every spend, and returns the
// stamped delete_at_height. When enable is false the setting gate must force the
// boundary to 0 (full retention) regardless of the published value.
func runEarlyStampCase(t *testing.T, enable bool, boundary uint32, minedHeight, spendA, spendB uint32) (dah *int64, ret int64) {
	t.Helper()
	store, ctx := setupTestStore(t)

	ret = int64(store.settings.GetUtxoStoreBlockHeightRetention())

	tx := newUniqueUnminedTx(t, store) // exactly 2 spendable P2PKH outputs
	mineTx(t, store, tx, minedHeight)
	spendVouts(t, store, tx, spendA, 0)
	spendVouts(t, store, tx, spendB, 1)

	// Safe tip above every spend so the whole range is below safe_tip and swept.
	maxSpend := spendA
	if spendB > maxSpend {
		maxSpend = spendB
	}
	safeTip := int64(maxSpend) + 10
	require.NoError(t, store.SetBlockHeight(uint32(safeTip)))

	store.settings.UtxoStore.EarlyDAHBelowCheckpoint = enable
	store.SetEarlyDAHBoundary(boundary)

	store.sweepAllPartitionsOnce(ctx, safeTip, int32(ret)) //nolint:gosec // small positive retention

	return dahOfTx(t, store, ctx, tx), ret
}

// TestEarlyDAHStampBelowBoundary (Case 1): a tx whose mined height and every
// folded spend height are strictly below the checkpoint boundary is stamped with
// an immediate delete-at-height (completion + 1, NO retention wait) because a
// reorg there is impossible by consensus rule.
func TestEarlyDAHStampBelowBoundary(t *testing.T) {
	// mined at 100, both outputs spent at 105 and 110; boundary 200.
	dah, _ := runEarlyStampCase(t, true, 200, 100, 105, 110)
	require.NotNil(t, dah, "fully-spent mined tx below the boundary must be stamped")
	require.Equal(t, int64(110)+1, *dah,
		"below-boundary DAH = GREATEST(lastSpend=110, mined=100)+1 with NO retention")
}

// TestEarlyDAHStampStraddlesBoundary (Case 2): a tx with any folded height
// ABOVE the boundary keeps the full retention wait — the CASE keys on the same
// GREATEST as the stamp, so one above-boundary spend disqualifies the whole tx.
func TestEarlyDAHStampStraddlesBoundary(t *testing.T) {
	// mined at 100, spends at 105 and 250; boundary 200.
	dah, ret := runEarlyStampCase(t, true, 200, 100, 105, 250)
	require.NotNil(t, dah, "fully-spent mined tx must still be stamped")
	require.Equal(t, int64(250)+1+ret, *dah,
		"straddling the boundary must keep full retention (GREATEST=250 > 200)")
}

// TestEarlyDAHStampBoundaryOffKeepsRetention (Case 3): with the feature disabled
// the setting gate forces the boundary to 0 even though a boundary was published,
// so every stamp keeps full retention — identical to v16 behaviour.
func TestEarlyDAHStampBoundaryOffKeepsRetention(t *testing.T) {
	// mined at 100, spends at 105, 110; feature OFF (published boundary ignored).
	dah, ret := runEarlyStampCase(t, false, 200, 100, 105, 110)
	require.NotNil(t, dah, "fully-spent mined tx must be stamped")
	require.Equal(t, int64(110)+1+ret, *dah,
		"feature off must keep full retention (boundary forced to 0)")
}

// TestEarlyDAHStampExactlyAtBoundary (Case 4): the boundary comparison is
// at-or-below (<=), so a tx whose highest folded height equals the boundary
// exactly still gets the immediate stamp.
func TestEarlyDAHStampExactlyAtBoundary(t *testing.T) {
	// mined at 100, last spend at exactly 200; boundary 200.
	dah, _ := runEarlyStampCase(t, true, 200, 100, 105, 200)
	require.NotNil(t, dah, "fully-spent mined tx exactly at the boundary must be stamped")
	require.Equal(t, int64(200)+1, *dah,
		"exactly-at-boundary DAH = GREATEST(lastSpend=200, mined=100)+1 with NO retention (<= comparison)")
}
