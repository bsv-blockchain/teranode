package netsync

import (
	"testing"

	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestFallbackSubtreeDAH_IsCommittedTipPlusReadAheadDepthPlusRetention is the
// review round 1 gap: fallbackSubtreeDAH's own arithmetic had no direct test
// before this one — only exercised indirectly, through a full pipelineBlockSink
// conversion, where a wrong answer would still pass every existing assertion
// (none of them check the delete-at-height a converted record's subtrees carry
// when the height was unresolved). This pins the formula on its own: committed
// tip + read-ahead depth (legacy_blockDownloadWindow) + configured retention.
func TestFallbackSubtreeDAH_IsCommittedTipPlusReadAheadDepthPlusRetention(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.BlockDownloadWindow = 500
	tSettings.GlobalBlockHeightRetention = 10
	tSettings.SubtreeValidation.BlockHeightRetentionAdjustment = 0

	sm := &SyncManager{settings: tSettings}
	mockCommittedTip(t, sm, 2000, 0)

	retention := tSettings.GetSubtreeValidationBlockHeightRetention()
	require.NotZero(t, retention, "sanity: a zero retention would make this indistinguishable from a formula that dropped the term")

	want := uint32(2000) + uint32(500) + retention

	require.Equal(t, want, sm.fallbackSubtreeDAH(),
		"fallbackSubtreeDAH must be committed tip + read-ahead depth + retention")
}

// TestFallbackSubtreeDAH_FloorsTheReadAheadDepthAtOne pins the guard against a
// misconfigured or disabled legacy_blockDownloadWindow (0, or negative) zeroing
// the whole read-ahead term instead of merely not widening it — a depth of 0
// would let the sum collapse to tip + retention, no safety margin at all for a
// block that has not committed yet.
func TestFallbackSubtreeDAH_FloorsTheReadAheadDepthAtOne(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.BlockDownloadWindow = 0
	tSettings.GlobalBlockHeightRetention = 10
	tSettings.SubtreeValidation.BlockHeightRetentionAdjustment = 0

	sm := &SyncManager{settings: tSettings}
	mockCommittedTip(t, sm, 5, 0)

	retention := tSettings.GetSubtreeValidationBlockHeightRetention()
	want := uint32(5) + uint32(1) + retention

	require.Equal(t, want, sm.fallbackSubtreeDAH(),
		"the read-ahead depth must floor at 1, not 0, when legacy_blockDownloadWindow is misconfigured")
}

// TestFallbackSubtreeDAH_DefaultsToZeroTipBeforeAnyCommit pins the other end:
// with no blockchain client at all, committedTip() reports ok=false and its
// own documented zero value, and fallbackSubtreeDAH must use that 0 rather
// than a negative or undefined tip.
func TestFallbackSubtreeDAH_DefaultsToZeroTipBeforeAnyCommit(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.BlockDownloadWindow = 500
	tSettings.GlobalBlockHeightRetention = 10
	tSettings.SubtreeValidation.BlockHeightRetentionAdjustment = 0

	sm := &SyncManager{settings: tSettings}
	// No blockchainClient set: committedTip() must default to height 0.

	retention := tSettings.GetSubtreeValidationBlockHeightRetention()
	want := uint32(0) + uint32(500) + retention

	require.Equal(t, want, sm.fallbackSubtreeDAH())
}
