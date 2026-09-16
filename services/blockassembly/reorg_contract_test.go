// Package blockassembly — adversarial tests for the Chain-Tip and Reorg Contract.
//
// These tests target BA-REORG-NNN in services/blockassembly/spec.md and focus on
// scenarios the existing suite (block_assembler_catchup_test.go) does not cover.
// They are written to break the orchestration layer in BlockAssembler.go against
// the spec, not to re-test the subtree-processor internals (BA-REORG-010/011),
// which are covered under subtreeprocessor/.
//
// Coverage map:
//   - TestBlockAssembler_Reorg_DeepReorgGate         -> BA-REORG-006 (both-conditions gate)
//   - TestBlockAssembler_Reorg_MaturityDepthBoundary -> BA-REORG-006 (">" vs ">=" boundary)
//   - TestBlockAssembler_Reorg_NoWedgeOnFailure      -> BA-REORG-007, BA-REORG-008, BA-STATE-002
//   - TestBlockAssembler_Reorg_Idempotency           -> BA-REORG-002, BA-REORG-009
package blockassembly

import (
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// newReorgMockStp installs a MockSubtreeProcessor with the reorg-decision methods
// stubbed to succeed: the in-place Reorg path (Reorg) and the full-reset path
// (WaitForPendingBlocks + Reset) both no-op. Stubbing BOTH lets a test assert which
// branch handleReorg's deep-reorg gate actually took (via AssertCalled/AssertNotCalled)
// instead of crashing with an unexpected-call panic when the branch differs from
// expectation — which is what makes the BA-REORG-006 spec probe fail cleanly.
func newReorgMockStp(t *testing.T, items *baTestItems) *subtreeprocessor.MockSubtreeProcessor {
	t.Helper()

	mockStp := &subtreeprocessor.MockSubtreeProcessor{}
	mockStp.On("Reorg", mock.Anything, mock.Anything).Return(nil)
	mockStp.On("Reset", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(subtreeprocessor.ResetResponse{})
	mockStp.On("WaitForPendingBlocks", mock.Anything).Return(nil)
	injectMockStp(t, items, mockStp)

	return mockStp
}

// runReorgDecision builds the canonical single-block competing fork and drives
// handleReorg directly, returning the mock STP (to inspect Reorg-vs-Reset) and the
// error handleReorg produced.
//
// Fixture: genesis->a1 (BA's chain) and genesis->b1 (competing tip). Both a1 and b1
// are at real height 1, so getReorgBlockHeaders computes startingHeight = min(cached,1)
// = 1 and the block locators resolve against the real store even though the cached
// height is spoofed to arm the >1000 gate. moveBack=[a1] (depth 1), moveForward=[b1]
// (depth 1). This is the same fixture the existing "large reorg triggers reset" test
// uses; only cachedHeight and CoinbaseMaturity vary.
func runReorgDecision(t *testing.T, cachedHeight uint32, coinbaseMaturity uint16) (*subtreeprocessor.MockSubtreeProcessor, error) {
	t.Helper()

	items := setupBlockAssemblyTest(t)
	genesis := genesisHeader(t, items)

	a1 := buildChain(genesis, 1, 600)[0]
	b1 := buildChain(genesis, 1, 700)[0]
	require.NoError(t, items.addBlock(t.Context(), a1))
	require.NoError(t, items.addBlock(t.Context(), b1))

	mockStp := newReorgMockStp(t, items)

	// CoinbaseMaturity is a private copy of RegressionNetParams (see
	// CreateBaseTestSettings), so tuning it here is isolated to this test.
	items.blockAssembler.settings.ChainCfgParams.CoinbaseMaturity = coinbaseMaturity

	// Park BA at a1 with a spoofed height to arm/disarm the >1000 gate.
	items.blockAssembler.setBestBlockHeader(a1, cachedHeight)

	// b1 is at real height 1; the cached height only feeds the deep-reorg gate.
	err := items.blockAssembler.handleReorg(t.Context(), b1, 1)

	return mockStp, err
}

// assertReorgedInPlace asserts the in-place Reorg path was taken and no full Reset fired.
func assertReorgedInPlace(t *testing.T, mockStp *subtreeprocessor.MockSubtreeProcessor, err error) {
	t.Helper()

	require.NoError(t, err, "must reorg in place, not reset")
	mockStp.AssertCalled(t, "Reorg", mock.Anything, mock.Anything)
	mockStp.AssertNotCalled(t, "Reset", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

// TestBlockAssembler_Reorg_DeepReorgGate covers the untested negative branch of
// BA-REORG-006: a full reset requires BOTH depth >= maturity AND currentHeight > 1000.
// The existing suite only exercises the positive case (height 1001 -> reset). Here the
// depth term is satisfied (CoinbaseMaturity=1, moveBack=1) but the height gate is not,
// so the service MUST reorg in place, never reset.
func TestBlockAssembler_Reorg_DeepReorgGate(t *testing.T) {
	initPrometheusMetrics()

	t.Run("deep depth but height 999 (< 1000) reorgs in place", func(t *testing.T) {
		mockStp, err := runReorgDecision(t, 999, 1)
		assertReorgedInPlace(t, mockStp, err)
	})

	// The spec says "greater than 1000" and the impl uses `> 1000`; at exactly 1000
	// the gate must NOT fire. Locks that height boundary.
	t.Run("deep depth but height 1000 (==, not >) reorgs in place", func(t *testing.T) {
		mockStp, err := runReorgDecision(t, 1000, 1)
		assertReorgedInPlace(t, mockStp, err)
	})
}

// TestBlockAssembler_Reorg_MaturityDepthBoundary probes the BA-REORG-006 depth
// comparison at the exact boundary depth == maturity.
func TestBlockAssembler_Reorg_MaturityDepthBoundary(t *testing.T) {
	initPrometheusMetrics()

	// Below threshold: maturity=2, depth=1 (1 < 2). Spec and impl agree: reorg in place.
	t.Run("depth below maturity reorgs in place", func(t *testing.T) {
		mockStp, err := runReorgDecision(t, 1001, 2)
		assertReorgedInPlace(t, mockStp, err)
	})

	// SPEC PROBE (BA-REORG-006) — EXPECTED TO FAIL against the current implementation.
	//
	// Spec: "When a competing chain's depth from the common ancestor *exceeds* the
	// coinbase-maturity threshold AND the current chain height is greater than 1000,
	// the service MUST perform a full reset". "exceeds" means strictly greater (>),
	// and both the state diagram ("depth > maturity") and AC-BA-REORG-006.1 agree.
	//
	// Here depth (moveBack=1) == maturity (1) and height 1001 > 1000. Because the depth
	// does NOT exceed the threshold, the spec requires an in-place reorg. The
	// implementation at BlockAssembler.go:1656 uses `>=` (len(moveBack) >= CoinbaseMaturity
	// || len(moveForward) >= CoinbaseMaturity), so it performs a full RESET instead.
	//
	// This assertion encodes the spec-required behavior; it will FAIL until the `>=`
	// off-by-one at BlockAssembler.go:1656 is changed to `>`.
	t.Run("depth == maturity must reorg in place (EXPECTED FAIL: impl uses >=)", func(t *testing.T) {
		mockStp, err := runReorgDecision(t, 1001, 1)
		require.NoError(t, err,
			"BA-REORG-006: depth == maturity does not *exceed* maturity; must reorg in place, not reset")
		mockStp.AssertCalled(t, "Reorg", mock.Anything, mock.Anything)
		mockStp.AssertNotCalled(t, "Reset", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	})
}

// assertRunningAndTipUnchanged asserts the no-wedge (BA-REORG-008 / BA-STATE-002) and
// atomicity (BA-REORG-007) guarantees after a failed reconciliation cycle.
func assertRunningAndTipUnchanged(t *testing.T, ba *BlockAssembler, wantHeader *model.BlockHeader, wantHeight uint32) {
	t.Helper()

	require.Equal(t, StateRunning, ba.GetCurrentRunningState(),
		"BA-REORG-008: reconciliation must return to Running, never wedge in a transient state")

	h, ht := ba.CurrentBlock()
	require.Equal(t, wantHeader.Hash().String(), h.Hash().String(),
		"BA-REORG-007: best-block reference must be unchanged on partial failure")
	require.Equal(t, wantHeight, ht,
		"BA-REORG-007: best-block height must be unchanged on partial failure")
}

// TestBlockAssembler_Reorg_NoWedgeOnFailure drives processNewBlockAnnouncement into
// each failure path and asserts the service returns to Running (BA-REORG-008) with the
// best-block reference untouched (BA-REORG-007). The existing "catch-up mid-error"
// test checks the tip but never asserts the operational state.
func TestBlockAssembler_Reorg_NoWedgeOnFailure(t *testing.T) {
	initPrometheusMetrics()

	// BA-REORG-003: a failed authoritative-state read must not advance or wedge.
	t.Run("GetBestBlockHeader error", func(t *testing.T) {
		items := setupBlockAssemblyTest(t)
		genesis := genesisHeader(t, items)
		items.blockAssembler.setBestBlockHeader(genesis, 0)

		mockBC := &blockchain.Mock{}
		mockBC.On("GetBestBlockHeader", mock.Anything).
			Return(nil, nil, errors.NewServiceError("blockchain unreachable"))
		items.blockAssembler.blockchainClient = mockBC

		items.blockAssembler.processNewBlockAnnouncement(t.Context())

		assertRunningAndTipUnchanged(t, items.blockAssembler, genesis, 0)
		mockBC.AssertCalled(t, "GetBestBlockHeader", mock.Anything)
	})

	// Linear move-up (gap=1) where fetching the successor block fails.
	t.Run("GetBlock error on linear move-up", func(t *testing.T) {
		items := setupBlockAssemblyTest(t)
		genesis := genesisHeader(t, items)
		items.blockAssembler.setBestBlockHeader(genesis, 0)

		// child's PreviousHash == genesis == BA tip => classified as linear move-up.
		child := buildChain(genesis, 1, 300)[0]

		mockBC := &blockchain.Mock{}
		mockBC.On("GetBestBlockHeader", mock.Anything).
			Return(child, &model.BlockHeaderMeta{Height: 1}, nil)
		mockBC.On("GetBlock", mock.Anything, mock.Anything).
			Return(nil, errors.NewServiceError("block not available"))
		items.blockAssembler.blockchainClient = mockBC

		items.blockAssembler.processNewBlockAnnouncement(t.Context())

		assertRunningAndTipUnchanged(t, items.blockAssembler, genesis, 0)
	})

	// BA-REORG-007: a move-forward failure must leave the best-block reference unchanged.
	t.Run("MoveForwardBlock error on linear move-up", func(t *testing.T) {
		items := setupBlockAssemblyTest(t)
		genesis := genesisHeader(t, items)

		chain := buildChain(genesis, 1, 210)
		addChain(t, items, chain)

		mockStp := &subtreeprocessor.MockSubtreeProcessor{}
		mockStp.On("MoveForwardBlock", mock.Anything).
			Return(errors.NewProcessingError("move forward failed"))
		injectMockStp(t, items, mockStp)

		items.blockAssembler.setBestBlockHeader(genesis, 0)

		items.blockAssembler.processNewBlockAnnouncement(t.Context())

		assertRunningAndTipUnchanged(t, items.blockAssembler, genesis, 0)
	})

	// Catch-up (moveBack=0, gap>=2) where the subtree-processor reconciliation fails.
	t.Run("Reorg error on catch-up", func(t *testing.T) {
		items := setupBlockAssemblyTest(t)
		genesis := genesisHeader(t, items)

		chain := buildChain(genesis, 3, 820)
		addChain(t, items, chain)

		mockStp := &subtreeprocessor.MockSubtreeProcessor{}
		mockStp.On("Reorg", mock.Anything, mock.Anything).
			Return(errors.NewProcessingError("reorg failed"))
		injectMockStp(t, items, mockStp)

		items.blockAssembler.setBestBlockHeader(genesis, 0)

		items.blockAssembler.processNewBlockAnnouncement(t.Context())

		assertRunningAndTipUnchanged(t, items.blockAssembler, genesis, 0)
	})
}

// TestBlockAssembler_Reorg_Idempotency covers BA-REORG-009 (duplicate/repeated
// wake-ups are idempotent) and BA-REORG-002 (the notification payload is untrusted;
// reconciliation is driven solely by GetBestBlockHeader).
func TestBlockAssembler_Reorg_Idempotency(t *testing.T) {
	initPrometheusMetrics()

	// Firing the reconcile three times for a single-block advance must apply the
	// move-forward exactly once; subsequent passes observe the same authoritative tip
	// and no-op.
	t.Run("repeated wake-ups advance exactly once", func(t *testing.T) {
		items := setupBlockAssemblyTest(t)
		genesis := genesisHeader(t, items)

		chain := buildChain(genesis, 1, 410)
		addChain(t, items, chain)

		mockStp := &subtreeprocessor.MockSubtreeProcessor{}
		mockStp.On("MoveForwardBlock", mock.Anything).Return(nil)
		injectMockStp(t, items, mockStp)

		items.blockAssembler.setBestBlockHeader(genesis, 0)

		for i := 0; i < 3; i++ {
			items.blockAssembler.processNewBlockAnnouncement(t.Context())
		}

		mockStp.AssertNumberOfCalls(t, "MoveForwardBlock", 1)

		h, ht := items.blockAssembler.CurrentBlock()
		require.Equal(t, chain[0].Hash().String(), h.Hash().String(),
			"BA must be parked on the observed tip after the first pass")
		require.Equal(t, uint32(1), ht)
	})

	// BA already at the authoritative tip: a spurious wake-up (e.g. a duplicate or
	// out-of-order notification whose payload is ignored) is a redundant no-op.
	t.Run("wake-up while already at tip is a no-op", func(t *testing.T) {
		items := setupBlockAssemblyTest(t)
		genesis := genesisHeader(t, items)

		mockStp := &subtreeprocessor.MockSubtreeProcessor{}
		injectMockStp(t, items, mockStp)

		// Blockchain best is still genesis (nothing added); BA is already there.
		items.blockAssembler.setBestBlockHeader(genesis, 0)

		items.blockAssembler.processNewBlockAnnouncement(t.Context())

		mockStp.AssertNotCalled(t, "MoveForwardBlock", mock.Anything)
		mockStp.AssertNotCalled(t, "Reorg", mock.Anything, mock.Anything)

		h, _ := items.blockAssembler.CurrentBlock()
		require.Equal(t, genesis.Hash().String(), h.Hash().String(),
			"redundant wake-up must not change the tip")
	})
}
