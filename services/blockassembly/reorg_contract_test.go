// Package blockassembly — adversarial tests for the Chain-Tip and Reorg Contract.
//
// These tests target BA-REORG-NNN in services/blockassembly/spec.md and focus on
// scenarios the existing suite (block_assembler_catchup_test.go) does not cover.
// They are written to break the orchestration layer in BlockAssembler.go against
// the spec, not to re-test the subtree-processor internals (BA-REORG-010/011),
// which are covered under subtreeprocessor/.
//
// Coverage map:
//   - TestHandleReorg_DeepReorgResetNeedsHeightAboveThousand  -> BA-REORG-006 (height term)
//   - TestHandleReorg_ResetsWhenDepthReachesCoinbaseMaturity -> BA-REORG-006 (depth boundary)
//   - TestProcessNewBlockAnnouncement_FailureLeavesRunningAndTip -> BA-REORG-007, BA-REORG-008, BA-STATE-002
//     (best-tip read, linear move-up, catch-up, and competing-fork reorg + fallback reset failures)
//   - TestProcessNewBlockAnnouncement_RepeatedWakeUpsAreIdempotent -> BA-REORG-009
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
// expectation.
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

// TestHandleReorg_DeepReorgResetNeedsHeightAboveThousand covers the negative branch
// of the maturity rule in BA-REORG-006: a full reset on depth >= coinbase maturity
// also requires the current chain height to be greater than 1000. Here the depth
// term is satisfied (CoinbaseMaturity=1, moveBack=1) but the height term is not, so
// handleReorg MUST reorg in place, never reset. The decision itself is unit-tested by
// TestReorgNeedsReset; this drives it through handleReorg.
func TestHandleReorg_DeepReorgResetNeedsHeightAboveThousand(t *testing.T) {
	initPrometheusMetrics()

	t.Run("deep depth but height 999 (< 1000) reorgs in place", func(t *testing.T) {
		mockStp, err := runReorgDecision(t, 999, 1)
		assertReorgedInPlace(t, mockStp, err)
	})

	// The spec says "greater than 1000"; at exactly 1000 the gate must NOT fire.
	t.Run("deep depth but height 1000 (==, not >) reorgs in place", func(t *testing.T) {
		mockStp, err := runReorgDecision(t, 1000, 1)
		assertReorgedInPlace(t, mockStp, err)
	})
}

// TestHandleReorg_ResetsWhenDepthReachesCoinbaseMaturity pins the depth boundary of
// BA-REORG-006 through handleReorg: above height 1000, a reorg whose deeper side is
// at least the coinbase-maturity threshold MUST take the full-reset path, and one
// below it MUST reorg in place.
func TestHandleReorg_ResetsWhenDepthReachesCoinbaseMaturity(t *testing.T) {
	initPrometheusMetrics()

	t.Run("depth below maturity reorgs in place", func(t *testing.T) {
		mockStp, err := runReorgDecision(t, 1001, 2)
		assertReorgedInPlace(t, mockStp, err)
	})

	// The spec says "at least the coinbase-maturity threshold", so depth == maturity
	// (moveBack=1, CoinbaseMaturity=1) at height 1001 resets.
	t.Run("depth equal to maturity resets", func(t *testing.T) {
		mockStp, err := runReorgDecision(t, 1001, 1)
		// moveToBlock branches on this error, so pin it rather than drop it.
		require.ErrorIs(t, err, errors.ErrBlockAssemblyReset)
		mockStp.AssertCalled(t, "Reset", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
		mockStp.AssertNotCalled(t, "Reorg", mock.Anything, mock.Anything)
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

// TestProcessNewBlockAnnouncement_FailureLeavesRunningAndTip drives processNewBlockAnnouncement into
// five failures and asserts each leaves the service Running (BA-REORG-008) with the
// best-block reference untouched (BA-REORG-007): a failed best-tip read, a failed block
// fetch and a failed move-forward on a linear move-up, a failed catch-up, and a
// competing-fork reorg whose fallback reset fails too. The existing "catch-up mid-error"
// test checks the tip but never asserts the operational state.
func TestProcessNewBlockAnnouncement_FailureLeavesRunningAndTip(t *testing.T) {
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
		mockBC.On("SetState", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		items.blockAssembler.blockchainClient = mockBC

		items.blockAssembler.processNewBlockAnnouncement(t.Context())

		assertRunningAndTipUnchanged(t, items.blockAssembler, genesis, 0)
		mockBC.AssertCalled(t, "GetBlock", mock.Anything, mock.Anything)
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

	// Competing fork (moveBack=1): the only case here that enters the reorg state. The
	// in-place Reorg fails, handleReorg falls back to a full reset, and the reset fails
	// too. Block assembly must stay on its own tip a1 and return to Running.
	t.Run("Reorg and fallback Reset both fail on a competing fork", func(t *testing.T) {
		items := setupBlockAssemblyTest(t)
		genesis := genesisHeader(t, items)

		// genesis->a1 is block assembly's chain; genesis->b1->b2 is longer and wins.
		a1 := buildChain(genesis, 1, 900)[0]
		addChain(t, items, []*model.BlockHeader{a1})
		addChain(t, items, buildChain(genesis, 2, 950))

		mockStp := &subtreeprocessor.MockSubtreeProcessor{}
		mockStp.On("Reorg", mock.Anything, mock.Anything).
			Return(errors.NewProcessingError("reorg failed"))
		mockStp.On("WaitForPendingBlocks", mock.Anything).Return(nil)
		mockStp.On("Reset", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(subtreeprocessor.ResetResponse{Err: errors.NewProcessingError("reset failed")})
		// The failed reset leaves the subtree processor where it was, on a1.
		mockStp.On("GetCurrentBlockHeader").Return(a1)
		injectMockStp(t, items, mockStp)

		items.blockAssembler.setBestBlockHeader(a1, 1)

		items.blockAssembler.processNewBlockAnnouncement(t.Context())

		mockStp.AssertCalled(t, "Reorg", mock.Anything, mock.Anything)
		mockStp.AssertCalled(t, "Reset", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
		assertRunningAndTipUnchanged(t, items.blockAssembler, a1, 1)
	})
}

// TestProcessNewBlockAnnouncement_RepeatedWakeUpsAreIdempotent: repeated wake-ups apply a
// move once, through the best-tip replay (BA-REORG-009).
func TestProcessNewBlockAnnouncement_RepeatedWakeUpsAreIdempotent(t *testing.T) {
	initPrometheusMetrics()

	// Firing the reconcile three times for a single-block advance must apply the
	// move-forward exactly once; later passes find nothing left to replay and no-op.
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

	// BA already at the chain's best tip: a repeated wake-up is a redundant no-op.
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
