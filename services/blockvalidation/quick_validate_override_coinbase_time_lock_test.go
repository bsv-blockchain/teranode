//go:build bvgaps

package blockvalidation

import (
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/stretchr/testify/require"
)

// Quick validation trusts blocks at or below the catch-up checkpoint and skips most body checks.
// For the hardcoded checkpoints that is safe: the network accepted those blocks long ago
// (BV-QUICK-030). An operator can move the checkpoint higher with the catch-up checkpoint override
// (blockvalidation_catchup_checkpoint_hash / _height). The blocks it covers are then recent and
// were never vetted by anyone, so BV-QUICK-031 says coinbase finality SHOULD still be enforced on
// them.
//
// checkQuickValidationCoinbase calls model.CoinbaseFinalityViolation with a parent median time past
// of 0. From CSV onwards that makes it skip every time-based coinbase lock, so a coinbase that
// bitcoin-sv rejects as bad-txns-nonfinal (ContextualCheckBlock -> IsFinalTx against the parent's
// median time past) is accepted by the quick route.

// quickValidateUnderCheckpointOverride sets the operator's catch-up checkpoint override to the
// candidate itself, so the candidate is a block the override puts on the quick route, and runs it
// through the quick route.
func (chain *coinbaseFinalityChain) quickValidateUnderCheckpointOverride(block *model.Block) error {
	chain.t.Helper()

	chain.bv.settings.BlockValidation.CatchupCheckpointHash = block.Hash().String()
	chain.bv.settings.BlockValidation.CatchupCheckpointHeight = int32(block.Height) //nolint:gosec // small fixture height

	return chain.bv.quickValidateBlock(chain.ctx, block, "peer", coinbaseFinalityPeerURL)
}

// requireAboveHardcodedCheckpoints asserts the fixture precondition that no hardcoded checkpoint
// covers the candidate, so only the operator override can put it on the quick route.
func (chain *coinbaseFinalityChain) requireAboveHardcodedCheckpoints() {
	chain.t.Helper()

	for _, checkpoint := range chain.bv.settings.ChainCfgParams.Checkpoints {
		require.Less(chain.t, uint32(checkpoint.Height), chain.height, //nolint:gosec // regtest heights
			"fixture precondition: the candidate must be above every hardcoded checkpoint")
	}
}

// requireQuickRouteRejectedAsNonFinal asserts the quick route rejected the block ERR_BLOCK_INVALID
// with bitcoin-sv's bad-txns-nonfinal and the block did not become the tip.
func (chain *coinbaseFinalityChain) requireQuickRouteRejectedAsNonFinal(block *model.Block, err error, msgAndArgs ...any) {
	chain.t.Helper()

	require.Error(chain.t, err, msgAndArgs...)
	require.True(chain.t, errors.Is(err, errors.ErrBlockInvalid), "must be ERR_BLOCK_INVALID on a bound body, got: %v", err)
	require.Contains(chain.t, err.Error(), "bad-txns-nonfinal")

	best, _, bestErr := chain.client.GetBestBlockHeader(chain.ctx)
	require.NoError(chain.t, bestErr)
	require.Equal(chain.t, chain.tip.Hash().String(), best.Hash().String(), "a rejected block must not become the tip")
}

// requireQuickRouteAcceptedAsNewTip asserts the quick route committed the block as the new tip.
func (chain *coinbaseFinalityChain) requireQuickRouteAcceptedAsNewTip(block *model.Block, err error) {
	chain.t.Helper()

	require.NoError(chain.t, err)

	best, _, bestErr := chain.client.GetBestBlockHeader(chain.ctx)
	require.NoError(chain.t, bestErr)
	require.Equal(chain.t, block.Hash().String(), best.Hash().String(), "an accepted block extends the chain")
}

// TestKnownDefect_QuickValidationAboveCheckpointOverride_RejectsNonFinalCoinbaseTimeLock shows the
// quick route accepting a coinbase whose time lock has not passed, on a block that only the
// operator's checkpoint override put on that route.
//
// Spec: BV-QUICK-031, BV-BODY-019 and BV-BODY-015. Tracked as GAP-BV-062.
//
// Scenario: Given a regtest chain of genesis plus 5 stored blocks 600s apart, CSV active from
// height 1, and a candidate at height 6 stamped after them. Regtest has no hardcoded checkpoints;
// the operator's catch-up checkpoint override is set to the candidate. The candidate's coinbase has
// input sequence 0 and a time-based nLockTime halfway between the parent's median time past and
// the candidate's header time. bitcoin-sv measures a post-CSV time lock against the parent's median
// time past, so that coinbase is not final. When the quick route validates the candidate, Then it
// must reject it ERR_BLOCK_INVALID with bad-txns-nonfinal and keep the old tip.
//
// Today the quick route passes a median time past of 0, skips the time lock and commits the block.
//
// Controls, on the same chain and fixture:
//   - full validation rejects the same block as bad-txns-nonfinal, so the rule applies to it;
//   - the quick route rejects a height-based coinbase lock at the same height, so the quick
//     route's finality check does run here;
//   - the quick route commits a coinbase locked one second below the parent's median time past,
//     so the route can accept a post-CSV block on this fixture and the fix has a passing case.
func TestKnownDefect_QuickValidationAboveCheckpointOverride_RejectsNonFinalCoinbaseTimeLock(t *testing.T) {
	const (
		csvActiveFromHeight1 = uint32(1)
		nonFinalSequence     = uint32(0)
	)

	t.Run("time lock after the parent's median time past is rejected as non-final", func(t *testing.T) {
		// Given
		chain := newCoinbaseFinalityChain(t, csvActiveFromHeight1)
		chain.requireAboveHardcodedCheckpoints()

		lockedUntilAfterParentMTP := chain.timeBetweenParentMTPAndHeaderTime()
		block := chain.candidateWithCoinbaseLock(lockedUntilAfterParentMTP, nonFinalSequence)

		require.Equal(t, "bad-txns-nonfinal",
			model.CoinbaseFinalityViolation(block.CoinbaseTx, block.Header.Timestamp, block.Height, chain.bv.settings.ChainCfgParams, chain.parentMTP),
			"fixture precondition: measured against the parent's median time past, as bitcoin-sv does, the coinbase is not final")

		// When
		err := chain.quickValidateUnderCheckpointOverride(block)

		// Then
		chain.requireQuickRouteRejectedAsNonFinal(block, err,
			"BV-QUICK-031: coinbase nLockTime %d is not before the parent's median time past %d at post-CSV height %d, a block only the checkpoint override put on the quick route; it must be rejected as non-final",
			lockedUntilAfterParentMTP, chain.parentMTP, chain.height)
	})

	t.Run("control: full validation rejects the same block as non-final", func(t *testing.T) {
		// Given
		chain := newCoinbaseFinalityChain(t, csvActiveFromHeight1)
		block := chain.candidateWithCoinbaseLock(chain.timeBetweenParentMTPAndHeaderTime(), nonFinalSequence)

		// When
		err := chain.validateThroughService(block)

		// Then
		chain.requireRejectedAsNonFinal(block, err)
	})

	t.Run("control: the quick route rejects a height lock at the same height", func(t *testing.T) {
		// Given
		chain := newCoinbaseFinalityChain(t, csvActiveFromHeight1)
		chain.requireAboveHardcodedCheckpoints()

		lockedUntilOwnHeight := chain.height
		block := chain.candidateWithCoinbaseLock(lockedUntilOwnHeight, nonFinalSequence)

		// When
		err := chain.quickValidateUnderCheckpointOverride(block)

		// Then
		chain.requireQuickRouteRejectedAsNonFinal(block, err, "a height lock equal to the block's own height is not final")
	})

	t.Run("control: the quick route commits a time lock just below the parent's median time past", func(t *testing.T) {
		// Given
		chain := newCoinbaseFinalityChain(t, csvActiveFromHeight1)
		chain.requireAboveHardcodedCheckpoints()

		lockedUntilJustBeforeParentMTP := chain.parentMTP - 1
		block := chain.candidateWithCoinbaseLock(lockedUntilJustBeforeParentMTP, nonFinalSequence)

		// When
		err := chain.quickValidateUnderCheckpointOverride(block)

		// Then
		chain.requireQuickRouteAcceptedAsNewTip(block, err)
	})
}
