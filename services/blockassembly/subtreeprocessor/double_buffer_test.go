package subtreeprocessor

import (
	"context"
	"testing"

	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestDoubleBuffer_SwapAndClearLifecycle exercises the swap-on-reset /
// swap-back-on-rollback / clear-on-commit invariants used by the in-memory
// currentTxMap pool. We drive the helpers directly to avoid pulling in
// goroutine-driven moveForwardBlock plumbing.
func TestDoubleBuffer_SwapAndClearLifecycle(t *testing.T) {
	settings := test.CreateBaseTestSettings(t)
	settings.BlockAssembly.SplitMapBuckets = 16

	stp, err := NewSubtreeProcessor(
		context.Background(), ulogger.TestLogger{}, settings,
		nil, nil, nil, make(chan NewSubtreeRequest, 1),
	)
	require.NoError(t, err)

	require.NotNil(t, stp.currentTxMapShadow, "in-memory mode must pre-allocate the shadow")

	originalCurrent := stp.currentTxMap
	originalShadow := stp.currentTxMapShadow

	// Seed the original map so we can detect the swap by content as well as
	// pointer identity.
	hash := makeHash(0xAA)
	_, ok := originalCurrent.(*SplitTxInpointsMap).SetIfNotExists(hash, &subtreepkg.TxInpoints{})
	require.True(t, ok)
	require.Equal(t, 1, originalCurrent.(*SplitTxInpointsMap).Length())
	require.Equal(t, 0, originalShadow.Length())

	// Simulate the swap that resetSubtreeState performs on the in-memory path.
	stp.currentTxMap, stp.currentTxMapShadow = stp.currentTxMapShadow, stp.currentTxMap.(*SplitTxInpointsMap)
	require.Equal(t, 0, stp.currentTxMap.(*SplitTxInpointsMap).Length(), "freshly-current map must be empty")
	require.Equal(t, 1, stp.currentTxMapShadow.Length(), "shadow must hold the pre-reset data")

	// Rollback path: swap-back must restore the original assignment exactly.
	stp.swapCurrentTxMapBack()
	require.Same(t, originalCurrent.(*SplitTxInpointsMap), stp.currentTxMap, "swap-back must restore original currentTxMap pointer")
	require.Same(t, originalShadow, stp.currentTxMapShadow, "swap-back must restore original shadow pointer")
	require.Equal(t, 1, stp.currentTxMap.(*SplitTxInpointsMap).Length(), "rollback must preserve pre-reset content")

	// Commit path simulation: swap, then clear shadow. After clear the shadow
	// must be empty and ready for the next reset's swap.
	stp.currentTxMap, stp.currentTxMapShadow = stp.currentTxMapShadow, stp.currentTxMap.(*SplitTxInpointsMap)
	stp.clearCurrentTxMapShadow()
	require.Equal(t, 0, stp.currentTxMapShadow.Length(), "commit must leave shadow empty")
}

// TestDoubleBuffer_DisableViaFlag verifies that setting
// disableCurrentTxMapPool causes swapCurrentTxMapBack and
// clearCurrentTxMapShadow to no-op, matching the reorgBlocks rollback
// contract.
func TestDoubleBuffer_DisableViaFlag(t *testing.T) {
	settings := test.CreateBaseTestSettings(t)
	settings.BlockAssembly.SplitMapBuckets = 16

	stp, err := NewSubtreeProcessor(
		context.Background(), ulogger.TestLogger{}, settings,
		nil, nil, nil, make(chan NewSubtreeRequest, 1),
	)
	require.NoError(t, err)

	originalCurrent := stp.currentTxMap
	originalShadow := stp.currentTxMapShadow

	// Mark the shadow so we can confirm the no-op clear leaves it alone.
	originalShadow.SetIfNotExists(makeHash(0xBB), &subtreepkg.TxInpoints{})
	require.Equal(t, 1, originalShadow.Length())

	stp.disableCurrentTxMapPool = true

	stp.swapCurrentTxMapBack()
	require.Same(t, originalCurrent.(*SplitTxInpointsMap), stp.currentTxMap, "swap-back must be a no-op when pool is disabled")
	require.Same(t, originalShadow, stp.currentTxMapShadow)

	stp.clearCurrentTxMapShadow()
	require.Equal(t, 1, stp.currentTxMapShadow.Length(), "clear must be a no-op when pool is disabled")
}
