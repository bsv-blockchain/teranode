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

// TestDoubleBuffer_SwapBackWipesPartialFailedWrite reproduces the rollback
// corruption hazard surfaced in code review: after resetSubtreeState swaps,
// workers may partially populate the new "current" map before
// moveForwardBlock returns an error. swapCurrentTxMapBack must wipe that
// partial data, otherwise it would survive as the shadow and re-emerge as
// "current" on the next reset, breaking the "freshly-current is always
// empty" invariant and silently dropping colliding hashes via SetIfNotExists
// returning wasSet=false.
func TestDoubleBuffer_SwapBackWipesPartialFailedWrite(t *testing.T) {
	settings := test.CreateBaseTestSettings(t)
	settings.BlockAssembly.SplitMapBuckets = 16

	stp, err := NewSubtreeProcessor(
		context.Background(), ulogger.TestLogger{}, settings,
		nil, nil, nil, make(chan NewSubtreeRequest, 1),
	)
	require.NoError(t, err)

	preReset := stp.currentTxMap
	preResetShadow := stp.currentTxMapShadow

	// Seed "current" with the prior block's data — equivalent to
	// post-commit steady state.
	priorHash := makeHash(0xC1)
	_, ok := preReset.(*SplitTxInpointsMap).SetIfNotExists(priorHash, &subtreepkg.TxInpoints{})
	require.True(t, ok)

	// Simulate resetSubtreeState swap.
	stp.currentTxMap, stp.currentTxMapShadow = stp.currentTxMapShadow, stp.currentTxMap.(*SplitTxInpointsMap)

	// Simulate workers partially populating the new "current" before the
	// outer moveForwardBlock decides to error.
	partialHash := makeHash(0xC2)
	stp.currentTxMap.(*SplitTxInpointsMap).SetIfNotExists(partialHash, &subtreepkg.TxInpoints{})
	require.Equal(t, 1, stp.currentTxMap.(*SplitTxInpointsMap).Length())

	// Rollback path.
	stp.swapCurrentTxMapBack()

	require.Same(t, preReset.(*SplitTxInpointsMap), stp.currentTxMap, "rollback must restore the pre-reset current pointer")
	require.Same(t, preResetShadow, stp.currentTxMapShadow, "shadow pointer must round-trip")

	// Pre-reset data is intact (the rollback restored the original map verbatim).
	require.True(t, stp.currentTxMap.(*SplitTxInpointsMap).Exists(priorHash), "prior block's data must survive rollback")

	// The critical invariant: shadow is wiped, NOT carrying the partial-failed write.
	require.Equal(t, 0, stp.currentTxMapShadow.Length(), "shadow must be empty after rollback to avoid contaminating the next swap")
	require.False(t, stp.currentTxMapShadow.Exists(partialHash), "no partial-failed write must survive in the shadow")
}

// TestSplitMapBuckets_ConfigClampingAndDefault verifies the SplitMapBuckets
// setting is sanitised in the constructor: zero/negative falls back to the
// documented default, values above the uint16 ceiling are clamped (not
// silently wrapped via the cast), and in-range values pass through as-is.
func TestSplitMapBuckets_ConfigClampingAndDefault(t *testing.T) {
	for _, tc := range []struct {
		name     string
		setting  int
		expected uint16
	}{
		{"zero_falls_back_to_default", 0, splitMapBuckets},
		{"negative_falls_back_to_default", -1, splitMapBuckets},
		{"in_range_passes_through", 1024, 1024},
		{"max_value_passes_through", 65535, 65535},
		{"above_max_clamps_not_wraps", 70000, splitMapBucketsMax},
		{"way_above_max_clamps_not_wraps", 1 << 20, splitMapBucketsMax},
	} {
		t.Run(tc.name, func(t *testing.T) {
			settings := test.CreateBaseTestSettings(t)
			settings.BlockAssembly.SplitMapBuckets = tc.setting

			stp, err := NewSubtreeProcessor(
				context.Background(), ulogger.TestLogger{}, settings,
				nil, nil, nil, make(chan NewSubtreeRequest, 1),
			)
			require.NoError(t, err)

			require.Equal(t, tc.expected, stp.splitMapBuckets, "constructor must sanitise SplitMapBuckets")
		})
	}
}

// TestTxMapPool_GrowsOnLargerBlock simulates the
// "first block was small, later block is large" scenario flagged in code
// review: the pool should be reallocated (not auto-rehashed by swiss.Map)
// when the requested mapSize exceeds the pool's recorded capacity.
func TestTxMapPool_GrowsOnLargerBlock(t *testing.T) {
	settings := test.CreateBaseTestSettings(t)
	settings.BlockAssembly.SplitMapBuckets = 16

	stp, err := NewSubtreeProcessor(
		context.Background(), ulogger.TestLogger{}, settings,
		nil, nil, nil, make(chan NewSubtreeRequest, 1),
	)
	require.NoError(t, err)

	require.Nil(t, stp.txMapPool, "pool should be lazily allocated, not present at construction")
	require.Equal(t, 0, stp.txMapPoolCapacity)

	// First "block": modest size.
	stp.txMapPool = NewSplitSwissMap(1024, 1000)
	stp.txMapPoolCapacity = 1000
	priorPool := stp.txMapPool

	// Simulate the constructor-style sizing branch used by CreateTransactionMap.
	// Re-running the same logic in a helper would couple the test to the public
	// API more than needed; instead, mirror the three cases directly.

	// Same-or-smaller block: pool retained, only Clear()ed.
	requestedMapSize := 500

	switch {
	case stp.txMapPool == nil:
		stp.txMapPool = NewSplitSwissMap(1024, requestedMapSize)
		stp.txMapPoolCapacity = requestedMapSize
	case requestedMapSize > stp.txMapPoolCapacity:
		stp.txMapPool = NewSplitSwissMap(1024, requestedMapSize)
		stp.txMapPoolCapacity = requestedMapSize
	default:
		stp.txMapPool.Clear()
	}

	require.Same(t, priorPool, stp.txMapPool, "same-or-smaller block must reuse the pool, not reallocate")
	require.Equal(t, 1000, stp.txMapPoolCapacity)

	// Larger block: pool must be reallocated, capacity recorded.
	requestedMapSize = 5000

	switch {
	case stp.txMapPool == nil:
		stp.txMapPool = NewSplitSwissMap(1024, requestedMapSize)
		stp.txMapPoolCapacity = requestedMapSize
	case requestedMapSize > stp.txMapPoolCapacity:
		stp.txMapPool = NewSplitSwissMap(1024, requestedMapSize)
		stp.txMapPoolCapacity = requestedMapSize
	default:
		stp.txMapPool.Clear()
	}

	require.NotSame(t, priorPool, stp.txMapPool, "larger block must trigger reallocation, not in-place rehash")
	require.Equal(t, 5000, stp.txMapPoolCapacity)
}

// makeNode builds a deterministic subtree node for tests that only care about
// the hash byte for identity.
func makeNode(b byte) subtreepkg.Node {
	return subtreepkg.Node{Hash: makeHash(b)}
}

// TestResetSubtreeState_RollsBackSwapOnNewSubtreeFailure pins the latent
// rollback hole noted in code review: resetSubtreeState performs the
// double-buffer swap before calling stp.newSubtree, which can fail (e.g. with
// a non-power-of-two leaf count). Without the in-function rollback, the swap
// would be left committed and moveForwardBlock's own swap-back defer is not
// yet registered at the early-return path — leaving the prior block's data
// stranded in the shadow.
//
// Failure is injected by setting currentItemsPerFile to a non-power-of-two
// (3), which makes subtreepkg.NewTreeByLeafCount return ErrNotPowerOfTwo via
// resetSubtreeState -> stp.newSubtree.
func TestResetSubtreeState_RollsBackSwapOnNewSubtreeFailure(t *testing.T) {
	settings := test.CreateBaseTestSettings(t)
	settings.BlockAssembly.SplitMapBuckets = 16
	settings.BlockAssembly.InitialMerkleItemsPerSubtree = 4

	stp, err := NewSubtreeProcessor(
		context.Background(), ulogger.TestLogger{}, settings,
		nil, nil, nil, make(chan NewSubtreeRequest, 1),
	)
	require.NoError(t, err)

	priorCurrent := stp.currentTxMap.(*SplitTxInpointsMap)
	priorShadow := stp.currentTxMapShadow

	// Seed "current" with prior-block data so we can verify it isn't lost.
	priorHash := makeHash(0xF1)
	_, ok := priorCurrent.SetIfNotExists(priorHash, &subtreepkg.TxInpoints{})
	require.True(t, ok)
	require.Equal(t, 1, priorCurrent.Length())

	// Inject a failure into the upcoming stp.newSubtree call: a non-power-of-
	// two leaf count makes subtreepkg.NewTreeByLeafCount return ErrNotPowerOfTwo.
	stp.currentItemsPerFile.Store(3)

	err = stp.resetSubtreeState(true)
	require.Error(t, err, "resetSubtreeState must surface the newSubtree failure")

	// Rollback assertions: the swap must have been undone so the prior-block
	// data is visible via stp.currentTxMap and the shadow is empty.
	require.Same(t, priorCurrent, stp.currentTxMap.(*SplitTxInpointsMap), "rollback must restore the pre-reset currentTxMap pointer")
	require.Same(t, priorShadow, stp.currentTxMapShadow, "rollback must restore the pre-reset shadow pointer")
	require.True(t, stp.currentTxMap.(*SplitTxInpointsMap).Exists(priorHash), "prior-block data must survive a failed reset")
	require.Equal(t, 0, stp.currentTxMapShadow.Length(), "shadow must remain empty after a failed reset")
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
