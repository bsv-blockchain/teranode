package netsync

import (
	"context"
	"math"
	"runtime/debug"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestNewBlockDispatcher_DepthFollowsTheSharedSettingsRule pins legacy sync's effective depth
// to the settings helper block validation resolves its own window depth with. The expected
// numbers are written out rather than derived, so the test is not the code's own oracle; the
// second assertion is what makes a future divergence between the two services fail here.
func TestNewBlockDispatcher_DepthFollowsTheSharedSettingsRule(t *testing.T) {
	cases := []struct {
		name            string
		blocks          int
		skipLock        bool
		maxBlocksBehind int
		expected        int
	}{
		{name: "skip-lock off forces one", blocks: 4, skipLock: false, maxBlocksBehind: 20, expected: 1},
		{name: "capped at half the gate allowance", blocks: 20, skipLock: true, maxBlocksBehind: 20, expected: 10},
		{name: "under the cap the setting stands", blocks: 3, skipLock: true, maxBlocksBehind: 20, expected: 3},
		{name: "zero leaves the dispatcher at one, unused", blocks: 0, skipLock: true, maxBlocksBehind: 20, expected: 1},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := test.CreateBaseTestSettings(t)
			s.BlockValidation.QuickWindowBlocks = tc.blocks
			s.BlockValidation.QuickValidateSkipUtxoLock = tc.skipLock
			s.BlockValidation.MaxBlocksBehindBlockAssembly = tc.maxBlocksBehind

			sm := &SyncManager{logger: ulogger.TestLogger{}, settings: s, ctx: context.Background()}
			bd := newBlockDispatcher(sm)

			require.Equal(t, tc.expected, bd.depth)

			if tc.blocks > 0 {
				fromSettings, _ := s.BlockValidation.QuickWindowConfiguredDepth()
				require.Equal(t, fromSettings, bd.depth, "legacy must run the depth block validation resolves")
			}
		})
	}
}

// TestEffectiveDepth_BlockAssemblyLagArm covers the arm that keeps a window block out of the
// block-assembly gate's retry ladder: the configured depth is reduced to the gate's allowance
// minus block assembly's observed lag minus two, floored at one. The second half proves the
// 250 ms cache, so a per-block admission does not cost a per-block RPC.
func TestEffectiveDepth_BlockAssemblyLagArm(t *testing.T) {
	cases := []struct {
		name     string
		baHeight uint32
		expected int
	}{
		{name: "no lag leaves the configured depth", baHeight: 100, expected: 10},
		{name: "a lag of twelve leaves room for six", baHeight: 88, expected: 6},
		{name: "a lag past the allowance floors at one", baHeight: 70, expected: 1},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := test.CreateBaseTestSettings(t)
			s.BlockValidation.QuickWindowBlocks = 10
			s.BlockValidation.QuickValidateSkipUtxoLock = true
			s.BlockValidation.MaxBlocksBehindBlockAssembly = 20

			ba := blockassembly.NewMock()
			ba.On("GetBlockAssemblyState", mock.Anything).Return(&blockassembly_api.StateMessage{CurrentHeight: tc.baHeight}, nil)

			sm := &SyncManager{logger: ulogger.TestLogger{}, settings: s, ctx: context.Background(), blockAssembly: ba}
			bd := newBlockDispatcher(sm)
			require.Equal(t, 10, bd.depth, "precondition: the configured depth is what the lag arm reduces")

			// The frontier tail is the height the lag is measured from.
			bd.frontier = append(bd.frontier, &frontierEntry{height: 100})

			require.Equal(t, tc.expected, bd.effectiveDepth())

			// Second call inside the cache window: same answer, and block assembly is not
			// asked again.
			require.Equal(t, tc.expected, bd.effectiveDepth())
			ba.AssertNumberOfCalls(t, "GetBlockAssemblyState", 1)
		})
	}
}

// TestWindowBudgetBytes covers the three ways the byte budget is resolved.
func TestWindowBudgetBytes(t *testing.T) {
	t.Run("an operator MiB value wins", func(t *testing.T) {
		require.Equal(t, int64(256)<<20, windowBudgetBytes(256))
	})

	t.Run("with no MiB set it is a tenth of the Go memory limit", func(t *testing.T) {
		previous := debug.SetMemoryLimit(-1)
		t.Cleanup(func() { debug.SetMemoryLimit(previous) })

		debug.SetMemoryLimit(10 << 30)
		require.Equal(t, int64(1)<<30, windowBudgetBytes(0))
	})

	t.Run("with no limit at all it is the fixed fallback", func(t *testing.T) {
		previous := debug.SetMemoryLimit(-1)
		t.Cleanup(func() { debug.SetMemoryLimit(previous) })

		debug.SetMemoryLimit(math.MaxInt64)
		require.Equal(t, int64(defaultWindowBudget), windowBudgetBytes(0))
	})
}

// TestDispatchBlocks_SettingZeroIsATrueBypass proves that at
// blockvalidation_quick_window_blocks=0 dispatchBlocks routes straight to
// consumeBlocksSerially: every commit comes off sm.parkCommits, one block at a
// time, and the dispatcher — which the windowed route alone uses — is never
// touched. There is no decoded block queue left to bypass; the routing itself,
// not a head/work ordering on it, is what this setting now controls (see
// dispatchBlocks, manager.go).
func TestDispatchBlocks_SettingZeroIsATrueBypass(t *testing.T) {
	h := newParkWiringHarness(t, true)
	h.sm.settings.BlockValidation.QuickWindowBlocks = 0
	// New() builds this channel unconditionally; a struct-literal harness needs
	// it wired by hand so submitParkCommit posts to it instead of committing
	// inline, which would make this test pass without ever exercising the
	// routing it is about.
	h.sm.parkCommits = make(chan parkCommit, parkSweepRPCBudget)
	h.sm.quit = make(chan struct{})
	t.Cleanup(func() { close(h.sm.quit) })

	// A dispatcher is wired only so a call into it is observable; the assertion
	// is that this path never makes one.
	h.sm.dispatcher = newBlockDispatcher(h.sm)

	var dispatched atomic.Bool

	h.sm.dispatcher.parkedRun = func(context.Context, *blockDispatch) error {
		dispatched.Store(true)

		return nil
	}

	child := h.blocks[1].MsgBlock().BlockHash()

	require.NoError(t, h.deliver(t, 1))
	require.Equal(t, 1, h.sm.blockPark.Len(), "the child parks behind its missing parent")

	go h.sm.dispatchBlocks()

	// The parent arrives and, with QuickWindowBlocks 0, is committed by
	// consumeBlocksSerially reading sm.parkCommits — see submitParkCommit.
	h.chainHolds(t, h.blocks[0].MsgBlock().Header.PrevBlock)
	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(true, nil)

	require.NoError(t, h.deliver(t, 0))

	// Both blocks leave the park. The parent's own entry is still in the index
	// when the on-disk handler posts it, and consumeBlocksSerially used to commit
	// it directly and leave it there; it now puts it back and drains it, as the
	// windowed consumer does, so nothing stale is left.
	require.True(t, WaitUntil(func() bool { return h.sm.blockPark.Len() == 0 }, 5*time.Second),
		"both blocks must be committed off sm.parkCommits by consumeBlocksSerially, leaving nothing in the park")

	_, failed := h.sm.recentlyFailedBlocks.Get(child)
	require.False(t, failed, "the child was committed, not given up on")

	require.False(t, dispatched.Load(), "the dispatcher is not used when the window is off")
}
