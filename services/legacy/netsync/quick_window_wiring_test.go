package netsync

import (
	"context"
	"math"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockvalidation"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/services/legacy/testdata"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
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

// serialProbeClient records when a block's head ran (the FSM state call, which only
// handleBlockMsgHead makes) and when its own work started (the block-exists call, which is the
// first thing HandleBlockDirect does), and parks the work until the test releases it. Returning
// true from GetBlockExists makes HandleBlockDirect return immediately after that, so each block
// is one head event and one work event.
type serialProbeClient struct {
	*blockchain.Mock

	mu     sync.Mutex
	events []string
	gate   chan struct{}
}

func (c *serialProbeClient) record(what string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.events = append(c.events, what)
}

func (c *serialProbeClient) seen() []string {
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]string(nil), c.events...)
}

func (c *serialProbeClient) GetFSMCurrentState(_ context.Context) (*blockchain.FSMStateType, error) {
	c.record("head")

	state := blockchain.FSMStateCATCHINGBLOCKS

	return &state, nil
}

func (c *serialProbeClient) GetBlockExists(_ context.Context, _ *chainhash.Hash) (bool, error) {
	c.record("work")
	<-c.gate

	return true, nil
}

// TestDispatchBlocks_SettingZeroIsATrueBypass proves that at
// blockvalidation_quick_window_blocks=0 the consumer is the pre-window one: block N+1's head
// (the FSM state call, the requestedBlocks and headerList bookkeeping, the size sampling, the
// cascade marks) does not run until block N's work and tail have finished. At depth 1 the
// dispatcher would have split those apart and run N+1's head alongside N's work, which is why
// 0 has to bypass the dispatcher rather than configure it.
func TestDispatchBlocks_SettingZeroIsATrueBypass(t *testing.T) {
	h := newLoopHarness(t, 2)
	h.sm.settings.BlockValidation.QuickWindowBlocks = 0

	probe := &serialProbeClient{Mock: h.client, gate: make(chan struct{})}
	h.sm.blockchainClient = probe

	t.Cleanup(func() {
		select {
		case <-probe.gate:
		default:
			close(probe.gate)
		}
	})

	// Nothing may reach the dispatcher on this path.
	var dispatched atomic.Bool

	h.sm.dispatcher.run = func(context.Context, *blockDispatch, *inflightParent) error {
		dispatched.Store(true)

		return nil
	}

	go h.sm.dispatchBlocks(h.queue)

	first := h.enqueue(0)
	second := h.enqueue(1)

	// The first block is parked inside its own work.
	require.Eventually(t, func() bool { return len(probe.seen()) == 2 }, 5*time.Second, 5*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, []string{"head", "work"}, probe.seen(), "the second block's head must not run while the first is in flight")

	close(probe.gate)

	require.NoError(t, requireReply(t, first, "the first block"))
	require.NoError(t, requireReply(t, second, "the second block"))

	require.Equal(t, []string{"head", "work", "head", "work"}, probe.seen(), "blocks run strictly one at a time, head to tail")
	require.False(t, dispatched.Load(), "the dispatcher is not used when the window is off")
	require.True(t, h.sm.dispatcher.frontierEmpty())
	require.Equal(t, int64(0), h.sm.blockBacklog.Load())
}

// processBlockRecorder counts ProcessBlock calls so the hand-shake tests can tell "the child
// went ahead" from "the child short-circuited".
type processBlockRecorder struct {
	*blockvalidation.MockBlockValidation

	called atomic.Int32
}

func (m *processBlockRecorder) ProcessBlock(_ context.Context, _ *model.Block, _ uint32, _, _ string, _ uint32) error {
	m.called.Add(1)

	return nil
}

// newHandShakeHarness is the minimum HandleBlockDirect needs to reach its ordering hand-shake
// with a real block: the fixture arrives with its height unset and the resolved parent puts it
// at 100, which is the height the subtree-validation mock is scripted for, so a wrong height
// fails as an unexpected call rather than passing silently.
func newHandShakeHarness(t *testing.T) (*SyncManager, *processBlockRecorder, *bsvutil.Block) {
	t.Helper()

	initPrometheusMetrics()

	block, err := testdata.ReadBlockFromFile("../testdata/00000000000000000ad4cd15bbeaf6cb4583c93e13e311f9774194aadea87386.bin")
	require.NoError(t, err)
	require.LessOrEqual(t, block.Height(), int32(0), "fixture must arrive with height unset, like a real wire block")

	blockchainClient := &blockchain.Mock{}
	blockchainClient.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)
	blockchainClient.On("GetBlockIsMined", mock.Anything, mock.Anything).Return(true, nil)

	blockAssembly := blockassembly.NewMock()
	blockAssembly.On("GetBlockAssemblyState", mock.Anything).Return(&blockassembly_api.StateMessage{CurrentHeight: 100}, nil)

	subtreeValidationClient := &subtreevalidation.MockSubtreeValidation{}
	subtreeValidationClient.On("CheckSubtreeFromBlock", mock.Anything, mock.Anything, "legacy", uint32(100), mock.Anything, mock.Anything).Return(nil)

	recorder := &processBlockRecorder{MockBlockValidation: &blockvalidation.MockBlockValidation{}}

	sm := &SyncManager{
		settings:          test.CreateBaseTestSettings(t),
		logger:            ulogger.TestLogger{},
		orphanTxs:         expiringmap.New[chainhash.Hash, *orphanTxAndParents](10),
		blockchainClient:  blockchainClient,
		blockAssembly:     blockAssembly,
		utxoStore:         &nullstore.NullStore{},
		subtreeStore:      memory.New(),
		subtreeValidation: subtreeValidationClient,
		blockValidation:   recorder,
	}
	t.Cleanup(sm.orphanTxs.Stop)

	return sm, recorder, block
}

// TestHandleBlockDirect_OrderingHandShake drives the wait immediately before ProcessBlock with
// live channels, in the three states a resolved in-flight parent can be in.
func TestHandleBlockDirect_OrderingHandShake(t *testing.T) {
	t.Run("a parent that started its RPC lets the child through", func(t *testing.T) {
		sm, recorder, block := newHandShakeHarness(t)

		parent := &frontierEntry{rpcStarted: make(chan struct{}), settled: make(chan struct{})}
		close(parent.rpcStarted)

		own := &frontierEntry{rpcStarted: make(chan struct{}), settled: make(chan struct{})}
		ctx := contextWithFrontierEntry(context.Background(), own)

		err := sm.HandleBlockDirect(ctx, &peer.Peer{}, *block.Hash(), block.MsgBlock(), &inflightParent{height: 99, entry: parent})
		require.NoError(t, err)
		require.Equal(t, int32(1), recorder.called.Load(), "the child must reach its own RPC")

		select {
		case <-own.rpcStarted:
		default:
			t.Fatal("the child must mark its own entry started before its RPC, or its successor waits forever")
		}
	})

	t.Run("a parent that failed before its RPC short-circuits the child", func(t *testing.T) {
		sm, recorder, block := newHandShakeHarness(t)

		parent := &frontierEntry{rpcStarted: make(chan struct{}), settled: make(chan struct{})}
		parent.settle(errors.NewProcessingError("the parent broke"))

		own := &frontierEntry{rpcStarted: make(chan struct{}), settled: make(chan struct{})}
		ctx := contextWithFrontierEntry(context.Background(), own)

		err := sm.HandleBlockDirect(ctx, &peer.Peer{}, *block.Hash(), block.MsgBlock(), &inflightParent{height: 99, entry: parent})
		require.Error(t, err)
		require.True(t, errors.IsTransientLocalError(err), "a predecessor's failure is our fault, not the peer's: %v", err)
		require.Equal(t, int32(0), recorder.called.Load(), "the child must never start its own RPC")
	})

	t.Run("a parent that started and then failed still lets the child through", func(t *testing.T) {
		sm, recorder, block := newHandShakeHarness(t)

		parent := &frontierEntry{rpcStarted: make(chan struct{}), settled: make(chan struct{})}
		parent.markRPCStarted()
		parent.settle(errors.NewProcessingError("the parent broke after starting"))

		own := &frontierEntry{rpcStarted: make(chan struct{}), settled: make(chan struct{})}
		ctx := contextWithFrontierEntry(context.Background(), own)

		// Both arms are ready, and rpcStarted must win every time: the server-side window is
		// what aborts this block, with the predecessor's recorded error.
		err := sm.HandleBlockDirect(ctx, &peer.Peer{}, *block.Hash(), block.MsgBlock(), &inflightParent{height: 99, entry: parent})
		require.NoError(t, err)
		require.Equal(t, int32(1), recorder.called.Load(), "rpcStarted wins over a settled failure")
	})
}
