package httpimpl

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// testBestHeader returns a header that survives Hash() (non-nil hash pointers).
func testBestHeaderForCache() *model.BlockHeader {
	return &model.BlockHeader{
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
	}
}

// windowMetas builds BlockHeaderMeta entries for consecutive IDs/heights:
// IDs startID..startID+n-1 at heights startHeight..startHeight+n-1.
func windowMetas(startID, startHeight uint32, n int) []*model.BlockHeaderMeta {
	metas := make([]*model.BlockHeaderMeta, n)
	for i := 0; i < n; i++ {
		metas[i] = &model.BlockHeaderMeta{ID: startID + uint32(i), Height: startHeight + uint32(i)}
	}
	return metas
}

// expectWindow registers the two RPCs a rebuild makes, returning the given metas.
func expectWindow(bcMock *blockchain.Mock, metas []*model.BlockHeaderMeta, once bool) {
	best := bcMock.On("GetBestBlockHeader", mock.Anything).
		Return(testBestHeaderForCache(), &model.BlockHeaderMeta{}, nil)
	headers := bcMock.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).
		Return([]*model.BlockHeader{}, metas, nil)
	if once {
		best.Once()
		headers.Once()
	}
}

// populatedCache builds a cache whose window covers IDs 10..12 at heights
// 100..102 by invoking rebuild directly (no Start, no subscription).
func populatedCache(t *testing.T, bcMock *blockchain.Mock) *mainChainCache {
	t.Helper()
	expectWindow(bcMock, windowMetas(10, 100, 3), true)
	c := newMainChainCache(bcMock, ulogger.TestLogger{}, 3)
	c.rebuild(context.Background())
	require.True(t, c.windowHealthy, "window must be healthy after successful rebuild")
	return c
}

func TestMainChainCache_WindowAuthoritative(t *testing.T) {
	bcMock := &blockchain.Mock{}
	c := populatedCache(t, bcMock)

	// In-window hit: present in the set → true, zero RPC.
	onChain, err := c.IsOnMainChain(context.Background(), 10, 100)
	require.NoError(t, err)
	assert.True(t, onChain)

	// In-window miss: height claims window range but the ID is not in the
	// set → authoritatively NOT on the main chain, zero RPC.
	onChain, err = c.IsOnMainChain(context.Background(), 99, 101)
	require.NoError(t, err)
	assert.False(t, onChain)

	// No CheckBlockIsInCurrentChain expectation was registered: any call would
	// have panicked the mock. Assert explicitly for clarity.
	bcMock.AssertNotCalled(t, "CheckBlockIsInCurrentChain", mock.Anything, mock.Anything)
	bcMock.AssertExpectations(t)
}

func TestMainChainCache_BelowWindowFallbackCached(t *testing.T) {
	bcMock := &blockchain.Mock{}
	c := populatedCache(t, bcMock)

	// Height 50 < windowMinHeight 100 → RPC fallback, exactly once.
	bcMock.On("CheckBlockIsInCurrentChain", mock.Anything, []uint32{7}).Return(true, nil).Once()

	onChain, err := c.IsOnMainChain(context.Background(), 7, 50)
	require.NoError(t, err)
	assert.True(t, onChain)

	// Second call: oldCache hit, no RPC (Once() above fails otherwise).
	onChain, err = c.IsOnMainChain(context.Background(), 7, 50)
	require.NoError(t, err)
	assert.True(t, onChain)

	bcMock.AssertExpectations(t)
}

func TestMainChainCache_BelowWindowNegativeCached(t *testing.T) {
	bcMock := &blockchain.Mock{}
	c := populatedCache(t, bcMock)

	bcMock.On("CheckBlockIsInCurrentChain", mock.Anything, []uint32{8}).Return(false, nil).Once()

	for i := 0; i < 3; i++ {
		onChain, err := c.IsOnMainChain(context.Background(), 8, 50)
		require.NoError(t, err)
		assert.False(t, onChain)
	}

	bcMock.AssertExpectations(t)
}

func TestMainChainCache_BelowWindowRPCErrorNotCached(t *testing.T) {
	bcMock := &blockchain.Mock{}
	c := populatedCache(t, bcMock)

	// Errors must not be cached: both calls hit RPC.
	bcMock.On("CheckBlockIsInCurrentChain", mock.Anything, []uint32{7}).
		Return(false, assert.AnError).Twice()

	_, err := c.IsOnMainChain(context.Background(), 7, 50)
	require.Error(t, err)
	_, err = c.IsOnMainChain(context.Background(), 7, 50)
	require.Error(t, err)

	bcMock.AssertExpectations(t)
}

// TestMainChainCache_BelowWindowFillPanicReturnsErrorToWaiters pins that a panic in
// the shared singleflight fill is recovered and turned into an error for every
// waiter, rather than taking the process down. DoChan re-panics in a fresh
// goroutine for any caller beyond the first when the fill is not guarded, which
// bypasses echo's request-goroutine Recover entirely.
func TestMainChainCache_BelowWindowFillPanicReturnsErrorToWaiters(t *testing.T) {
	bcMock := &blockchain.Mock{}
	c := populatedCache(t, bcMock)

	bcMock.On("CheckBlockIsInCurrentChain", mock.Anything, []uint32{7}).
		Panic("boom").Once()

	require.NotPanics(t, func() {
		_, err := c.IsOnMainChain(context.Background(), 7, 50)
		require.Error(t, err)
	})

	bcMock.AssertExpectations(t)
}

// TestMainChainCache_WriteBackPanicDoesNotLeaveMutexLocked pins a defect in the panic
// recovery itself: the below-window write-back took c.mu.Lock() and released it with a
// plain statement, not a defer. Now that a panic in the fill is recovered instead of
// crashing the process, a panic between Lock and that plain Unlock would leave mu locked
// forever, deadlocking every subsequent lookup instead of taking the process down.
func TestMainChainCache_WriteBackPanicDoesNotLeaveMutexLocked(t *testing.T) {
	bcMock := &blockchain.Mock{}
	c := populatedCache(t, bcMock)

	bcMock.On("CheckBlockIsInCurrentChain", mock.Anything, []uint32{7}).Return(true, nil).Once()

	// Force the write-back's map assignment to panic (assignment to a nil map) while
	// the generation guard would otherwise let it through.
	c.mu.Lock()
	c.oldCache = nil
	c.mu.Unlock()

	_, err := c.IsOnMainChain(context.Background(), 7, 50)
	require.Error(t, err)

	locked := c.mu.TryLock()
	require.True(t, locked, "mu must not stay locked after a panic during the write-back")

	if locked {
		c.mu.Unlock()
	}
}

func TestMainChainCache_AboveWindowFallbackUncached(t *testing.T) {
	bcMock := &blockchain.Mock{}
	c := populatedCache(t, bcMock)

	// Height 200 > windowMaxHeight 102: a freshly mined block the window has
	// not caught up to yet. Direct RPC each time, never cached.
	bcMock.On("CheckBlockIsInCurrentChain", mock.Anything, []uint32{13}).Return(true, nil).Twice()

	for i := 0; i < 2; i++ {
		onChain, err := c.IsOnMainChain(context.Background(), 13, 200)
		require.NoError(t, err)
		assert.True(t, onChain)
	}

	bcMock.AssertExpectations(t)
}

func TestMainChainCache_UnhealthyFallsBackToRPCUncached(t *testing.T) {
	bcMock := &blockchain.Mock{}
	// Unstarted cache: window never populated → every lookup is a direct RPC,
	// regardless of height, and nothing is cached.
	bcMock.On("CheckBlockIsInCurrentChain", mock.Anything, []uint32{42}).Return(true, nil).Twice()

	c := newMainChainCache(bcMock, ulogger.TestLogger{}, 0)

	for i := 0; i < 2; i++ {
		onChain, err := c.IsOnMainChain(context.Background(), 42, 100)
		require.NoError(t, err)
		assert.True(t, onChain)
	}

	bcMock.AssertExpectations(t)
}

func TestMainChainCache_RebuildFailureDegradesToRPC(t *testing.T) {
	bcMock := &blockchain.Mock{}
	c := populatedCache(t, bcMock)

	// Prime oldCache via a below-window lookup.
	bcMock.On("CheckBlockIsInCurrentChain", mock.Anything, []uint32{7}).Return(true, nil).Once()
	_, err := c.IsOnMainChain(context.Background(), 7, 50)
	require.NoError(t, err)

	// Next rebuild fails → window zeroed, oldCache cleared.
	bcMock.On("GetBestBlockHeader", mock.Anything).Return(nil, nil, assert.AnError).Once()
	c.rebuild(context.Background())
	require.False(t, c.windowHealthy)

	// All lookups — even previously cached and previously in-window ones — now
	// go to RPC.
	bcMock.On("CheckBlockIsInCurrentChain", mock.Anything, []uint32{7}).Return(true, nil).Once()
	bcMock.On("CheckBlockIsInCurrentChain", mock.Anything, []uint32{10}).Return(true, nil).Once()

	onChain, err := c.IsOnMainChain(context.Background(), 7, 50)
	require.NoError(t, err)
	assert.True(t, onChain)

	onChain, err = c.IsOnMainChain(context.Background(), 10, 100)
	require.NoError(t, err)
	assert.True(t, onChain)

	bcMock.AssertExpectations(t)
}

func TestMainChainCache_RebuildSwapsWindowAndClearsOldCache(t *testing.T) {
	bcMock := &blockchain.Mock{}
	c := populatedCache(t, bcMock) // window: IDs 10..12 @ heights 100..102

	// Prime oldCache below the window.
	bcMock.On("CheckBlockIsInCurrentChain", mock.Anything, []uint32{7}).Return(true, nil).Once()
	_, err := c.IsOnMainChain(context.Background(), 7, 50)
	require.NoError(t, err)

	// Rebuild with a new window: IDs 20..22 @ heights 101..103 (reorg).
	expectWindow(bcMock, windowMetas(20, 101, 3), true)
	c.rebuild(context.Background())

	// New window is authoritative; old IDs at in-window heights are now false.
	onChain, err := c.IsOnMainChain(context.Background(), 20, 101)
	require.NoError(t, err)
	assert.True(t, onChain)

	onChain, err = c.IsOnMainChain(context.Background(), 11, 101)
	require.NoError(t, err)
	assert.False(t, onChain)

	// oldCache was cleared by the rebuild (deep-reorg safety).
	c.mu.RLock()
	oldLen := len(c.oldCache)
	c.mu.RUnlock()
	assert.Zero(t, oldLen, "oldCache must be cleared on rebuild")

	bcMock.AssertExpectations(t)
}

func TestMainChainCache_GenerationGuardDropsStaleWrite(t *testing.T) {
	bcMock := &blockchain.Mock{}
	c := populatedCache(t, bcMock)

	entered := make(chan struct{})
	release := make(chan struct{})
	bcMock.On("CheckBlockIsInCurrentChain", mock.Anything, []uint32{7}).
		Run(func(mock.Arguments) {
			close(entered)
			<-release
		}).
		Return(true, nil).Once()

	done := make(chan struct{})
	go func() {
		defer close(done)
		onChain, err := c.IsOnMainChain(context.Background(), 7, 50)
		assert.NoError(t, err)
		assert.True(t, onChain)
	}()

	// Wait until the RPC is in flight, then invalidate (generation bump).
	<-entered
	c.invalidate()
	close(release)
	<-done

	// The in-flight result must NOT have been written back: the generation
	// changed while the RPC was in flight.
	c.mu.RLock()
	_, cached := c.oldCache[7]
	c.mu.RUnlock()
	assert.False(t, cached, "stale result must not be cached after invalidation")

	bcMock.AssertExpectations(t)
}

func TestMainChainCache_SingleflightDedup(t *testing.T) {
	bcMock := &blockchain.Mock{}
	c := populatedCache(t, bcMock)

	var calls int32
	release := make(chan struct{})
	bcMock.On("CheckBlockIsInCurrentChain", mock.Anything, []uint32{7}).
		Run(func(mock.Arguments) {
			atomic.AddInt32(&calls, 1)
			<-release
		}).
		Return(true, nil)

	const n = 16
	var wg sync.WaitGroup
	started := make(chan struct{}, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			started <- struct{}{}
			onChain, err := c.IsOnMainChain(context.Background(), 7, 50)
			assert.NoError(t, err)
			assert.True(t, onChain)
		}()
	}
	for i := 0; i < n; i++ {
		<-started
	}
	// Wait for the leader to be in flight, then release everyone. Stragglers
	// either joined the in-flight singleflight call or re-check oldCache after
	// the leader's write-back — never a second RPC.
	require.Eventually(t, func() bool { return atomic.LoadInt32(&calls) >= 1 }, time.Second, time.Millisecond)
	close(release)
	wg.Wait()

	assert.Equal(t, int32(1), atomic.LoadInt32(&calls), "concurrent same-ID lookups must coalesce into one RPC")
}

func TestMainChainCache_StartPopulatesAndRebuildsOnNotification(t *testing.T) {
	notifyCh := make(chan *blockchain_api.Notification, 1)
	bcMock := &blockchain.Mock{}
	bcMock.On("Subscribe", mock.Anything, "asset-mainchain-cache").Return(notifyCh, nil)

	// First rebuild (initial populate): window IDs 10..12 @ 100..102.
	expectWindow(bcMock, windowMetas(10, 100, 3), true)
	// Second rebuild (after notification): window IDs 11..13 @ 101..103.
	expectWindow(bcMock, windowMetas(11, 101, 3), true)

	c := newMainChainCache(bcMock, ulogger.TestLogger{}, 3)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, c.Start(ctx))

	// Initial async populate lands.
	require.Eventually(t, func() bool {
		c.mu.RLock()
		defer c.mu.RUnlock()
		return c.windowHealthy && c.windowMaxHeight == 102
	}, time.Second, time.Millisecond)

	// Block notification → window rebuilt against the new tip.
	notifyCh <- &blockchain_api.Notification{Type: model.NotificationType_Block}
	require.Eventually(t, func() bool {
		c.mu.RLock()
		defer c.mu.RUnlock()
		return c.windowHealthy && c.windowMaxHeight == 103
	}, time.Second, time.Millisecond)

	// The new tip is answered authoritatively from the swapped-in window.
	onChain, err := c.IsOnMainChain(ctx, 13, 103)
	require.NoError(t, err)
	assert.True(t, onChain)

	bcMock.AssertExpectations(t)
}

func TestMainChainCache_NotificationChannelCloseDisablesCache(t *testing.T) {
	notifyCh := make(chan *blockchain_api.Notification)
	bcMock := &blockchain.Mock{}
	bcMock.On("Subscribe", mock.Anything, "asset-mainchain-cache").Return(notifyCh, nil)
	expectWindow(bcMock, windowMetas(10, 100, 3), false)

	c := newMainChainCache(bcMock, ulogger.TestLogger{}, 3)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, c.Start(ctx))

	require.Eventually(t, func() bool {
		c.mu.RLock()
		defer c.mu.RUnlock()
		return c.windowHealthy
	}, time.Second, time.Millisecond)

	// Closing the subscription stops the consumer and disables the cache:
	// without invalidation events the window would go stale silently.
	close(notifyCh)

	select {
	case <-c.consumeDone:
	case <-time.After(time.Second):
		t.Fatal("consume goroutine did not stop after channel close")
	}

	c.mu.RLock()
	healthy := c.windowHealthy
	c.mu.RUnlock()
	assert.False(t, healthy, "cache must be unhealthy after subscription loss")

	// Lookups still work via direct RPC.
	bcMock.On("CheckBlockIsInCurrentChain", mock.Anything, []uint32{10}).Return(true, nil).Once()
	onChain, err := c.IsOnMainChain(ctx, 10, 100)
	require.NoError(t, err)
	assert.True(t, onChain)

	bcMock.AssertExpectations(t)
}

func TestMainChainCache_ConcurrentLookupsSafe(t *testing.T) {
	bcMock := &blockchain.Mock{}
	c := populatedCache(t, bcMock)
	bcMock.On("CheckBlockIsInCurrentChain", mock.Anything, mock.Anything).Return(true, nil).Maybe()

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n uint32) {
			defer wg.Done()
			// Mix of in-window, below-window and above-window heights.
			_, _ = c.IsOnMainChain(context.Background(), 10+n%3, 100+n%3)
			_, _ = c.IsOnMainChain(context.Background(), n%5, 50)
			_, _ = c.IsOnMainChain(context.Background(), 100+n, 200)
		}(uint32(i))
	}
	// Concurrent rebuilds and invalidations must not race with lookups.
	expectWindow(bcMock, windowMetas(10, 100, 3), false)
	c.rebuild(context.Background())
	c.invalidate()
	wg.Wait()
}

// ctxAwareChainMock behaves like a real gRPC client for CheckBlockIsInCurrentChain:
// the call blocks until released and fails if its context is cancelled while it is
// in flight. blockchain.Mock returns its canned value regardless of context, which
// would hide context-ownership bugs.
type ctxAwareChainMock struct {
	*blockchain.Mock

	entered chan struct{}
	release chan struct{}

	calls int32

	mu     sync.Mutex
	rpcCtx context.Context
}

func newCtxAwareChainMock(inner *blockchain.Mock) *ctxAwareChainMock {
	return &ctxAwareChainMock{
		Mock:    inner,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (m *ctxAwareChainMock) CheckBlockIsInCurrentChain(ctx context.Context, _ []uint32) (bool, error) {
	if atomic.AddInt32(&m.calls, 1) == 1 {
		m.mu.Lock()
		m.rpcCtx = ctx
		m.mu.Unlock()
		close(m.entered)
	}

	select {
	case <-m.release:
	case <-ctx.Done():
		return false, ctx.Err()
	}

	if err := ctx.Err(); err != nil {
		return false, err
	}

	return true, nil
}

func (m *ctxAwareChainMock) sharedCtx() context.Context {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.rpcCtx
}

// ctxAwareCache builds a cache whose window covers IDs 10..12 at heights 100..102,
// backed by a context-honouring CheckBlockIsInCurrentChain.
func ctxAwareCache(t *testing.T) (*mainChainCache, *ctxAwareChainMock) {
	t.Helper()

	bcMock := &blockchain.Mock{}
	expectWindow(bcMock, windowMetas(10, 100, 3), true)

	client := newCtxAwareChainMock(bcMock)
	c := newMainChainCache(client, ulogger.TestLogger{}, 3)
	c.rebuild(context.Background())
	require.True(t, c.windowHealthy)

	return c, client
}

// TestMainChainCache_SharedFillUsesServiceContext pins the context boundary: the
// deduplicated below-window RPC is shared by unrelated requests, so it must not run
// on the leader's request context. Cancelling the leader must leave the shared call's
// context alive.
func TestMainChainCache_SharedFillUsesServiceContext(t *testing.T) {
	c, client := ctxAwareCache(t)

	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	defer cancelLeader()

	leaderDone := make(chan struct{})

	go func() {
		defer close(leaderDone)

		_, _ = c.IsOnMainChain(leaderCtx, 7, 50)
	}()

	<-client.entered // the shared RPC is in flight; client.rpcCtx is published

	cancelLeader()

	require.NoError(t, client.sharedCtx().Err(),
		"shared singleflight fill must not run on the leader's request context")

	close(client.release)
	<-leaderDone
}

// TestMainChainCache_LeaderCancelDoesNotFailWaiters is the end-to-end form of the
// same defect: one client disconnecting must not turn every concurrent waiter's
// lookup into an error.
func TestMainChainCache_LeaderCancelDoesNotFailWaiters(t *testing.T) {
	c, client := ctxAwareCache(t)

	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	defer cancelLeader()

	leaderDone := make(chan struct{})

	go func() {
		defer close(leaderDone)

		_, _ = c.IsOnMainChain(leaderCtx, 7, 50)
	}()

	<-client.entered

	type waiterResult struct {
		onChain bool
		err     error
	}

	waiterCh := make(chan waiterResult, 1)

	go func() {
		onChain, err := c.IsOnMainChain(context.Background(), 7, 50)
		waiterCh <- waiterResult{onChain: onChain, err: err}
	}()

	// Let the waiter reach the singleflight join before the leader walks away.
	// The call-count assertion below fails loudly if it did not get there.
	time.Sleep(100 * time.Millisecond)

	cancelLeader()

	// The leader stops waiting immediately; the shared fill keeps running.
	select {
	case <-leaderDone:
	case <-time.After(time.Second):
		t.Fatal("leader did not return after its own context was cancelled")
	}

	close(client.release)

	res := <-waiterCh
	require.NoError(t, res.err, "waiter must not inherit the leader's cancellation")
	require.True(t, res.onChain)

	require.Equal(t, int32(1), atomic.LoadInt32(&client.calls),
		"the waiter must have joined the shared fill, not issued a second RPC")
}

// TestMainChainCache_ServiceContextCancelledOnShutdown pins that the service-owned
// context used for shared fills dies with the service, so a shutdown cannot leave a
// deduplicated RPC running forever.
func TestMainChainCache_ServiceContextCancelledOnShutdown(t *testing.T) {
	notifyCh := make(chan *blockchain_api.Notification)
	bcMock := &blockchain.Mock{}
	bcMock.On("Subscribe", mock.Anything, "asset-mainchain-cache").Return(notifyCh, nil)
	expectWindow(bcMock, windowMetas(10, 100, 3), false)

	c := newMainChainCache(bcMock, ulogger.TestLogger{}, 3)

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, c.Start(ctx))

	require.Eventually(t, func() bool {
		c.mu.RLock()
		defer c.mu.RUnlock()

		return c.windowHealthy
	}, time.Second, time.Millisecond)

	require.NoError(t, c.sfCtx.Err(), "service context must be live while the cache runs")

	cancel()

	select {
	case <-c.consumeDone:
	case <-time.After(time.Second):
		t.Fatal("consume goroutine did not stop after shutdown")
	}

	select {
	case <-c.sfCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("service-owned context was not cancelled on shutdown")
	}
}
