// Package blockchain — subscription fan-out tests.
//
// These tests pin down the contract for per-subscriber delivery in
// startSubscriptions:
//
//  1. A slow subscriber must not block delivery to other subscribers
//     (head-of-line isolation).
//  2. A subscriber whose per-subscriber buffer overflows must be marked dead
//     and removed from the subscribers map, so it cannot continue blocking
//     producers.
//  3. Per-subscriber delivery order must be preserved (a single subscriber
//     receives notifications in the order they were produced).
//
// Regression context: issue #872 — synchronous fan-out caused one slow Send
// to stall all subscribers, leaving block-assembly without notifications for
// 15+ hours on teratestnet. The visible symptom was the heartbeat warning
// "[Blockchain][broadcastHeartbeat] Notifications channel full, skipping
// heartbeat" coming from the producer side as b.notifications filled.
package blockchain

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/stretchr/testify/require"
)

// slowMockSubscribeServer is a controllable mock of
// blockchain_api.BlockchainAPI_SubscribeServer.
//
// Send blocks on the gate channel if non-nil. Close(gate) to release any
// pending Send (and any future calls).
type slowMockSubscribeServer struct {
	blockchain_api.BlockchainAPI_SubscribeServer
	ctx      context.Context
	cancel   context.CancelFunc
	mu       sync.Mutex
	received []*blockchain_api.Notification
	gate     chan struct{} // nil = pass-through; non-nil = Send blocks until closed
	sendErr  atomic.Value  // stores error to return from Send; nil = success
}

func newSlowMockSubscribeServer() *slowMockSubscribeServer {
	ctx, cancel := context.WithCancel(context.Background())
	return &slowMockSubscribeServer{ctx: ctx, cancel: cancel}
}

func (m *slowMockSubscribeServer) Send(n *blockchain_api.Notification) error {
	if g := m.gate; g != nil {
		select {
		case <-g:
		case <-m.ctx.Done():
			return m.ctx.Err()
		}
	}
	if v := m.sendErr.Load(); v != nil {
		if err, ok := v.(error); ok && err != nil {
			return err
		}
	}
	m.mu.Lock()
	m.received = append(m.received, n)
	m.mu.Unlock()
	return nil
}

func (m *slowMockSubscribeServer) Context() context.Context {
	return m.ctx
}

func (m *slowMockSubscribeServer) Cancel() {
	m.cancel()
}

func (m *slowMockSubscribeServer) Received() []*blockchain_api.Notification {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*blockchain_api.Notification, len(m.received))
	copy(out, m.received)
	return out
}

// waitForSubscriptionManagerReady polls until the server signals readiness,
// then asserts. Bounded by a generous timeout to keep the test deterministic
// on slow CI.
func waitForSubscriptionManagerReady(t *testing.T, b *Blockchain) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if b.subscriptionManagerReady.Load() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("subscription manager did not become ready within 2s")
}

// waitForSubscriberCount polls until the subscribers map reaches want or the
// deadline elapses.
func waitForSubscriberCount(t *testing.T, b *Blockchain, want int, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		b.subscribersMu.RLock()
		n := len(b.subscribers)
		b.subscribersMu.RUnlock()
		if n == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	b.subscribersMu.RLock()
	got := len(b.subscribers)
	b.subscribersMu.RUnlock()
	t.Fatalf("subscriber count: want=%d got=%d within=%s", want, got, within)
}

// TestStartSubscriptions_SlowSubscriberDoesNotBlockFastOnes is the central
// regression test for #872. Two subscribers are registered: one whose Send
// blocks indefinitely (the "slow" sub representing a stalled or backed-up
// gRPC stream), and one whose Send is instant (the "fast" sub representing
// a healthy consumer like block-assembly).
//
// With the previous synchronous fan-out, the slow Send blocked the inner
// loop in startSubscriptions, so the fast subscriber would receive only the
// first notification (and not even that, if the slow one registered first
// because sendInitialNotification was also synchronous). With per-subscriber
// fan-out, the fast subscriber must receive all N notifications regardless
// of the slow one's state.
func TestStartSubscriptions_SlowSubscriberDoesNotBlockFastOnes(t *testing.T) {
	tc := setup(t)
	go tc.server.startSubscriptions()
	waitForSubscriptionManagerReady(t, tc.server)

	slow := newSlowMockSubscribeServer()
	slow.gate = make(chan struct{}) // never close → Send blocks forever
	defer slow.Cancel()

	fast := newSlowMockSubscribeServer()
	defer fast.Cancel()

	// Register slow first to maximise pressure (sendInitialNotification on
	// slow used to block all further subscription handling).
	tc.server.newSubscriptions <- subscriber{
		subscription: slow,
		done:         make(chan struct{}),
		source:       "slow",
		pending:      make(chan *blockchain_api.Notification, subscriberBufferSize),
	}
	tc.server.newSubscriptions <- subscriber{
		subscription: fast,
		done:         make(chan struct{}),
		source:       "fast",
		pending:      make(chan *blockchain_api.Notification, subscriberBufferSize),
	}

	waitForSubscriberCount(t, tc.server, 2, time.Second)

	// Push N notifications via the producer-side channel.
	const N = 5
	for i := 0; i < N; i++ {
		tc.server.notifications <- &blockchain_api.Notification{
			Type: model.NotificationType_Block,
		}
	}

	// Fast subscriber must receive an initial notification + N broadcasts
	// (or just N, depending on whether sendInitialNotification is moved into
	// the per-subscriber path). Assert >= N within a tight deadline.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(fast.Received()) >= N {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	require.GreaterOrEqual(t, len(fast.Received()), N,
		"fast subscriber should receive >=%d notifications despite slow subscriber blocking; got=%d",
		N, len(fast.Received()))
}

// TestStartSubscriptions_FullBufferMarksSubscriberDead verifies that when a
// subscriber's per-subscriber buffer fills (because their Send is blocked),
// the subscriber is marked dead and removed from the subscribers map.
// This bounds how long a stuck subscriber can apply backpressure to the
// system.
func TestStartSubscriptions_FullBufferMarksSubscriberDead(t *testing.T) {
	tc := setup(t)
	go tc.server.startSubscriptions()
	waitForSubscriptionManagerReady(t, tc.server)

	stuck := newSlowMockSubscribeServer()
	stuck.gate = make(chan struct{}) // never close → Send blocks forever
	defer stuck.Cancel()

	tc.server.newSubscriptions <- subscriber{
		subscription: stuck,
		done:         make(chan struct{}),
		source:       "stuck",
		pending:      make(chan *blockchain_api.Notification, subscriberBufferSize),
	}
	waitForSubscriberCount(t, tc.server, 1, time.Second)

	// Push subscriberBufferSize+10 notifications. The first one will be
	// in-flight inside Send (blocked on gate). The next subscriberBufferSize
	// will queue in pending. The rest must trigger the dead-on-full path.
	for i := 0; i < subscriberBufferSize+10; i++ {
		tc.server.notifications <- &blockchain_api.Notification{
			Type: model.NotificationType_Block,
		}
	}

	// The subscriber must be removed from the map within reasonable time.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		tc.server.subscribersMu.RLock()
		n := len(tc.server.subscribers)
		tc.server.subscribersMu.RUnlock()
		if n == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	tc.server.subscribersMu.RLock()
	got := len(tc.server.subscribers)
	tc.server.subscribersMu.RUnlock()
	t.Fatalf("stuck subscriber should have been marked dead; subscribers remaining=%d", got)
}

// extractTestIndex returns the sequence index encoded in a test notification's
// Hash field (first byte), plus true. Returns 0, false for notifications that
// were not produced by the order-preservation test (wrong type, wrong length,
// or non-zero trailing bytes).
func extractTestIndex(n *blockchain_api.Notification) (int, bool) {
	if n.Type != model.NotificationType_Block || len(n.Hash) != 32 {
		return 0, false
	}
	for _, b := range n.Hash[1:] {
		if b != 0 {
			return 0, false
		}
	}
	return int(n.Hash[0]), true
}

// TestStartSubscriptions_PerSubscriberOrderPreserved verifies that for a
// single subscriber, notifications arrive in the order they were produced.
// The fan-out redesign uses one goroutine per subscriber, which preserves
// per-subscriber ordering even if cross-subscriber order is not guaranteed.
func TestStartSubscriptions_PerSubscriberOrderPreserved(t *testing.T) {
	tc := setup(t)
	go tc.server.startSubscriptions()
	waitForSubscriptionManagerReady(t, tc.server)

	sub := newSlowMockSubscribeServer()
	defer sub.Cancel()

	tc.server.newSubscriptions <- subscriber{
		subscription: sub,
		done:         make(chan struct{}),
		source:       "ordered",
		pending:      make(chan *blockchain_api.Notification, subscriberBufferSize),
	}
	waitForSubscriberCount(t, tc.server, 1, time.Second)

	// Send N notifications with distinguishable Hashes (encode the index in
	// the first byte).
	const N = 20
	for i := 0; i < N; i++ {
		hash := make([]byte, 32)
		hash[0] = byte(i)
		tc.server.notifications <- &blockchain_api.Notification{
			Type: model.NotificationType_Block,
			Hash: hash,
		}
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(sub.Received()) >= N {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	received := sub.Received()
	require.GreaterOrEqual(t, len(received), N,
		"subscriber should receive all %d notifications; got=%d", N, len(received))

	// Filter to just the Block notifications we pushed (skip the initial
	// notification that startSubscriptions may have sent).
	var indices []int
	for _, n := range received {
		idx, ok := extractTestIndex(n)
		if ok {
			indices = append(indices, idx)
		}
	}

	require.GreaterOrEqual(t, len(indices), N,
		"expected at least %d of our test notifications; got=%d", N, len(indices))
	for i := 1; i < N; i++ {
		require.Equal(t, indices[i-1]+1, indices[i],
			"per-subscriber order broken at i=%d: prev=%d cur=%d (full=%v)",
			i, indices[i-1], indices[i], indices)
	}
}
