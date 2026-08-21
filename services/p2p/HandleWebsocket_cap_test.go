// Package p2p provides peer-to-peer networking functionality for the Teranode system.
package p2p

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

// newTestWebSocketServer wires the real /p2p-ws handler onto an httptest
// server and hands back its connection limiter, so a test can observe
// whether a connection's cap slot is actually released.
func newTestWebSocketServer(t *testing.T, maxConns, maxConnsPerIP int) (*httptest.Server, chan *notificationMsg, *wsConnLimiter) {
	t.Helper()

	s := &Server{
		gCtx:   t.Context(),
		logger: &ulogger.TestLogger{},
		settings: &settings.Settings{
			P2P: settings.P2PSettings{
				ListenMode:            settings.ListenModeFull,
				EnableNAT:             false,
				WSMaxConnections:      maxConns,
				WSMaxConnectionsPerIP: maxConnsPerIP,
			},
		},
	}

	notificationCh := make(chan *notificationMsg, 16)
	handler, limiter := s.handleWebSocket(notificationCh)

	e := echo.New()
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = handler(e.NewContext(r, w))
	}))
	t.Cleanup(httpServer.Close)

	return httpServer, notificationCh, limiter
}

func waitForSlots(t *testing.T, limiter *wsConnLimiter, want int, timeout time.Duration, msg string) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if limiter.count() == want {
			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("%s: connection limiter holds %d slots, want %d", msg, limiter.count(), want)
}

// TestHandleWebSocket_ClosedConnectionFreesCapSlot verifies the happy path of
// the connection cap: a client that disconnects normally gives its slot back,
// so the endpoint stays usable.
func TestHandleWebSocket_ClosedConnectionFreesCapSlot(t *testing.T) {
	httpServer, _, limiter := newTestWebSocketServer(t, 1, 1)

	wsURL := "ws" + strings.TrimPrefix(httpServer.URL, "http")

	ws, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	require.NoError(t, err)

	waitForSlots(t, limiter, 1, 5*time.Second, "after connecting")

	// The cap is 1, so a second connection must be refused while the first
	// is open.
	_, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
	require.Error(t, err, "second connection must be rejected while the cap is full")
	require.NotNil(t, resp)
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	_ = resp.Body.Close()

	require.NoError(t, ws.Close())

	waitForSlots(t, limiter, 0, 5*time.Second, "after the client closed")

	ws2, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	require.NoError(t, err, "the freed slot must be reusable")

	_ = ws2.Close()
}

// TestHandleWebSocket_StalledClientFreesCapSlot is the regression test for the
// one-way ratchet: a client that stops reading is dropped from the broadcast
// set after the send timeout, and that drop must also tear the connection
// down. Before the fix, removal touched only the broadcast set - nothing ever
// sent on the client's channel again, its writer goroutine parked forever and
// the cap slot was held for the life of the process, so a handful of silent
// clients could 503 the endpoint permanently.
func TestHandleWebSocket_StalledClientFreesCapSlot(t *testing.T) {
	httpServer, notificationCh, limiter := newTestWebSocketServer(t, 1, 1)

	wsURL := "ws" + strings.TrimPrefix(httpServer.URL, "http")

	// Dial and then never read a single frame.
	ws, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	require.NoError(t, err)

	defer ws.Close()

	waitForSlots(t, limiter, 1, 5*time.Second, "after connecting")

	// Push enough large notifications to fill the client's 100-deep channel
	// and the kernel socket buffers, so a broadcast send times out and the
	// client is dropped.
	padding := strings.Repeat("x", 64*1024)

	go func() {
		for i := 0; i < 500; i++ {
			select {
			case notificationCh <- &notificationMsg{Type: "stall_test", BaseURL: padding}:
			case <-t.Context().Done():
				return
			}
		}
	}()

	waitForSlots(t, limiter, 0, 30*time.Second,
		"a client that stopped reading must be torn down, not left holding its slot forever")

	ws2, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	require.NoError(t, err, "the endpoint must still accept connections after a stalled client is reaped")

	_ = ws2.Close()
}

// TestHandleWebSocket_OversizedClientFrameFreesCapSlot is the regression test
// for ChiR8: before the fix, startReadPump never called SetReadLimit, so a
// client could declare an arbitrarily large frame and have the server grow a
// single unbounded heap buffer to match it. /p2p-ws is push-only, so any
// data frame from a client is already anomalous; a client that sends one
// larger than wsMaxClientMessageBytes must be torn down (not have its
// payload buffered) and its cap slot released.
func TestHandleWebSocket_OversizedClientFrameFreesCapSlot(t *testing.T) {
	httpServer, _, limiter := newTestWebSocketServer(t, 1, 1)

	wsURL := "ws" + strings.TrimPrefix(httpServer.URL, "http")

	ws, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	require.NoError(t, err)

	defer ws.Close()

	waitForSlots(t, limiter, 1, 5*time.Second, "after connecting")

	// Send a frame well over the server's read limit. gorilla checks the
	// declared frame length against SetReadLimit before allocating the
	// payload buffer, so the server must reject and close the connection
	// rather than buffering this.
	oversized := make([]byte, wsMaxClientMessageBytes*2)
	require.NoError(t, ws.WriteMessage(websocket.BinaryMessage, oversized))

	waitForSlots(t, limiter, 0, 5*time.Second,
		"a client sending an over-limit frame must be torn down and release its slot")

	ws2, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	require.NoError(t, err, "the endpoint must still accept connections after the oversized-frame client is reaped")

	_ = ws2.Close()
}

// TestNotificationProcessor_ConnectThenImmediateDisconnectLeavesNoOrphan is
// the regression test for ChiR9: registration used to happen asynchronously
// - only when the notification processor dequeued a separate "new client"
// channel - while deregistration was synchronous, called directly from
// connection teardown. If a connection ended (and its teardown path called
// clientChannels.remove) before the processor got around to dequeuing the
// registration, the later dequeue would re-insert a channel whose reader
// goroutine had already returned: an orphan entry holding a buffered channel
// and a broadcast goroutine slot forever.
//
// The fix (see handleWebSocket) registers the client synchronously via
// clientChannels.addClient on the connecting goroutine itself, with no
// hand-off to the notification processor at all, so insertion strictly
// precedes any possible removal regardless of how busy the processor is.
// This test reproduces the worst-case timing directly: it stalls the
// processor inside a slow broadcast, then registers and immediately removes
// a client - mirroring handleWebSocket's exact call sequence - while the
// processor cannot do anything else, and asserts no orphan remains.
func TestNotificationProcessor_ConnectThenImmediateDisconnectLeavesNoOrphan(t *testing.T) {
	s := &Server{
		logger:   &ulogger.TestLogger{},
		settings: &settings.Settings{},
	}

	clientChannels := newClientChannelMap()
	deadClientCh := make(chan chan []byte, 10)
	notificationCh := make(chan *notificationMsg, 10)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	processorDone := make(chan struct{})
	go func() {
		defer close(processorDone)
		s.startNotificationProcessor(clientChannels, deadClientCh, notificationCh, ctx)
	}()

	// A stalling client with no reader forces any broadcast to block for the
	// full 1s send timeout, giving a deterministic window during which the
	// processor is busy and could not have processed anything else.
	stallCh := make(chan []byte)
	clientChannels.addClient(&wsClient{ch: stallCh})

	notificationCh <- &notificationMsg{Type: "stall_trigger"}

	// Give the processor a moment to pick up the notification and enter the
	// blocking broadcast before we register/remove our test client.
	time.Sleep(50 * time.Millisecond)

	// Mirror handleWebSocket's exact sequence: register synchronously, then
	// (as if the connection ended immediately) remove directly - both calls
	// made independently of the processor, which is still blocked inside the
	// broadcast at this point.
	clientCh := make(chan []byte, 100)
	client := &wsClient{ch: clientCh}
	clientChannels.addClient(client)
	clientChannels.remove(client.ch)

	require.False(t, clientChannels.contains(client.ch),
		"client must already be gone immediately after remove")

	// Let the processor finish the stalled broadcast (~1s).
	require.Eventually(t, func() bool { return !clientChannels.contains(stallCh) },
		3*time.Second, 20*time.Millisecond, "stalling client should eventually be reaped by its own broadcast timeout")

	// The removed client must never be resurrected once the processor
	// catches up - there is no path left by which it could be.
	require.Never(t, func() bool { return clientChannels.contains(client.ch) },
		500*time.Millisecond, 20*time.Millisecond,
		"a client removed while the processor was busy must not be resurrected (orphan entry)")

	cancel()

	select {
	case <-processorDone:
	case <-time.After(2 * time.Second):
		t.Fatal("processor did not stop")
	}
}

// TestClientChannelMap_RemoveCancelsConnection pins the contract that makes
// the stalled-client teardown possible: dropping a client from the broadcast
// set must cancel its connection context. Without it nothing will ever send
// on that client's channel again, so its writer goroutine parks on the
// channel receive forever and never runs its deferred release().
func TestClientChannelMap_RemoveCancelsConnection(t *testing.T) {
	cm := newClientChannelMap()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := make(chan []byte, 1)
	cm.addClient(&wsClient{ch: ch, cancel: cancel})

	require.NoError(t, ctx.Err(), "context must still be live while the client is registered")

	cm.remove(ch)

	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("removing a client must cancel its connection context")
	}

	// Removing an unknown channel must not panic.
	cm.remove(make(chan []byte))
}

// TestWSConnLimiter_LiveCounterSurvivesKeyChurn verifies the per-IP counters
// can't be flushed by key churn. A 50k-entry LRU could be evicted on demand
// (a single IPv6 /48 yields 65 536 /64 buckets), which silently reset the cap
// for an IP that still held open connections. Refcounted map entries are
// bounded by live connections instead, so churn is irrelevant.
func TestWSConnLimiter_LiveCounterSurvivesKeyChurn(t *testing.T) {
	limiter := newWSConnLimiter(0, 1)

	release, ok := limiter.acquire("10.0.0.1")
	require.True(t, ok)

	defer release()

	// Churn far more distinct keys than any plausible LRU capacity, each
	// acquired and immediately released.
	for i := 0; i < 100_000; i++ {
		churnRelease, churnOK := limiter.acquire(churnIPv6(i))
		require.True(t, churnOK)
		churnRelease()
	}

	_, ok = limiter.acquire("10.0.0.1")
	require.False(t, ok,
		"10.0.0.1 still holds a live connection, so its per-IP cap must not have been reset by key churn")

	require.Equal(t, 1, limiter.count(), "only the live connection should be accounted for")
}

// churnIPv6 produces distinct /64 buckets, the cheapest way for an attacker
// to generate unbounded distinct keys.
func churnIPv6(i int) string {
	return "2001:db8:" + itoaHex(i>>16) + ":" + itoaHex(i&0xffff) + "::1"
}

func itoaHex(v int) string {
	const digits = "0123456789abcdef"

	if v == 0 {
		return "0"
	}

	var buf [4]byte

	n := len(buf)

	for v > 0 {
		n--
		buf[n] = digits[v&0xf]
		v >>= 4
	}

	return string(buf[n:])
}
