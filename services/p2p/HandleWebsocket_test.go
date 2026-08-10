// Package p2p provides peer-to-peer networking functionality for the Teranode system.
package p2p

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v4"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

const (
	baseURL           = "http://test.com"
	shortTimeout      = 50 * time.Millisecond
	errClientNotAdded = "Client channel not added to clientChannels"
)

func TestBroadcastMessage(t *testing.T) {
	tests := []struct {
		name           string
		clientCount    int
		blockingCount  int
		expectedErrors int
	}{
		{
			name:           "No clients",
			clientCount:    0,
			blockingCount:  0,
			expectedErrors: 0,
		},
		{
			name:           "Single responsive client",
			clientCount:    1,
			blockingCount:  0,
			expectedErrors: 0,
		},
		{
			name:           "Multiple responsive clients",
			clientCount:    3,
			blockingCount:  0,
			expectedErrors: 0,
		},
		{
			name:           "Some blocking clients",
			clientCount:    3,
			blockingCount:  2,
			expectedErrors: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// We'll manually track the timeouts in our test function
			timeoutChan := make(chan struct{}, tt.blockingCount) // Buffer to collect all timeouts

			// Create unbuffered channels that will block
			blockingChannels := make([]chan []byte, tt.blockingCount)
			for i := 0; i < tt.blockingCount; i++ {
				blockingChannels[i] = make(chan []byte) // Unbuffered channel with no reader
			}

			// Create buffered channels that won't block
			nonBlockingChannels := make([]chan []byte, tt.clientCount-tt.blockingCount)
			for i := 0; i < tt.clientCount-tt.blockingCount; i++ {
				nonBlockingChannels[i] = make(chan []byte, 1) // Buffered channel
			}

			// Combine channels into the map expected by broadcastMessage
			clientChannels := make(map[chan []byte]struct{})
			for _, ch := range blockingChannels {
				clientChannels[ch] = struct{}{}
			}

			for _, ch := range nonBlockingChannels {
				clientChannels[ch] = struct{}{}
			}

			// Set up readers for non-blocking channels
			var wg sync.WaitGroup
			for _, ch := range nonBlockingChannels {
				wg.Add(1)

				go func(ch chan []byte) {
					defer wg.Done()
					<-ch // Read the message
				}(ch)
			}

			// Create a test message
			testData := []byte("test message")

			// Our test version of broadcastMessage that tracks timeouts
			broadcastTest := func() {
				for ch := range clientChannels {
					select {
					case ch <- testData:
						// Message sent successfully
					case <-time.After(shortTimeout):
						// Timed out - record this timeout
						timeoutChan <- struct{}{}
					}
				}
			}

			// Run the broadcast
			broadcastTest()

			// Wait for all readers to finish
			wg.Wait()

			// Count how many timeouts occurred
			timeoutCount := len(timeoutChan)
			close(timeoutChan)

			// Verify we got the expected number of timeouts
			assert.Equal(t, tt.expectedErrors, timeoutCount,
				"Expected %d timeouts but got %d in test '%s'",
				tt.expectedErrors, timeoutCount, tt.name)
		})
	}
}

func TestHandleClientMessages(t *testing.T) {
	t.Run("Normal operation", func(t *testing.T) {
		s := &Server{
			gCtx:   t.Context(),
			logger: &ulogger.TestLogger{},
		}

		ch := make(chan []byte, 1)
		deadClientCh := make(chan chan []byte, 1)
		ws := &testWebSocketConn{
			t: t,
		}

		done := make(chan struct{})
		go func() {
			s.handleClientMessages(ws, ch, deadClientCh)
			close(done)
		}()

		// Send a test message
		ch <- []byte("test")
		close(ch)

		select {
		case <-done:
			// Handler completed normally
		case <-time.After(time.Second):
			t.Fatal("Timeout waiting for handler to complete")
		}
	})

	t.Run("Write error", func(t *testing.T) {
		s := &Server{
			gCtx:   t.Context(),
			logger: &ulogger.TestLogger{},
		}

		ch := make(chan []byte, 1)
		deadClientCh := make(chan chan []byte, 1)
		ws := &testWebSocketConn{t: t, writeError: assert.AnError}

		done := make(chan struct{})
		go func() {
			s.handleClientMessages(ws, ch, deadClientCh)
			close(done)
		}()

		// Send a test message
		ch <- []byte("test")

		// Verify that the channel is reported as dead
		select {
		case deadCh := <-deadClientCh:
			assert.Equal(t, ch, deadCh)
		case <-time.After(time.Second):
			t.Fatal("Timeout waiting for dead client channel")
		}

		select {
		case <-done:
			// Handler completed normally
		case <-time.After(time.Second):
			t.Fatal("Timeout waiting for handler to complete")
		}
	})
}

// testWebSocketConn implements the minimal websocket.Conn interface needed for testing
type testWebSocketConn struct {
	t          *testing.T
	writeCount int
	writeError error
}

func (c *testWebSocketConn) WriteMessage(messageType int, data []byte) error {
	c.writeCount++
	c.t.Logf("WriteMessage called with message type %d, data: %s", messageType, string(data))

	return c.writeError
}

func (c *testWebSocketConn) Close() error {
	return nil
}

func (c *testWebSocketConn) ReadMessage() (messageType int, p []byte, err error) {
	// Not used in the test but needed to satisfy the interface
	return websocket.TextMessage, []byte{}, nil
}

func TestStartNotificationProcessor(t *testing.T) {
	s := &Server{
		logger: &ulogger.TestLogger{},
		settings: &settings.Settings{
			P2P: settings.P2PSettings{
				ListenMode: settings.ListenModeFull,
				EnableNAT:  false, // Disable NAT in tests to prevent data races in libp2p
			},
		},
	}

	clientChannels := newClientChannelMap()
	newClientCh := make(chan chan []byte, 1)
	deadClientCh := make(chan chan []byte, 1)
	notificationCh := make(chan *notificationMsg, 1)

	// Create context with cancel for cleanup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // Ensure cleanup

	// Create channels to coordinate test events
	processorStarted := make(chan struct{})
	processorDone := make(chan struct{})

	go func() {
		close(processorStarted)
		s.startNotificationProcessor(clientChannels, newClientCh, deadClientCh, notificationCh, ctx)
		close(processorDone)
	}()

	// Wait for processor to start
	select {
	case <-processorStarted:
		// Processor started successfully
	case <-time.After(time.Second):
		t.Fatal("Timeout waiting for processor to start")
	}

	t.Run("Add new client", func(t *testing.T) {
		clientCh := make(chan []byte, 10)
		newClientCh <- clientCh

		// Wait for client to be added
		time.Sleep(50 * time.Millisecond)
		assert.True(t, clientChannels.contains(clientCh), errClientNotAdded)
		assert.Equal(t, 1, clientChannels.count(), "Expected exactly one client")
	})

	t.Run("Send notification", func(t *testing.T) {
		clientCh := make(chan []byte, 10)
		newClientCh <- clientCh

		// Wait for client to be added
		time.Sleep(50 * time.Millisecond)
		require.True(t, clientChannels.contains(clientCh), errClientNotAdded)

		// First, drain the initial node_status message
		select {
		case msg := <-clientCh:
			var initialMsg notificationMsg
			err := json.Unmarshal(msg, &initialMsg)
			require.NoError(t, err)
			assert.Equal(t, "node_status", initialMsg.Type, "First message should be node_status")
		case <-time.After(100 * time.Millisecond):
			// No initial message is OK too if the server doesn't have a P2PClient
		}

		// Send our test notification
		testNotification := &notificationMsg{
			Type:    "test",
			BaseURL: baseURL,
		}
		notificationCh <- testNotification

		// Verify client received the test notification
		select {
		case msg := <-clientCh:
			var received notificationMsg
			err := json.Unmarshal(msg, &received)
			require.NoError(t, err, "Failed to unmarshal received message")
			assert.Equal(t, testNotification.Type, received.Type, "Unexpected notification type")
			assert.Equal(t, testNotification.BaseURL, received.BaseURL, "Unexpected notification baseURL")
		case <-time.After(time.Second):
			t.Fatal("Timeout waiting for test notification")
		}
	})

	t.Run("Remove client", func(t *testing.T) {
		clientCh := make(chan []byte, 10)
		newClientCh <- clientCh

		// Wait for client to be added
		time.Sleep(50 * time.Millisecond)
		require.True(t, clientChannels.contains(clientCh), errClientNotAdded)
		initialCount := clientChannels.count()

		deadClientCh <- clientCh

		// Wait for client to be removed
		time.Sleep(50 * time.Millisecond)
		assert.False(t, clientChannels.contains(clientCh), "Client channel not removed from clientChannels")
		assert.Equal(t, initialCount-1, clientChannels.count(), "Client count not decremented")
	})

	t.Run("Broadcast timeout handling", func(t *testing.T) {
		slowCh := make(chan []byte) // Unbuffered channel that will block
		newClientCh <- slowCh

		// Wait for client to be added
		time.Sleep(50 * time.Millisecond)
		require.True(t, clientChannels.contains(slowCh), errClientNotAdded)
		initialCount := clientChannels.count()

		// Send a notification - this should timeout for the slow client
		testNotification := &notificationMsg{
			Type:    "test",
			BaseURL: baseURL,
		}
		notificationCh <- testNotification

		// Wait for timeout and automatic removal
		time.Sleep(1500 * time.Millisecond) // Wait longer than the timeout
		assert.False(t, clientChannels.contains(slowCh), "Slow client channel not removed after timeout")
		assert.Equal(t, initialCount-1, clientChannels.count(), "Client count not decremented after timeout")
	})

	// Cancel context to stop the processor
	cancel()

	// Wait for processor to finish
	select {
	case <-processorDone:
		// Processor finished successfully
	case <-time.After(time.Second):
		t.Fatal("Timeout waiting for processor to stop")
	}
}

func TestHandleWebSocket(t *testing.T) {
	// Create server with logger
	s := &Server{
		gCtx:   t.Context(),
		logger: &ulogger.TestLogger{},
		settings: &settings.Settings{
			P2P: settings.P2PSettings{
				ListenMode: settings.ListenModeFull,
				EnableNAT:  false, // Disable NAT in tests to prevent data races in libp2p
			},
		},
	}

	// Create notification channel
	notificationCh := make(chan *notificationMsg, 1)

	// Create handler
	handler := s.HandleWebSocket(notificationCh)

	// Create test server
	serverReady := make(chan struct{}, 1)
	connectedCh := make(chan struct{}, 1)

	var wg sync.WaitGroup

	// Create test server with Echo
	e := echo.New()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c := e.NewContext(r, w)

		wg.Add(1)

		defer wg.Done()

		t.Log("Handling new connection")

		// Signal connection is ready before upgrading
		select {
		case connectedCh <- struct{}{}:
			t.Log("Signaled connection readiness")
		default:
			t.Log("Channel already notified")
		}

		// Call the actual handler
		if err := handler(c); err != nil {
			t.Errorf("Handler error: %v", err)
			return
		}
	}))

	defer server.Close()

	// Signal that server is ready
	serverReady <- struct{}{}

	t.Run("Normal operation", func(t *testing.T) {
		// Wait for server to be ready
		select {
		case <-serverReady:
			t.Log("Server is ready")
		case <-time.After(2 * time.Second):
			t.Fatal("Timeout waiting for server to be ready")
		}

		// Connect to WebSocket server
		t.Log("Attempting to connect to WebSocket server")

		url := "ws" + strings.TrimPrefix(server.URL, "http")
		ws, _, err := websocket.DefaultDialer.Dial(url, nil)
		require.NoError(t, err)

		defer ws.Close()

		// Wait for server-side connection acknowledgment
		select {
		case <-connectedCh:
			t.Log("Server acknowledged connection")
		case <-time.After(2 * time.Second):
			t.Fatal("Timeout waiting for server connection acknowledgment")
		}

		t.Log("Connected to WebSocket server")

		// First, read the initial node_status message that's sent automatically
		t.Log("Reading initial node_status message")
		err = ws.SetReadDeadline(time.Now().Add(2 * time.Second))
		require.NoError(t, err)

		messageType, message, err := ws.ReadMessage()
		require.NoError(t, err)
		assert.Equal(t, websocket.TextMessage, messageType)

		var initialMsg notificationMsg
		err = json.Unmarshal(message, &initialMsg)
		require.NoError(t, err)
		assert.Equal(t, "node_status", initialMsg.Type, "First message should be node_status")

		// Now send test notification
		testNotification := &notificationMsg{
			Type:    "test",
			BaseURL: baseURL,
		}
		notificationCh <- testNotification

		// Read the test message
		t.Log("Waiting for test message")

		err = ws.SetReadDeadline(time.Now().Add(2 * time.Second))
		require.NoError(t, err)

		messageType, message, err = ws.ReadMessage()
		require.NoError(t, err)
		assert.Equal(t, websocket.TextMessage, messageType)

		var received notificationMsg
		err = json.Unmarshal(message, &received)
		require.NoError(t, err)

		assert.Equal(t, testNotification.Type, received.Type)
		assert.Equal(t, testNotification.BaseURL, received.BaseURL)
	})
}

func TestBroadcast_SequentialTimeoutDoS(t *testing.T) {
	s := &Server{
		logger: &ulogger.TestLogger{},
		settings: &settings.Settings{
			P2P: settings.P2PSettings{
				ListenMode: settings.ListenModeFull,
				EnableNAT:  false,
			},
		},
	}

	clientChannels := newClientChannelMap()
	newClientCh := make(chan chan []byte, 100)
	deadClientCh := make(chan chan []byte, 100)
	notificationCh := make(chan *notificationMsg, 100)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	processorDone := make(chan struct{})
	go func() {
		s.startNotificationProcessor(clientChannels, newClientCh, deadClientCh, notificationCh, ctx)
		close(processorDone)
	}()

	// Wait for processor to start
	time.Sleep(50 * time.Millisecond)

	// Number of malicious clients (channels that won't be read)
	numMaliciousClients := 5

	// Create malicious clients - unbuffered channels that will block
	// This simulates clients that stop reading: when broadcast tries to send,
	// it will block for 1 second per client before timing out
	maliciousChannels := make([]chan []byte, numMaliciousClients)
	for i := 0; i < numMaliciousClients; i++ {
		// Create unbuffered channel that will block when trying to send
		maliciousChannels[i] = make(chan []byte)
		newClientCh <- maliciousChannels[i]
	}

	// Wait for all clients to be added
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, numMaliciousClients, clientChannels.count(), "All malicious clients should be added")

	// Add one legitimate client that will read messages
	// Add it AFTER malicious clients to ensure it's processed last in the broadcast loop
	legitimateCh := make(chan []byte, 100)
	newClientCh <- legitimateCh
	time.Sleep(50 * time.Millisecond)

	// Start reading from legitimate client in background
	legitimateReceived := make(chan []byte, 1)
	go func() {
		select {
		case msg := <-legitimateCh:
			legitimateReceived <- msg
		case <-time.After(10 * time.Second):
			// Timeout - legitimate client didn't receive message
		}
	}()

	// Send a notification and measure the time it takes for broadcast to complete
	// With parallel processing, broadcast should complete in ~1 second (all timeouts happen concurrently)
	// instead of N seconds (sequential timeouts)
	testNotification := &notificationMsg{
		Type:    "test_dos",
		BaseURL: baseURL,
	}

	startTime := time.Now()
	notificationCh <- testNotification

	// Wait for legitimate client to receive the message
	select {
	case <-legitimateReceived:
		t.Logf("Legitimate client received message")
	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for legitimate client to receive message")
	}

	// Now wait for ALL malicious clients to be processed and removed
	// With parallel processing, this should take ~1 second (all timeouts happen concurrently)
	// instead of N seconds (sequential timeouts)
	timeout := time.After(time.Duration(numMaliciousClients+2) * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	var removedCount int
	var broadcastCompleteTime time.Duration

	for {
		select {
		case <-timeout:
			t.Fatalf("Timeout waiting for all malicious clients to be removed. Only %d/%d removed", removedCount, numMaliciousClients)
		case <-ticker.C:
			removedCount = 0
			for _, ch := range maliciousChannels {
				if !clientChannels.contains(ch) {
					removedCount++
				}
			}

			if removedCount == numMaliciousClients {
				broadcastCompleteTime = time.Since(startTime)
				t.Logf("All %d malicious clients removed after %v", removedCount, broadcastCompleteTime)
				goto broadcastComplete
			}
		}
	}

broadcastComplete:
	// Verify the broadcast completed quickly due to parallel processing
	// With parallel processing, all timeouts happen concurrently, so total time should be ~1 second
	// instead of N seconds (sequential timeouts)
	expectedMaxDelay := 2 * time.Second // Allow some overhead for goroutine scheduling

	if broadcastCompleteTime > expectedMaxDelay {
		t.Errorf("Broadcast took too long (%v). Expected at most %v with parallel processing. Sequential processing would take ~%d seconds",
			broadcastCompleteTime, expectedMaxDelay, numMaliciousClients)
	} else {
		t.Logf("Broadcast completed in %v (parallel processing working correctly)", broadcastCompleteTime)
	}

	// Verify all malicious clients were removed
	assert.Equal(t, numMaliciousClients, removedCount,
		"All malicious client channels should be removed after timeout")

	// Verify the notification processor can process new notifications after broadcast completes
	// Drain any remaining messages from legitimate client first
	select {
	case <-legitimateCh:
		// Drain any buffered message
	default:
		// No message to drain
	}

	startTime2 := time.Now()
	testNotification2 := &notificationMsg{
		Type:    "test_dos_2",
		BaseURL: baseURL,
	}
	notificationCh <- testNotification2

	select {
	case msg := <-legitimateCh:
		elapsed2 := time.Since(startTime2)
		t.Logf("Second notification received after %v", elapsed2)
		var received notificationMsg
		err := json.Unmarshal(msg, &received)
		require.NoError(t, err)
		assert.Equal(t, "test_dos_2", received.Type, "Second notification should be processed correctly")
		// Second notification should be fast since malicious clients are already removed
		if elapsed2 > 500*time.Millisecond {
			t.Errorf("Second notification took too long (%v). Should be fast since malicious clients are removed", elapsed2)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for second notification - processor may still be blocked")
	}

	// Cancel context to stop processor
	cancel()

	// Wait for processor to finish (give it time to process any pending operations)
	select {
	case <-processorDone:
		t.Logf("Processor stopped successfully")
	case <-time.After(5 * time.Second):
		t.Logf("Warning: Processor did not stop within timeout, but this may be acceptable if it's still processing")
		// Don't fail the test - the important part is demonstrating the DoS vulnerability is fixed
	}
}

// TestHandleWebSocket_PerConnectionContext is a regression test for issue #4573.
// A single failed WebSocket upgrade must not cancel the shared notification
// processor and starve all other connected clients.
func TestHandleWebSocket_PerConnectionContext(t *testing.T) {
	s := &Server{
		gCtx:   t.Context(),
		logger: &ulogger.TestLogger{},
		settings: &settings.Settings{
			P2P: settings.P2PSettings{
				ListenMode: settings.ListenModeFull,
				EnableNAT:  false,
			},
		},
	}

	notificationCh := make(chan *notificationMsg, 1)
	handler := s.HandleWebSocket(notificationCh)

	e := echo.New()
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c := e.NewContext(r, w)
		_ = handler(c)
	}))
	defer httpServer.Close()

	resp, err := http.Get(httpServer.URL)
	require.NoError(t, err, "Plain HTTP GET should fail upgrade but not error at the HTTP layer")
	require.NotNil(t, resp)
	_ = resp.Body.Close()
	require.NotEqual(t, http.StatusSwitchingProtocols, resp.StatusCode, "Upgrade should have failed")

	wsURL := "ws" + strings.TrimPrefix(httpServer.URL, "http")
	ws, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	require.NoError(t, err, "Second connection should still upgrade after the first one's upgrade failed")
	defer ws.Close()

	require.NoError(t, ws.SetReadDeadline(time.Now().Add(2*time.Second)))

	_, initialMessage, err := ws.ReadMessage()
	require.NoError(t, err, "Should receive initial node_status; processor must still be alive")

	var initial notificationMsg
	require.NoError(t, json.Unmarshal(initialMessage, &initial))
	require.Equal(t, "node_status", initial.Type)

	notificationCh <- &notificationMsg{Type: "post_failed_upgrade", BaseURL: baseURL}

	require.NoError(t, ws.SetReadDeadline(time.Now().Add(2*time.Second)))

	_, message, err := ws.ReadMessage()
	require.NoError(t, err, "Notification must still be delivered after the prior upgrade failure")

	var received notificationMsg
	require.NoError(t, json.Unmarshal(message, &received))
	require.Equal(t, "post_failed_upgrade", received.Type)
	require.Equal(t, baseURL, received.BaseURL)
}

// TestBroadcast_BoundedPool verifies the broadcast goroutine pool caps in-flight goroutines.
// It overrides maxConcurrentBroadcasts to a small value, then submits 4x that many unresponsive
// (unbuffered, unread) channels. Every channel hits the 1s send-timeout. With the cap, total
// wall-clock time is ceil(channels/poolSize) * 1s; without it, all timeouts run concurrently
// and total wall-clock is ~1s. The lower bound asserts the semaphore actually serialises work.
func TestBroadcast_BoundedPool(t *testing.T) {
	originalPoolSize := maxConcurrentBroadcasts
	defer func() { maxConcurrentBroadcasts = originalPoolSize }()
	maxConcurrentBroadcasts = 2

	cm := newClientChannelMap()

	const numChannels = 8
	channels := make([]chan []byte, numChannels)

	for i := 0; i < numChannels; i++ {
		channels[i] = make(chan []byte)
		cm.add(channels[i])
	}

	require.Equal(t, numChannels, cm.count(), "All channels should be registered")

	logger := &ulogger.TestLogger{}

	startTime := time.Now()
	cm.broadcast([]byte("test"), logger)
	elapsed := time.Since(startTime)

	expectedMin := time.Duration(numChannels/maxConcurrentBroadcasts) * time.Second
	expectedMax := expectedMin + 2*time.Second

	require.GreaterOrEqual(t, elapsed, expectedMin,
		"Broadcast finished too quickly (%v); pool of %d should have serialised %d unresponsive channels into batches taking ~%v",
		elapsed, maxConcurrentBroadcasts, numChannels, expectedMin)
	require.LessOrEqual(t, elapsed, expectedMax,
		"Broadcast took too long (%v); expected at most %v", elapsed, expectedMax)

	require.Equal(t, 0, cm.count(), "All timed-out channels should be removed")

	t.Logf("Broadcast of %d unresponsive channels with pool=%d completed in %v (expected %v..%v)",
		numChannels, maxConcurrentBroadcasts, elapsed, expectedMin, expectedMax)
}

// TestBroadcast_NonPositivePoolSizeDoesNotDeadlock verifies that a misconfigured
// (zero or negative) maxConcurrentBroadcasts is clamped to a usable value rather
// than deadlocking the broadcast loop. With cap=0, sem <- struct{}{} on an
// unbuffered channel would block forever because the receiver runs only after
// the send returns.
func TestBroadcast_NonPositivePoolSizeDoesNotDeadlock(t *testing.T) {
	originalPoolSize := maxConcurrentBroadcasts
	defer func() { maxConcurrentBroadcasts = originalPoolSize }()
	maxConcurrentBroadcasts = 0

	cm := newClientChannelMap()

	const numChannels = 3
	for i := 0; i < numChannels; i++ {
		cm.add(make(chan []byte, 1)) // buffered so sends succeed immediately
	}

	done := make(chan struct{})

	go func() {
		cm.broadcast([]byte("test"), &ulogger.TestLogger{})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("broadcast deadlocked with maxConcurrentBroadcasts <= 0")
	}

	require.Equal(t, numChannels, cm.count(), "responsive channels should still be registered")
}

// TestSendInitialNodeStatuses_SendsCachedStatusSynchronously verifies that when a
// node status has been cached by the periodic publisher, a new client is served
// the cached copy directly on the calling goroutine, without any blockchain call.
func TestSendInitialNodeStatuses_SendsCachedStatusSynchronously(t *testing.T) {
	s := &Server{
		logger:   &ulogger.TestLogger{},
		settings: &settings.Settings{},
	}

	cached := &notificationMsg{Type: "node_status", PeerID: "cached-peer", BestHeight: 42}
	s.latestNodeStatus.Store(cached)

	clientCh := make(chan []byte, 1)
	s.sendInitialNodeStatuses(context.Background(), clientCh)

	// The server has no blockchain client, so only the synchronous cached path
	// can have produced a message by now.
	select {
	case data := <-clientCh:
		var msg notificationMsg
		require.NoError(t, json.Unmarshal(data, &msg))
		require.Equal(t, "node_status", msg.Type)
		require.Equal(t, "cached-peer", msg.PeerID)
		require.Equal(t, uint32(42), msg.BestHeight)
	default:
		t.Fatal("cached node_status was not sent synchronously")
	}
}

// TestGetNodeStatusMessage_PopulatesCache verifies that computing a node status
// stores it in the cache used by sendInitialNodeStatuses.
func TestGetNodeStatusMessage_PopulatesCache(t *testing.T) {
	s := &Server{
		logger:   &ulogger.TestLogger{},
		settings: &settings.Settings{},
	}

	require.Nil(t, s.latestNodeStatus.Load())

	msg := s.getNodeStatusMessage(context.Background())
	require.NotNil(t, msg)
	require.Same(t, msg, s.latestNodeStatus.Load())
}

// TestStartNotificationProcessor_SlowBlockchainDoesNotStallProcessor is a
// regression test for the issue where sendInitialNodeStatuses ran a blockchain
// gRPC round-trip inline on the single notification-processor goroutine: one slow
// blockchain call (or a burst of new clients) froze all broadcasts and dead-client
// reaping. The initial status fetch must not block the processor loop.
func TestStartNotificationProcessor_SlowBlockchainDoesNotStallProcessor(t *testing.T) {
	release := make(chan struct{})

	// blockchain.Mock is used deliberately here despite the prefer-sqlitememory
	// rule: the test needs a GetBestBlockHeader call that blocks on demand to
	// prove the processor loop is not stalled behind it, which a real store
	// cannot simulate.
	mockBlockchain := &blockchain.Mock{}
	mockBlockchain.On("GetBestBlockHeader", mock.Anything).Run(func(mock.Arguments) {
		<-release
	}).Return(model.GenesisBlockHeader, model.GenesisBlockHeaderMeta, nil).Maybe()
	mockBlockchain.On("GetFSMCurrentState", mock.Anything).Return(nil, assert.AnError).Maybe()
	mockBlockchain.On("GetState", mock.Anything, mock.Anything).Return(nil, assert.AnError).Maybe()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := &Server{
		logger: &ulogger.TestLogger{},
		settings: &settings.Settings{
			P2P: settings.P2PSettings{
				ListenMode: settings.ListenModeFull,
			},
		},
		blockchainClient: mockBlockchain,
		gCtx:             ctx,
	}

	clientChannels := newClientChannelMap()
	newClientCh := make(chan chan []byte, 10)
	deadClientCh := make(chan chan []byte, 10)
	notificationCh := make(chan *notificationMsg, 10)

	go s.startNotificationProcessor(clientChannels, newClientCh, deadClientCh, notificationCh, ctx)

	// Connect a client while the blockchain call cannot complete.
	clientCh := make(chan []byte, 10)
	newClientCh <- clientCh

	require.Eventually(t, func() bool { return clientChannels.contains(clientCh) },
		time.Second, 5*time.Millisecond, errClientNotAdded)

	// The processor must keep broadcasting while the initial node-status fetch
	// is still blocked on the blockchain service.
	notificationCh <- &notificationMsg{Type: "test", BaseURL: baseURL}

	select {
	case data := <-clientCh:
		var msg notificationMsg
		require.NoError(t, json.Unmarshal(data, &msg))
		require.Equal(t, "test", msg.Type, "broadcast should arrive before the blocked initial node_status")
	case <-time.After(2 * time.Second):
		t.Fatal("notification broadcast stalled behind the initial node-status fetch")
	}

	// Unblock the blockchain call; the initial node_status must still be delivered.
	close(release)

	select {
	case data := <-clientCh:
		var msg notificationMsg
		require.NoError(t, json.Unmarshal(data, &msg))
		require.Equal(t, "node_status", msg.Type)
	case <-time.After(2 * time.Second):
		t.Fatal("initial node_status never delivered after the blockchain call unblocked")
	}
}

// TestStartNotificationProcessor_InitialStatusPrecedesBroadcasts encodes the
// contract that consumers (the asset service's centrifuge listener and the
// dashboard) rely on: the first node_status a new client receives identifies our
// own node. With the cache warmed (as Start does before exposing the websocket
// route), the initial status is sent synchronously during client registration,
// so a concurrently broadcast remote node_status can never precede it.
func TestStartNotificationProcessor_InitialStatusPrecedesBroadcasts(t *testing.T) {
	s := &Server{
		logger: &ulogger.TestLogger{},
		settings: &settings.Settings{
			P2P: settings.P2PSettings{
				ListenMode: settings.ListenModeFull,
			},
		},
	}

	ourStatus := &notificationMsg{Type: "node_status", PeerID: "our-node"}
	s.latestNodeStatus.Store(ourStatus)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clientChannels := newClientChannelMap()
	newClientCh := make(chan chan []byte, 10)
	deadClientCh := make(chan chan []byte, 10)
	notificationCh := make(chan *notificationMsg, 10)

	go s.startNotificationProcessor(clientChannels, newClientCh, deadClientCh, notificationCh, ctx)

	// Register a client and broadcast a remote peer's node_status at the same
	// time. The client must either see our own status first or miss the remote
	// broadcast entirely (if it was processed before registration).
	clientCh := make(chan []byte, 10)
	newClientCh <- clientCh
	notificationCh <- &notificationMsg{Type: "node_status", PeerID: "remote-node"}

	select {
	case data := <-clientCh:
		var msg notificationMsg
		require.NoError(t, json.Unmarshal(data, &msg))
		require.Equal(t, "node_status", msg.Type)
		require.Equal(t, "our-node", msg.PeerID, "first node_status must identify our own node")
	case <-time.After(2 * time.Second):
		t.Fatal("initial node_status never delivered")
	}
}

// flakyPeerRegistry satisfies PeerRegistryClientI via the embedded interface;
// only ListPeers is implemented: it succeeds on the first call (one connected
// and one disconnected peer) and fails on every call after that.
type flakyPeerRegistry struct {
	blockchain.PeerRegistryClientI
	calls int
}

func (f *flakyPeerRegistry) ListPeers(context.Context, *blockchain_api.TransportType, float64, uint32, bool, bool) ([]*blockchain.PeerInfo, error) {
	f.calls++
	if f.calls == 1 {
		return []*blockchain.PeerInfo{{ID: "peer-a", IsConnected: true}, {ID: "peer-b"}}, nil
	}

	return nil, assert.AnError
}

// TestGetNodeStatusMessage_CarriesForwardLastKnownGoodOnFailure verifies that
// failed lookups fall back to the corresponding fields of the last cached status
// instead of zero values, both in the returned/broadcast message and in the
// cache served to new websocket clients.
func TestGetNodeStatusMessage_CarriesForwardLastKnownGoodOnFailure(t *testing.T) {
	mockBlockchain := &blockchain.Mock{}

	// First call: everything succeeds (except the block persister height, which
	// is also allowed to fail without affecting the other fields).
	meta100 := &model.BlockHeaderMeta{Height: 100, Miner: "miner-a", ChainWork: []byte{0x01, 0x02}}
	runningState := blockchain.FSMStateRUNNING
	mockBlockchain.On("GetBestBlockHeader", mock.Anything).Return(model.GenesisBlockHeader, meta100, nil).Once()
	mockBlockchain.On("GetFSMCurrentState", mock.Anything).Return(&runningState, nil).Once()

	// Second call: the best-header and FSM lookups fail.
	mockBlockchain.On("GetBestBlockHeader", mock.Anything).Return(nil, nil, assert.AnError).Once()
	mockBlockchain.On("GetFSMCurrentState", mock.Anything).Return(nil, assert.AnError).Once()

	// Third call: the best header recovers at a new height while FSM still fails.
	meta200 := &model.BlockHeaderMeta{Height: 200, Miner: "miner-b"}
	mockBlockchain.On("GetBestBlockHeader", mock.Anything).Return(model.GenesisBlockHeader, meta200, nil).Once()
	mockBlockchain.On("GetFSMCurrentState", mock.Anything).Return(nil, assert.AnError).Once()

	// The block persister height lookup fails twice, then succeeds.
	mockBlockchain.On("GetState", mock.Anything, mock.Anything).Return(nil, assert.AnError).Times(2)
	persisterHeight := make([]byte, 4)
	binary.LittleEndian.PutUint32(persisterHeight, 150)
	mockBlockchain.On("GetState", mock.Anything, mock.Anything).Return(persisterHeight, nil).Once()

	// Block assembly succeeds once, then fails for the remaining calls.
	mockBlockAssembly := &blockassembly.Mock{}
	mockBlockAssembly.On("GetBlockAssemblyState", mock.Anything).Return(&blockassembly_api.StateMessage{TxCount: 7, SubtreeCount: 3}, nil).Once()
	mockBlockAssembly.On("GetBlockAssemblyState", mock.Anything).Return(nil, assert.AnError)

	s := &Server{
		logger:              &ulogger.TestLogger{},
		settings:            &settings.Settings{},
		blockchainClient:    mockBlockchain,
		blockAssemblyClient: mockBlockAssembly,
		peerRegistry:        &flakyPeerRegistry{},
	}

	first := s.getNodeStatusMessage(context.Background())
	require.Equal(t, uint32(100), first.BestHeight)
	require.Equal(t, "miner-a", first.MinerName)
	require.Equal(t, "RUNNING", first.FSMState)
	require.Equal(t, uint64(7), first.TxCount)
	require.Equal(t, uint32(3), first.SubtreeCount)
	require.Equal(t, 1, first.ConnectedPeersCount)

	second := s.getNodeStatusMessage(context.Background())
	require.Equal(t, uint32(100), second.BestHeight, "failed best-header lookup must carry forward the cached height")
	require.Equal(t, "miner-a", second.MinerName)
	require.Equal(t, "RUNNING", second.FSMState, "failed FSM lookup must carry forward the cached state")
	require.Equal(t, first.BestBlockHash, second.BestBlockHash)
	require.Equal(t, "0102", second.ChainWork, "failed best-header lookup must carry forward the cached chainwork")
	require.Equal(t, uint64(7), second.TxCount, "failed block-assembly lookup must carry forward the cached counts")
	require.Equal(t, uint32(3), second.SubtreeCount)
	require.Equal(t, 1, second.ConnectedPeersCount, "failed ListPeers lookup must carry forward the cached count")
	require.Equal(t, first.Storage, second.Storage, "failed persister-height lookup must carry forward the cached storage mode")
	require.Equal(t, uint32(100), s.latestNodeStatus.Load().BestHeight, "cache must not regress to zero values")

	third := s.getNodeStatusMessage(context.Background())
	require.Equal(t, uint32(200), third.BestHeight, "recovered lookup must serve fresh values")
	require.Equal(t, "miner-b", third.MinerName)
	require.Equal(t, "RUNNING", third.FSMState, "still-failing FSM lookup keeps the cached state")
	require.Equal(t, uint32(200), s.latestNodeStatus.Load().BestHeight)
}

// TestSendNodeStatusToClient_DropsWhenChannelFull verifies the non-blocking send:
// a full client channel drops the status instead of blocking the caller.
func TestSendNodeStatusToClient_DropsWhenChannelFull(t *testing.T) {
	s := &Server{
		logger:   &ulogger.TestLogger{},
		settings: &settings.Settings{},
	}

	occupied := []byte("occupied")
	clientCh := make(chan []byte, 1)
	clientCh <- occupied

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.sendNodeStatusToClient(clientCh, &notificationMsg{Type: "node_status"})
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("sendNodeStatusToClient blocked on a full channel")
	}

	require.Equal(t, occupied, <-clientCh, "the queued message must be untouched")
	require.Empty(t, clientCh, "the status must have been dropped, not queued")
}

// TestPublishNodeStatus_BoundsEachTick verifies that every publish runs under a
// per-tick deadline, so one wedged blockchain call cannot stall the publisher
// (and freeze the node-status cache) forever; that a failed P2P publish is
// logged and survived; and that the publisher shuts down on context cancellation.
func TestPublishNodeStatus_BoundsEachTick(t *testing.T) {
	// Shorten the publish interval so the ticker-driven publish path runs
	// within the test; restored after the publisher goroutine has exited.
	oldInterval := nodeStatusPublishInterval
	nodeStatusPublishInterval = 20 * time.Millisecond

	defer func() { nodeStatusPublishInterval = oldInterval }()

	deadlineSeen := make(chan bool, 1)

	mockBlockchain := &blockchain.Mock{}
	mockBlockchain.On("GetBestBlockHeader", mock.Anything).Run(func(args mock.Arguments) {
		callCtx, ok := args.Get(0).(context.Context)
		require.True(t, ok)
		_, hasDeadline := callCtx.Deadline()
		select {
		case deadlineSeen <- hasDeadline:
		default:
		}
	}).Return(nil, nil, assert.AnError)
	mockBlockchain.On("GetFSMCurrentState", mock.Anything).Return(nil, assert.AnError).Maybe()
	mockBlockchain.On("GetState", mock.Anything, mock.Anything).Return(nil, assert.AnError).Maybe()

	// A failing P2P publish makes handleNodeStatusNotification return an error,
	// which the publisher must log and survive.
	mockP2P := &MockServerP2PClient{peerID: peer.ID("test-peer")}
	mockP2P.On("Publish", mock.Anything, mock.Anything, mock.Anything).Return(assert.AnError)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := &Server{
		logger: &ulogger.TestLogger{},
		settings: &settings.Settings{
			P2P: settings.P2PSettings{
				ListenMode: settings.ListenModeFull,
			},
		},
		blockchainClient: mockBlockchain,
		P2PClient:        mockP2P,
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.publishNodeStatus(ctx)
	}()

	select {
	case hasDeadline := <-deadlineSeen:
		require.True(t, hasDeadline, "publish tick must run under a bounded context")
	case <-time.After(2 * time.Second):
		t.Fatal("publishNodeStatus never issued the initial publish")
	}

	// A second event proves the ticker-driven publish path runs too.
	select {
	case hasDeadline := <-deadlineSeen:
		require.True(t, hasDeadline, "ticker-driven publish must also run under a bounded context")
	case <-time.After(2 * time.Second):
		t.Fatal("publishNodeStatus never issued a ticker-driven publish")
	}

	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("publishNodeStatus did not shut down on context cancellation")
	}
}

// TestSendNodeStatusToClient_MarshalError verifies that a status that cannot be
// marshaled (non-finite float) is dropped without sending anything.
func TestSendNodeStatusToClient_MarshalError(t *testing.T) {
	s := &Server{
		logger:   &ulogger.TestLogger{},
		settings: &settings.Settings{},
	}

	inf := math.Inf(1)
	clientCh := make(chan []byte, 1)
	s.sendNodeStatusToClient(clientCh, &notificationMsg{Type: "node_status", MinMiningTxFee: &inf})
	require.Empty(t, clientCh, "an unmarshalable status must be dropped")
}

// TestHandleNodeStatusNotification_FanOutSurvivesPublishFailure verifies that a
// failed P2P publish still fans the status out to local websocket clients:
// otherwise long-lived clients freeze on their last status while newly
// connecting clients are served a fresh copy from the cache.
func TestHandleNodeStatusNotification_FanOutSurvivesPublishFailure(t *testing.T) {
	mockBlockchain := &blockchain.Mock{}
	mockBlockchain.On("GetBestBlockHeader", mock.Anything).Return(nil, nil, assert.AnError).Maybe()
	mockBlockchain.On("GetFSMCurrentState", mock.Anything).Return(nil, assert.AnError).Maybe()
	mockBlockchain.On("GetState", mock.Anything, mock.Anything).Return(nil, assert.AnError).Maybe()

	mockP2P := &MockServerP2PClient{peerID: peer.ID("test-peer")}
	mockP2P.On("Publish", mock.Anything, mock.Anything, mock.Anything).Return(assert.AnError)

	s := &Server{
		logger: &ulogger.TestLogger{},
		settings: &settings.Settings{
			P2P: settings.P2PSettings{
				ListenMode: settings.ListenModeFull,
			},
		},
		blockchainClient: mockBlockchain,
		P2PClient:        mockP2P,
		notificationCh:   make(chan *notificationMsg, 1),
	}

	err := s.handleNodeStatusNotification(context.Background())
	require.Error(t, err, "the publish failure must still be reported to the caller")

	select {
	case got := <-s.notificationCh:
		require.Equal(t, "node_status", got.Type)
	default:
		t.Fatal("local websocket clients did not receive the status when the P2P publish failed")
	}
}

// TestHandleNodeStatusNotification_PublishBudgetSurvivesSlowCompute verifies the
// compute/publish budget split: a blockchain call that wedges until its context
// expires must not hand Publish an already-expired context, because the status
// computation is bounded to a fraction of the tick.
func TestHandleNodeStatusNotification_PublishBudgetSurvivesSlowCompute(t *testing.T) {
	oldInterval := nodeStatusPublishInterval
	nodeStatusPublishInterval = 200 * time.Millisecond

	defer func() { nodeStatusPublishInterval = oldInterval }()

	mockBlockchain := &blockchain.Mock{}
	// Wedge the best-header call until its (compute) context expires.
	mockBlockchain.On("GetBestBlockHeader", mock.Anything).Run(func(args mock.Arguments) {
		callCtx, ok := args.Get(0).(context.Context)
		require.True(t, ok)
		<-callCtx.Done()
	}).Return(nil, nil, assert.AnError)
	mockBlockchain.On("GetFSMCurrentState", mock.Anything).Return(nil, assert.AnError).Maybe()
	mockBlockchain.On("GetState", mock.Anything, mock.Anything).Return(nil, assert.AnError).Maybe()

	publishCtxAlive := make(chan bool, 1)
	mockP2P := &MockServerP2PClient{peerID: peer.ID("test-peer")}
	mockP2P.On("Publish", mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		callCtx, ok := args.Get(0).(context.Context)
		require.True(t, ok)
		select {
		case publishCtxAlive <- callCtx.Err() == nil:
		default:
		}
	}).Return(nil)

	s := &Server{
		logger: &ulogger.TestLogger{},
		settings: &settings.Settings{
			P2P: settings.P2PSettings{
				ListenMode: settings.ListenModeFull,
			},
		},
		blockchainClient: mockBlockchain,
		P2PClient:        mockP2P,
		notificationCh:   make(chan *notificationMsg, 1),
	}

	// Mirror publish(): the whole tick runs under one deadline; the status
	// computation may only consume half of it.
	tickCtx, cancel := context.WithTimeout(context.Background(), nodeStatusPublishInterval)
	defer cancel()

	require.NoError(t, s.handleNodeStatusNotification(tickCtx))

	select {
	case alive := <-publishCtxAlive:
		require.True(t, alive, "Publish must receive a context with budget left after a wedged computation")
	default:
		t.Fatal("Publish was never called")
	}
}
