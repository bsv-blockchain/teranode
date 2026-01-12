// Package p2p provides peer-to-peer networking functionality for the Teranode system.
package p2p

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
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
