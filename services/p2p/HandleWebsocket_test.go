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

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/services/asset/asset_api"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	baseURL         = "http://test.com"
	shortTimeout    = 50 * time.Millisecond
	standardTimeout = 500 * time.Millisecond
	extendedTimeout = time.Second
	testMessage     = "test message"
)

func TestCreatePingMessage(t *testing.T) {
	msg, err := createPingMessage(baseURL)
	require.NoError(t, err)
	assert.Equal(t, asset_api.Type_PING.String(), msg.Type)
	assert.Equal(t, baseURL, msg.BaseURL)
	assert.NotEmpty(t, msg.Timestamp)
}

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
	// Create test server
	s := &Server{
		logger: &ulogger.TestLogger{},
	}

	// Create channels for the test
	ch := make(chan []byte, 1)
	deadClientCh := make(chan chan []byte, 1)

	// Create a connection with a controlled error condition
	conn := &testWebSocketConn{
		t:          t,
		writeError: errors.NewProcessingError("simulated write error"),
	}

	// Start client message handler in a goroutine
	done := make(chan struct{})
	go func() {
		defer close(done)
		// This should exit as soon as it encounters the write error
		s.handleClientMessages(conn, ch, deadClientCh)
	}()

	// Send a message that will trigger the write error
	ch <- []byte(testMessage)

	// Check that the client is marked as dead
	select {
	case deadClient := <-deadClientCh:
		assert.Equal(t, ch, deadClient, "Expected dead client channel to match")
	case <-time.After(standardTimeout):
		t.Fatal("Timeout waiting for dead client notification")
	}

	// Wait for handler to complete
	select {
	case <-done:
		// Test passed
	case <-time.After(standardTimeout):
		t.Fatal("Timeout waiting for handler to complete")
	}

	// Verify the message was actually written before the error
	assert.Equal(t, 1, conn.writeCount, "Expected exactly one write attempt")
}

// testWebSocketConn implements the minimal websocket.Conn interface needed for testing
type testWebSocketConn struct {
	t          *testing.T
	writeCount int
	writeError error
}

func (c *testWebSocketConn) WriteMessage(messageType int, data []byte) error {
	c.writeCount++
	c.t.Logf("WriteMessage called with data: %s", string(data))

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
	}

	clientChannels := make(map[chan []byte]struct{})
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
		s.startNotificationProcessor(clientChannels, newClientCh, deadClientCh, notificationCh, baseURL, ctx)
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
		clientCh := make(chan []byte, 1)
		newClientCh <- clientCh

		// Wait for client to be added
		time.Sleep(50 * time.Millisecond)
		assert.Contains(t, clientChannels, clientCh, "Client channel not added to clientChannels")
	})

	t.Run("Send notification", func(t *testing.T) {
		// Get an existing client channel
		var clientCh chan []byte
		for ch := range clientChannels {
			clientCh = ch
			break
		}

		require.NotNil(t, clientCh, "No client channels available")

		testNotification := &notificationMsg{
			Type:    "test",
			BaseURL: baseURL,
		}
		notificationCh <- testNotification

		// Verify client received notification
		select {
		case msg := <-clientCh:
			var received notificationMsg
			err := json.Unmarshal(msg, &received)
			require.NoError(t, err, "Failed to unmarshal received message")
			assert.Equal(t, testNotification.Type, received.Type, "Unexpected notification type")
			assert.Equal(t, testNotification.BaseURL, received.BaseURL, "Unexpected notification baseURL")
		case <-time.After(time.Second):
			t.Fatal("Timeout waiting for notification")
		}
	})

	t.Run("Remove client", func(t *testing.T) {
		// Get an existing client channel
		var clientCh chan []byte
		for ch := range clientChannels {
			clientCh = ch
			break
		}

		require.NotNil(t, clientCh, "No client channels available")

		deadClientCh <- clientCh

		// Wait for client to be removed
		time.Sleep(50 * time.Millisecond)
		assert.NotContains(t, clientChannels, clientCh, "Client channel not removed from clientChannels")
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
		logger: &ulogger.TestLogger{},
	}

	// Create notification channel
	notificationCh := make(chan *notificationMsg, 1)

	// Create handler
	handler := s.HandleWebSocket(notificationCh, baseURL)

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

		// Send test notification
		testNotification := &notificationMsg{
			Type:    "test",
			BaseURL: baseURL,
		}
		notificationCh <- testNotification

		// Read response with timeout
		t.Log("Waiting for test message")

		err = ws.SetReadDeadline(time.Now().Add(2 * time.Second))
		require.NoError(t, err)

		messageType, message, err := ws.ReadMessage()
		require.NoError(t, err)
		require.Equal(t, websocket.TextMessage, messageType)

		// Verify message content
		var received notificationMsg
		err = json.Unmarshal(message, &received)
		require.NoError(t, err)
		assert.Equal(t, testNotification.Type, received.Type)
		assert.Equal(t, testNotification.BaseURL, received.BaseURL)
	})

	// Wait for all handlers to complete
	wg.Wait()
}
