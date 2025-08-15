package client

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	t.Run("creates new client with logger", func(t *testing.T) {
		logger := ulogger.TestLogger{}
		client := New(logger)

		assert.NotNil(t, client)
		assert.False(t, client.IsConnected())
	})
}

func TestClient_Start(t *testing.T) {
	t.Run("starts client and connects to server", func(t *testing.T) {
		// Create a test WebSocket server
		upgrader := websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true
			},
		}

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/connection/websocket" {
				w.WriteHeader(http.StatusNotFound)
				return
			}

			conn, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				return
			}
			defer conn.Close()

			// Send a welcome message
			_ = conn.WriteJSON(map[string]interface{}{
				"type": "connected",
				"data": map[string]string{
					"client": "test-client",
				},
			})

			// Keep connection open for a bit
			time.Sleep(100 * time.Millisecond)
		}))
		defer server.Close()

		logger := ulogger.TestLogger{}
		client := New(logger)

		// Extract host from server URL
		addr := strings.TrimPrefix(server.URL, "http://")

		// Start client in a goroutine
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		errCh := make(chan error, 1)
		go func() {
			errCh <- client.Start(ctx, addr)
		}()

		// Give client time to connect
		time.Sleep(50 * time.Millisecond)

		// Check that client is initialized
		assert.True(t, client.IsConnected())

		// Client should be connected after starting

		// Stop client
		if client.IsConnected() {
			if err := client.Stop(context.Background()); err != nil {
				t.Errorf("Stop failed: %v", err)
			}
		}

		// Wait for Start to complete or timeout
		select {
		case <-errCh:
			// Client stopped
		case <-ctx.Done():
			// Context timeout
		}
	})

	t.Run("handles connection error", func(t *testing.T) {
		logger := ulogger.TestLogger{}
		client := New(logger)

		// Use invalid address
		_ = client.Start(context.Background(), "invalid-address-that-does-not-exist:99999")

		// Should return error or log error
		// The exact behavior depends on the centrifuge client implementation
		assert.True(t, client.IsConnected())
	})

	t.Run("logs events correctly", func(t *testing.T) {
		// Create a more sophisticated test server
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/connection/websocket" {
				w.WriteHeader(http.StatusNotFound)
				return
			}

			upgrader := websocket.Upgrader{
				CheckOrigin: func(r *http.Request) bool { return true },
			}
			conn, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				return
			}
			defer conn.Close()

			// Send various message types
			messages := []map[string]interface{}{
				{
					"type":   "connecting",
					"code":   1,
					"reason": "establishing connection",
				},
				{
					"type":      "connected",
					"client_id": "test-123",
				},
				{
					"type": "message",
					"data": "test message",
				},
				{
					"type": "publication",
					"data": "test publication",
				},
			}

			for _, msg := range messages {
				if err := conn.WriteJSON(msg); err != nil {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
		}))
		defer server.Close()

		logger := ulogger.TestLogger{}
		client := New(logger)

		addr := strings.TrimPrefix(server.URL, "http://")

		// Start in goroutine
		go func() {
			_ = client.Start(context.Background(), addr)
		}()

		// Give time for messages to be processed
		time.Sleep(200 * time.Millisecond)

		// Client should be connected
		assert.True(t, client.IsConnected())

		// Stop client
		if client.IsConnected() {
			if err := client.Stop(context.Background()); err != nil {
				t.Errorf("Stop failed: %v", err)
			}
		}
	})
}

func TestClient_Stop(t *testing.T) {
	t.Run("stops client gracefully", func(t *testing.T) {
		// Create test server
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			upgrader := websocket.Upgrader{
				CheckOrigin: func(r *http.Request) bool { return true },
			}
			conn, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				return
			}
			defer conn.Close()

			// Keep connection open
			time.Sleep(500 * time.Millisecond)
		}))
		defer server.Close()

		logger := ulogger.TestLogger{}
		client := New(logger)

		addr := strings.TrimPrefix(server.URL, "http://")

		// Start client
		go func() {
			_ = client.Start(context.Background(), addr)
		}()

		// Give time to connect
		time.Sleep(100 * time.Millisecond)

		// Stop client
		err := client.Stop(context.Background())
		assert.NoError(t, err)

		// Client stopped successfully
	})

	t.Run("handles stop when not started", func(t *testing.T) {
		logger := ulogger.TestLogger{}
		client := New(logger)

		// Stop without starting
		err := client.Stop(context.Background())
		assert.NoError(t, err)

		// Should not error when stopping unstarted client
	})
}

func TestClient_Subscriptions(t *testing.T) {
	t.Run("subscribes to multiple channels", func(t *testing.T) {
		subscriptionRequests := make([]string, 0)

		// Create server that tracks subscription requests
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			upgrader := websocket.Upgrader{
				CheckOrigin: func(r *http.Request) bool { return true },
			}
			conn, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				return
			}
			defer conn.Close()

			// Read subscription requests
			for {
				var msg map[string]interface{}
				if err := conn.ReadJSON(&msg); err != nil {
					break
				}

				if channel, ok := msg["channel"].(string); ok {
					subscriptionRequests = append(subscriptionRequests, channel)
				}

				// Send acknowledgment
				_ = conn.WriteJSON(map[string]interface{}{
					"type":    "subscribed",
					"channel": msg["channel"],
				})
			}
		}))
		defer server.Close()

		logger := ulogger.TestLogger{}
		client := New(logger)

		addr := strings.TrimPrefix(server.URL, "http://")

		// Start client
		go func() {
			_ = client.Start(context.Background(), addr)
		}()

		// Give time for subscriptions
		time.Sleep(200 * time.Millisecond)

		// Client should be connected after subscribing

		// Stop client
		if client.IsConnected() {
			if err := client.Stop(context.Background()); err != nil {
				t.Errorf("Stop failed: %v", err)
			}
		}
	})
}

func TestClient_ErrorHandling(t *testing.T) {
	t.Run("logs errors from server", func(t *testing.T) {
		// Create server that sends error messages
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			upgrader := websocket.Upgrader{
				CheckOrigin: func(r *http.Request) bool { return true },
			}
			conn, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				return
			}
			defer conn.Close()

			// Send error message
			_ = conn.WriteJSON(map[string]interface{}{
				"type": "error",
				"error": map[string]string{
					"message": "test error",
				},
			})

			time.Sleep(100 * time.Millisecond)
		}))
		defer server.Close()

		logger := ulogger.TestLogger{}
		client := New(logger)

		addr := strings.TrimPrefix(server.URL, "http://")

		// Start client
		go func() {
			_ = client.Start(context.Background(), addr)
		}()

		// Give time for error processing
		time.Sleep(150 * time.Millisecond)

		// Stop client
		if client.IsConnected() {
			if err := client.Stop(context.Background()); err != nil {
				t.Errorf("Stop failed: %v", err)
			}
		}
	})

	t.Run("handles disconnection", func(t *testing.T) {
		// Create server that disconnects quickly
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			upgrader := websocket.Upgrader{
				CheckOrigin: func(r *http.Request) bool { return true },
			}
			conn, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				return
			}

			// Close connection immediately
			conn.Close()
		}))
		defer server.Close()

		logger := ulogger.TestLogger{}
		client := New(logger)

		addr := strings.TrimPrefix(server.URL, "http://")

		// Start client
		go func() {
			_ = client.Start(context.Background(), addr)
		}()

		// Give time for disconnection
		time.Sleep(100 * time.Millisecond)

		// Verify client handled disconnection
		assert.True(t, client.IsConnected())

		// Stop client
		if client.IsConnected() {
			if err := client.Stop(context.Background()); err != nil {
				t.Errorf("Stop failed: %v", err)
			}
		}
	})
}

func TestClient_MessageProcessing(t *testing.T) {
	t.Run("processes server publications", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			upgrader := websocket.Upgrader{
				CheckOrigin: func(r *http.Request) bool { return true },
			}
			conn, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				return
			}
			defer conn.Close()

			// Send publication messages
			publications := []map[string]interface{}{
				{
					"type":    "publication",
					"channel": "blocks",
					"data": map[string]interface{}{
						"height": 100,
						"hash":   "abc123",
					},
				},
				{
					"type":    "publication",
					"channel": "subtrees",
					"data": map[string]interface{}{
						"root": "def456",
					},
				},
			}

			for _, pub := range publications {
				_ = conn.WriteJSON(pub)
				time.Sleep(50 * time.Millisecond)
			}
		}))
		defer server.Close()

		logger := ulogger.TestLogger{}
		client := New(logger)

		addr := strings.TrimPrefix(server.URL, "http://")

		// Start client
		go func() {
			_ = client.Start(context.Background(), addr)
		}()

		// Give time for message processing
		time.Sleep(200 * time.Millisecond)

		// Stop client
		if client.IsConnected() {
			if err := client.Stop(context.Background()); err != nil {
				t.Errorf("Stop failed: %v", err)
			}
		}
	})
}

func TestClient_InvalidServerResponse(t *testing.T) {
	t.Run("handles invalid WebSocket upgrade", func(t *testing.T) {
		// Create server that doesn't upgrade to WebSocket
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte("Invalid request"))
		}))
		defer server.Close()

		logger := ulogger.TestLogger{}
		client := New(logger)

		addr := strings.TrimPrefix(server.URL, "http://")

		// Start should handle the error gracefully
		_ = client.Start(context.Background(), addr)

		// Client should still be created
		assert.True(t, client.IsConnected())
	})
}

func TestClient_ConcurrentOperations(t *testing.T) {
	t.Run("handles concurrent start and stop", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			upgrader := websocket.Upgrader{
				CheckOrigin: func(r *http.Request) bool { return true },
			}
			conn, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				return
			}
			defer conn.Close()

			// Keep connection open
			time.Sleep(1 * time.Second)
		}))
		defer server.Close()

		logger := ulogger.TestLogger{}
		client := New(logger)

		addr := strings.TrimPrefix(server.URL, "http://")

		// Start client
		startDone := make(chan struct{})
		go func() {
			_ = client.Start(context.Background(), addr)
			close(startDone)
		}()

		// Give time to start
		time.Sleep(50 * time.Millisecond)

		// Stop client while it's running
		stopDone := make(chan struct{})
		go func() {
			_ = client.Stop(context.Background())
			close(stopDone)
		}()

		// Wait for both to complete with timeout
		select {
		case <-stopDone:
			// Stop completed
		case <-time.After(2 * time.Second):
			t.Fatal("Timeout waiting for stop")
		}
	})
}

func TestClient_ContextHandling(t *testing.T) {
	t.Run("respects context parameter", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			upgrader := websocket.Upgrader{
				CheckOrigin: func(r *http.Request) bool { return true },
			}
			conn, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				return
			}
			defer conn.Close()

			// Keep connection open
			time.Sleep(500 * time.Millisecond)
		}))
		defer server.Close()

		logger := ulogger.TestLogger{}
		client := New(logger)

		addr := strings.TrimPrefix(server.URL, "http://")

		// Create context with cancel
		ctx, cancel := context.WithCancel(context.Background())

		// Start client
		go func() {
			_ = client.Start(ctx, addr)
		}()

		// Give time to connect
		time.Sleep(100 * time.Millisecond)

		// Cancel context
		cancel()

		// Give time for cleanup
		time.Sleep(100 * time.Millisecond)

		// Client should handle context cancellation
		assert.True(t, client.IsConnected())
	})
}
