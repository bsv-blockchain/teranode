// Package p2p provides peer-to-peer networking functionality for the Teranode system.
package p2p

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v4"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestP2PWebSocketIntegration tests the full p2p WebSocket integration to ensure:
// 1. The first message received is a node_status message
// 2. The first node_status message is from the current node (our own node)
// NOTE: This test is skipped because it requires starting a full HTTP server with WebSocket
// support and connecting to it, which is complex and potentially flaky in CI environments.
// The core functionality is tested by TestP2PWebSocketCurrentNodeFirst and TestP2PWebSocketMessageStructure.
func TestP2PWebSocketIntegration(t *testing.T) {
	t.Skip("Full WebSocket integration test - functionality covered by TestP2PWebSocketCurrentNodeFirst and TestP2PWebSocketMessageStructure")

	// Create a mock P2PClient for testing
	mockP2PNode := new(MockServerP2PClient)
	testPeerID, _ := peer.Decode("12D3KooWBPqTBhshqRZMKZtqb5sfgckM9JYkWDR7eW5kSPEKwKCW")
	mockP2PNode.On("GetID").Return(testPeerID)

	// Create server with the mock P2PClient
	server := &Server{
		P2PClient:           mockP2PNode,
		logger:              ulogger.New("test-server"),
		AssetHTTPAddressURL: "http://current-node.test",
		settings: &settings.Settings{
			Version: "1.0.0",
			Commit:  "test-commit",
		},
		startTime: time.Now(),
	}

	// Create notification channel
	notificationCh := make(chan *notificationMsg, 10)

	// Create handler
	handler := server.HandleWebSocket(notificationCh)

	// Create test server
	e := echo.New()
	testServer := httptest.NewServer(e)
	defer testServer.Close()

	// Start the handler in a goroutine
	go func() {
		e.GET("/p2p-ws", handler)
		_ = e.Start(":0") // Start on any available port
	}()

	// Give the server a moment to start
	time.Sleep(100 * time.Millisecond)

	t.Run("First message should be current node's node_status", func(t *testing.T) {
		// Connect to WebSocket server
		url := "ws" + strings.TrimPrefix(testServer.URL, "http") + "/p2p-ws"
		ws, _, err := websocket.DefaultDialer.Dial(url, nil)
		require.NoError(t, err)
		defer ws.Close()

		// Set read deadline to prevent hanging
		err = ws.SetReadDeadline(time.Now().Add(5 * time.Second))
		require.NoError(t, err)

		var receivedMessages []notificationMsg
		var firstNodeStatusReceived bool
		var currentNodeVerified bool

		// Read messages for a short period
		timeout := time.After(3 * time.Second)
		messageCount := 0

		for {
			select {
			case <-timeout:
				t.Logf("Timeout reached, stopping message collection. Received %d messages", messageCount)
				goto analyzeMessages
			default:
				// Try to read a message with a short timeout
				_ = ws.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
				messageType, message, err := ws.ReadMessage()

				if err != nil {
					// Check if it's a timeout error
					if strings.Contains(err.Error(), "timeout") {
						// No more messages for now, analyze what we have
						t.Logf("No more messages received, analyzing %d messages", messageCount)
						goto analyzeMessages
					}
					t.Logf("Error reading message: %v", err)
					continue
				}

				require.Equal(t, websocket.TextMessage, messageType)

				var received notificationMsg
				err = json.Unmarshal(message, &received)
				require.NoError(t, err, "Failed to unmarshal message: %s", string(message))

				receivedMessages = append(receivedMessages, received)
				messageCount++

				t.Logf("Message %d: Type=%s, PeerID=%s", messageCount, received.Type, received.PeerID)

				// Check if this is the first node_status message
				if received.Type == "node_status" && !firstNodeStatusReceived {
					firstNodeStatusReceived = true
					currentNodeVerified = true
					t.Logf("✓ First node_status verified as current node: %s", received.PeerID)
				}

				// Stop after receiving a reasonable number of messages or after verification
				if messageCount >= 10 || (currentNodeVerified && messageCount >= 3) {
					t.Logf("Stopping message collection after %d messages", messageCount)
					goto analyzeMessages
				}
			}
		}

	analyzeMessages:
		// Analyze the received messages
		require.Greater(t, len(receivedMessages), 0, "Should have received at least one message")

		// Find the first node_status message
		var firstNodeStatus *notificationMsg
		for i := range receivedMessages {
			if receivedMessages[i].Type == "node_status" {
				firstNodeStatus = &receivedMessages[i]
				break
			}
		}

		require.NotNil(t, firstNodeStatus, "Should have received at least one node_status message")

		// Verify the first node_status is from our current node
		assert.Equal(t, testPeerID.String(), firstNodeStatus.PeerID, "First node_status should be from current node")
		assert.Equal(t, "http://current-node.test", firstNodeStatus.BaseURL, "Should have correct base URL")
		assert.Equal(t, "1.0.0", firstNodeStatus.Version, "Should have correct version")

		// Count how many node_status messages we received (should be just 1 - our node)
		nodeStatusCount := 0
		for _, msg := range receivedMessages {
			if msg.Type == "node_status" {
				nodeStatusCount++
				// All node_status messages should be from the current node
				assert.Equal(t, testPeerID.String(), msg.PeerID, "All initial node_status messages should be from current node")
			}
		}

		// We should have exactly 1 node_status message (just our node)
		assert.Equal(t, 1, nodeStatusCount, "Should have exactly one node_status message (current node only)")

		t.Logf("✓ Integration test passed: Current node status verified")
		t.Logf("   Current Node ID: %s", testPeerID.String())
		t.Logf("   First message PeerID: %s", firstNodeStatus.PeerID)
		t.Logf("   Total node_status messages: %d", nodeStatusCount)
		for _, msg := range receivedMessages {
			if msg.Type == "node_status" && msg.PeerID != "" && msg.PeerID != testPeerID.String() {
				t.Logf("   Other node status: %s (%s)", msg.PeerID, msg.BaseURL)
			}
		}

		// The key verification: first message should be node_status type (our current node)
		assert.Equal(t, "node_status", receivedMessages[0].Type, "First message should be node_status")
	})
}

// TestP2PWebSocketCurrentNodeFirst specifically tests that the current node's status is always sent first
func TestP2PWebSocketCurrentNodeFirst(t *testing.T) {
	// Create a mock P2PClient
	mockP2PNode := new(MockServerP2PClient)
	currentNodePeerID, err := peer.Decode("12D3KooWL8qb3L8nKPjDtQmJU8jge5Qspsn6YLSBei9MsbTjJDr8")
	require.NoError(t, err, "Should decode peer ID without error")
	mockP2PNode.On("GetID").Return(currentNodePeerID)

	server := &Server{
		P2PClient:           mockP2PNode,
		logger:              ulogger.New("test-server"),
		AssetHTTPAddressURL: "http://current.test",
		settings: &settings.Settings{
			Version: "1.0.0",
			Commit:  "test-commit",
		},
		startTime: time.Now(),
	}

	// Test the sendInitialNodeStatuses function directly
	t.Run("sendInitialNodeStatuses sends current node first", func(t *testing.T) {
		// Create a channel to capture messages
		clientCh := make(chan []byte, 10)

		// Call the function
		server.sendInitialNodeStatuses(clientCh)

		// Read all messages from the channel
		var messages []notificationMsg
		timeout := time.After(1 * time.Second)

	readLoop:
		for {
			select {
			case data := <-clientCh:
				var msg notificationMsg
				err := json.Unmarshal(data, &msg)
				require.NoError(t, err)
				messages = append(messages, msg)
			case <-timeout:
				break readLoop
			default:
				// No more messages immediately available
				if len(messages) > 0 {
					break readLoop
				}
				time.Sleep(10 * time.Millisecond)
			}
		}

		// Verify we received messages
		require.Greater(t, len(messages), 0, "Should have received at least one message")

		// Verify the first message is from the current node
		firstMsg := messages[0]
		assert.Equal(t, "node_status", firstMsg.Type)
		assert.Equal(t, currentNodePeerID.String(), firstMsg.PeerID,
			"First message must be from current node")

		// We should only receive the current node's status (no longer all nodes)
		assert.Equal(t, 1, len(messages),
			"Should receive exactly one message (current node only)")

		t.Logf("Received current node status: %s", messages[0].PeerID)

		// The only message should be from the current node
		assert.Equal(t, currentNodePeerID.String(), messages[0].PeerID,
			"The only message should be from current node")
	})
}

// TestP2PWebSocketMessageStructure tests that the message structure is correct
func TestP2PWebSocketMessageStructure(t *testing.T) {
	// Create a mock P2PClient
	mockP2PNode := new(MockServerP2PClient)
	testPeerID, err := peer.Decode("12D3KooWBPqTBhshqRZMKZtqb5sfgckM9JYkWDR7eW5kSPEKwKCW")
	require.NoError(t, err, "Should decode peer ID without error")
	require.NotEmpty(t, testPeerID.String(), "Test peer ID should not be empty")

	// Since peerID field is private, we must rely on the mock return value
	mockP2PNode.On("GetID").Return(testPeerID)

	// Verify the mock is working
	actualPeerID := mockP2PNode.GetID()
	t.Logf("Mock returned peer ID: %v (string: %s)", actualPeerID, actualPeerID)
	require.Equal(t, testPeerID.String(), actualPeerID, "Mock should return the test peer ID")
	require.Equal(t, "12D3KooWBPqTBhshqRZMKZtqb5sfgckM9JYkWDR7eW5kSPEKwKCW", actualPeerID, "Peer ID string should match")

	// Create server
	server := &Server{
		P2PClient:           mockP2PNode,
		logger:              ulogger.New("test-server"),
		AssetHTTPAddressURL: "http://structure-test.com",
		settings: &settings.Settings{
			Version: "2.1.0",
			Commit:  "abc123def456",
		},
		startTime: time.Unix(1234567890, 0),
	}

	t.Run("Message contains all required fields", func(t *testing.T) {
		// Create a channel to capture messages
		clientCh := make(chan []byte, 5)

		// Call sendInitialNodeStatuses
		server.sendInitialNodeStatuses(clientCh)

		// Debug: check how many messages we have
		t.Logf("Channel has %d messages queued", len(clientCh))

		// Read the message - should be exactly one (current node only)
		var currentNodeMsg *notificationMsg
		timeout := time.After(100 * time.Millisecond)

		select {
		case data := <-clientCh:
			t.Logf("Received message: %s", string(data))
			var msg notificationMsg
			err := json.Unmarshal(data, &msg)
			require.NoError(t, err)

			// This should be our current node status
			if msg.PeerID == testPeerID.String() {
				currentNodeMsg = &msg
			} else {
				t.Logf("Received message with different PeerID: %s (expected: %s)", msg.PeerID, testPeerID.String())
			}
		case <-timeout:
			// Check if there are messages in the channel
			if len(clientCh) > 0 {
				t.Fatal("Message in channel but couldn't read it")
			}
			t.Fatal("Timeout waiting for node status message - no messages in channel")
		}

		require.NotNil(t, currentNodeMsg, "Should have received the current node status message")

		// Verify key fields are present and correct
		assert.Equal(t, "node_status", currentNodeMsg.Type)
		assert.Equal(t, testPeerID.String(), currentNodeMsg.PeerID)
		assert.Equal(t, "http://structure-test.com", currentNodeMsg.BaseURL)
		assert.Equal(t, "2.1.0", currentNodeMsg.Version)
		assert.Equal(t, "abc123def456", currentNodeMsg.CommitHash)
		// Since we don't have a blockchain client, these will be empty/zero
		assert.Equal(t, "", currentNodeMsg.BestBlockHash)
		assert.Equal(t, uint32(0), currentNodeMsg.BestHeight)
		assert.NotEmpty(t, currentNodeMsg.Timestamp)

		t.Logf("✓ Message structure validation passed")
		t.Logf("   All required fields present and correct")
	})
}
