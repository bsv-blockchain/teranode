// Package p2p provides peer-to-peer networking functionality for the Teranode system.
package p2p

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLiveP2PWebSocketIntegration tests the real p2p WebSocket service to ensure:
// 1. We can connect to the live p2p service
// 2. The first message received is a node_status message from the current node
// 3. Subsequent messages are from other nodes in the network
//
// This test requires a running p2p service. Run with:
// go test -v ./services/p2p/ -run TestLiveP2PWebSocketIntegration
//
// Make sure teranode is running first: make dev
func TestLiveP2PWebSocketIntegration(t *testing.T) {
	t.SkipNow()

	// Skip this test unless explicitly requested
	if testing.Short() {
		t.Skip("Skipping live integration test in short mode")
	}

	// Connect to the live p2p WebSocket service
	// Default p2p service runs on localhost:9906
	url := "ws://localhost:9906/p2p-ws"

	t.Logf("Connecting to live p2p service at: %s", url)

	ws, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Skipf("Could not connect to p2p service at %s: %v. Make sure teranode is running with 'make dev'", url, err)
	}
	defer ws.Close()

	t.Logf("✓ Successfully connected to p2p WebSocket")

	// Set read deadline to prevent hanging
	err = ws.SetReadDeadline(time.Now().Add(10 * time.Second))
	require.NoError(t, err)

	var receivedMessages []notificationMsg
	var firstNodeStatusReceived bool
	var currentNodePeerID string

	// Read messages for a reasonable period
	timeout := time.After(8 * time.Second)
	messageCount := 0

	t.Logf("Listening for messages...")

	for {
		select {
		case <-timeout:
			t.Logf("Timeout reached, analyzing %d received messages", messageCount)
			goto analyzeMessages
		default:
			// Try to read a message with a short timeout
			_ = ws.SetReadDeadline(time.Now().Add(1 * time.Second))
			messageType, message, err := ws.ReadMessage()

			if err != nil {
				// Check if it's a timeout error
				if strings.Contains(err.Error(), "timeout") {
					// No more messages for now, analyze what we have if we got some
					if messageCount > 0 {
						t.Logf("No more messages, analyzing %d received messages", messageCount)
						goto analyzeMessages
					}
					continue
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
				currentNodePeerID = received.PeerID
				t.Logf("✓ First node_status received from peer: %s", received.PeerID)
				t.Logf("   Base URL: %s", received.BaseURL)
				t.Logf("   Best Height: %d", received.BestHeight)
				t.Logf("   FSM State: %s", received.FSMState)
			}

			// Stop after receiving a reasonable number of messages
			if messageCount >= 20 {
				t.Logf("Collected %d messages, analyzing...", messageCount)
				goto analyzeMessages
			}
		}
	}

analyzeMessages:
	// Analyze the received messages
	require.Greater(t, len(receivedMessages), 0, "Should have received at least one message")

	// Find all node_status messages
	var nodeStatusMessages []notificationMsg
	for _, msg := range receivedMessages {
		if msg.Type == "node_status" {
			nodeStatusMessages = append(nodeStatusMessages, msg)
		}
	}

	require.Greater(t, len(nodeStatusMessages), 0, "Should have received at least one node_status message")

	// Verify the first message overall is a node_status
	firstMessage := receivedMessages[0]
	assert.Equal(t, "node_status", firstMessage.Type, "First message should be node_status from current node")

	// Verify we have a current node peer ID
	require.NotEmpty(t, currentNodePeerID, "Should have identified the current node peer ID")

	t.Logf("✓ Live integration test results:")
	t.Logf("   Total messages received: %d", len(receivedMessages))
	t.Logf("   Node status messages: %d", len(nodeStatusMessages))
	t.Logf("   Current node peer ID: %s", currentNodePeerID)
	t.Logf("   First message type: %s", firstMessage.Type)

	// Count current node vs other node messages
	currentNodeMessages := 0
	otherNodeMessages := 0

	for _, msg := range nodeStatusMessages {
		if msg.PeerID == currentNodePeerID {
			currentNodeMessages++
		} else {
			otherNodeMessages++
			t.Logf("   Other node: %s (%s)", msg.PeerID, msg.BaseURL)
		}
	}

	t.Logf("   Current node messages: %d", currentNodeMessages)
	t.Logf("   Other node messages: %d", otherNodeMessages)

	// Verify we got our current node status
	assert.GreaterOrEqual(t, currentNodeMessages, 1, "Should have received current node status")

	// Verify message structure of the first node_status
	firstNodeStatus := nodeStatusMessages[0]
	assert.Equal(t, "node_status", firstNodeStatus.Type)
	assert.NotEmpty(t, firstNodeStatus.PeerID, "Node status should have peer ID")
	assert.NotEmpty(t, firstNodeStatus.Timestamp, "Node status should have timestamp")

	// Log the complete first node status for inspection
	t.Logf("✓ First node_status message structure:")
	t.Logf("   Type: %s", firstNodeStatus.Type)
	t.Logf("   PeerID: %s", firstNodeStatus.PeerID)
	t.Logf("   BaseURL: %s", firstNodeStatus.BaseURL)
	t.Logf("   Version: %s", firstNodeStatus.Version)
	t.Logf("   BestHeight: %d", firstNodeStatus.BestHeight)
	t.Logf("   BestBlockHash: %s", firstNodeStatus.BestBlockHash)
	t.Logf("   FSMState: %s", firstNodeStatus.FSMState)
	t.Logf("   ChainWork: %s", firstNodeStatus.ChainWork)
	t.Logf("   Timestamp: %s", firstNodeStatus.Timestamp)

	t.Logf("✅ Live integration test PASSED")
	t.Logf("   ✓ Connected to live p2p service")
	t.Logf("   ✓ First message is node_status (current node)")
	t.Logf("   ✓ Current node identified: %s", currentNodePeerID)
	t.Logf("   ✓ Message structure is correct")
}

// TestLiveP2PWebSocketOrdering specifically tests the message ordering
func TestLiveP2PWebSocketOrdering(t *testing.T) {
	// Skip this test unless explicitly requested
	if testing.Short() {
		t.Skip("Skipping live integration test in short mode")
	}

	// Connect to the live p2p WebSocket service
	url := "ws://localhost:9906/p2p-ws"

	ws, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Skipf("Could not connect to p2p service at %s: %v", url, err)
	}
	defer ws.Close()

	// Collect just the first few messages to verify ordering
	var messages []notificationMsg

	// Read exactly the first 5 messages
	for i := 0; i < 5; i++ {
		_ = ws.SetReadDeadline(time.Now().Add(3 * time.Second))
		_, message, err := ws.ReadMessage()
		if err != nil {
			if i == 0 {
				t.Fatalf("Failed to read first message: %v", err)
			}
			break // Timeout or error, analyze what we have
		}

		var msg notificationMsg
		err = json.Unmarshal(message, &msg)
		require.NoError(t, err)

		messages = append(messages, msg)
		t.Logf("Message %d: Type=%s, PeerID=%s", i+1, msg.Type, msg.PeerID)
	}

	require.Greater(t, len(messages), 0, "Should have received at least one message")

	// The critical test: first message should be node_status
	firstMsg := messages[0]
	assert.Equal(t, "node_status", firstMsg.Type,
		"CRITICAL: First message must be node_status from current node")

	// If we got multiple node_status messages, the first should be the current node
	nodeStatusCount := 0
	for _, msg := range messages {
		if msg.Type == "node_status" {
			nodeStatusCount++
		}
	}

	t.Logf("✓ Ordering verification:")
	t.Logf("   First message type: %s ✓", firstMsg.Type)
	t.Logf("   First message peer: %s", firstMsg.PeerID)
	t.Logf("   Total node_status messages: %d", nodeStatusCount)

	if firstMsg.Type == "node_status" {
		t.Logf("✅ PASS: First message is correctly node_status from current node")
	} else {
		t.Errorf("❌ FAIL: First message should be node_status, got: %s", firstMsg.Type)
	}
}
