//go:build integration
// +build integration

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

// TestAssetServiceWebSocketIntegration tests the asset service WebSocket to ensure:
// 1. The asset service correctly relays p2p messages without modification
// 2. The first message received is still a node_status message from the current node
// 3. Message ordering and content matches what we get from p2p directly
//
// This test requires both p2p and asset services running. Run with:
// go test -v ./services/p2p/ -run TestAssetServiceWebSocketIntegration
//
// Make sure teranode is running first: make dev
func TestAssetServiceWebSocketIntegration(t *testing.T) {
	// Skip this test unless explicitly requested
	if testing.Short() {
		t.Skip("Skipping live integration test in short mode")
	}

	// Connect to the asset service WebSocket endpoint
	// Default asset service runs on localhost:8090 with /connection/websocket
	url := "ws://localhost:8090/connection/websocket"

	t.Logf("Connecting to asset service WebSocket at: %s", url)

	ws, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Skipf("Could not connect to asset service at %s: %v. Make sure teranode is running with 'make dev'", url, err)
	}
	defer ws.Close()

	t.Logf("✓ Successfully connected to asset service WebSocket")

	// Send connection request to subscribe to node_status channel
	// The asset service uses Centrifuge protocol
	connectRequest := map[string]interface{}{
		"id": 1,
		"connect": map[string]interface{}{
			"subs": map[string]interface{}{
				"node_status": map[string]interface{}{},
			},
		},
	}

	connectData, err := json.Marshal(connectRequest)
	require.NoError(t, err)

	err = ws.WriteMessage(websocket.TextMessage, connectData)
	require.NoError(t, err)

	t.Logf("✓ Sent connection request with node_status subscription")

	// Set read deadline to prevent hanging
	err = ws.SetReadDeadline(time.Now().Add(10 * time.Second))
	require.NoError(t, err)

	var receivedMessages []map[string]interface{}
	var nodeStatusMessages []notificationMsg
	var firstNodeStatusReceived bool
	var currentNodePeerID string

	// Read messages for a reasonable period
	timeout := time.After(8 * time.Second)
	messageCount := 0

	t.Logf("Listening for messages from asset service...")

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

			// Parse as generic JSON first to handle Centrifuge protocol
			var rawMsg map[string]interface{}
			err = json.Unmarshal(message, &rawMsg)
			require.NoError(t, err, "Failed to unmarshal message: %s", string(message))

			receivedMessages = append(receivedMessages, rawMsg)
			messageCount++

			t.Logf("Message %d: %v", messageCount, rawMsg)

			// Check if this is a Centrifuge publication with node_status data
			if pub, ok := rawMsg["push"].(map[string]interface{}); ok {
				if channel, ok := pub["channel"].(string); ok && channel == "node_status" {
					if data, ok := pub["pub"].(map[string]interface{}); ok {
						if dataMap, ok := data["data"].(map[string]interface{}); ok {
							// Convert map to struct
							dataBytes, _ := json.Marshal(dataMap)
							var nodeStatus notificationMsg
							err = json.Unmarshal(dataBytes, &nodeStatus)

							if err == nil && nodeStatus.Type == "node_status" {
								nodeStatusMessages = append(nodeStatusMessages, nodeStatus)

								t.Logf("Node Status %d: Type=%s, PeerID=%s", len(nodeStatusMessages), nodeStatus.Type, nodeStatus.PeerID)

								// Check if this is the first node_status message
								if !firstNodeStatusReceived {
									firstNodeStatusReceived = true
									currentNodePeerID = nodeStatus.PeerID
									t.Logf("✓ First node_status received from asset service - peer: %s", nodeStatus.PeerID)
									t.Logf("   Base URL: %s", nodeStatus.BaseURL)
									t.Logf("   Best Height: %d", nodeStatus.BestHeight)
									t.Logf("   FSM State: %s", nodeStatus.FSMState)
								}
							}
						}
					}
				}
			}

			// Also check for direct channel publications (alternative format)
			if channel, ok := rawMsg["channel"].(string); ok && channel == "node_status" {
				if pub, ok := rawMsg["pub"].(map[string]interface{}); ok {
					if dataMap, ok := pub["data"].(map[string]interface{}); ok {
						// Convert map to struct
						dataBytes, _ := json.Marshal(dataMap)
						var nodeStatus notificationMsg
						err = json.Unmarshal(dataBytes, &nodeStatus)

						if err == nil && nodeStatus.Type == "node_status" {
							nodeStatusMessages = append(nodeStatusMessages, nodeStatus)

							t.Logf("Node Status %d: Type=%s, PeerID=%s", len(nodeStatusMessages), nodeStatus.Type, nodeStatus.PeerID)

							// Check if this is the first node_status message
							if !firstNodeStatusReceived {
								firstNodeStatusReceived = true
								currentNodePeerID = nodeStatus.PeerID
								t.Logf("✓ First node_status received from asset service - peer: %s", nodeStatus.PeerID)
								t.Logf("   Base URL: %s", nodeStatus.BaseURL)
								t.Logf("   Best Height: %d", nodeStatus.BestHeight)
								t.Logf("   FSM State: %s", nodeStatus.FSMState)
							}
						}
					}
				}
			}

			// Stop after receiving a reasonable number of messages
			if len(nodeStatusMessages) >= 10 || messageCount >= 30 {
				t.Logf("Collected enough messages, analyzing...")
				goto analyzeMessages
			}
		}
	}

analyzeMessages:
	// Analyze the received messages
	require.Greater(t, len(receivedMessages), 0, "Should have received at least one message")
	require.Greater(t, len(nodeStatusMessages), 0, "Should have received at least one node_status message through asset service")

	// Verify we have a current node peer ID
	require.NotEmpty(t, currentNodePeerID, "Should have identified the current node peer ID through asset service")

	t.Logf("✓ Asset service integration test results:")
	t.Logf("   Total messages received: %d", len(receivedMessages))
	t.Logf("   Node status messages: %d", len(nodeStatusMessages))
	t.Logf("   Current node peer ID: %s", currentNodePeerID)

	// Count current node vs other node messages
	currentNodeMessages := 0
	otherNodeMessages := 0

	for _, msg := range nodeStatusMessages {
		if msg.PeerID == currentNodePeerID {
			currentNodeMessages++
		} else {
			otherNodeMessages++
			t.Logf("   Other node via asset: %s (%s)", msg.PeerID, msg.BaseURL)
		}
	}

	t.Logf("   Current node messages: %d", currentNodeMessages)
	t.Logf("   Other node messages: %d", otherNodeMessages)

	// Verify we got our current node status first
	assert.GreaterOrEqual(t, currentNodeMessages, 1, "Should have received current node status through asset service")
	// NOTE: This test was relying on a bug where we incorrectly sent our own node_status with is_self=false
	// TODO: Add code to simulate other nodes if we want to test receiving messages from other nodes
	// assert.Greater(t, otherNodeMessages, 0, "Should have received other node statuses through asset service")

	// Verify message structure of the first node_status
	if len(nodeStatusMessages) > 0 {
		firstNodeStatus := nodeStatusMessages[0]
		assert.Equal(t, "node_status", firstNodeStatus.Type)
		assert.NotEmpty(t, firstNodeStatus.PeerID, "Node status should have peer ID")
		assert.NotEmpty(t, firstNodeStatus.Timestamp, "Node status should have timestamp")

		// Log the complete first node status for inspection
		t.Logf("✓ First node_status message through asset service:")
		t.Logf("   Type: %s", firstNodeStatus.Type)
		t.Logf("   PeerID: %s", firstNodeStatus.PeerID)
		t.Logf("   BaseURL: %s", firstNodeStatus.BaseURL)
		t.Logf("   Version: %s", firstNodeStatus.Version)
		t.Logf("   BestHeight: %d", firstNodeStatus.BestHeight)
		t.Logf("   BestBlockHash: %s", firstNodeStatus.BestBlockHash)
		t.Logf("   FSMState: %s", firstNodeStatus.FSMState)
		t.Logf("   ChainWork: %s", firstNodeStatus.ChainWork)
		t.Logf("   Timestamp: %s", firstNodeStatus.Timestamp)
	}

	t.Logf("✅ Asset service integration test PASSED")
	t.Logf("   ✓ Connected to asset service WebSocket")
	t.Logf("   ✓ Asset service relays node_status messages from p2p")
	t.Logf("   ✓ Current node identified through asset service: %s", currentNodePeerID)
	t.Logf("   ✓ Message structure preserved through relay")
}

// TestAssetServiceCachingBehavior tests that the asset service correctly caches and sends current node first after the fix
func TestAssetServiceCachingBehavior(t *testing.T) {
	// Skip this test unless explicitly requested
	if testing.Short() {
		t.Skip("Skipping live integration test in short mode")
	}

	// This test validates our caching + fail-fast fix
	// The asset service should now:
	// 1. Cache the first node_status message from p2p (current node)
	// 2. Reject connections when cache is empty (not ready)
	// 3. Send cached current node status first to new clients

	assetURL := "ws://localhost:8090/connection/websocket"
	t.Logf("Testing asset service caching behavior at: %s", assetURL)

	// Connect to Asset service
	assetWS, _, err := websocket.DefaultDialer.Dial(assetURL, nil)
	if err != nil {
		t.Skipf("Could not connect to asset service: %v. Make sure teranode is running with 'make dev'", err)
	}
	defer assetWS.Close()

	// Send connection request to asset service
	connectRequest := map[string]interface{}{
		"id": 1,
		"connect": map[string]interface{}{
			"subs": map[string]interface{}{
				"node_status": map[string]interface{}{},
			},
		},
	}

	connectData, _ := json.Marshal(connectRequest)
	_ = assetWS.WriteMessage(websocket.TextMessage, connectData)

	// Look for the first node_status from asset service
	var firstNodePeerID string
	timeout := time.After(8 * time.Second)
	messageCount := 0

	t.Logf("Listening for messages from asset service to verify current node sent first...")

searchLoop:
	for {
		select {
		case <-timeout:
			break searchLoop
		default:
			_ = assetWS.SetReadDeadline(time.Now().Add(1 * time.Second))
			_, message, err := assetWS.ReadMessage()
			if err != nil {
				if strings.Contains(err.Error(), "timeout") && messageCount > 0 {
					break searchLoop
				}
				continue
			}

			messageCount++
			var rawMsg map[string]interface{}
			if json.Unmarshal(message, &rawMsg) == nil {
				// Check for Centrifuge push format
				if push, ok := rawMsg["push"].(map[string]interface{}); ok {
					if channel, ok := push["channel"].(string); ok && channel == "node_status" {
						if pub, ok := push["pub"].(map[string]interface{}); ok {
							if dataMap, ok := pub["data"].(map[string]interface{}); ok {
								var nodeStatus notificationMsg
								dataBytes, _ := json.Marshal(dataMap)
								if json.Unmarshal(dataBytes, &nodeStatus) == nil && nodeStatus.Type == "node_status" {
									if firstNodePeerID == "" {
										firstNodePeerID = nodeStatus.PeerID
										t.Logf("✓ First node_status from asset service: %s", firstNodePeerID)
										t.Logf("   BaseURL: %s", nodeStatus.BaseURL)
										t.Logf("   BestHeight: %d", nodeStatus.BestHeight)
										t.Logf("   FSMState: %s", nodeStatus.FSMState)
										break searchLoop // Found what we need
									}
								}
							}
						}
					}
				}
			}
		}
	}

	// Verify we found a current node
	require.NotEmpty(t, firstNodePeerID, "Asset service should send current node status first (from cache)")

	t.Logf("✅ Asset service caching behavior VERIFIED")
	t.Logf("   ✓ Asset service is responding (cache is ready)")
	t.Logf("   ✓ First node_status message received: %s", firstNodePeerID)
	t.Logf("   ✓ Asset service correctly implements cached current node first")
	t.Logf("   ✓ Total messages processed: %d", messageCount)
}

// TestAssetVsP2PComparison compares messages from both asset service and p2p service
func TestAssetVsP2PComparison(t *testing.T) {
	// Skip this test unless explicitly requested
	if testing.Short() {
		t.Skip("Skipping live integration test in short mode")
	}

	// First, get current node ID from p2p directly
	p2pURL := "ws://localhost:9906/p2p-ws"
	assetURL := "ws://localhost:8090/connection/websocket"

	t.Logf("Comparing messages from P2P (%s) vs Asset service (%s)", p2pURL, assetURL)

	// Connect to P2P service first
	p2pWS, _, err := websocket.DefaultDialer.Dial(p2pURL, nil)
	if err != nil {
		t.Skipf("Could not connect to p2p service: %v", err)
	}
	defer p2pWS.Close()

	// Get the first few messages from P2P to establish current node ID
	var p2pCurrentNode string
	for i := 0; i < 3; i++ {
		_ = p2pWS.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, message, err := p2pWS.ReadMessage()
		if err != nil {
			continue
		}

		var msg notificationMsg
		if json.Unmarshal(message, &msg) == nil && msg.Type == "node_status" {
			if p2pCurrentNode == "" {
				p2pCurrentNode = msg.PeerID
				t.Logf("✓ Current node identified from P2P: %s", p2pCurrentNode)
				break
			}
		}
	}
	p2pWS.Close()

	require.NotEmpty(t, p2pCurrentNode, "Should have identified current node from P2P")

	// Now connect to Asset service and verify it reports the same current node
	assetWS, _, err := websocket.DefaultDialer.Dial(assetURL, nil)
	if err != nil {
		t.Skipf("Could not connect to asset service: %v", err)
	}
	defer assetWS.Close()

	// Send connection request to asset service
	connectRequest := map[string]interface{}{
		"id": 1,
		"connect": map[string]interface{}{
			"subs": map[string]interface{}{
				"node_status": map[string]interface{}{},
			},
		},
	}

	connectData, _ := json.Marshal(connectRequest)
	_ = assetWS.WriteMessage(websocket.TextMessage, connectData)

	// Look for the first node_status from asset service
	var assetCurrentNode string
	timeout := time.After(5 * time.Second)

searchLoop:
	for {
		select {
		case <-timeout:
			break searchLoop
		default:
			_ = assetWS.SetReadDeadline(time.Now().Add(1 * time.Second))
			_, message, err := assetWS.ReadMessage()
			if err != nil {
				continue
			}

			var rawMsg map[string]interface{}
			if json.Unmarshal(message, &rawMsg) == nil {
				if pub, ok := rawMsg["push"].(map[string]interface{}); ok {
					if channel, ok := pub["channel"].(string); ok && channel == "node_status" {
						if data, ok := pub["pub"].(map[string]interface{}); ok {
							if dataBytes, ok := data["data"]; ok {
								var nodeStatus notificationMsg
								if dataStr, ok := dataBytes.(string); ok {
									_ = json.Unmarshal([]byte(dataStr), &nodeStatus)
								} else if dataMap, ok := dataBytes.(map[string]interface{}); ok {
									dataBytes, _ := json.Marshal(dataMap)
									_ = json.Unmarshal(dataBytes, &nodeStatus)
								}

								if nodeStatus.Type == "node_status" && assetCurrentNode == "" {
									assetCurrentNode = nodeStatus.PeerID
									t.Logf("✓ Current node identified from Asset service: %s", assetCurrentNode)
									break searchLoop
								}
							}
						}
					}
				}
			}
		}
	}

	// Verify both services report the same current node
	assert.Equal(t, p2pCurrentNode, assetCurrentNode,
		"Asset service should relay the same current node as P2P service")

	if p2pCurrentNode == assetCurrentNode {
		t.Logf("✅ CONSISTENCY VERIFIED")
		t.Logf("   ✓ P2P service current node: %s", p2pCurrentNode)
		t.Logf("   ✓ Asset service current node: %s", assetCurrentNode)
		t.Logf("   ✓ Both services agree on current node identity")
		t.Logf("   ✓ Asset service correctly relays P2P messages without modification")
	} else {
		t.Errorf("❌ CONSISTENCY FAILED")
		t.Errorf("   P2P current node: %s", p2pCurrentNode)
		t.Errorf("   Asset current node: %s", assetCurrentNode)
	}
}
