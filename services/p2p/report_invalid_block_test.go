package p2p

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestReportInvalidBlock tests the ReportInvalidBlock functionality
func TestReportInvalidBlock(t *testing.T) {
	// Setup test context
	ctx := context.Background()

	// Create a test peer ID
	testPeerID, err := peer.Decode("12D3KooWJpBNhwgvoZ15EB1JwRTRpxgM9NVaqpDtWZXfTf6CpCQd")
	require.NoError(t, err)

	// Create a mock P2P node with the test peer ID
	mockNode := &MockServerP2PNode{peerID: testPeerID}

	// Create test server with minimal required fields
	server := &Server{
		blockPeerMap:   sync.Map{},
		logger:         ulogger.TestLogger{},
		notificationCh: make(chan *notificationMsg, 10),
		P2PNode:        mockNode,
	}

	// Initialize ban manager with test handler
	server.banManager = &PeerBanManager{
		ctx:           ctx,
		peerBanScores: make(map[string]*BanScore),
		reasonPoints: map[BanReason]int{
			ReasonInvalidBlock: 10,
		},
		banThreshold:  100,
		banDuration:   time.Hour,
		decayInterval: time.Hour,
		decayAmount:   1,
		handler:       &myBanEventHandler{server: server},
	}

	// Test case 1: Successful report
	t.Run("successful report", func(t *testing.T) {
		blockHash := "0000000000000000000000000000000000000000000000000000000000000000"
		peerID := "test-peer-1"

		// Store the peer ID in the blockPeerMap
		server.blockPeerMap.Store(blockHash, peerID)

		err := server.ReportInvalidBlock(ctx, blockHash, "test reason")
		require.NoError(t, err)

		// Verify the block was removed from the map
		_, exists := server.blockPeerMap.Load(blockHash)
		assert.False(t, exists, "Block should be removed from map after reporting")
	})

	// Test case 2: Block not found
	t.Run("block not found", func(t *testing.T) {
		blockHash := "nonexistent-block-hash"

		err := server.ReportInvalidBlock(ctx, blockHash, "test reason")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no peer found for invalid block", "error should indicate block not found")
	})

	// Test case 3: Invalid peer ID in map
	t.Run("invalid peer ID type", func(t *testing.T) {
		invalidBlockHash := "invalid-peer-block"
		// Store an actual invalid type (int) instead of string
		server.blockPeerMap.Store(invalidBlockHash, 123)

		err := server.ReportInvalidBlock(ctx, invalidBlockHash, "test reason")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "peer ID for block", "error should indicate invalid peer ID type")

		// Cleanup
		server.blockPeerMap.Delete(invalidBlockHash)
	})
}
