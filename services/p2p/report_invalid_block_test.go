package p2p

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/ulogger"
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
	mockNode := &MockServerP2PClient{peerID: testPeerID}

	// Create peer registry
	peerRegistry := NewPeerRegistry()

	// Create test server with minimal required fields
	server := &Server{
		blockPeerMap:   sync.Map{},
		logger:         ulogger.TestLogger{},
		notificationCh: make(chan *notificationMsg, 10),
		P2PClient:      mockNode,
		peerRegistry:   peerRegistry,
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
		peerRegistry:  peerRegistry,
	}

	// Test case 1: Successful report
	t.Run("successful report", func(t *testing.T) {
		blockHash := "0000000000000000000000000000000000000000000000000000000000000000"
		peerIDStr := testPeerID.String()

		// Register the peer in the peer registry using the string representation
		// Note: ReportInvalidBlock uses peer.ID(string) which is the raw string cast,
		// so we need to register with the same key it will look up
		peerRegistry.Put(peer.ID(peerIDStr), "", 0, nil, "")

		// Store the peer ID in the blockPeerMap with timestamp
		entry := peerMapEntry{
			peerID:    peerIDStr,
			timestamp: time.Now(),
		}
		server.blockPeerMap.Store(blockHash, entry)

		err := server.ReportInvalidBlock(ctx, blockHash, "test reason")
		require.NoError(t, err)

		// Verify the block was removed from the map
		_, exists := server.blockPeerMap.Load(blockHash)
		assert.False(t, exists, "Block should be removed from map after reporting")

		// Verify the peer was marked as having malicious interaction
		peerInfo, found := peerRegistry.Get(peer.ID(peerIDStr))
		require.True(t, found, "Peer should exist in registry")
		assert.Equal(t, int64(1), peerInfo.MaliciousCount, "Peer should have 1 malicious interaction recorded")
	})

	// Test case 2: Block not found
	t.Run("block not found", func(t *testing.T) {
		blockHash := "nonexistent-block-hash"

		err := server.ReportInvalidBlock(ctx, blockHash, "test reason")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no peer found for block", "error should indicate block not found")
	})

	// Test case 3: Invalid peer ID in map
	t.Run("invalid peer ID type", func(t *testing.T) {
		invalidBlockHash := "invalid-peer-block"
		// Store an actual invalid type (int) instead of peerMapEntry
		server.blockPeerMap.Store(invalidBlockHash, 123)

		err := server.ReportInvalidBlock(ctx, invalidBlockHash, "test reason")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "peer entry for block", "error should indicate invalid peer entry type")

		// Cleanup
		server.blockPeerMap.Delete(invalidBlockHash)
	})
}

// TestPeerMapCleanup tests the cleanup mechanism for peer maps
func TestPeerMapCleanup(t *testing.T) {
	// Create test server with minimal required fields
	server := &Server{
		blockPeerMap:   sync.Map{},
		subtreePeerMap: sync.Map{},
		logger:         ulogger.TestLogger{},
		peerMapMaxSize: 5,                      // Small size for testing
		peerMapTTL:     100 * time.Millisecond, // Short TTL for testing
	}

	// Test TTL-based cleanup
	t.Run("TTL cleanup", func(t *testing.T) {
		// Add entries with old timestamps
		oldEntry := peerMapEntry{
			peerID:    "old-peer",
			timestamp: time.Now().Add(-200 * time.Millisecond), // Older than TTL
		}
		server.blockPeerMap.Store("old-block", oldEntry)

		// Add entries with recent timestamps
		newEntry := peerMapEntry{
			peerID:    "new-peer",
			timestamp: time.Now(),
		}
		server.blockPeerMap.Store("new-block", newEntry)

		// Run cleanup
		server.cleanupPeerMaps()

		// Check that old entry was removed
		_, exists := server.blockPeerMap.Load("old-block")
		assert.False(t, exists, "Old entry should be removed")

		// Check that new entry still exists
		_, exists = server.blockPeerMap.Load("new-block")
		assert.True(t, exists, "New entry should still exist")
	})

	// Test size-based cleanup
	t.Run("Size limit cleanup", func(t *testing.T) {
		// Clear the map first
		server.blockPeerMap.Range(func(key, value interface{}) bool {
			server.blockPeerMap.Delete(key)
			return true
		})

		// Add more entries than the limit
		for i := 0; i < 10; i++ {
			entry := peerMapEntry{
				peerID:    string(rune('a' + i)),
				timestamp: time.Now().Add(time.Duration(i) * time.Second),
			}
			server.blockPeerMap.Store(string(rune('a'+i)), entry)
		}

		// Run cleanup
		server.cleanupPeerMaps()

		// Count remaining entries
		count := 0
		server.blockPeerMap.Range(func(key, value interface{}) bool {
			count++
			return true
		})

		assert.Equal(t, server.peerMapMaxSize, count, "Map should be reduced to max size")
	})
}
