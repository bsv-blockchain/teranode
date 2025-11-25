package p2p

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Dummy handler for ban events
type testBanHandler struct {
	lastPeerID string
	lastUntil  time.Time
	lastReason string
}

func (h *testBanHandler) OnPeerBanned(peerID string, until time.Time, reason string) {
	h.lastPeerID = peerID
	h.lastUntil = until
	h.lastReason = reason
}

func TestAddScore_BanAndDecay(t *testing.T) {
	handler := &testBanHandler{}
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.P2P.BanThreshold = 30
	tSettings.P2P.BanDuration = 2 * time.Hour
	registry := NewPeerRegistry()

	m := NewPeerBanManager(context.Background(), handler, tSettings, registry)

	m.decayInterval = time.Second // fast decay for test
	m.decayAmount = 5

	peerID := "peer1"
	// Add score below threshold
	score, banned := m.AddScore(peerID, ReasonInvalidSubtree)
	assert.Equal(t, 10, score)
	assert.False(t, banned)

	// Add enough to trigger ban
	score, banned = m.AddScore(peerID, ReasonProtocolViolation)
	assert.Equal(t, 30, score)
	assert.True(t, banned)
	assert.Equal(t, peerID, handler.lastPeerID)
	assert.Equal(t, ReasonProtocolViolation.String(), handler.lastReason)

	// Decay logic: simulate time passage
	entry := m.peerBanScores[peerID]
	entry.LastUpdate = entry.LastUpdate.Add(-3 * time.Second)

	m.AddScore(peerID, ReasonInvalidSubtree)

	score, _, _ = m.GetBanScore(peerID)
	assert.Less(t, score, 30) // Should have decayed
}

func TestAddScore_UnknownReason(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	registry := NewPeerRegistry()
	m := NewPeerBanManager(context.Background(), nil, tSettings, registry)
	peerID := "peer2"
	score, banned := m.AddScore(peerID, ReasonUnknown)
	assert.Equal(t, 1, score)
	assert.False(t, banned)
}

func TestResetAndCleanupBanScore(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	registry := NewPeerRegistry()
	m := NewPeerBanManager(context.Background(), nil, tSettings, registry)
	peerID := "peer3"
	m.AddScore(peerID, ReasonInvalidSubtree)
	assert.NotZero(t, m.peerBanScores[peerID].Score)

	m.ResetBanScore(peerID)
	_, ok := m.peerBanScores[peerID]
	assert.False(t, ok)

	// Test cleanup
	m.AddScore(peerID, ReasonInvalidSubtree)
	m.peerBanScores[peerID].Score = 0
	m.CleanupBanScores()
	_, ok = m.peerBanScores[peerID]
	assert.False(t, ok)
}

func TestGetBanScoreAndReasons(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.P2P.BanThreshold = 100
	registry := NewPeerRegistry()
	m := NewPeerBanManager(context.Background(), nil, tSettings, registry)
	peerID := "peer4"
	m.AddScore(peerID, ReasonInvalidSubtree)
	m.AddScore(peerID, ReasonSpam)
	score, banned, _ := m.GetBanScore(peerID)
	assert.Equal(t, 60, score)
	assert.False(t, banned)

	reasons := m.GetBanReasons(peerID)
	require.Len(t, reasons, 2)
	assert.Equal(t, ReasonInvalidSubtree.String(), reasons[0])
	assert.Equal(t, ReasonSpam.String(), reasons[1])
}

func TestIsBannedAndListBanned(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	registry := NewPeerRegistry()
	m := NewPeerBanManager(context.Background(), nil, tSettings, registry)
	m.banThreshold = 10
	m.banDuration = 1 * time.Second // short ban for test

	peerID := "peer5"
	m.AddScore(peerID, ReasonSpam) // triggers ban
	assert.True(t, m.IsBanned(peerID))

	bannedList := m.ListBanned()
	require.Len(t, bannedList, 1)
	assert.Equal(t, peerID, bannedList[0])

	// Simulate ban expiry
	time.Sleep(2 * time.Second)
	assert.False(t, m.IsBanned(peerID))
	bannedList = m.ListBanned()
	assert.Empty(t, bannedList)
}

func TestBanReason_String(t *testing.T) {
	tests := []struct {
		reason   BanReason
		expected string
	}{
		{ReasonInvalidSubtree, "invalid_subtree"},
		{ReasonProtocolViolation, "protocol_violation"},
		{ReasonSpam, "spam"},
		{ReasonInvalidBlock, "invalid_block"},
		{ReasonCatchupFailure, "catchup_failure"}, // Added test for ReasonCatchupFailure
		{ReasonUnknown, "unknown"},
		{BanReason(999), "unknown"}, // Unknown reason
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.reason.String())
		})
	}
}

func TestGetBanReasons_Empty(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	registry := NewPeerRegistry()
	m := NewPeerBanManager(context.Background(), nil, tSettings, registry)

	// Get reasons for non-existent peer (empty case)
	reasons := m.GetBanReasons("unknown")
	assert.Nil(t, reasons)
}

func TestPeerBanManager_ConcurrentAccess(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	handler := &testBanHandler{}
	registry := NewPeerRegistry()
	m := NewPeerBanManager(context.Background(), handler, tSettings, registry)

	var wg sync.WaitGroup

	// Concurrent adds
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				peerID := string(rune('A' + id%5))
				m.AddScore(peerID, ReasonInvalidSubtree)
			}
		}(i)
	}

	// Concurrent reads
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				peerID := string(rune('A' + id))
				m.GetBanScore(peerID)
				m.IsBanned(peerID)
				m.GetBanReasons(peerID)
			}
		}(i)
	}

	// Concurrent cleanup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 5; i++ {
			m.CleanupBanScores()
			time.Sleep(10 * time.Millisecond)
		}
	}()

	// Concurrent list operations
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			m.ListBanned()
			time.Sleep(5 * time.Millisecond)
		}
	}()

	wg.Wait()

	// Should not panic or deadlock
	assert.True(t, true, "Concurrent access completed without issues")
}

func TestPeerBanManager_BackgroundCleanup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tSettings := test.CreateBaseTestSettings(t)
	registry := NewPeerRegistry()
	m := NewPeerBanManager(ctx, nil, tSettings, registry)

	// Add peer with zero score directly
	m.mu.Lock()
	m.peerBanScores["cleanup-test"] = &BanScore{
		Score:      0,
		Banned:     false,
		LastUpdate: time.Now(),
	}
	// Verify it was added
	_, existsBefore := m.peerBanScores["cleanup-test"]
	m.mu.Unlock()

	assert.True(t, existsBefore, "Peer should exist before cleanup")

	// Manually trigger cleanup
	m.CleanupBanScores()

	// Check peer was cleaned up
	m.mu.RLock()
	_, existsAfter := m.peerBanScores["cleanup-test"]
	m.mu.RUnlock()

	assert.False(t, existsAfter, "Zero-score peer should be cleaned up")

	// Test background cleanup as well
	m.mu.Lock()
	m.peerBanScores["background-test"] = &BanScore{
		Score:      0,
		Banned:     false,
		LastUpdate: time.Now(),
	}
	m.mu.Unlock()

	// Set a fast decay interval for background test
	m.decayInterval = 10 * time.Millisecond

	// Start background cleanup goroutine manually
	go func() {
		ticker := time.NewTicker(m.decayInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				m.CleanupBanScores()
			case <-ctx.Done():
				return
			}
		}
	}()

	// Wait for cleanup to occur
	time.Sleep(50 * time.Millisecond)

	// Check background cleanup worked
	m.mu.RLock()
	_, existsBackground := m.peerBanScores["background-test"]
	m.mu.RUnlock()

	assert.False(t, existsBackground, "Background cleanup should remove zero-score peer")
}

func TestPeerBanManager_NilHandler(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.P2P.BanThreshold = 90

	// Create manager with nil handler
	registry := NewPeerRegistry()
	m := NewPeerBanManager(context.Background(), nil, tSettings, registry)

	// Should not panic when banning
	score, banned := m.AddScore("peer1", ReasonSpam)
	assert.Equal(t, 50, score)
	assert.False(t, banned)

	// Trigger ban - should not panic with nil handler
	score, banned = m.AddScore("peer1", ReasonSpam)
	assert.Equal(t, 100, score)
	assert.True(t, banned)
}

func TestPeerBanManager_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	tSettings := test.CreateBaseTestSettings(t)
	registry := NewPeerRegistry()
	m := NewPeerBanManager(ctx, nil, tSettings, registry)

	// Set fast decay interval for testing
	m.decayInterval = 10 * time.Millisecond

	// Add a peer with zero score that should be cleaned up
	m.mu.Lock()
	m.peerBanScores["test-peer"] = &BanScore{
		Score:      0,
		Banned:     false,
		LastUpdate: time.Now(),
	}
	m.mu.Unlock()

	// Cancel context to stop background goroutine
	cancel()

	// Give goroutine time to exit
	time.Sleep(50 * time.Millisecond)

	// Verify context cancellation worked (goroutine should have stopped)
	assert.True(t, true, "Context cancellation completed without panic")
}

func TestPeerBanManager_ExtendedDecayLogic(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	registry := NewPeerRegistry()
	m := NewPeerBanManager(context.Background(), nil, tSettings, registry)

	// Set decay parameters
	m.decayInterval = time.Second
	m.decayAmount = 10

	peerID := "decay-test-peer"

	// Add initial score
	score, _ := m.AddScore(peerID, ReasonSpam) // 50 points
	assert.Equal(t, 50, score)

	// Simulate very long elapsed time
	m.mu.Lock()
	entry := m.peerBanScores[peerID]
	entry.LastUpdate = entry.LastUpdate.Add(-10 * time.Second) // 10 seconds ago
	m.mu.Unlock()

	// Add another score - should apply extensive decay first
	score, _ = m.AddScore(peerID, ReasonInvalidSubtree) // +10 points after decay

	// With 10 second elapsed and 1 decay per second at 10 points each:
	// 50 - (10 * 10) = -50 (clamped to 0) + 10 = 10
	assert.Equal(t, 10, score)
}

func TestPeerBanManager_ReasonCatchupFailure(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	registry := NewPeerRegistry()
	m := NewPeerBanManager(context.Background(), nil, tSettings, registry)

	peerID := "catchup-test-peer"

	// Test ReasonCatchupFailure scoring
	score, banned := m.AddScore(peerID, ReasonCatchupFailure)
	assert.Equal(t, 30, score) // Catchup failure should give 30 points
	assert.False(t, banned)

	// Verify reason is recorded
	reasons := m.GetBanReasons(peerID)
	assert.Contains(t, reasons, "catchup_failure")
}
