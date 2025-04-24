package p2p

import (
	"context"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/util/test"
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
	tSettings := test.CreateBaseTestSettings()
	tSettings.P2P.BanThreshold = 30
	tSettings.P2P.BanDuration = 2 * time.Hour

	m := NewPeerBanManager(context.Background(), handler, tSettings)

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
	tSettings := test.CreateBaseTestSettings()
	m := NewPeerBanManager(context.Background(), nil, tSettings)
	peerID := "peer2"
	score, banned := m.AddScore(peerID, ReasonUnknown)
	assert.Equal(t, 1, score)
	assert.False(t, banned)
}

func TestResetAndCleanupBanScore(t *testing.T) {
	tSettings := test.CreateBaseTestSettings()
	m := NewPeerBanManager(context.Background(), nil, tSettings)
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
	tSettings := test.CreateBaseTestSettings()
	tSettings.P2P.BanThreshold = 100
	m := NewPeerBanManager(context.Background(), nil, tSettings)
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
	tSettings := test.CreateBaseTestSettings()
	m := NewPeerBanManager(context.Background(), nil, tSettings)
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
