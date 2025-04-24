package p2p

import (
	"context"
	"sync"
	"time"

	"github.com/bitcoin-sv/teranode/settings"
)

// BanReason is an enum for ban reasons.
type BanReason int

const (
	ReasonUnknown BanReason = iota
	ReasonInvalidSubtree
	ReasonProtocolViolation
	ReasonSpam
)

func (r BanReason) String() string {
	switch r {
	case ReasonInvalidSubtree:
		return "invalid_subtree"
	case ReasonProtocolViolation:
		return "protocol_violation"
	case ReasonSpam:
		return "spam"
	default:
		return "unknown"
	}
}

// BanScore holds the score and ban status for a peer.
type BanScore struct {
	Score      int
	Banned     bool
	BanUntil   time.Time
	LastUpdate time.Time
	Reasons    []string // history of reasons (with optional timestamps)
}

// BanEventHandler allows the system to react to ban events.
type BanEventHandler interface {
	OnPeerBanned(peerID string, until time.Time, reason string)
}

// PeerBanManager manages all peer scores and bans.
type PeerBanManager struct {
	ctx           context.Context
	mu            sync.RWMutex
	peerBanScores map[string]*BanScore
	reasonPoints  map[BanReason]int
	banThreshold  int
	banDuration   time.Duration
	decayInterval time.Duration
	decayAmount   int
	handler       BanEventHandler
}

// NewPeerBanManager creates a new ban manager with sensible defaults.
func NewPeerBanManager(ctx context.Context, handler BanEventHandler, tSettings *settings.Settings) *PeerBanManager {
	m := &PeerBanManager{
		ctx:           ctx,
		peerBanScores: make(map[string]*BanScore),
		reasonPoints: map[BanReason]int{
			ReasonInvalidSubtree:    10,
			ReasonProtocolViolation: 20,
			ReasonSpam:              50,
		},
		banThreshold:  tSettings.P2P.BanThreshold,
		banDuration:   tSettings.P2P.BanDuration,
		decayInterval: time.Minute,
		decayAmount:   1,
		handler:       handler,
	}
	// Start background cleanup loop
	interval := m.decayInterval
	go func(interval time.Duration) {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				m.CleanupBanScores()
			case <-m.ctx.Done():
				return
			}
		}
	}(interval)

	return m
}

// AddScore increments the score for a peer, applies decay, and handles banning.
func (m *PeerBanManager) AddScore(peerID string, reason BanReason) (score int, banned bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()

	entry, ok := m.peerBanScores[peerID]
	if !ok {
		entry = &BanScore{LastUpdate: now}
		m.peerBanScores[peerID] = entry
	}

	// Decay logic
	elapsed := now.Sub(entry.LastUpdate)

	decaySteps := int(elapsed / m.decayInterval)
	if decaySteps > 0 {
		entry.Score -= decaySteps * m.decayAmount
		if entry.Score < 0 {
			entry.Score = 0
		}

		entry.LastUpdate = now
	}

	// Add reason to history
	entry.Reasons = append(entry.Reasons, reason.String())

	// Add points
	points, found := m.reasonPoints[reason]
	if !found {
		points = 1 // unknown reason, default to 1
	}

	entry.Score += points

	// Ban enforcement
	if entry.Score >= m.banThreshold && !entry.Banned {
		entry.Banned = true
		entry.BanUntil = now.Add(m.banDuration)
		banned = true

		if m.handler != nil {
			m.handler.OnPeerBanned(peerID, entry.BanUntil, reason.String())
		}
	}

	return entry.Score, entry.Banned
}

// GetBanScore returns the current ban score and ban status for a given peer.
func (m *PeerBanManager) GetBanScore(peerID string) (score int, banned bool, banUntil time.Time) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entry, ok := m.peerBanScores[peerID]
	if !ok {
		return 0, false, time.Time{}
	}

	return entry.Score, entry.Banned, entry.BanUntil
}

// ResetBanScore clears the ban score and ban status for a peer.
func (m *PeerBanManager) ResetBanScore(peerID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.peerBanScores, peerID)
}

// IsBanned returns true if the peer is currently banned, and unbans if expired.
func (m *PeerBanManager) IsBanned(peerID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.peerBanScores[peerID]
	if !ok || !entry.Banned {
		return false
	}

	if time.Now().After(entry.BanUntil) {
		// Ban expired, reset
		delete(m.peerBanScores, peerID)
		return false
	}

	return true
}

// ListBanned returns a slice of peer IDs that are currently banned.
func (m *PeerBanManager) ListBanned() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var banned []string

	now := time.Now()

	for peerID, entry := range m.peerBanScores {
		if entry.Banned && now.Before(entry.BanUntil) {
			banned = append(banned, peerID)
		}
	}

	return banned
}

// CleanupBanScores removes peers with zero score and not banned.
func (m *PeerBanManager) CleanupBanScores() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for peerID, entry := range m.peerBanScores {
		if entry.Score == 0 && !entry.Banned {
			delete(m.peerBanScores, peerID)
		}
	}
}

// GetBanReasons returns the reasons for a peer's ban score.
func (m *PeerBanManager) GetBanReasons(peerID string) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entry, ok := m.peerBanScores[peerID]
	if !ok {
		return nil
	}

	return append([]string{}, entry.Reasons...)
}
