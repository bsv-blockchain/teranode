// Package p2p provides peer-to-peer networking functionality for the Teranode system.
// The ban management subsystem implements fine-grained control over peer scoring,
// ban durations, and ban events to ensure network health and security.
package p2p

import (
	"context"
	"sync"
	"time"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/libp2p/go-libp2p/core/peer"
)

// BanReason is an enum for ban reasons.
// It represents standardized categories for why a peer might receive ban score points,
// allowing for consistent policy enforcement and metrics collection.
//
// Using typed reasons rather than strings enables structured reasoning about ban patterns
// and provides better support for localization and audit logging.
type BanReason int

const (
	ReasonUnknown BanReason = iota
	ReasonInvalidSubtree
	ReasonProtocolViolation
	ReasonSpam
	ReasonInvalidBlock
	ReasonCatchupFailure
)

func (r BanReason) String() string {
	switch r {
	case ReasonInvalidSubtree:
		return "invalid_subtree"
	case ReasonProtocolViolation:
		return "protocol_violation"
	case ReasonSpam:
		return "spam"
	case ReasonInvalidBlock:
		return "invalid_block"
	case ReasonCatchupFailure:
		return "catchup_failure"
	default:
		return "unknown"
	}
}

// BanScore holds the score and ban status for a peer.
// It tracks accumulating penalties for misbehavior and manages the ban state.
//
// The ban score system works on a threshold principle - when a peer's score exceeds
// a configured threshold, the peer is banned for a period of time. The score decays over
// time, providing forgiveness for temporary issues or minor violations.
//
// This structure maintains a history of reasons that contributed to the current score,
// enabling analysis of patterns of misbehavior and informed decisions about permanent bans.
type BanScore struct {
	Score      int       // Current numerical score for the peer (higher is worse)
	Banned     bool      // Whether the peer is currently banned
	BanUntil   time.Time // Time when the ban expires
	LastUpdate time.Time // Time of the last score update (for decay calculations)
	Reasons    []string  // History of reasons for score increases (with timestamps)
}

// BanEventHandler allows the system to react to ban events.
// This interface provides a hook for other components to be notified when a peer is banned,
// enabling coordinated responses across the system (such as disconnecting from the peer,
// logging the event, or updating metrics).
//
// Implementations of this interface should be thread-safe as ban events may be
// triggered from multiple goroutines concurrently.
type BanEventHandler interface {
	// OnPeerBanned is called when a peer has been banned.
	// Parameters:
	// - peerID: Identifier of the banned peer
	// - until: Time when the ban expires
	// - reason: Human-readable reason for the ban
	OnPeerBanned(peerID string, until time.Time, reason string)
}

// PeerBanManagerI defines the interface for peer ban management functionality.
// This interface abstracts the implementation details of peer ban management,
// allowing different implementations to be used (such as in-memory, database-backed,
// or mock implementations) while providing a consistent API.
type PeerBanManagerI interface {
	// IsBanned checks if a peer is currently banned by PeerID.
	// Returns true if the peer is currently banned, false otherwise.
	IsBanned(peerID string) bool

	// GetBanScore returns the current ban score and ban status for a given peer.
	GetBanScore(peerID string) (score int, banned bool, banUntil time.Time)

	// AddScore adds points to a peer's ban score for a specific reason.
	// Returns the peer's current score after adjustment and whether the peer is now banned.
	AddScore(peerID string, reason BanReason) (score int, banned bool)
}

// PeerBanManager manages all peer scores and bans.
// It implements a reputation system that tracks peer behavior, applies score penalties
// for violations, and enforces temporary bans when scores exceed configured thresholds.
//
// The manager provides automatic score decay over time to allow peers to recover from
// temporary issues or minor violations. It also maintains a history of ban reasons to
// enable analysis of behavior patterns.
//
// All operations are thread-safe for concurrent access from multiple goroutines.
type PeerBanManager struct {
	ctx           context.Context      // Context for lifecycle management
	mu            sync.RWMutex         // Mutex for thread-safe operations
	peerBanScores map[string]*BanScore // Map of peer IDs to their ban scores
	reasonPoints  map[BanReason]int    // Mapping of ban reasons to their penalty points
	banThreshold  int                  // Score threshold that triggers a ban
	banDuration   time.Duration        // Duration of bans when threshold is exceeded
	decayInterval time.Duration        // How often scores are reduced (decay period)
	decayAmount   int                  // How many points are removed during each decay
	handler       BanEventHandler      // Handler for ban events to notify other components
	peerRegistry  *PeerRegistry        // Peer registry to sync ban status with
}

// NewPeerBanManager creates a new ban manager with sensible defaults.
// This constructor initializes a PeerBanManager with configuration derived from the settings
// and reasonable default values for ban thresholds, durations, and score decay.
//
// The manager is configured with point values for different ban reasons based on their
// severity, with more serious violations (like spam) receiving higher penalties than
// minor issues (like invalid subtrees).
//
// Parameters:
// - ctx: Context for lifecycle management and cancellation
// - handler: Handler that will be notified when ban events occur
// - tSettings: Application settings containing ban-related configuration
// - peerRegistry: Optional peer registry to sync ban status with (can be nil)
//
// Returns a fully configured PeerBanManager ready for use
func NewPeerBanManager(ctx context.Context, handler BanEventHandler, tSettings *settings.Settings, peerRegistry *PeerRegistry) *PeerBanManager {
	m := &PeerBanManager{
		ctx:           ctx,
		peerBanScores: make(map[string]*BanScore),
		reasonPoints: map[BanReason]int{
			ReasonInvalidSubtree:    10,
			ReasonProtocolViolation: 20,
			ReasonSpam:              50,
			ReasonInvalidBlock:      10, // Using the same ban score value as SVNode
			ReasonCatchupFailure:    30, // Significant penalty for infrastructure failures during sync
		},
		banThreshold:  tSettings.P2P.BanThreshold,
		banDuration:   tSettings.P2P.BanDuration,
		decayInterval: time.Minute,
		decayAmount:   1,
		handler:       handler,
		peerRegistry:  peerRegistry,
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
// This method is the core of the peer reputation system, responsible for accumulating
// misbehavior scores and enforcing bans when thresholds are exceeded.
//
// When called, it performs several operations:
// - Applies time-based score decay based on elapsed time since last update
// - Adds penalty points based on the specified reason
// - Applies ban if score exceeds threshold
// - Records the reason in ban history
// - Notifies ban event handler if a ban is triggered
//
// Score decay allows peers to gradually recover from penalties over time, while
// the reason tracking provides an audit trail of behavior problems.
//
// Parameters:
// - peerID: Identifier of the peer to apply score to
// - reason: Categorized reason for the score increase
//
// Returns:
// - score: The peer's current score after adjustment
// - banned: Whether the peer is now banned as a result of this score increase
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

	// Sync ban status with peer registry
	if m.peerRegistry != nil {
		if pID, err := peer.Decode(peerID); err == nil {
			m.peerRegistry.UpdateBanStatus(pID, entry.Score, entry.Banned)
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

	// Sync with peer registry
	if m.peerRegistry != nil {
		if pID, err := peer.Decode(peerID); err == nil {
			m.peerRegistry.UpdateBanStatus(pID, 0, false)
		}
	}
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

		// Sync with peer registry
		if m.peerRegistry != nil {
			if pID, err := peer.Decode(peerID); err == nil {
				m.peerRegistry.UpdateBanStatus(pID, 0, false)
			}
		}

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
