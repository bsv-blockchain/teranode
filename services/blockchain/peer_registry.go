package blockchain

import (
	"context"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// maxPeerNameLength caps untrusted peer-supplied client names to prevent
// resource exhaustion and oversized log lines.
const maxPeerNameLength = 128

// sanitizeClientName scrubs an untrusted peer-supplied client name string.
// Control characters and high-Unicode are dropped; XSS-flavoured ASCII
// (`<>&"'\`) is dropped; the result is trimmed and capped to maxPeerNameLength.
// An empty input returns an empty string.
func sanitizeClientName(name string) string {
	if name == "" {
		return ""
	}
	if len(name) > maxPeerNameLength {
		name = name[:maxPeerNameLength]
	}

	var cleaned strings.Builder
	cleaned.Grow(len(name))

	for _, r := range name {
		switch {
		case (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9'),
			r == ' ' || r == '-' || r == '_' || r == '.' || r == '/':
			cleaned.WriteRune(r)
		case unicode.IsPrint(r) && r < 128:
			if r != '<' && r != '>' && r != '&' && r != '\'' && r != '"' && r != '\\' {
				cleaned.WriteRune(r)
			}
		}
	}

	return strings.TrimSpace(cleaned.String())
}

// PeerInfo holds transport-agnostic information about a peer known to the node.
// It is used across all transport types (HTTP DataHub, wire protocol, etc.).
type PeerInfo struct {
	ID                     string
	TransportType          blockchain_api.TransportType
	TransportTypeSet       bool // When true, TransportType was explicitly set by the caller
	ClientName             string
	Height                 uint32
	DataHubURL             string
	NetworkAddress         string
	IsBanned               bool
	BanScore               int32
	Storage                string
	BytesSent              uint64
	BytesReceived          uint64
	InteractionAttempts    int64
	InteractionSuccesses   int64
	InteractionFailures    int64
	MaliciousCount         int64
	ReputationScore        float64
	AvgResponseTimeMs      int64
	ConnectedAt            time.Time
	LastMessageTime        time.Time
	LastInteractionAttempt time.Time
	LastInteractionSuccess time.Time
	LastInteractionFailure time.Time
	LastSeen               time.Time
	BlockHash              *chainhash.Hash

	// P2P-domain fields. Migrated from services/p2p so the blockchain registry is
	// the single source of truth for peer state across libp2p connections.
	IsConnected          bool
	LastBlockTime        time.Time
	BlocksReceived       int64
	SubtreesReceived     int64
	TransactionsReceived int64
	CatchupBlocks        int64
	LastSyncAttempt      time.Time
	SyncAttemptCount     int32
	LastReputationReset  time.Time
	ReputationResetCount int32
	LastCatchupError     string
	LastCatchupErrorTime time.Time
}

// banEntry tracks ban scoring state for a single peer.
type banEntry struct {
	Score     int32
	Banned    bool
	BanUntil  time.Time
	LastDecay time.Time
	Reasons   []string
}

// BanConfig holds ban management configuration.
type BanConfig struct {
	Threshold     int32         // Score threshold that triggers a ban (default: 100)
	Duration      time.Duration // How long a ban lasts (default: 24h)
	DecayInterval time.Duration // How often scores decay (default: 1min)
	DecayAmount   int32         // Points removed per decay interval (default: 1)
	// Reason -> points mapping
	ReasonPoints map[string]int32
}

// DefaultBanConfig returns sensible defaults matching the existing P2P BanManager.
func DefaultBanConfig() BanConfig {
	return BanConfig{
		Threshold:     100,
		Duration:      24 * time.Hour,
		DecayInterval: time.Minute,
		DecayAmount:   1,
		ReasonPoints: map[string]int32{
			"invalid_subtree":    10,
			"protocol_violation": 20,
			"spam":               50,
			"invalid_block":      10,
			"catchup_failure":    30,
		},
	}
}

// CentralizedPeerRegistry is a thread-safe, in-memory store of peer information
// shared across all transport types in the blockchain service.
type CentralizedPeerRegistry struct {
	mu        sync.RWMutex
	peers     map[string]*PeerInfo
	banScores map[string]*banEntry
	banConfig BanConfig

	// saveMu serializes Save() so concurrent callers (periodic ticker +
	// shutdown) cannot persist an older snapshot over a newer one. Without
	// it, two saves can each take their own consistent state-snapshot, then
	// race the rename: whichever rename wins is unrelated to which snapshot
	// is fresher. saveMu is taken outside r.mu to keep the read-snapshot
	// portion fast; Save never holds both at once.
	saveMu sync.Mutex

	// Background-goroutine lifecycle. stopCh is closed once by Close() to signal
	// the ban-decay loop (and any future loops) to exit; wg waits for those
	// loops to finish so Close() can give callers a synchronous shutdown.
	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup

	// logger is used for non-fatal diagnostics during Load / Save (e.g. corrupt
	// persisted blob). Optional — defaults to a "CentralizedPeerRegistry"
	// logger if unset. Stored as an atomic.Value so SetLogger / log() don't
	// need to coordinate with r.mu (which guards the in-memory peer maps,
	// not the logger). Writes go through Store; reads through Load.
	logger atomic.Value // ulogger.Logger

	// lastCorruptArchiveKey records the most recent sidecar key produced by
	// Load when it found a corrupt persisted blob. Useful for operator
	// post-mortems and as a test seam (avoids tests having to probe the
	// keyspace based on wall-clock time). Empty string if no corruption has
	// been observed in this process lifetime.
	lastCorruptArchiveKey atomic.Pointer[string]

	// saveDisabled blocks all persistence-mutating paths (Save and
	// StartPeriodicSave). Set when Load detects an envelope whose Version is
	// newer than this binary understands — without it, the next save tick
	// would overwrite the operator's future-version blob with a downgraded
	// Version=1 envelope, destroying the data the version check was meant to
	// protect. The flag is one-way: once set, the only way to clear it is
	// to construct a fresh registry.
	saveDisabled atomic.Bool
}

// NewCentralizedPeerRegistry creates an empty peer registry with the given ban configuration.
func NewCentralizedPeerRegistry(banCfg BanConfig) *CentralizedPeerRegistry {
	return &CentralizedPeerRegistry{
		peers:     make(map[string]*PeerInfo),
		banScores: make(map[string]*banEntry),
		banConfig: banCfg,
		stopCh:    make(chan struct{}),
	}
}

// SetLogger installs a logger for non-fatal diagnostics during Load / Save.
// Idempotent and safe to call concurrently with log() — the underlying field
// is an atomic.Value. Passing nil is a no-op (use the default fallback).
func (r *CentralizedPeerRegistry) SetLogger(l ulogger.Logger) {
	if l == nil {
		return
	}
	r.logger.Store(loggerHolder{l})
}

// loggerHolder lets us atomically Store interface values whose concrete type
// can vary (ulogger.New(...) vs ulogger.TestLogger{}). atomic.Value requires
// every Store to use the same concrete type; wrapping in a struct gives us
// exactly one concrete type at the Store boundary.
type loggerHolder struct{ l ulogger.Logger }

// log returns the configured logger or a sensible default. Never returns nil
// so callers don't have to nil-check on every error path.
func (r *CentralizedPeerRegistry) log() ulogger.Logger {
	if v := r.logger.Load(); v != nil {
		return v.(loggerHolder).l
	}
	return ulogger.New("CentralizedPeerRegistry")
}

// LastCorruptArchiveKey returns the blob-store key under which the most
// recent corrupt-blob sidecar was archived, or "" if no corruption has been
// observed in this process. Operators can use this for post-mortem (e.g.
// expose via gRPC / a diagnostics endpoint).
func (r *CentralizedPeerRegistry) LastCorruptArchiveKey() string {
	if v := r.lastCorruptArchiveKey.Load(); v != nil {
		return *v
	}
	return ""
}

// SaveDisabled reports whether persistence is currently blocked. Set by Load
// when an envelope from a future Version is detected; subsequent Save and
// StartPeriodicSave calls become no-ops to preserve the operator's blob.
func (r *CentralizedPeerRegistry) SaveDisabled() bool {
	return r.saveDisabled.Load()
}

// Register adds a new peer or updates non-zero fields of an existing peer.
// For new peers, ConnectedAt and LastSeen are initialised and reputation starts
// at the neutral value of 50.
func (r *CentralizedPeerRegistry) Register(info *PeerInfo) {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()

	existing, exists := r.peers[info.ID]
	if !exists {
		entry := *info
		if info.BlockHash != nil {
			hashCopy := *info.BlockHash
			entry.BlockHash = &hashCopy
		}
		// Strip dangerous characters out of the peer-supplied client name so
		// it can't break logs / dashboards / JSON consumers.
		entry.ClientName = sanitizeClientName(entry.ClientName)
		entry.ConnectedAt = now
		entry.LastSeen = now
		entry.LastMessageTime = now
		entry.ReputationScore = 50.0
		// Reconcile against persisted ban state: a peer can be evicted from the
		// in-memory peer map (TTL/restart/Cleanup) while its banScore entry
		// outlives the eviction. When the same peer reattaches, the caller has
		// no way to know it was banned — so trust banScores, not the
		// caller-supplied IsBanned/BanScore fields.
		if banned, ok := r.banScores[info.ID]; ok {
			entry.IsBanned = banned.Banned
			entry.BanScore = banned.Score
		} else {
			entry.IsBanned = false
			entry.BanScore = 0
		}
		r.peers[info.ID] = &entry
		return
	}

	// Update only fields that carry meaningful new data.
	if info.ClientName != "" {
		existing.ClientName = sanitizeClientName(info.ClientName)
	}
	if info.Height > 0 {
		existing.Height = info.Height
	}
	if info.DataHubURL != "" {
		existing.DataHubURL = info.DataHubURL
	}
	if info.NetworkAddress != "" {
		existing.NetworkAddress = info.NetworkAddress
	}
	if info.Storage != "" {
		existing.Storage = info.Storage
	}
	if info.BlockHash != nil {
		hashCopy := *info.BlockHash
		existing.BlockHash = &hashCopy
	}
	// Only update TransportType when the caller explicitly set it.
	if info.TransportTypeSet {
		existing.TransportType = info.TransportType
	}
	// Bump LastSeen to record that we received fresh metadata for this peer.
	// LastMessageTime is intentionally NOT updated here — it is only set by
	// UpdateLastMessageTime, which is called on actual wire-protocol messages.
	// Updating it here would prevent TTL eviction for peers that send metadata
	// updates but no actual messages.
	existing.LastSeen = now
}

// UpdateMetrics atomically applies delta network counters and interaction outcome
// flags for a peer, then recalculates its reputation score.
func (r *CentralizedPeerRegistry) UpdateMetrics(
	peerID string,
	height uint32,
	bytesSentDelta, bytesRecvDelta uint64,
	recordSuccess, recordFailure, recordMalicious bool,
	responseTimeMs int64,
) {
	r.mu.Lock()
	defer r.mu.Unlock()

	info, exists := r.peers[peerID]
	if !exists {
		return
	}

	now := time.Now()

	if height > 0 {
		info.Height = height
	}

	info.BytesSent += bytesSentDelta
	info.BytesReceived += bytesRecvDelta
	info.LastSeen = now

	if recordMalicious {
		info.MaliciousCount++
		info.InteractionAttempts++
		info.InteractionFailures++
		info.LastInteractionAttempt = now
		info.LastInteractionFailure = now
	} else if recordSuccess {
		info.InteractionAttempts++
		info.InteractionSuccesses++
		info.LastInteractionAttempt = now
		info.LastInteractionSuccess = now

		// Running weighted average — weight recent observations at 20% to smooth spikes.
		// A response time of 0 means not applicable, not a zero-duration sample.
		if responseTimeMs > 0 {
			if info.AvgResponseTimeMs == 0 {
				info.AvgResponseTimeMs = responseTimeMs
			} else {
				info.AvgResponseTimeMs = int64(float64(info.AvgResponseTimeMs)*0.8 + float64(responseTimeMs)*0.2)
			}
		}
	} else if recordFailure {
		info.InteractionAttempts++
		info.InteractionFailures++
		info.LastInteractionAttempt = now
		info.LastInteractionFailure = now
	}

	r.calculateAndUpdateReputation(info)
}

// Remove deletes a peer entry from the registry. The peer's ban-score entry is
// intentionally preserved — bans must outlive peer disconnects, otherwise an
// offending peer could clear its own ban simply by reconnecting. ClearBannedPeers
// is the explicit knob for wiping ban state.
func (r *CentralizedPeerRegistry) Remove(peerID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.peers, peerID)
}

// Get returns a copy of the peer info for peerID, or false if not found.
// Returning a copy prevents callers from modifying registry state without
// locking. Uses a read lock for the common case; upgrades to a write lock
// only when the target peer carries a stale IsBanned=true flag that needs
// to be normalised before the snapshot is produced.
func (r *CentralizedPeerRegistry) Get(peerID string) (*PeerInfo, bool) {
	r.mu.RLock()
	info, exists := r.peers[peerID]
	if !exists {
		r.mu.RUnlock()
		return nil, false
	}

	// Fast path: no ban expiry needed for this peer. Copy under the read lock.
	needsExpiry := info.IsBanned && r.isBanExpiredLocked(peerID)
	if !needsExpiry {
		peerCopy := *info
		if info.BlockHash != nil {
			hashCopy := *info.BlockHash
			peerCopy.BlockHash = &hashCopy
		}
		r.mu.RUnlock()
		return &peerCopy, true
	}
	r.mu.RUnlock()

	// Slow path: this peer has an expired ban — take the write lock, expire it,
	// then return the updated snapshot.
	r.mu.Lock()
	defer r.mu.Unlock()
	r.expireBansLocked()

	info, exists = r.peers[peerID]
	if !exists {
		return nil, false
	}
	peerCopy := *info
	if info.BlockHash != nil {
		hashCopy := *info.BlockHash
		peerCopy.BlockHash = &hashCopy
	}
	return &peerCopy, true
}

// isBanExpiredLocked reports whether peerID has an entry in banScores whose
// ban window has already passed. Must be called with at least the read lock.
func (r *CentralizedPeerRegistry) isBanExpiredLocked(peerID string) bool {
	entry, ok := r.banScores[peerID]
	if !ok {
		return false
	}
	return entry.Banned && time.Now().After(entry.BanUntil)
}

// List returns copies of peers that pass all active filters, sorted by reputation
// descending. Passing nil for transportFilter disables that filter. When
// sortByStorage is true, "full" peers sort ahead of "pruned" peers ahead of
// unknown — applied as the primary key, with reputation as the secondary key.
//
// List always acquires the write lock so it can normalise expired ban state
// inline. Even when the caller doesn't ask for excludeBanned, the snapshots
// returned to dashboards / RPC clients must not show IsBanned=true for peers
// whose ban window has already passed.
func (r *CentralizedPeerRegistry) List(
	transportFilter *blockchain_api.TransportType,
	minReputation float64,
	minHeight uint32,
	excludeBanned bool,
	sortByStorage bool,
) []*PeerInfo {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.expireBansLocked()

	now := time.Now()
	result := make([]*PeerInfo, 0, len(r.peers))

	for _, info := range r.peers {
		if excludeBanned && r.isPeerBannedLocked(info.ID, now) {
			continue
		}
		if transportFilter != nil && info.TransportType != *transportFilter {
			continue
		}
		if info.ReputationScore < minReputation {
			continue
		}
		if info.Height < minHeight {
			continue
		}

		peerCopy := *info
		if info.BlockHash != nil {
			hashCopy := *info.BlockHash
			peerCopy.BlockHash = &hashCopy
		}
		result = append(result, &peerCopy)
	}

	sort.Slice(result, func(i, j int) bool {
		if sortByStorage {
			si, sj := storagePreference(result[i].Storage), storagePreference(result[j].Storage)
			if si != sj {
				return si > sj
			}
		}
		if result[i].ReputationScore != result[j].ReputationScore {
			return result[i].ReputationScore > result[j].ReputationScore
		}
		// Stable secondary ordering by most recent successful interaction.
		return result[i].LastInteractionSuccess.After(result[j].LastInteractionSuccess)
	})

	return result
}

// storagePreference returns a relative ordering for storage modes used by
// catchup peer selection: full > pruned > unknown.
func storagePreference(mode string) int {
	switch mode {
	case "full":
		return 3
	case "pruned":
		return 2
	default:
		return 1
	}
}

// Count returns the number of peers currently in the registry.
func (r *CentralizedPeerRegistry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return len(r.peers)
}

// isPeerBannedLocked checks if a peer is currently banned considering expiry.
// now is passed in so callers that already have a timestamp don't need an
// extra time.Now() call per iteration. Must be called with r.mu held.
func (r *CentralizedPeerRegistry) isPeerBannedLocked(peerID string, now time.Time) bool {
	entry, ok := r.banScores[peerID]
	if !ok || !entry.Banned {
		return false
	}
	return now.Before(entry.BanUntil)
}

// expireBansLocked sweeps banScores for entries whose BanUntil has passed,
// clears their Banned flag and score, and syncs the result onto PeerInfo so
// downstream readers see consistent state. Must be called with the write lock.
func (r *CentralizedPeerRegistry) expireBansLocked() {
	now := time.Now()
	for peerID, entry := range r.banScores {
		if !entry.Banned || now.Before(entry.BanUntil) {
			continue
		}
		entry.Banned = false
		entry.Score = 0
		entry.Reasons = nil
		if peer, exists := r.peers[peerID]; exists {
			peer.IsBanned = false
			peer.BanScore = 0
		}
	}
}

// calculateAndUpdateReputation recalculates and stores the reputation score for
// info. Must be called with the write lock held.
//
// Algorithm mirrors the P2P registry's scoring so that peer quality signals are
// comparable regardless of which transport discovered the peer.
func (r *CentralizedPeerRegistry) calculateAndUpdateReputation(info *PeerInfo) {
	const (
		baseScore     = 50.0
		successWeight = 0.6
		recencyBonus  = 10.0
		recencyWindow = 1 * time.Hour
	)

	// Malicious peers are pinned at a near-zero score; no other factor redeems them.
	if info.MaliciousCount > 0 {
		info.ReputationScore = 5.0
		return
	}

	totalAttempts := info.InteractionSuccesses + info.InteractionFailures

	if totalAttempts == 0 {
		info.ReputationScore = baseScore
		return
	}

	successRate := (float64(info.InteractionSuccesses) / float64(totalAttempts)) * 100.0

	score := successRate*successWeight + baseScore*(1.0-successWeight)

	if !info.LastInteractionFailure.IsZero() && time.Since(info.LastInteractionFailure) < recencyWindow {
		score -= 15.0
	}

	if !info.LastInteractionSuccess.IsZero() && time.Since(info.LastInteractionSuccess) < recencyWindow {
		score += recencyBonus
	}

	score *= calculateSpeedFactorMs(info.AvgResponseTimeMs)

	if score < 0 {
		score = 0
	} else if score > 100 {
		score = 100
	}

	info.ReputationScore = score
}

// calculateSpeedFactorMs returns a reputation multiplier based on average response
// time in milliseconds. Fast peers are rewarded; slow peers are penalised.
func calculateSpeedFactorMs(avgMs int64) float64 {
	if avgMs == 0 {
		return 1.0
	}

	switch {
	case avgMs < 200:
		return 1.2
	case avgMs < 500:
		return 1.1
	case avgMs < 2000:
		return 1.0
	case avgMs < 5000:
		return 0.9
	case avgMs < 10000:
		return 0.8
	case avgMs < 30000:
		return 0.7
	default:
		return 0.6
	}
}

// ---------------------------------------------------------------------------
// Ban Management
// ---------------------------------------------------------------------------

// AddBanScore adds penalty points to a peer's ban score, applying decay first.
// Returns the updated score and whether the peer is now banned.
//
// defaultPoints is used only when the supplied reason has no entry in
// banConfig.ReasonPoints. A reason that IS configured wins regardless of what
// defaultPoints is set to — this lets operators tune severity by reason
// without coordinating callers. Pass 0 for the common case (rely on config).
func (r *CentralizedPeerRegistry) AddBanScore(peerID string, reason string, defaultPoints int32) (int32, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	entry, ok := r.banScores[peerID]
	if !ok {
		entry = &banEntry{LastDecay: now}
		r.banScores[peerID] = entry
	}

	// Expire a stale ban before scoring. If a previous ban window has already
	// elapsed but no IsBannedPeer() / decay sweep has run yet, treat the peer
	// as un-banned for this call: clear Banned/Score/Reasons so the new
	// violation accumulates fresh and can re-arm the ban via the threshold
	// check below. Without this, AddBanScore would return Banned=true while
	// counting nothing, and the next IsBannedPeer call would wipe the score
	// entirely.
	if entry.Banned && now.After(entry.BanUntil) {
		entry.Banned = false
		entry.Score = 0
		entry.Reasons = nil
		entry.LastDecay = now
		if peer, exists := r.peers[peerID]; exists {
			peer.IsBanned = false
			peer.BanScore = 0
		}
	}

	// Apply decay (guard against zero interval to avoid division by zero).
	// Advance LastDecay by exactly decaySteps*DecayInterval — NOT to "now" —
	// so the leftover sub-interval elapsed (decaySteps*interval < elapsed <
	// (decaySteps+1)*interval) is preserved for the next call. Setting
	// LastDecay = now would silently bleed a small amount of time on every
	// invocation, lengthening effective ban duration over many calls.
	if r.banConfig.DecayInterval > 0 {
		elapsed := now.Sub(entry.LastDecay)
		decaySteps := int32(elapsed / r.banConfig.DecayInterval)
		if decaySteps > 0 {
			entry.Score -= decaySteps * r.banConfig.DecayAmount
			if entry.Score < 0 {
				entry.Score = 0
			}
			entry.LastDecay = entry.LastDecay.Add(time.Duration(decaySteps) * r.banConfig.DecayInterval)
		}
	}

	// Cap reason history to a small window — operators only need recent context
	// for diagnosing a ban; the full history isn't useful and would scale to
	// noticeable memory at fleet scale (peers × maxReasonHistory × ~20 bytes).
	const maxReasonHistory = 20
	entry.Reasons = append(entry.Reasons, reason)
	if len(entry.Reasons) > maxReasonHistory {
		entry.Reasons = entry.Reasons[len(entry.Reasons)-maxReasonHistory:]
	}

	// Resolve the score delta: configured reasons override the caller-supplied
	// default. This is intentional — operators control severity by reason via
	// banConfig.ReasonPoints; callers pass defaultPoints only as a fallback
	// for unknown / ad-hoc reasons.
	points := defaultPoints
	if configPoints, found := r.banConfig.ReasonPoints[reason]; found {
		points = configPoints
	}
	entry.Score += points

	// Check threshold
	wasBanned := entry.Banned
	if entry.Score >= r.banConfig.Threshold && !entry.Banned {
		entry.Banned = true
		entry.BanUntil = now.Add(r.banConfig.Duration)
	}

	// Sync ban status to peer info
	if peer, exists := r.peers[peerID]; exists {
		peer.BanScore = entry.Score
		peer.IsBanned = entry.Banned
	}

	return entry.Score, entry.Banned && !wasBanned // return true only on NEW ban
}

// IsBannedPeer checks if a peer is currently banned, auto-unbanning if expired.
func (r *CentralizedPeerRegistry) IsBannedPeer(peerID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	entry, ok := r.banScores[peerID]
	if !ok || !entry.Banned {
		return false
	}

	if time.Now().After(entry.BanUntil) {
		// Ban expired
		entry.Banned = false
		entry.Score = 0
		entry.Reasons = nil
		// Sync to peer info
		if peer, exists := r.peers[peerID]; exists {
			peer.BanScore = 0
			peer.IsBanned = false
		}
		return false
	}

	return true
}

// ListBannedPeers returns all currently banned peer IDs. Expired bans are
// normalised first so callers don't see entries whose window has elapsed.
func (r *CentralizedPeerRegistry) ListBannedPeers() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.expireBansLocked()

	now := time.Now()
	var result []string
	for peerID, entry := range r.banScores {
		if entry.Banned && now.Before(entry.BanUntil) {
			result = append(result, peerID)
		}
	}
	return result
}

// ClearBannedPeers removes all ban entries and resets ban status on all peers.
func (r *CentralizedPeerRegistry) ClearBannedPeers() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.banScores = make(map[string]*banEntry)
	for _, peer := range r.peers {
		peer.IsBanned = false
		peer.BanScore = 0
	}
}

// Close signals every background goroutine started by the registry (currently
// just the ban-decay loop in StartBanDecay) to exit and waits for them. Safe
// to call multiple times. Pairs with the service's Stop() so a clean shutdown
// blocks until decay traffic has actually drained, instead of relying on the
// service-manager's context cancellation racing with Stop's return.
func (r *CentralizedPeerRegistry) Close() {
	r.stopOnce.Do(func() { close(r.stopCh) })
	r.wg.Wait()
}

// StartBanDecay starts a background goroutine that decays ban scores periodically
// and removes zero-score entries. Call this once during service startup. The
// goroutine exits on the first of: ctx cancellation OR the registry's Close().
func (r *CentralizedPeerRegistry) StartBanDecay(ctx context.Context) {
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		ticker := time.NewTicker(r.banConfig.DecayInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-r.stopCh:
				return
			case <-ticker.C:
				r.decayBanScores()
			}
		}
	}()
}

// StartPeriodicSave starts a background goroutine that calls Save(ctx, store)
// every interval, tracked on the registry's WaitGroup so Close() drains it
// alongside ban-decay and cleanup. Exits on the first of: ctx cancellation
// OR Close(). A zero or negative interval disables the loop.
//
// Save errors are logged via the registry's configured logger; they do not
// stop the loop — a transient blob-store failure shouldn't terminate
// persistence for the rest of the process lifetime.
func (r *CentralizedPeerRegistry) StartPeriodicSave(ctx context.Context, interval time.Duration, store blob.Store) {
	if interval <= 0 || store == nil {
		return
	}
	// Refuse to start when persistence has been disabled (e.g. Load saw a
	// future-version blob). Without this, the first tick would overwrite the
	// operator's blob with a downgraded Version=1 envelope.
	if r.saveDisabled.Load() {
		r.log().Warnf("peer registry: periodic save not started because persistence is disabled")
		return
	}

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-r.stopCh:
				return
			case <-ticker.C:
				if err := r.Save(ctx, store); err != nil {
					r.log().Warnf("peer registry: periodic save failed: %v", err)
				}
			}
		}
	}()
}

// StartCleanup starts a background goroutine that periodically runs
// Cleanup(maxSize, ttl) so the in-memory registry cannot grow unboundedly
// under churn. Connected and banned peers are exempt; expired bans are
// normalised before each pass so a stale IsBanned flag can't keep an idle
// peer alive forever. The goroutine exits on the first of: ctx cancellation
// OR Close().
//
// A zero or negative interval disables the loop (caller's choice — useful
// when the operator only wants TTL-on-load semantics).
func (r *CentralizedPeerRegistry) StartCleanup(ctx context.Context, interval, ttl time.Duration, maxSize int) {
	if interval <= 0 {
		return
	}

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-r.stopCh:
				return
			case <-ticker.C:
				r.Cleanup(maxSize, ttl)
			}
		}
	}()
}

func (r *CentralizedPeerRegistry) decayBanScores() {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	for peerID, entry := range r.banScores {
		elapsed := now.Sub(entry.LastDecay)
		decaySteps := int32(elapsed / r.banConfig.DecayInterval)
		if decaySteps > 0 {
			entry.Score -= decaySteps * r.banConfig.DecayAmount
			if entry.Score < 0 {
				entry.Score = 0
			}
			// Preserve the sub-interval remainder so accumulated drift doesn't
			// extend the effective ban window. See AddBanScore for rationale.
			entry.LastDecay = entry.LastDecay.Add(time.Duration(decaySteps) * r.banConfig.DecayInterval)

			// Sync to peer info
			if peer, exists := r.peers[peerID]; exists {
				peer.BanScore = entry.Score
			}
		}

		// Cleanup: remove zero-score unbanned entries
		if entry.Score == 0 && !entry.Banned {
			delete(r.banScores, peerID)
		}
	}
}

// ---------------------------------------------------------------------------
// Additional Peer Methods
// ---------------------------------------------------------------------------

// ReconsiderBadPeers resets reputation for peers that have been bad for a while.
// Returns the number of peers that had their reputation recovered. Mirrors the
// semantics of the original P2P registry: only peers with reputation < 20 are
// candidates, the configured cooldown applies after the most recent failure,
// and previously-reset peers must wait an exponentially longer cooldown
// (3× per prior reset) before being eligible again.
func (r *CentralizedPeerRegistry) ReconsiderBadPeers(cooldown time.Duration) int {
	r.mu.Lock()
	defer r.mu.Unlock()

	peersRecovered := 0
	now := time.Now()

	for _, info := range r.peers {
		if info.ReputationScore >= 20 {
			continue
		}
		if info.LastInteractionFailure.IsZero() || now.Sub(info.LastInteractionFailure) < cooldown {
			continue
		}

		if !info.LastReputationReset.IsZero() {
			requiredCooldown := cooldown
			for i := 0; i < int(info.ReputationResetCount); i++ {
				requiredCooldown *= 3
			}
			if now.Sub(info.LastReputationReset) < requiredCooldown {
				continue
			}
		}

		info.ReputationScore = 30
		info.MaliciousCount = 0
		info.LastReputationReset = now
		info.ReputationResetCount++

		peersRecovered++
	}

	return peersRecovered
}

// ResetReputation resets reputation metrics for a specific peer or all peers.
// When peerID is empty, every peer is reset. Returns the number of peers reset.
func (r *CentralizedPeerRegistry) ResetReputation(peerID string) int {
	r.mu.Lock()
	defer r.mu.Unlock()

	resetOne := func(info *PeerInfo) {
		info.InteractionAttempts = 0
		info.InteractionSuccesses = 0
		info.InteractionFailures = 0
		info.LastInteractionAttempt = time.Time{}
		info.LastInteractionSuccess = time.Time{}
		info.LastInteractionFailure = time.Time{}

		info.ReputationScore = 50.0
		info.MaliciousCount = 0
		info.AvgResponseTimeMs = 0

		info.LastCatchupError = ""
		info.LastCatchupErrorTime = time.Time{}

		info.LastReputationReset = time.Time{}
		info.ReputationResetCount = 0

		info.LastSyncAttempt = time.Time{}
		info.SyncAttemptCount = 0
	}

	if peerID == "" {
		for _, info := range r.peers {
			resetOne(info)
		}
		return len(r.peers)
	}

	info, ok := r.peers[peerID]
	if !ok {
		return 0
	}
	resetOne(info)
	return 1
}

// UpdateConnectionState flips the IsConnected flag for an existing peer entry.
// No-op when the peer is unknown.
func (r *CentralizedPeerRegistry) UpdateConnectionState(peerID string, connected bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if info, ok := r.peers[peerID]; ok {
		info.IsConnected = connected
	}
}

// UpdateLastMessageTime sets the peer's last-message timestamp to now.
func (r *CentralizedPeerRegistry) UpdateLastMessageTime(peerID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if info, ok := r.peers[peerID]; ok {
		info.LastMessageTime = time.Now()
	}
}

// UpdateStorage records the peer's storage mode (e.g. "full", "pruned").
func (r *CentralizedPeerRegistry) UpdateStorage(peerID string, storage string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if info, ok := r.peers[peerID]; ok {
		info.Storage = storage
	}
}

// RecordSyncAttempt notes that we attempted to sync with the peer for backoff tracking.
func (r *CentralizedPeerRegistry) RecordSyncAttempt(peerID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if info, ok := r.peers[peerID]; ok {
		info.LastSyncAttempt = time.Now()
		info.SyncAttemptCount++
	}
}

// ClearAllSyncAttempts resets sync-attempt tracking on every peer that has any
// and returns the number cleared. Used when every peer has been tried and we
// want to retry from scratch.
func (r *CentralizedPeerRegistry) ClearAllSyncAttempts() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	cleared := 0
	for _, info := range r.peers {
		if !info.LastSyncAttempt.IsZero() {
			info.LastSyncAttempt = time.Time{}
			cleared++
		}
	}
	return cleared
}

// recordReceivedSuccessLocked is the shared body of RecordBlockReceived /
// RecordSubtreeReceived: increment the supplied counter, mark a successful
// interaction, blend the response time into the rolling average, recompute
// reputation. Caller must hold the write lock.
func (r *CentralizedPeerRegistry) recordReceivedSuccessLocked(info *PeerInfo, counter *int64, responseTimeMs int64) {
	*counter++
	info.InteractionAttempts++
	info.InteractionSuccesses++
	info.LastInteractionAttempt = time.Now()
	info.LastInteractionSuccess = info.LastInteractionAttempt
	info.LastSeen = info.LastInteractionAttempt

	if responseTimeMs > 0 {
		if info.AvgResponseTimeMs == 0 {
			info.AvgResponseTimeMs = responseTimeMs
		} else {
			info.AvgResponseTimeMs = int64(float64(info.AvgResponseTimeMs)*0.8 + float64(responseTimeMs)*0.2)
		}
	}

	r.calculateAndUpdateReputation(info)
}

// RecordBlockReceived increments BlocksReceived, sets LastBlockTime, blends the
// response time into the running average, and updates reputation.
func (r *CentralizedPeerRegistry) RecordBlockReceived(peerID string, responseTimeMs int64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if info, ok := r.peers[peerID]; ok {
		info.LastBlockTime = time.Now()
		r.recordReceivedSuccessLocked(info, &info.BlocksReceived, responseTimeMs)
	}
}

// RecordSubtreeReceived increments SubtreesReceived and records a successful interaction.
func (r *CentralizedPeerRegistry) RecordSubtreeReceived(peerID string, responseTimeMs int64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if info, ok := r.peers[peerID]; ok {
		r.recordReceivedSuccessLocked(info, &info.SubtreesReceived, responseTimeMs)
	}
}

// RecordTransactionReceived increments TransactionsReceived and records a successful interaction.
// Transactions are broadcast so we do not track per-tx response time.
func (r *CentralizedPeerRegistry) RecordTransactionReceived(peerID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if info, ok := r.peers[peerID]; ok {
		info.TransactionsReceived++
		info.InteractionSuccesses++
		info.LastInteractionSuccess = time.Now()
		info.LastSeen = info.LastInteractionSuccess
		r.calculateAndUpdateReputation(info)
	}
}

// RecordCatchupError stores the most recent catchup error reported against the peer.
func (r *CentralizedPeerRegistry) RecordCatchupError(peerID string, errMsg string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if info, ok := r.peers[peerID]; ok {
		info.LastCatchupError = errMsg
		info.LastCatchupErrorTime = time.Now()
	}
}

// Cleanup evicts stale peers to bound memory and lookup cost. Phase 1 (TTL)
// drops peers whose recency timestamp is older than ttl. Phase 2 (LRU) then
// drops oldest-first until the non-exempt portion of the registry fits under
// maxSize. Connected peers and banned peers are exempt from both phases.
// A maxSize of 0 disables the LRU phase. Returns (expired, lru) counts.
//
// Recency is taken from peerActivity(info), which uses LastSeen as the canonical
// freshness signal (every successful interaction refreshes it) and falls back
// to LastMessageTime for entries that pre-date that convention or were
// restored from older persisted state.
func (r *CentralizedPeerRegistry) Cleanup(maxSize int, ttl time.Duration) (int, int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Normalise expired bans first so isCleanupExempt() doesn't keep an idle
	// peer alive on the strength of a stale IsBanned flag.
	r.expireBansLocked()

	now := time.Now()
	expired := 0

	for id, info := range r.peers {
		if isCleanupExempt(info) {
			continue
		}
		if last := peerActivity(info); !last.IsZero() && now.Sub(last) <= ttl {
			continue
		}
		delete(r.peers, id)
		expired++
	}

	if maxSize <= 0 {
		return expired, 0
	}

	type candidate struct {
		id   string
		last time.Time
	}
	candidates := make([]candidate, 0, len(r.peers))
	exemptCount := 0
	for id, info := range r.peers {
		if isCleanupExempt(info) {
			exemptCount++
			continue
		}
		candidates = append(candidates, candidate{id: id, last: peerActivity(info)})
	}

	target := maxSize - exemptCount
	if target < 0 {
		target = 0
	}
	toEvict := len(candidates) - target
	if toEvict <= 0 {
		return expired, 0
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].last.Before(candidates[j].last)
	})

	for i := 0; i < toEvict; i++ {
		delete(r.peers, candidates[i].id)
	}

	return expired, toEvict
}

// isCleanupExempt reports whether a peer must be retained regardless of TTL or
// size pressure. Caller must hold the registry lock.
func isCleanupExempt(info *PeerInfo) bool {
	return info.IsConnected || info.IsBanned
}

// peerActivity returns the canonical freshness timestamp used by Cleanup —
// the newer of LastSeen and LastMessageTime.
//
// Most successful-interaction paths refresh LastSeen (UpdateMetrics,
// RecordBlockReceived/Subtree/Transaction, Register), but the explicit
// UpdateLastMessageTime RPC bumps only LastMessageTime. Picking the newer
// of the two means recent activity through either field counts as recent,
// and older persisted records that only have LastMessageTime still get a
// sensible recency value. Persistence Load uses LastSeen as its TTL signal,
// which is consistent because Load also restores LastMessageTime so both
// fields are present.
func peerActivity(info *PeerInfo) time.Time {
	if info.LastSeen.After(info.LastMessageTime) {
		return info.LastSeen
	}
	return info.LastMessageTime
}
