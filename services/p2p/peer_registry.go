package p2p

import (
	"sync"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/libp2p/go-libp2p/core/peer"
)

// PeerRegistry maintains peer information
// This is a pure data store with no business logic
type PeerRegistry struct {
	mu    sync.RWMutex
	peers map[peer.ID]*PeerInfo
}

// NewPeerRegistry creates a new peer registry
func NewPeerRegistry() *PeerRegistry {
	return &PeerRegistry{
		peers: make(map[peer.ID]*PeerInfo),
	}
}

// Put adds or updates a peer atomically
func (pr *PeerRegistry) Put(id peer.ID, clientName string, height uint32, blockHash *chainhash.Hash, dataHubURL string) {
	pr.mu.Lock()
	defer pr.mu.Unlock()

	now := time.Now()

	if _, exists := pr.peers[id]; !exists {
		pr.peers[id] = &PeerInfo{
			ID:              id,
			ClientName:      clientName,
			Height:          height,
			BlockHash:       blockHash,
			DataHubURL:      dataHubURL,
			ConnectedAt:     now,
			LastMessageTime: now,  // Initialize to connection time
			ReputationScore: 50.0, // Start with neutral reputation
		}
	} else {
		info := pr.peers[id]

		if clientName != "" {
			info.ClientName = clientName
		}

		if height > 0 {
			info.Height = height
		}

		if blockHash != nil {
			info.BlockHash = blockHash
		}

		if dataHubURL != "" {
			info.DataHubURL = dataHubURL
		}

		info.LastMessageTime = now
	}
}

// Remove removes a peer
func (pr *PeerRegistry) Remove(id peer.ID) {
	pr.mu.Lock()
	defer pr.mu.Unlock()

	delete(pr.peers, id)
}

// Get returns peer info
func (pr *PeerRegistry) Get(id peer.ID) (*PeerInfo, bool) {
	pr.mu.RLock()
	defer pr.mu.RUnlock()

	info, exists := pr.peers[id]
	if !exists {
		return nil, false
	}

	// Return a copy to prevent external modification
	copy := *info
	return &copy, true
}

// GetAll returns all peer information
func (pr *PeerRegistry) GetAll() []*PeerInfo {
	pr.mu.RLock()
	defer pr.mu.RUnlock()

	result := make([]*PeerInfo, 0, len(pr.peers))

	for _, info := range pr.peers {
		copy := *info
		result = append(result, &copy)
	}

	return result
}

// UpdateBanStatus updates a peer's ban status
func (pr *PeerRegistry) UpdateBanStatus(id peer.ID, score int, banned bool) {
	pr.mu.Lock()
	defer pr.mu.Unlock()

	if info, exists := pr.peers[id]; exists {
		info.BanScore = score
		info.IsBanned = banned
	}
}

// UpdateNetworkStats updates network statistics for a peer
func (pr *PeerRegistry) UpdateNetworkStats(id peer.ID, bytesReceived uint64) {
	pr.mu.Lock()
	defer pr.mu.Unlock()

	if info, exists := pr.peers[id]; exists {
		info.BytesReceived = bytesReceived
		info.LastBlockTime = time.Now()
	}
}

// UpdateLastMessageTime updates the last time we received a message from a peer
func (pr *PeerRegistry) UpdateLastMessageTime(id peer.ID) {
	pr.mu.Lock()
	defer pr.mu.Unlock()

	if info, exists := pr.peers[id]; exists {
		info.LastMessageTime = time.Now()
	}
}

// UpdateStorage updates a peer's node mode (full/pruned)
func (pr *PeerRegistry) UpdateStorage(id peer.ID, mode string) {
	pr.mu.Lock()
	defer pr.mu.Unlock()

	if info, exists := pr.peers[id]; exists {
		info.Storage = mode
	}
}

// PeerCount returns the number of peers
func (pr *PeerRegistry) PeerCount() int {
	pr.mu.RLock()
	defer pr.mu.RUnlock()

	return len(pr.peers)
}

// UpdateConnectionState updates whether a peer is directly connected
func (pr *PeerRegistry) UpdateConnectionState(id peer.ID, connected bool) {
	pr.mu.Lock()
	defer pr.mu.Unlock()

	if info, exists := pr.peers[id]; exists {
		info.IsConnected = connected
	}
}

// GetConnectedPeers returns only directly connected peers
func (pr *PeerRegistry) GetConnectedPeers() []*PeerInfo {
	pr.mu.RLock()
	defer pr.mu.RUnlock()

	result := make([]*PeerInfo, 0, len(pr.peers))
	for _, info := range pr.peers {
		if info.IsConnected {
			copy := *info
			result = append(result, &copy)
		}
	}
	return result
}

// RecordInteractionAttempt records that an interaction attempt was made to a peer
func (pr *PeerRegistry) RecordInteractionAttempt(id peer.ID) {
	pr.mu.Lock()
	defer pr.mu.Unlock()

	if info, exists := pr.peers[id]; exists {
		info.InteractionAttempts++
		info.LastInteractionAttempt = time.Now()
	}
}

// RecordCatchupAttempt is deprecated - use RecordInteractionAttempt instead
// Maintained for backward compatibility
func (pr *PeerRegistry) RecordCatchupAttempt(id peer.ID) {
	pr.RecordInteractionAttempt(id)
}

// RecordInteractionSuccess records a successful interaction from a peer
// Updates success count and calculates running average response time
// Automatically recalculates reputation score based on success/failure ratio
func (pr *PeerRegistry) RecordInteractionSuccess(id peer.ID, duration time.Duration) {
	pr.mu.Lock()
	defer pr.mu.Unlock()

	if info, exists := pr.peers[id]; exists {
		info.InteractionSuccesses++
		info.LastInteractionSuccess = time.Now()

		// Calculate running average response time
		if info.AvgResponseTime == 0 {
			info.AvgResponseTime = duration
		} else {
			// Weighted average: 80% previous average, 20% new value
			info.AvgResponseTime = time.Duration(
				int64(float64(info.AvgResponseTime)*0.8 + float64(duration)*0.2),
			)
		}

		// Automatically update reputation score based on metrics
		pr.calculateAndUpdateReputation(info)
	}
}

// RecordCatchupSuccess is deprecated - use RecordInteractionSuccess instead
// Maintained for backward compatibility
func (pr *PeerRegistry) RecordCatchupSuccess(id peer.ID, duration time.Duration) {
	pr.RecordInteractionSuccess(id, duration)
	// Also increment CatchupBlocks for backward compatibility
	pr.mu.Lock()
	defer pr.mu.Unlock()
	if info, exists := pr.peers[id]; exists {
		info.CatchupBlocks++
	}
}

// RecordInteractionFailure records a failed interaction attempt from a peer
// Automatically recalculates reputation score based on success/failure ratio
func (pr *PeerRegistry) RecordInteractionFailure(id peer.ID) {
	pr.mu.Lock()
	defer pr.mu.Unlock()

	if info, exists := pr.peers[id]; exists {
		info.InteractionFailures++
		info.LastInteractionFailure = time.Now()

		// Check for repeated failures in a short time window
		recentFailureWindow := 5 * time.Minute
		if !info.LastInteractionSuccess.IsZero() &&
			time.Since(info.LastInteractionSuccess) < recentFailureWindow {
			// Multiple failures since last success - apply harsh penalty
			failuresSinceSuccess := info.InteractionFailures - info.InteractionSuccesses
			if failuresSinceSuccess > 2 {
				info.ReputationScore = 15.0 // Drop to very low score
				return
			}
		}

		// Normal reputation calculation for isolated failures
		pr.calculateAndUpdateReputation(info)
	}
}

// RecordCatchupFailure is deprecated - use RecordInteractionFailure instead
// Maintained for backward compatibility
func (pr *PeerRegistry) RecordCatchupFailure(id peer.ID) {
	pr.RecordInteractionFailure(id)
}

// UpdateCatchupError stores the last catchup error for a peer
func (pr *PeerRegistry) UpdateCatchupError(id peer.ID, errorMsg string) {
	pr.mu.Lock()
	defer pr.mu.Unlock()

	if info, exists := pr.peers[id]; exists {
		info.LastCatchupError = errorMsg
		info.LastCatchupErrorTime = time.Now()
	}
}

// RecordMaliciousInteraction records malicious behavior detected during any interaction
// Significantly reduces reputation score for malicious activity
func (pr *PeerRegistry) RecordMaliciousInteraction(id peer.ID) {
	pr.mu.Lock()
	defer pr.mu.Unlock()

	if info, exists := pr.peers[id]; exists {
		info.MaliciousCount++
		info.InteractionFailures++ // Also count as a failed interaction
		info.LastInteractionFailure = time.Now()

		// Immediately drop reputation to very low value for malicious behavior
		// Providing invalid blocks is serious - don't trust this peer
		info.ReputationScore = 5.0 // Very low score, well below selection threshold

		// Log would be helpful here but PeerRegistry doesn't have a logger
		// The impact is still significant - reputation dropped to 5.0
	}
}

// RecordCatchupMalicious is deprecated - use RecordMaliciousInteraction instead
// Maintained for backward compatibility
func (pr *PeerRegistry) RecordCatchupMalicious(id peer.ID) {
	pr.RecordMaliciousInteraction(id)
}

// UpdateReputation updates the reputation score for a peer
// Score should be between 0 and 100
func (pr *PeerRegistry) UpdateReputation(id peer.ID, score float64) {
	pr.mu.Lock()
	defer pr.mu.Unlock()

	if info, exists := pr.peers[id]; exists {
		// Clamp score to valid range
		if score < 0 {
			score = 0
		} else if score > 100 {
			score = 100
		}
		info.ReputationScore = score
	}
}

// UpdateCatchupReputation is deprecated - use UpdateReputation instead
// Maintained for backward compatibility
func (pr *PeerRegistry) UpdateCatchupReputation(id peer.ID, score float64) {
	pr.UpdateReputation(id, score)
}

// calculateAndUpdateReputation calculates and updates the reputation score based on metrics
// This method should be called with the lock already held
//
// Reputation algorithm:
// - Base score: 50 (neutral)
// - Success rate (0-100): weight 60%
// - Malicious penalty: -20 per malicious attempt (capped at -50)
// - Recency bonus: +10 if successful in last hour
// - Final score is clamped to 0-100 range
func (pr *PeerRegistry) calculateAndUpdateReputation(info *PeerInfo) {
	const (
		baseScore     = 50.0
		successWeight = 0.6
		// maliciousPenalty = 20.0
		// maliciousCap     = 50.0
		recencyBonus  = 10.0
		recencyWindow = 1 * time.Hour
	)

	// If peer has been marked malicious, keep reputation very low
	if info.MaliciousCount > 0 {
		// Malicious peers get minimal reputation
		info.ReputationScore = 5.0
		return
	}

	// Calculate success rate (0-100)
	totalAttempts := info.InteractionSuccesses + info.InteractionFailures

	var successRate float64

	if totalAttempts > 0 {
		successRate = (float64(info.InteractionSuccesses) / float64(totalAttempts)) * 100.0
	} else {
		// No history yet, use neutral score
		info.ReputationScore = baseScore
		return
	}

	// Start with weighted success rate
	score := successRate * successWeight

	// Add base score weighted component
	score += baseScore * (1.0 - successWeight)

	// Apply additional penalty for recent failures
	recentFailurePenalty := 0.0
	if !info.LastInteractionFailure.IsZero() && time.Since(info.LastInteractionFailure) < recencyWindow {
		recentFailurePenalty = 15.0 // Penalty for recent failure
	}
	score -= recentFailurePenalty

	// Add recency bonus if peer was successful recently
	if !info.LastInteractionSuccess.IsZero() && time.Since(info.LastInteractionSuccess) < recencyWindow {
		score += recencyBonus
	}

	// Clamp to valid range
	if score < 0 {
		score = 0
	} else if score > 100 {
		score = 100
	}

	info.ReputationScore = score
}

// RecordBlockReceived records when a block is successfully received from a peer
func (pr *PeerRegistry) RecordBlockReceived(id peer.ID, duration time.Duration) {
	pr.mu.Lock()
	defer pr.mu.Unlock()

	if info, exists := pr.peers[id]; exists {
		info.BlocksReceived++
		// Also record as a successful interaction
		info.InteractionSuccesses++
		info.LastInteractionSuccess = time.Now()

		// Update average response time
		if info.AvgResponseTime == 0 {
			info.AvgResponseTime = duration
		} else {
			info.AvgResponseTime = time.Duration(
				int64(float64(info.AvgResponseTime)*0.8 + float64(duration)*0.2),
			)
		}

		pr.calculateAndUpdateReputation(info)
	}
}

// RecordSubtreeReceived records when a subtree is successfully received from a peer
func (pr *PeerRegistry) RecordSubtreeReceived(id peer.ID, duration time.Duration) {
	pr.mu.Lock()
	defer pr.mu.Unlock()

	if info, exists := pr.peers[id]; exists {
		info.SubtreesReceived++
		// Also record as a successful interaction
		info.InteractionSuccesses++
		info.LastInteractionSuccess = time.Now()

		// Update average response time
		if info.AvgResponseTime == 0 {
			info.AvgResponseTime = duration
		} else {
			info.AvgResponseTime = time.Duration(
				int64(float64(info.AvgResponseTime)*0.8 + float64(duration)*0.2),
			)
		}

		pr.calculateAndUpdateReputation(info)
	}
}

// RecordTransactionReceived records when a transaction is successfully received from a peer
func (pr *PeerRegistry) RecordTransactionReceived(id peer.ID) {
	pr.mu.Lock()
	defer pr.mu.Unlock()

	if info, exists := pr.peers[id]; exists {
		info.TransactionsReceived++
		// For transactions, we don't track response time as they're broadcast
		// but we still count them as successful interactions
		info.InteractionSuccesses++
		info.LastInteractionSuccess = time.Now()

		pr.calculateAndUpdateReputation(info)
	}
}

// GetPeersByReputation returns peers sorted by reputation score
// Filters for peers that are not banned
func (pr *PeerRegistry) GetPeersByReputation() []*PeerInfo {
	pr.mu.RLock()
	defer pr.mu.RUnlock()

	result := make([]*PeerInfo, 0, len(pr.peers))
	for _, info := range pr.peers {
		// Only include peers that are not banned
		if !info.IsBanned {
			copy := *info
			result = append(result, &copy)
		}
	}

	// Sort by reputation score (highest first)
	// Secondary sort by last success time (most recent first)
	for i := 0; i < len(result); i++ {
		for j := i + 1; j < len(result); j++ {
			// Compare reputation scores
			if result[i].ReputationScore < result[j].ReputationScore {
				result[i], result[j] = result[j], result[i]
			} else if result[i].ReputationScore == result[j].ReputationScore {
				// If same reputation, prefer more recently successful peer
				if result[i].LastInteractionSuccess.Before(result[j].LastInteractionSuccess) {
					result[i], result[j] = result[j], result[i]
				}
			}
		}
	}

	return result
}

// RecordSyncAttempt records that we attempted to sync with a peer
func (pr *PeerRegistry) RecordSyncAttempt(id peer.ID) {
	pr.mu.Lock()
	defer pr.mu.Unlock()

	if info, exists := pr.peers[id]; exists {
		info.LastSyncAttempt = time.Now()
		info.SyncAttemptCount++
	}
}

// ClearAllSyncAttempts clears the LastSyncAttempt time for all peers,
// allowing them to be retried immediately. This is used when all peers
// have been attempted and we want to refresh and try again.
func (pr *PeerRegistry) ClearAllSyncAttempts() int {
	pr.mu.Lock()
	defer pr.mu.Unlock()

	peersCleared := 0
	for _, info := range pr.peers {
		if !info.LastSyncAttempt.IsZero() {
			info.LastSyncAttempt = time.Time{}
			peersCleared++
		}
	}
	return peersCleared
}

// ReconsiderBadPeers resets reputation for peers that have been bad for a while
// Returns the number of peers that had their reputation recovered
func (pr *PeerRegistry) ReconsiderBadPeers(cooldownPeriod time.Duration) int {
	pr.mu.Lock()
	defer pr.mu.Unlock()

	peersRecovered := 0

	for _, info := range pr.peers {
		// Only consider peers with very low reputation
		if info.ReputationScore >= 20 {
			continue
		}

		// Check if enough time has passed since last failure
		if info.LastInteractionFailure.IsZero() ||
			time.Since(info.LastInteractionFailure) < cooldownPeriod {
			continue
		}

		// Check if we haven't already reset this peer recently
		if !info.LastReputationReset.IsZero() {
			// Calculate exponential cooldown based on reset count
			requiredCooldown := cooldownPeriod
			for i := 0; i < info.ReputationResetCount; i++ {
				requiredCooldown *= 3 // Triple cooldown for each reset
			}

			if time.Since(info.LastReputationReset) < requiredCooldown {
				continue // Not enough time since last reset
			}
		}

		// Reset reputation to a low but eligible value
		oldReputation := info.ReputationScore
		info.ReputationScore = 30 // Below neutral (50) but above threshold (20)
		info.MaliciousCount = 0   // Clear malicious count for fresh start
		info.LastReputationReset = time.Now()
		info.ReputationResetCount++

		// Log recovery details (would be better with logger but PeerRegistry doesn't have one)
		// The sync coordinator will log the count of recovered peers
		_ = oldReputation // Avoid unused variable warning

		peersRecovered++
	}

	return peersRecovered
}

// ResetReputation resets reputation metrics for a specific peer or all peers.
// If peerID is empty, resets all peers. Otherwise, resets only the specified peer.
// Returns the number of peers that had their reputation reset.
func (pr *PeerRegistry) ResetReputation(peerIDStr string) int {
	pr.mu.Lock()
	defer pr.mu.Unlock()

	peersReset := 0

	// Helper function to reset a single peer's reputation
	resetPeer := func(info *PeerInfo) {
		// Reset interaction metrics
		info.InteractionAttempts = 0
		info.InteractionSuccesses = 0
		info.InteractionFailures = 0
		info.LastInteractionAttempt = time.Time{}
		info.LastInteractionSuccess = time.Time{}
		info.LastInteractionFailure = time.Time{}

		// Reset reputation score to neutral
		info.ReputationScore = 50.0
		info.MaliciousCount = 0
		info.AvgResponseTime = 0

		// Clear catchup error tracking
		info.LastCatchupError = ""
		info.LastCatchupErrorTime = time.Time{}

		// Clear reputation reset tracking to allow fresh start
		info.LastReputationReset = time.Time{}
		info.ReputationResetCount = 0

		// Clear sync attempt tracking
		info.LastSyncAttempt = time.Time{}
		info.SyncAttemptCount = 0
	}

	if peerIDStr == "" {
		// Reset all peers
		for _, info := range pr.peers {
			resetPeer(info)
			peersReset++
		}
	} else {
		// Reset specific peer - decode the peer ID string first
		peerID, err := peer.Decode(peerIDStr)
		if err != nil {
			// Invalid peer ID, return 0
			return 0
		}

		info, exists := pr.peers[peerID]
		if exists {
			resetPeer(info)
			peersReset = 1
		}
	}

	return peersReset
}

// GetPeersForCatchup returns peers suitable for catchup operations
// Filters for peers with DataHub URLs, sorted by reputation
// This is a specialized version of GetPeersByReputation for catchup operations
func (pr *PeerRegistry) GetPeersForCatchup() []*PeerInfo {
	pr.mu.RLock()
	defer pr.mu.RUnlock()

	result := make([]*PeerInfo, 0, len(pr.peers))
	for _, info := range pr.peers {
		// Only include peers with DataHub URLs that are not banned
		if info.DataHubURL != "" && !info.IsBanned {
			copy := *info
			result = append(result, &copy)
		}
	}

	// Sort by storage mode preference: full > pruned > unknown
	// Secondary sort by reputation score (highest first)
	// Tertiary sort by last success time (most recent first)
	for i := 0; i < len(result); i++ {
		for j := i + 1; j < len(result); j++ {
			if result[i].Storage != result[j].Storage {
				// Define storage preference order
				storagePreference := map[string]int{
					"full":   3,
					"pruned": 2,
					"":       1, // Unknown/old version
				}
				if storagePreference[result[i].Storage] < storagePreference[result[j].Storage] {
					result[i], result[j] = result[j], result[i]
				}
				continue
			}
			// Compare reputation scores
			if result[i].ReputationScore < result[j].ReputationScore {
				result[i], result[j] = result[j], result[i]
			} else if result[i].ReputationScore == result[j].ReputationScore {
				// If same reputation, prefer more recently successful peer
				if result[i].LastInteractionSuccess.Before(result[j].LastInteractionSuccess) {
					result[i], result[j] = result[j], result[i]
				}
			}
		}
	}

	return result
}
