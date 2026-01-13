package p2p

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/libp2p/go-libp2p/core/peer"
)

// PeerRegistryCacheVersion is the current version of the cache format
const PeerRegistryCacheVersion = "1.0"

// PeerRegistryCache represents the persistent cache structure for peer registry data
type PeerRegistryCache struct {
	Version     string                        `json:"version"`
	LastUpdated time.Time                     `json:"last_updated"`
	Peers       map[string]*CachedPeerMetrics `json:"peers"`
}

// CachedPeerMetrics represents the cached metrics for a single peer
type CachedPeerMetrics struct {
	// Interaction metrics - works for all types of interactions (blocks, subtrees, catchup, etc.)
	InteractionAttempts    int64     `json:"interaction_attempts"`
	InteractionSuccesses   int64     `json:"interaction_successes"`
	InteractionFailures    int64     `json:"interaction_failures"`
	LastInteractionAttempt time.Time `json:"last_interaction_attempt,omitempty"`
	LastInteractionSuccess time.Time `json:"last_interaction_success,omitempty"`
	LastInteractionFailure time.Time `json:"last_interaction_failure,omitempty"`
	ReputationScore        float64   `json:"reputation_score"`
	MaliciousCount         int64     `json:"malicious_count"`
	AvgResponseMS          int64     `json:"avg_response_ms"` // Duration in milliseconds

	// Interaction type breakdown
	BlocksReceived       int64 `json:"blocks_received,omitempty"`
	SubtreesReceived     int64 `json:"subtrees_received,omitempty"`
	TransactionsReceived int64 `json:"transactions_received,omitempty"`
	CatchupBlocks        int64 `json:"catchup_blocks,omitempty"`

	// Additional peer info worth persisting
	Height     int32  `json:"height,omitempty"`
	BlockHash  string `json:"block_hash,omitempty"`
	DataHubURL string `json:"data_hub_url,omitempty"`
	ClientName string `json:"client_name,omitempty"`
	Storage    string `json:"storage,omitempty"`

	// Legacy fields for backward compatibility (can read old cache files)
	CatchupAttempts        int64     `json:"catchup_attempts,omitempty"`
	CatchupSuccesses       int64     `json:"catchup_successes,omitempty"`
	CatchupFailures        int64     `json:"catchup_failures,omitempty"`
	CatchupLastAttempt     time.Time `json:"catchup_last_attempt,omitempty"`
	CatchupLastSuccess     time.Time `json:"catchup_last_success,omitempty"`
	CatchupLastFailure     time.Time `json:"catchup_last_failure,omitempty"`
	CatchupReputationScore float64   `json:"catchup_reputation_score,omitempty"`
	CatchupMaliciousCount  int64     `json:"catchup_malicious_count,omitempty"`
	CatchupAvgResponseMS   int64     `json:"catchup_avg_response_ms,omitempty"`
}

// getPeerRegistryCacheFilePath constructs the full path to the teranode_peer_registry.json file
func getPeerRegistryCacheFilePath(configuredDir string) string {
	var dir string
	if configuredDir != "" {
		dir = configuredDir
	} else {
		// Default to current directory
		dir = "."
	}
	return filepath.Join(dir, "teranode_peer_registry.json")
}

// SavePeerRegistryCache saves the peer registry data to a JSON file
func (pr *PeerRegistry) SavePeerRegistryCache(cacheDir string) error {
	pr.mu.RLock()
	defer pr.mu.RUnlock()

	cache := &PeerRegistryCache{
		Version:     PeerRegistryCacheVersion,
		LastUpdated: time.Now(),
		Peers:       make(map[string]*CachedPeerMetrics),
	}

	// Convert internal peer data to cache format
	for id, info := range pr.peers {
		// Only cache peers with meaningful metrics
		if info.InteractionAttempts > 0 || info.DataHubURL != "" || info.Height > 0 ||
			info.BlocksReceived > 0 || info.SubtreesReceived > 0 || info.TransactionsReceived > 0 {
			// Store peer ID as string
			cache.Peers[id.String()] = &CachedPeerMetrics{
				InteractionAttempts:    info.InteractionAttempts,
				InteractionSuccesses:   info.InteractionSuccesses,
				InteractionFailures:    info.InteractionFailures,
				LastInteractionAttempt: info.LastInteractionAttempt,
				LastInteractionSuccess: info.LastInteractionSuccess,
				LastInteractionFailure: info.LastInteractionFailure,
				ReputationScore:        info.ReputationScore,
				MaliciousCount:         info.MaliciousCount,
				AvgResponseMS:          info.AvgResponseTime.Milliseconds(),
				BlocksReceived:         info.BlocksReceived,
				SubtreesReceived:       info.SubtreesReceived,
				TransactionsReceived:   info.TransactionsReceived,
				CatchupBlocks:          info.CatchupBlocks,
				Height:                 info.Height,
				BlockHash:              info.BlockHash,
				DataHubURL:             info.DataHubURL,
				ClientName:             info.ClientName,
				Storage:                info.Storage,
			}
		}
	}

	// Marshal to JSON with indentation for readability
	data, err := json.MarshalIndent(cache, "", "  ")
	if err != nil {
		return errors.NewProcessingError("failed to marshal peer registry cache: %v", err)
	}

	// Write to temporary file first, then rename for atomicity
	cacheFile := getPeerRegistryCacheFilePath(cacheDir)
	// Use unique temp file name to avoid concurrent write conflicts
	tempFile := fmt.Sprintf("%s.tmp.%d", cacheFile, time.Now().UnixNano())

	if err := os.WriteFile(tempFile, data, 0600); err != nil {
		return errors.NewProcessingError("failed to write peer registry cache: %v", err)
	}

	// Atomic rename
	if err := os.Rename(tempFile, cacheFile); err != nil {
		// Clean up temp file if rename failed
		_ = os.Remove(tempFile)
		return errors.NewProcessingError("failed to finalize peer registry cache: %v", err)
	}

	return nil
}

// LoadPeerRegistryCache loads the peer registry data from the cache file
func (pr *PeerRegistry) LoadPeerRegistryCache(cacheDir string) error {
	cacheFile := getPeerRegistryCacheFilePath(cacheDir)

	// Check if file exists
	if _, err := os.Stat(cacheFile); os.IsNotExist(err) {
		// No cache file, not an error
		return nil
	}

	file, err := os.Open(cacheFile)
	if err != nil {
		return errors.NewProcessingError("failed to open peer registry cache: %v", err)
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return errors.NewProcessingError("failed to read peer registry cache: %v", err)
	}

	var cache PeerRegistryCache
	if err := json.Unmarshal(data, &cache); err != nil {
		// Log error but don't fail - cache might be corrupted
		return errors.NewProcessingError("failed to unmarshal peer registry cache (will start fresh): %v", err)
	}

	// Check version compatibility
	if cache.Version != PeerRegistryCacheVersion {
		// Different version, skip loading to avoid compatibility issues
		return errors.NewProcessingError("cache version mismatch (expected %s, got %s), will start fresh", PeerRegistryCacheVersion, cache.Version)
	}

	pr.mu.Lock()
	defer pr.mu.Unlock()

	// Restore metrics for each peer
	for idStr, metrics := range cache.Peers {
		// Try to decode as a peer ID
		// Note: peer.ID is just a string type, so we can cast it directly
		peerID, err := peer.Decode(idStr)
		if err != nil {
			// Invalid peer ID in cache, skip
			continue
		}

		// Check if peer exists in registry
		info, exists := pr.peers[peerID]
		if !exists {
			// Create new peer entry with cached data
			info = &PeerInfo{
				ID:              peerID,
				Height:          metrics.Height,
				BlockHash:       metrics.BlockHash,
				DataHubURL:      metrics.DataHubURL,
				Storage:         metrics.Storage,
				ReputationScore: 50.0, // Start with neutral reputation
			}
			pr.peers[peerID] = info
		}

		// Restore interaction metrics (prefer new fields, fall back to legacy)
		switch {
		case metrics.InteractionAttempts > 0:
			info.InteractionAttempts = metrics.InteractionAttempts
			info.InteractionSuccesses = metrics.InteractionSuccesses
			info.InteractionFailures = metrics.InteractionFailures
			info.LastInteractionAttempt = metrics.LastInteractionAttempt
			info.LastInteractionSuccess = metrics.LastInteractionSuccess
			info.LastInteractionFailure = metrics.LastInteractionFailure
			info.ReputationScore = metrics.ReputationScore
			info.MaliciousCount = metrics.MaliciousCount
			info.AvgResponseTime = time.Duration(metrics.AvgResponseMS) * time.Millisecond
		case metrics.CatchupAttempts > 0:
			// Fall back to legacy fields for backward compatibility
			info.InteractionAttempts = metrics.CatchupAttempts
			info.InteractionSuccesses = metrics.CatchupSuccesses
			info.InteractionFailures = metrics.CatchupFailures
			info.LastInteractionAttempt = metrics.CatchupLastAttempt
			info.LastInteractionSuccess = metrics.CatchupLastSuccess
			info.LastInteractionFailure = metrics.CatchupLastFailure
			info.ReputationScore = metrics.CatchupReputationScore
			info.MaliciousCount = metrics.CatchupMaliciousCount
			info.AvgResponseTime = time.Duration(metrics.CatchupAvgResponseMS) * time.Millisecond
			// Also count as catchup blocks for backward compatibility
			info.CatchupBlocks = metrics.CatchupSuccesses
		default:
			// No interaction history in cache, ensure default reputation
			if info.ReputationScore == 0 {
				info.ReputationScore = 50.0
			}
		}

		// Restore interaction type breakdown
		info.BlocksReceived = metrics.BlocksReceived
		info.SubtreesReceived = metrics.SubtreesReceived
		info.TransactionsReceived = metrics.TransactionsReceived
		// Only set CatchupBlocks if it hasn't been set by legacy field mapping
		if info.CatchupBlocks == 0 && metrics.CatchupBlocks > 0 {
			info.CatchupBlocks = metrics.CatchupBlocks
		}

		// Update DataHubURL and height if not already set
		if info.DataHubURL == "" && metrics.DataHubURL != "" {
			info.DataHubURL = metrics.DataHubURL
		}
		if info.Height == 0 && metrics.Height > 0 {
			info.Height = metrics.Height
			info.BlockHash = metrics.BlockHash
		}
	}

	return nil
}
