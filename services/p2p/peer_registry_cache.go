package p2p

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// PeerRegistryCacheVersion is the current version of the cache format
const PeerRegistryCacheVersion = "1.0"

// PeerRegistryCache represents the persistent cache structure for peer registry data
type PeerRegistryCache struct {
	Version     string                         `json:"version"`
	LastUpdated time.Time                      `json:"last_updated"`
	Peers       map[string]*CachedPeerMetrics `json:"peers"`
}

// CachedPeerMetrics represents the cached metrics for a single peer
type CachedPeerMetrics struct {
	// Catchup metrics
	CatchupAttempts        int64     `json:"catchup_attempts"`
	CatchupSuccesses       int64     `json:"catchup_successes"`
	CatchupFailures        int64     `json:"catchup_failures"`
	CatchupLastAttempt     time.Time `json:"catchup_last_attempt,omitempty"`
	CatchupLastSuccess     time.Time `json:"catchup_last_success,omitempty"`
	CatchupLastFailure     time.Time `json:"catchup_last_failure,omitempty"`
	CatchupReputationScore float64   `json:"catchup_reputation_score"`
	CatchupMaliciousCount  int64     `json:"catchup_malicious_count"`
	CatchupAvgResponseMS   int64     `json:"catchup_avg_response_ms"` // Duration in milliseconds

	// Additional peer info worth persisting
	Height     int32  `json:"height,omitempty"`
	BlockHash  string `json:"block_hash,omitempty"`
	DataHubURL string `json:"data_hub_url,omitempty"`
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
		if info.CatchupAttempts > 0 || info.DataHubURL != "" || info.Height > 0 {
			// Store peer ID as string
			cache.Peers[string(id)] = &CachedPeerMetrics{
				CatchupAttempts:        info.CatchupAttempts,
				CatchupSuccesses:       info.CatchupSuccesses,
				CatchupFailures:        info.CatchupFailures,
				CatchupLastAttempt:     info.CatchupLastAttempt,
				CatchupLastSuccess:     info.CatchupLastSuccess,
				CatchupLastFailure:     info.CatchupLastFailure,
				CatchupReputationScore: info.CatchupReputationScore,
				CatchupMaliciousCount:  info.CatchupMaliciousCount,
				CatchupAvgResponseMS:   info.CatchupAvgResponseTime.Milliseconds(),
				Height:                 info.Height,
				BlockHash:              info.BlockHash,
				DataHubURL:             info.DataHubURL,
			}
		}
	}

	// Marshal to JSON with indentation for readability
	data, err := json.MarshalIndent(cache, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal peer registry cache: %w", err)
	}

	// Write to temporary file first, then rename for atomicity
	cacheFile := getPeerRegistryCacheFilePath(cacheDir)
	// Use unique temp file name to avoid concurrent write conflicts
	tempFile := fmt.Sprintf("%s.tmp.%d", cacheFile, time.Now().UnixNano())

	if err := os.WriteFile(tempFile, data, 0644); err != nil {
		return fmt.Errorf("failed to write peer registry cache: %w", err)
	}

	// Atomic rename
	if err := os.Rename(tempFile, cacheFile); err != nil {
		// Clean up temp file if rename failed
		_ = os.Remove(tempFile)
		return fmt.Errorf("failed to finalize peer registry cache: %w", err)
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
		return fmt.Errorf("failed to open peer registry cache: %w", err)
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return fmt.Errorf("failed to read peer registry cache: %w", err)
	}

	var cache PeerRegistryCache
	if err := json.Unmarshal(data, &cache); err != nil {
		// Log error but don't fail - cache might be corrupted
		return fmt.Errorf("failed to unmarshal peer registry cache (will start fresh): %w", err)
	}

	// Check version compatibility
	if cache.Version != PeerRegistryCacheVersion {
		// Different version, skip loading to avoid compatibility issues
		return fmt.Errorf("cache version mismatch (expected %s, got %s), will start fresh", PeerRegistryCacheVersion, cache.Version)
	}

	pr.mu.Lock()
	defer pr.mu.Unlock()

	// Restore metrics for each peer
	for idStr, metrics := range cache.Peers {
		// Try to decode as a peer ID
		// Note: peer.ID is just a string type, so we can cast it directly
		peerID := peer.ID(idStr)

		// Check if peer exists in registry
		info, exists := pr.peers[peerID]
		if !exists {
			// Create new peer entry with cached data
			info = &PeerInfo{
				ID:         peerID,
				Height:     metrics.Height,
				BlockHash:  metrics.BlockHash,
				DataHubURL: metrics.DataHubURL,
				IsHealthy:  true, // Assume healthy initially
			}
			pr.peers[peerID] = info
		}

		// Restore catchup metrics
		info.CatchupAttempts = metrics.CatchupAttempts
		info.CatchupSuccesses = metrics.CatchupSuccesses
		info.CatchupFailures = metrics.CatchupFailures
		info.CatchupLastAttempt = metrics.CatchupLastAttempt
		info.CatchupLastSuccess = metrics.CatchupLastSuccess
		info.CatchupLastFailure = metrics.CatchupLastFailure
		info.CatchupReputationScore = metrics.CatchupReputationScore
		info.CatchupMaliciousCount = metrics.CatchupMaliciousCount
		info.CatchupAvgResponseTime = time.Duration(metrics.CatchupAvgResponseMS) * time.Millisecond

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