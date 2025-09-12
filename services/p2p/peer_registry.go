package p2p

import (
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// PeerInfo holds all information about a peer
type PeerInfo struct {
	ID              peer.ID
	Height          int32
	BlockHash       string
	DataHubURL      string
	IsHealthy       bool
	LastHealthCheck time.Time
	BanScore        int
	IsBanned        bool
	ConnectedAt     time.Time
	BytesReceived   uint64
	LastBlockTime   time.Time
	LastMessageTime time.Time // Last time we received any message from this peer
	URLResponsive   bool      // Whether the DataHub URL is responsive
	LastURLCheck    time.Time // Last time we checked URL responsiveness
}

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

// AddPeer adds or updates a peer
func (pr *PeerRegistry) AddPeer(id peer.ID) {
	pr.mu.Lock()
	defer pr.mu.Unlock()

	if _, exists := pr.peers[id]; !exists {
		now := time.Now()
		pr.peers[id] = &PeerInfo{
			ID:              id,
			ConnectedAt:     now,
			LastMessageTime: now,  // Initialize to connection time
			IsHealthy:       true, // Assume healthy until proven otherwise
		}
	}
}

// RemovePeer removes a peer
func (pr *PeerRegistry) RemovePeer(id peer.ID) {
	pr.mu.Lock()
	defer pr.mu.Unlock()

	delete(pr.peers, id)
}

// GetPeer returns peer info
func (pr *PeerRegistry) GetPeer(id peer.ID) (*PeerInfo, bool) {
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

// GetAllPeers returns all peer information
func (pr *PeerRegistry) GetAllPeers() []*PeerInfo {
	pr.mu.RLock()
	defer pr.mu.RUnlock()

	result := make([]*PeerInfo, 0, len(pr.peers))
	for _, info := range pr.peers {
		copy := *info
		result = append(result, &copy)
	}
	return result
}

// UpdateHeight updates a peer's height
func (pr *PeerRegistry) UpdateHeight(id peer.ID, height int32, blockHash string) {
	pr.mu.Lock()
	defer pr.mu.Unlock()

	if info, exists := pr.peers[id]; exists {
		info.Height = height
		info.BlockHash = blockHash
	}
}

// UpdateBlockHash updates only the peer's block hash
func (pr *PeerRegistry) UpdateBlockHash(id peer.ID, blockHash string) {
	pr.mu.Lock()
	defer pr.mu.Unlock()

	if info, exists := pr.peers[id]; exists {
		info.BlockHash = blockHash
	}
}

// UpdateDataHubURL updates a peer's DataHub URL
func (pr *PeerRegistry) UpdateDataHubURL(id peer.ID, url string) {
	pr.mu.Lock()
	defer pr.mu.Unlock()

	if info, exists := pr.peers[id]; exists {
		info.DataHubURL = url
	}
}

// UpdateHealth updates a peer's health status
func (pr *PeerRegistry) UpdateHealth(id peer.ID, healthy bool) {
	pr.mu.Lock()
	defer pr.mu.Unlock()

	if info, exists := pr.peers[id]; exists {
		info.IsHealthy = healthy
		info.LastHealthCheck = time.Now()
	}
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

// UpdateURLResponsiveness updates whether a peer's DataHub URL is responsive
func (pr *PeerRegistry) UpdateURLResponsiveness(id peer.ID, responsive bool) {
	pr.mu.Lock()
	defer pr.mu.Unlock()

	if info, exists := pr.peers[id]; exists {
		info.URLResponsive = responsive
		info.LastURLCheck = time.Now()
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

// PeerCount returns the number of peers
func (pr *PeerRegistry) PeerCount() int {
	pr.mu.RLock()
	defer pr.mu.RUnlock()

	return len(pr.peers)
}
