package p2p

import (
	"sync"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/ordishs/go-utils/expiringmap"
)

// peerSyncState stores additional information that the P2P service tracks
// about a peer for synchronization purposes. This mirrors the legacy
// netsync manager's peer state tracking.
type peerSyncState struct {
	syncCandidate   bool                                               // Whether this peer is a candidate for syncing
	requestQueue    *txmap.SyncedSlice[wire.InvVect]                   // Queue of block/tx requests
	requestedTxns   *expiringmap.ExpiringMap[chainhash.Hash, struct{}] // Tracks requested transactions
	requestedBlocks *expiringmap.ExpiringMap[chainhash.Hash, struct{}] // Tracks requested blocks
}

// syncPeerState stores additional info about the current sync peer.
// This tracks network performance and health metrics.
type syncPeerState struct {
	mu                sync.RWMutex // Protects all fields
	recvBytes         uint64       // Total bytes received from this peer
	recvBytesLastTick uint64       // Bytes received at last tick
	lastBlockTime     time.Time    // Time of last block received
	violations        int          // Number of network speed violations
	ticks             uint64       // Number of evaluation ticks
}

// validNetworkSpeed checks if the peer is maintaining adequate network speed.
// Returns the number of violations accumulated.
func (sps *syncPeerState) validNetworkSpeed(minSyncPeerNetworkSpeed uint64, tickInterval time.Duration) int {
	sps.mu.Lock()
	defer sps.mu.Unlock()

	// Fresh sync peer needs another tick for evaluation
	if sps.ticks == 0 {
		return 0
	}

	// Calculate bytes received in the last tick
	recvDiff := sps.recvBytes - sps.recvBytesLastTick

	// Check if peer is below minimum speed threshold
	bytesPerSecond := recvDiff / uint64(tickInterval.Seconds())
	if bytesPerSecond < minSyncPeerNetworkSpeed {
		sps.violations++
		return sps.violations
	}

	// No violation, reset counter
	sps.violations = 0
	return sps.violations
}

// updateNetwork updates network statistics for the sync peer
func (sps *syncPeerState) updateNetwork(bytesReceived uint64) {
	sps.mu.Lock()
	defer sps.mu.Unlock()

	sps.recvBytesLastTick = sps.recvBytes
	sps.recvBytes = bytesReceived
	sps.ticks++
}

// updateLastBlockTime updates the last block time
func (sps *syncPeerState) updateLastBlockTime() {
	sps.mu.Lock()
	defer sps.mu.Unlock()
	sps.lastBlockTime = time.Now()
}

// getLastBlockTime returns the last block time
func (sps *syncPeerState) getLastBlockTime() time.Time {
	sps.mu.RLock()
	defer sps.mu.RUnlock()
	return sps.lastBlockTime
}

// getViolations returns the current violation count
func (sps *syncPeerState) getViolations() int {
	sps.mu.RLock()
	defer sps.mu.RUnlock()
	return sps.violations
}

// PeerStateManager manages peer states for synchronization
type PeerStateManager struct {
	mu     sync.RWMutex
	states map[peer.ID]*peerSyncState
}

// NewPeerStateManager creates a new peer state manager
func NewPeerStateManager() *PeerStateManager {
	return &PeerStateManager{
		states: make(map[peer.ID]*peerSyncState),
	}
}

// AddPeer adds a new peer to the state manager
func (psm *PeerStateManager) AddPeer(peerID peer.ID, syncCandidate bool) {
	psm.mu.Lock()
	defer psm.mu.Unlock()

	psm.states[peerID] = &peerSyncState{
		syncCandidate:   syncCandidate,
		requestQueue:    txmap.NewSyncedSlice[wire.InvVect](wire.MaxInvPerMsg),
		requestedTxns:   expiringmap.New[chainhash.Hash, struct{}](10 * time.Second), // 10s timeout for tx requests
		requestedBlocks: expiringmap.New[chainhash.Hash, struct{}](60 * time.Minute), // 1h timeout for block requests
	}
}

// RemovePeer removes a peer from the state manager
func (psm *PeerStateManager) RemovePeer(peerID peer.ID) {
	psm.mu.Lock()
	defer psm.mu.Unlock()
	delete(psm.states, peerID)
}

// GetPeerState returns the state for a specific peer
func (psm *PeerStateManager) GetPeerState(peerID peer.ID) (*peerSyncState, bool) {
	psm.mu.RLock()
	defer psm.mu.RUnlock()
	state, exists := psm.states[peerID]
	return state, exists
}

// GetSyncCandidates returns all peers that are sync candidates
func (psm *PeerStateManager) GetSyncCandidates() []peer.ID {
	psm.mu.RLock()
	defer psm.mu.RUnlock()

	candidates := []peer.ID{}
	for peerID, state := range psm.states {
		if state.syncCandidate {
			candidates = append(candidates, peerID)
		}
	}
	return candidates
}

// SetSyncCandidate updates whether a peer is a sync candidate
func (psm *PeerStateManager) SetSyncCandidate(peerID peer.ID, candidate bool) {
	psm.mu.Lock()
	defer psm.mu.Unlock()

	if state, exists := psm.states[peerID]; exists {
		state.syncCandidate = candidate
	}
}

// GetAllPeers returns all peer IDs being tracked
func (psm *PeerStateManager) GetAllPeers() []peer.ID {
	psm.mu.RLock()
	defer psm.mu.RUnlock()

	peers := make([]peer.ID, 0, len(psm.states))
	for peerID := range psm.states {
		peers = append(peers, peerID)
	}
	return peers
}
