package p2p

import (
	"context"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/libp2p/go-libp2p/core/peer"
)

const (
	// minInFlightBlocks is the minimum number of blocks that should be
	// in the request queue for headers-first mode before requesting more
	minInFlightBlocks = 10

	// maxNetworkViolations is the max number of network violations a
	// sync peer can have before a new sync peer is found
	maxNetworkViolations = 3

	// maxLastBlockTime is the longest time in seconds that we will
	// stay with a sync peer while below the current blockchain height.
	// Set to 3 minutes (matching bitcoin-sv)
	maxLastBlockTime = 3 * time.Minute

	// syncPeerTickerInterval is how often we check the current
	// sync peer. Set to 30 seconds (matching bitcoin-sv)
	syncPeerTickerInterval = 30 * time.Second

	// minSyncPeerNetworkSpeed is the minimum network speed in bytes/second
	// Default to 10KB/s
	defaultMinSyncPeerNetworkSpeed = 10 * 1024
)

// SyncManager manages peer synchronization for the P2P service
// It ensures we sync from the best available peer and handles
// peer switching when the current sync peer becomes unhealthy
type SyncManager struct {
	mu                      sync.RWMutex
	logger                  ulogger.Logger
	chainParams             *chaincfg.Params
	peerStates              *PeerStateManager
	syncPeer                peer.ID
	syncPeerState           *syncPeerState
	syncPeerTicker          *time.Ticker
	minSyncPeerNetworkSpeed uint64
	headersFirstMode        bool
	shutdown                int32

	// Callbacks for getting peer information
	getPeerHeight  func(peer.ID) int32
	getLocalHeight func() uint32
	getPeerIPs     func(peer.ID) []string
}

// NewSyncManager creates a new sync manager
func NewSyncManager(logger ulogger.Logger, chainParams *chaincfg.Params) *SyncManager {
	return &SyncManager{
		logger:                  logger,
		chainParams:             chainParams,
		peerStates:              NewPeerStateManager(),
		minSyncPeerNetworkSpeed: defaultMinSyncPeerNetworkSpeed,
	}
}

// SetPeerHeightCallback sets the callback for getting peer height
func (sm *SyncManager) SetPeerHeightCallback(fn func(peer.ID) int32) {
	sm.getPeerHeight = fn
}

// SetLocalHeightCallback sets the callback for getting local height
func (sm *SyncManager) SetLocalHeightCallback(fn func() uint32) {
	sm.getLocalHeight = fn
}

// SetPeerIPsCallback sets the callback for getting peer IPs
func (sm *SyncManager) SetPeerIPsCallback(fn func(peer.ID) []string) {
	sm.getPeerIPs = fn
}

// AddPeer adds a new peer to the sync manager
func (sm *SyncManager) AddPeer(peerID peer.ID) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Just register the peer exists - don't determine sync candidacy yet
	// We need the peer's height to make that decision
	sm.peerStates.AddPeer(peerID, false)
	sm.logger.Infof("[SyncManager] Added peer %s (awaiting height for sync evaluation)", peerID)
}

// RemovePeer removes a peer from the sync manager
func (sm *SyncManager) RemovePeer(peerID peer.ID) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.peerStates.RemovePeer(peerID)

	// If this was our sync peer, we need to find a new one
	if sm.syncPeer == peerID {
		sm.logger.Infof("[SyncManager] Sync peer %s removed, finding new sync peer", peerID)
		sm.syncPeer = ""
		sm.syncPeerState = nil
		sm.selectSyncPeerLocked()
	}
}

// isSyncCandidate determines if a peer is eligible for syncing
func (sm *SyncManager) isSyncCandidate(peerID peer.ID) bool {
	// In regtest, accept all peers as sync candidates
	// This is for local testing where nodes connect via localhost
	if sm.chainParams == &chaincfg.RegressionNetParams {
		// For regtest, accept all peers regardless of IP
		// This fixes issues where peers might advertise 0.0.0.0 or have no IPs yet
		return true
	}

	// For mainnet/testnet, all peers that support our protocol are candidates
	// In the future, we might check for NODE_NETWORK service flag
	return true
}

// selectSyncPeer selects the best peer to sync from
// This implements the bitcoin-sv peer selection logic:
// 1. Group peers by height relative to ours
// 2. Prefer peers ahead of us (bestPeers)
// 3. Fallback to peers at same height (okPeers)
// 4. Ignore peers behind us
// 5. Random selection within the chosen group
func (sm *SyncManager) selectSyncPeer() peer.ID {
	if sm.getLocalHeight == nil || sm.getPeerHeight == nil {
		sm.logger.Warnf("[SyncManager] Height callbacks not set, cannot select sync peer")
		return ""
	}

	localHeight := int32(sm.getLocalHeight())
	candidates := sm.peerStates.GetSyncCandidates()

	if len(candidates) == 0 {
		sm.logger.Debugf("[SyncManager] No sync candidates available")
		return ""
	}

	var bestPeers []peer.ID // Peers ahead of us
	var okPeers []peer.ID   // Peers at same height

	for _, peerID := range candidates {
		peerHeight := sm.getPeerHeight(peerID)

		// Skip peers with unknown height
		if peerHeight <= 0 {
			continue
		}

		if peerHeight > localHeight {
			// Peer is ahead of us - good candidate
			bestPeers = append(bestPeers, peerID)
			sm.logger.Debugf("[SyncManager] Peer %s at height %d > local %d (bestPeer)",
				peerID, peerHeight, localHeight)
		} else if peerHeight == localHeight {
			// Peer at same height - acceptable candidate
			okPeers = append(okPeers, peerID)
			sm.logger.Debugf("[SyncManager] Peer %s at height %d == local %d (okPeer)",
				peerID, peerHeight, localHeight)
		} else {
			// Peer behind us - skip
			sm.logger.Debugf("[SyncManager] Peer %s at height %d < local %d (skipping)",
				peerID, peerHeight, localHeight)
		}
	}

	// Select from the best available group
	var selectedPeer peer.ID

	if len(bestPeers) > 0 {
		// Randomly select from peers ahead of us
		// #nosec G404 - Using weak random is acceptable for peer selection
		selectedPeer = bestPeers[rand.IntN(len(bestPeers))]
		sm.logger.Infof("[SyncManager] Selected best peer %s from %d peers ahead of us",
			selectedPeer, len(bestPeers))
	} else if len(okPeers) > 0 {
		// No peers ahead, randomly select from peers at same height
		// #nosec G404 - Using weak random is acceptable for peer selection
		selectedPeer = okPeers[rand.IntN(len(okPeers))]
		sm.logger.Infof("[SyncManager] No peers ahead, selected ok peer %s from %d peers at same height",
			selectedPeer, len(okPeers))
	} else {
		sm.logger.Warnf("[SyncManager] No suitable sync peers found")
	}

	return selectedPeer
}

// selectSyncPeerLocked selects a sync peer but does not trigger blockchain sync
// It assumes the lock is already held. Returns true if a sync peer was selected.
func (sm *SyncManager) selectSyncPeerLocked() bool {
	// Already syncing
	if sm.syncPeer != "" {
		sm.logger.Debugf("[SyncManager] Already have a sync peer, skipping selection")
		return false
	}

	// Select the best peer
	bestPeer := sm.selectSyncPeer()
	if bestPeer == "" {
		sm.logger.Warnf("[SyncManager] No sync peer available")
		return false
	}

	// Set as sync peer
	sm.syncPeer = bestPeer
	sm.syncPeerState = &syncPeerState{
		lastBlockTime: time.Now(),
		recvBytes:     0,
		violations:    0,
		ticks:         0,
	}

	sm.logger.Infof("[SyncManager] Selected sync peer %s", bestPeer)
	return true
}

// checkSyncPeer evaluates the current sync peer's health
func (sm *SyncManager) checkSyncPeer() {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.syncPeer == "" {
		// No sync peer, next peer that's ahead will be selected automatically
		return
	}

	if sm.syncPeerState == nil {
		sm.logger.Errorf("[SyncManager] Sync peer set but no state, resetting")
		sm.syncPeer = ""
		return
	}

	// Check network speed violations
	violations := sm.syncPeerState.validNetworkSpeed(sm.minSyncPeerNetworkSpeed, syncPeerTickerInterval)

	// Check time since last block
	timeSinceLastBlock := time.Since(sm.syncPeerState.getLastBlockTime())

	sm.logger.Debugf("[SyncManager] Sync peer %s check: violations=%d/%d, time since last block=%v/%v",
		sm.syncPeer, violations, maxNetworkViolations, timeSinceLastBlock, maxLastBlockTime)

	// Determine if we need to switch sync peer
	needSwitch := false
	switchReason := ""

	if violations >= maxNetworkViolations {
		needSwitch = true
		switchReason = "too many network speed violations"
	} else if timeSinceLastBlock > maxLastBlockTime {
		needSwitch = true
		switchReason = "no block received in time limit"
	}

	if needSwitch {
		sm.logger.Infof("[SyncManager] Switching sync peer %s due to: %s", sm.syncPeer, switchReason)
		sm.syncPeer = ""
		sm.syncPeerState = nil
		// Try to find a new sync peer using bitcoin-sv selection logic
		sm.selectSyncPeerLocked()
	}
}

// Start starts the sync manager's periodic evaluation
func (sm *SyncManager) Start(ctx context.Context) {
	sm.mu.Lock()
	sm.syncPeerTicker = time.NewTicker(syncPeerTickerInterval)
	sm.mu.Unlock()

	go func() {
		for {
			select {
			case <-ctx.Done():
				sm.logger.Infof("[SyncManager] Shutting down")
				sm.mu.Lock()
				if sm.syncPeerTicker != nil {
					sm.syncPeerTicker.Stop()
				}
				sm.mu.Unlock()
				return
			case <-sm.syncPeerTicker.C:
				sm.checkSyncPeer()
			}
		}
	}()
}

// GetSyncPeer returns the current sync peer
func (sm *SyncManager) GetSyncPeer() peer.ID {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.syncPeer
}

// IsSyncPeer checks if a given peer is the current sync peer
func (sm *SyncManager) IsSyncPeer(peerID peer.ID) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.syncPeer == peerID
}

// UpdatePeerHeight updates the height for a peer and evaluates sync candidacy
// This is where we evaluate sync candidacy since we now have height information
// Returns true if this peer was selected as the sync peer
func (sm *SyncManager) UpdatePeerHeight(peerID peer.ID, height int32) bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Update sync candidacy now that we have height
	if state, exists := sm.peerStates.GetPeerState(peerID); exists {
		// Re-evaluate sync candidacy with height information
		isSyncCandidate := sm.isSyncCandidate(peerID)
		if isSyncCandidate != state.syncCandidate {
			sm.peerStates.SetSyncCandidate(peerID, isSyncCandidate)
			sm.logger.Debugf("[SyncManager] Peer %s sync candidate status updated to %v (height: %d)",
				peerID, isSyncCandidate, height)
		}
	}

	// Check if we should select a sync peer (only if we don't have one)
	if sm.syncPeer == "" && sm.getLocalHeight != nil {
		localHeight := int32(sm.getLocalHeight())

		// If this peer is ahead of us, try to select a sync peer
		if height > localHeight {
			// Use the bitcoin-sv selection logic synchronously
			return sm.selectSyncPeerLocked()
		}
	}

	return false
}

// UpdateSyncPeerNetwork updates network stats for the sync peer
func (sm *SyncManager) UpdateSyncPeerNetwork(peerID peer.ID, bytesReceived uint64) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.syncPeer == peerID && sm.syncPeerState != nil {
		sm.syncPeerState.updateNetwork(bytesReceived)
	}
}

// UpdateSyncPeerBlockTime updates the last block time for the sync peer
func (sm *SyncManager) UpdateSyncPeerBlockTime(peerID peer.ID) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.syncPeer == peerID && sm.syncPeerState != nil {
		sm.syncPeerState.updateLastBlockTime()
	}
}
