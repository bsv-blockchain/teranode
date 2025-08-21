package p2p

import (
	"context"
	"sync"
	"time"

	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/libp2p/go-libp2p/core/peer"
)

const (
	// minInFlightBlocks is the minimum number of blocks that should be
	// in the request queue for headers-first mode before requesting more

	// minInFlightBlocks = 10

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

	emptyPeerID = peer.ID("")
)

// BlockAnnouncement represents a buffered block announcement
type BlockAnnouncement struct {
	Hash       string
	Height     uint32
	DataHubURL string
	PeerID     string
	From       string
	Timestamp  time.Time
}

// SyncManager manages peer synchronization for the P2P service
// It ensures we sync from the best available peer and handles
// peer switching when the current sync peer becomes unhealthy
type SyncManager struct {
	mu                      sync.RWMutex
	logger                  ulogger.Logger
	settings                *settings.Settings
	peerStates              *PeerStateManager
	syncPeer                peer.ID
	syncPeerState           *syncPeerState
	syncPeerTicker          *time.Ticker
	minSyncPeerNetworkSpeed uint64
	headersFirstMode        bool
	shutdown                int32
	startTime               time.Time
	initialSelectionDone    bool
	forceSyncPeer           peer.ID // Force sync from specific peer, overrides automatic selection

	// Block announcement buffering during initial sync period
	announcementBuffer []*BlockAnnouncement
	bufferMu           sync.Mutex

	// Callbacks for getting peer information
	getPeerHeight  func(peer.ID) int32
	getLocalHeight func() uint32
	getPeerIPs     func(peer.ID) []string
}

// NewSyncManager creates a new sync manager
func NewSyncManager(logger ulogger.Logger, settings *settings.Settings) *SyncManager {
	return &SyncManager{
		logger:                  logger,
		settings:                settings,
		peerStates:              NewPeerStateManager(),
		minSyncPeerNetworkSpeed: defaultMinSyncPeerNetworkSpeed,
		initialSelectionDone:    settings.P2P.MinPeersForSync == 0, // Only do initial selection if we expect peers
	}
}

// SetForceSyncPeer sets a forced sync peer that overrides automatic selection
// If peerID is empty, automatic selection is restored
func (sm *SyncManager) SetForceSyncPeer(peerID string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if peerID == "" {
		sm.logger.Infof("[SyncManager] Clearing forced sync peer, returning to automatic selection")
		sm.forceSyncPeer = emptyPeerID
		// If we had a sync peer that was forced, clear it so automatic selection can take over
		if sm.syncPeer != emptyPeerID {
			sm.syncPeer = emptyPeerID
			sm.syncPeerState = nil
			sm.selectSyncPeerLocked()
		}
		return nil
	}

	// Validate the peer ID format
	pid, err := peer.Decode(peerID)
	if err != nil {
		return err // peer.Decode returns descriptive error
	}

	sm.forceSyncPeer = pid
	sm.logger.Infof("[SyncManager] Setting forced sync peer to %s", pid)

	// If this peer exists and is connected, set it as sync peer immediately
	if _, exists := sm.peerStates.GetPeerState(pid); exists {
		sm.syncPeer = pid
		sm.syncPeerState = &syncPeerState{
			lastBlockTime: time.Now(),
			recvBytes:     0,
			violations:    0,
			ticks:         0,
		}
		sm.logger.Infof("[SyncManager] Forced sync peer %s is connected and set as active sync peer", pid)
	} else {
		sm.logger.Warnf("[SyncManager] Forced sync peer %s is not currently connected", pid)
	}

	return nil
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
		sm.syncPeer = emptyPeerID
		sm.syncPeerState = nil

		// Only select a new peer if we're not using a forced peer
		switch sm.forceSyncPeer {
		case emptyPeerID:
			sm.selectSyncPeerLocked()
		case peerID:
			sm.logger.Infof("[SyncManager] Forced sync peer %s disconnected, will wait for reconnection", peerID)
		default:
			// This was not the forced peer, so we can ignore its removal
			sm.logger.Debugf("[SyncManager] Non-forced peer %s removed, continuing to wait for forced peer %s", peerID, sm.forceSyncPeer)
		}
	}
}

// isSyncCandidate determines if a peer is eligible for syncing
func (sm *SyncManager) isSyncCandidate(peerID peer.ID) bool {
	_ = peerID

	// In regtest, accept all peers as sync candidates
	// This is for local testing where nodes connect via localhost
	if sm.settings.ChainCfgParams == &chaincfg.RegressionNetParams {
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
// 1. If a forced sync peer is configured and connected, use it
// 2. Otherwise, group peers by height relative to ours
// 3. Prefer peers ahead of us (bestPeers)
// 4. Fallback to peers at same height (okPeers)
// 5. Ignore peers behind us
// 6. Random selection within the chosen group
func (sm *SyncManager) selectSyncPeer() peer.ID {
	// If we have a forced sync peer configured, only use that peer
	if sm.forceSyncPeer != emptyPeerID {
		if _, exists := sm.peerStates.GetPeerState(sm.forceSyncPeer); exists {
			sm.logger.Infof("[SyncManager] Using forced sync peer %s", sm.forceSyncPeer)
			return sm.forceSyncPeer
		}
		// Forced peer not connected - wait for it rather than using another peer
		sm.logger.Infof("[SyncManager] Waiting for forced sync peer %s to connect (ignoring %d available peers)",
			sm.forceSyncPeer, len(sm.peerStates.GetSyncCandidates()))
		return emptyPeerID // Return empty to indicate no sync peer should be used
	}

	if sm.getLocalHeight == nil || sm.getPeerHeight == nil {
		sm.logger.Warnf("[SyncManager] Height callbacks not set, cannot select sync peer")
		return emptyPeerID
	}

	localHeight := int32(sm.getLocalHeight())
	candidates := sm.peerStates.GetSyncCandidates()

	if len(candidates) == 0 {
		sm.logger.Debugf("[SyncManager] No sync candidates available")
		return emptyPeerID
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
			sm.logger.Debugf("[SyncManager] Peer %s at height %d > local %d (bestPeer)", peerID, peerHeight, localHeight)
		} else if peerHeight == localHeight {
			// Peer at same height - acceptable candidate
			okPeers = append(okPeers, peerID)
			sm.logger.Debugf("[SyncManager] Peer %s at height %d == local %d (okPeer)", peerID, peerHeight, localHeight)
		} else {
			// Peer behind us - skip
			sm.logger.Debugf("[SyncManager] Peer %s at height %d < local %d (skipping)", peerID, peerHeight, localHeight)
		}
	}

	// Select from the best available group
	var selectedPeer peer.ID

	if len(bestPeers) > 0 {
		// Select the peer with the highest block height
		var highestPeer peer.ID
		highestHeight := int32(-1)

		for _, peerID := range bestPeers {
			peerHeight := sm.getPeerHeight(peerID)
			if peerHeight > highestHeight {
				highestHeight = peerHeight
				highestPeer = peerID
			} else if peerHeight == highestHeight && peerID.String() > highestPeer.String() {
				// Use peer ID as tiebreaker for deterministic selection
				highestPeer = peerID
			}
		}

		selectedPeer = highestPeer
		sm.logger.Infof("[SyncManager] Selected highest peer %s at height %d from %d peers ahead of us", selectedPeer, highestHeight, len(bestPeers))
	} else if len(okPeers) > 0 {
		// No peers ahead, select peer with highest ID for deterministic behavior
		var highestPeer peer.ID
		for _, peerID := range okPeers {
			if highestPeer == "" || peerID.String() > highestPeer.String() {
				highestPeer = peerID
			}
		}
		selectedPeer = highestPeer
		sm.logger.Infof("[SyncManager] No peers ahead, selected peer %s from %d peers at same height", selectedPeer, len(okPeers))
	} else {
		sm.logger.Warnf("[SyncManager] No suitable sync peers found")
	}

	return selectedPeer
}

// selectSyncPeerLocked selects a sync peer but does not trigger blockchain sync
// It assumes the lock is already held. Returns true if a sync peer was selected.
func (sm *SyncManager) selectSyncPeerLocked() bool {
	// Already syncing
	if sm.syncPeer != emptyPeerID {
		sm.logger.Debugf("[SyncManager] Already have a sync peer, skipping selection")
		return false
	}

	// If forced peer is configured, don't select any other peer
	if sm.forceSyncPeer != emptyPeerID {
		// Check if forced peer is now available
		if _, exists := sm.peerStates.GetPeerState(sm.forceSyncPeer); exists {
			sm.syncPeer = sm.forceSyncPeer
			sm.syncPeerState = &syncPeerState{
				lastBlockTime: time.Now(),
				recvBytes:     0,
				violations:    0,
				ticks:         0,
			}
			sm.logger.Infof("[SyncManager] Forced sync peer %s is now available, selecting it", sm.forceSyncPeer)
			return true
		}
		sm.logger.Debugf("[SyncManager] Waiting for forced sync peer %s to connect", sm.forceSyncPeer)
		return false
	}

	// Select the best peer
	bestPeer := sm.selectSyncPeer()
	if bestPeer == emptyPeerID {
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

	if sm.syncPeer == emptyPeerID {
		// If we have a forced peer configured, keep waiting for it
		if sm.forceSyncPeer != emptyPeerID {
			sm.logger.Debugf("[SyncManager] Still waiting for forced sync peer %s to connect", sm.forceSyncPeer)
			return
		}
		// No sync peer and no forced peer, next peer that's ahead will be selected automatically
		return
	}

	if sm.syncPeerState == nil {
		sm.logger.Errorf("[SyncManager] Sync peer set but no state, resetting")
		sm.syncPeer = emptyPeerID
		return
	}

	// Check if we've caught up to our sync peer
	if sm.getLocalHeight != nil && sm.getPeerHeight != nil {
		localHeight := int32(sm.getLocalHeight())
		syncPeerHeight := sm.getPeerHeight(sm.syncPeer)

		if localHeight >= syncPeerHeight {
			sm.logger.Infof("[SyncManager] Caught up to sync peer %s (local height %d >= peer height %d), clearing sync peer", sm.syncPeer, localHeight, syncPeerHeight)
			sm.syncPeer = ""
			sm.syncPeerState = nil
			return
		}
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

	// If using a forced sync peer, be more lenient
	if sm.forceSyncPeer != emptyPeerID && sm.syncPeer == sm.forceSyncPeer {
		// For forced peers, only switch if disconnected (handled in RemovePeer)
		// Log warnings but don't auto-switch
		if violations >= maxNetworkViolations {
			sm.logger.Warnf("[SyncManager] Forced sync peer %s has %d network violations but keeping as forced", sm.syncPeer, violations)
		}
		if timeSinceLastBlock > maxLastBlockTime {
			sm.logger.Warnf("[SyncManager] Forced sync peer %s hasn't sent blocks for %v but keeping as forced", sm.syncPeer, timeSinceLastBlock)
		}
		return
	}

	if violations >= maxNetworkViolations {
		needSwitch = true
		switchReason = "too many network speed violations"
	} else if timeSinceLastBlock > maxLastBlockTime {
		needSwitch = true
		switchReason = "no block received in time limit"
	}

	if needSwitch {
		sm.logger.Infof("[SyncManager] Switching sync peer %s due to: %s", sm.syncPeer, switchReason)
		sm.syncPeer = emptyPeerID
		sm.syncPeerState = nil
		// Try to find a new sync peer using bitcoin-sv selection logic
		sm.selectSyncPeerLocked()
	}
}

// Start starts the sync manager's periodic evaluation
func (sm *SyncManager) Start(ctx context.Context) {
	sm.mu.Lock()
	sm.syncPeerTicker = time.NewTicker(syncPeerTickerInterval)
	sm.startTime = time.Now()
	sm.mu.Unlock()

	sm.logger.Infof("[SyncManager] Started with %v initial delay, %d minimum peers, %v max wait", sm.settings.P2P.InitialSyncDelay, sm.settings.P2P.MinPeersForSync, sm.settings.P2P.MaxWaitForMinPeers)

	go func() {
		// Add initial delay for first sync peer selection check
		initialTimer := time.NewTimer(sm.settings.P2P.InitialSyncDelay)
		defer initialTimer.Stop()

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
			case <-initialTimer.C:
				// After initial delay, try to select sync peer if we haven't already
				sm.mu.Lock()
				if sm.syncPeer == "" && !sm.initialSelectionDone {
					sm.initialSelectionDone = true
					sm.logger.Infof("[SyncManager] Initial delay complete, attempting first sync peer selection")
					sm.selectSyncPeerLocked()
					// Buffered announcements will be processed by the Server
				}
				sm.mu.Unlock()
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

// GetPeerHeight returns the height of a given peer
func (sm *SyncManager) GetPeerHeight(peerID peer.ID) int32 {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	if sm.getPeerHeight == nil {
		return 0
	}
	return sm.getPeerHeight(peerID)
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

	// Add peer if it doesn't exist yet
	if _, exists := sm.peerStates.GetPeerState(peerID); !exists {
		// Add the peer and evaluate sync candidacy
		isSyncCandidate := sm.isSyncCandidate(peerID)
		sm.peerStates.AddPeer(peerID, isSyncCandidate)
		sm.logger.Infof("[SyncManager] Added peer %s with height %d (sync candidate: %v)", peerID, height, isSyncCandidate)
	} else {
		// Update sync candidacy now that we have height
		if state, exists := sm.peerStates.GetPeerState(peerID); exists {
			// Re-evaluate sync candidacy with height information
			isSyncCandidate := sm.isSyncCandidate(peerID)
			if isSyncCandidate != state.syncCandidate {
				sm.peerStates.SetSyncCandidate(peerID, isSyncCandidate)
				sm.logger.Debugf("[SyncManager] Peer %s sync candidate status updated to %v (height: %d)", peerID, isSyncCandidate, height)
			}
		}
	}

	// Check if we should select a sync peer (only if we don't have one)
	if sm.syncPeer == "" && sm.getLocalHeight != nil {
		// Check if we're still in the initial grace period
		if !sm.initialSelectionDone {
			timeSinceStart := time.Since(sm.startTime)
			if timeSinceStart < sm.settings.P2P.InitialSyncDelay {
				sm.logger.Debugf("[SyncManager] Delaying sync peer selection (%.1fs of %v grace period)", timeSinceStart.Seconds(), sm.settings.P2P.InitialSyncDelay)
				return false
			}

			// Check if we have enough peers or max wait time has passed
			candidates := sm.peerStates.GetSyncCandidates()
			if len(candidates) < sm.settings.P2P.MinPeersForSync && timeSinceStart < sm.settings.P2P.MaxWaitForMinPeers {
				sm.logger.Infof("[SyncManager] Waiting for more peers before initial selection (%d/%d, waited %.1fs of max %v)", len(candidates), sm.settings.P2P.MinPeersForSync, timeSinceStart.Seconds(), sm.settings.P2P.MaxWaitForMinPeers)
				return false
			}

			sm.initialSelectionDone = true
			if len(candidates) < sm.settings.P2P.MinPeersForSync {
				sm.logger.Warnf("[SyncManager] Maximum wait time reached (%v), proceeding with %d peers (wanted %d)", sm.settings.P2P.MaxWaitForMinPeers, len(candidates), sm.settings.P2P.MinPeersForSync)
			} else {
				sm.logger.Infof("[SyncManager] Initial grace period complete with %d peers, proceeding with sync peer selection", len(candidates))
			}

			// Initial selection is done, process buffered announcements if appropriate
			// This will be handled by the Server when it checks IsInitialSyncComplete()
		}

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

// ClearSyncPeer clears the current sync peer when we've caught up to the network
func (sm *SyncManager) ClearSyncPeer() {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.syncPeer != "" {
		sm.logger.Infof("[SyncManager] Clearing sync peer %s as we've caught up to the network", sm.syncPeer)
		sm.syncPeer = ""
		sm.syncPeerState = nil
	}
}

// BufferBlockAnnouncement buffers a block announcement during the initial sync period
// Returns true if the announcement was buffered, false if it should be processed immediately
func (sm *SyncManager) BufferBlockAnnouncement(announcement *BlockAnnouncement) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	// Only buffer during initial sync period
	if !sm.initialSelectionDone {
		sm.bufferMu.Lock()
		defer sm.bufferMu.Unlock()

		sm.announcementBuffer = append(sm.announcementBuffer, announcement)
		sm.logger.Debugf("[SyncManager] Buffered block announcement for %s (total buffered: %d)", announcement.Hash, len(sm.announcementBuffer))
		return true
	}

	return false
}

// GetBufferedAnnouncements returns and clears the buffered announcements
// This should be called after determining whether we need a sync peer
func (sm *SyncManager) GetBufferedAnnouncements() []*BlockAnnouncement {
	sm.bufferMu.Lock()
	defer sm.bufferMu.Unlock()

	announcements := sm.announcementBuffer
	sm.announcementBuffer = nil

	if len(announcements) > 0 {
		sm.logger.Infof("[SyncManager] Returning %d buffered announcements", len(announcements))
	}

	return announcements
}

// IsInitialSyncComplete returns true if the initial sync period is complete
func (sm *SyncManager) IsInitialSyncComplete() bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	return sm.initialSelectionDone
}

// NeedsSyncPeer returns true if we have a sync peer (meaning we're behind)
func (sm *SyncManager) NeedsSyncPeer() bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	return sm.syncPeer != ""
}
