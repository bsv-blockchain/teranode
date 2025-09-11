package p2p

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/services/blockchain/blockchain_api"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/kafka"
	kafkamessage "github.com/bitcoin-sv/teranode/util/kafka/kafka_message"
	"github.com/libp2p/go-libp2p/core/peer"
	"google.golang.org/protobuf/proto"
)

// SyncCoordinator orchestrates sync operations
// This is the single point of control for sync decisions
type SyncCoordinator struct {
	logger           ulogger.Logger
	settings         *settings.Settings
	registry         *PeerRegistry
	selector         *PeerSelector
	healthChecker    *PeerHealthChecker
	banManager       PeerBanManagerI
	blockchainClient blockchain.ClientI

	// Current sync state
	mu              sync.RWMutex
	currentSyncPeer peer.ID
	syncStartTime   time.Time
	lastFSMState    blockchain_api.FSMStateType
	lastSyncTrigger time.Time // Track when we last triggered sync
	lastLocalHeight uint32    // Track last known local height
	lastBlockHash   string    // Track last known block hash

	// Dependencies for sync operations
	blocksKafkaProducerClient kafka.KafkaAsyncProducerI // Kafka producer for blocks
	getLocalHeight            func() uint32

	// Control
	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewSyncCoordinator creates a new sync coordinator
func NewSyncCoordinator(
	logger ulogger.Logger,
	settings *settings.Settings,
	registry *PeerRegistry,
	selector *PeerSelector,
	healthChecker *PeerHealthChecker,
	banManager PeerBanManagerI,
	blockchainClient blockchain.ClientI,
	blocksKafkaProducerClient kafka.KafkaAsyncProducerI,
) *SyncCoordinator {
	return &SyncCoordinator{
		logger:                    logger,
		settings:                  settings,
		registry:                  registry,
		selector:                  selector,
		healthChecker:             healthChecker,
		banManager:                banManager,
		blockchainClient:          blockchainClient,
		blocksKafkaProducerClient: blocksKafkaProducerClient,
		stopCh:                    make(chan struct{}),
	}
}

// SetGetLocalHeightCallback sets the local height callback
func (sc *SyncCoordinator) SetGetLocalHeightCallback(getLocalHeight func() uint32) {
	sc.getLocalHeight = getLocalHeight
}

// Start begins the coordinator
func (sc *SyncCoordinator) Start(ctx context.Context) {
	sc.logger.Infof("[SyncCoordinator] Starting sync coordinator")

	// Start health checker
	sc.healthChecker.Start(ctx)

	// Start FSM monitoring
	sc.wg.Add(1)
	go sc.monitorFSM(ctx)

	// Start periodic sync evaluation
	sc.wg.Add(1)
	go sc.periodicEvaluation(ctx)

	sc.logger.Infof("[SyncCoordinator] Sync coordinator started")
}

// Stop stops the coordinator
func (sc *SyncCoordinator) Stop() {
	close(sc.stopCh)
	sc.healthChecker.Stop()
	sc.wg.Wait()
}

// GetCurrentSyncPeer returns the current sync peer
func (sc *SyncCoordinator) GetCurrentSyncPeer() peer.ID {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return sc.currentSyncPeer
}

// ClearSyncPeer clears the current sync peer
func (sc *SyncCoordinator) ClearSyncPeer() {
	sc.mu.Lock()
	oldPeer := sc.currentSyncPeer
	sc.currentSyncPeer = ""
	sc.mu.Unlock()

	if oldPeer != "" {
		sc.logger.Infof("[SyncCoordinator] Cleared sync peer %s", oldPeer)
	}
}

// TriggerSync triggers a new sync operation
func (sc *SyncCoordinator) TriggerSync() error {
	sc.logger.Debugf("[SyncCoordinator] Sync triggered")

	// Select new sync peer
	newPeer := sc.selectNewSyncPeer()
	if newPeer == "" {
		sc.logger.Warnf("[SyncCoordinator] No suitable sync peer found")
		return nil
	}

	// Update current sync peer
	sc.mu.Lock()
	oldPeer := sc.currentSyncPeer
	sc.currentSyncPeer = newPeer
	sc.syncStartTime = time.Now()
	sc.lastSyncTrigger = time.Now() // Track when we trigger sync
	sc.mu.Unlock()

	// Notify if peer changed
	if newPeer != oldPeer {
		sc.logger.Infof("[SyncCoordinator] Sync peer changed from %s to %s", oldPeer, newPeer)

		if err := sc.sendSyncMessage(newPeer); err != nil {
			sc.logger.Errorf("[SyncCoordinator] Failed to send sync message: %v", err)
			return err
		}
	}

	return nil
}

// HandlePeerDisconnected handles peer disconnection
func (sc *SyncCoordinator) HandlePeerDisconnected(peerID peer.ID) {
	sc.registry.RemovePeer(peerID)

	sc.mu.RLock()
	isSyncPeer := sc.currentSyncPeer == peerID
	sc.mu.RUnlock()

	if isSyncPeer {
		sc.logger.Infof("[SyncCoordinator] Sync peer %s disconnected", peerID)
		sc.ClearSyncPeer()

		// Trigger selection of new sync peer
		go func() {
			time.Sleep(1 * time.Second) // Brief delay to allow other peers to update
			_ = sc.TriggerSync()
		}()
	}
}

// HandleCatchupFailure handles catchup failures
func (sc *SyncCoordinator) HandleCatchupFailure(reason string) {
	sc.logger.Infof("[SyncCoordinator] Handling catchup failure: %s", reason)

	// Clear current sync peer
	sc.ClearSyncPeer()

	// Trigger new sync
	if err := sc.TriggerSync(); err != nil {
		sc.logger.Errorf("[SyncCoordinator] Failed to trigger sync after failure: %v", err)
	}
}

// selectNewSyncPeer selects a new sync peer based on current criteria
func (sc *SyncCoordinator) selectNewSyncPeer() peer.ID {
	// Get local height
	localHeight := int32(0)
	if sc.getLocalHeight != nil {
		localHeight = int32(sc.getLocalHeight())
	}

	// Get current sync peer to pass as previous peer
	sc.mu.RLock()
	previousPeer := sc.currentSyncPeer
	sc.mu.RUnlock()

	// Build selection criteria
	criteria := SelectionCriteria{
		LocalHeight:  localHeight,
		PreviousPeer: previousPeer,
	}

	// Check for forced peer
	if sc.settings.P2P.ForceSyncPeer != "" {
		// Try to decode as a proper peer ID first
		if forcedPeer, err := peer.Decode(sc.settings.P2P.ForceSyncPeer); err == nil {
			criteria.ForcedPeerID = forcedPeer
			sc.logger.Debugf("[SyncCoordinator] Using forced sync peer %s", forcedPeer)
		} else {
			// If decode fails, use it as a raw peer ID string
			criteria.ForcedPeerID = peer.ID(sc.settings.P2P.ForceSyncPeer)
			sc.logger.Debugf("[SyncCoordinator] Using forced sync peer %s", sc.settings.P2P.ForceSyncPeer)
		}
	}

	// Get all peers and select
	peers := sc.registry.GetAllPeers()

	// Check URL responsiveness before selecting
	sc.checkAndUpdateURLResponsiveness(peers)

	return sc.selector.SelectSyncPeer(peers, criteria)
}

// monitorFSM monitors FSM state changes
func (sc *SyncCoordinator) monitorFSM(ctx context.Context) {
	defer sc.wg.Done()

	sc.logger.Infof("[SyncCoordinator] Starting FSM monitor")
	ticker := time.NewTicker(2 * time.Second) // Check more frequently for state changes
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			sc.logger.Infof("[SyncCoordinator] FSM monitor stopping (context done)")
			return
		case <-sc.stopCh:
			sc.logger.Infof("[SyncCoordinator] FSM monitor stopping (stop requested)")
			return
		case <-ticker.C:
			sc.checkFSMState(ctx)
		}
	}
}

// checkFSMState checks FSM state and triggers sync if needed
func (sc *SyncCoordinator) checkFSMState(ctx context.Context) {
	if sc.blockchainClient == nil {
		sc.logger.Warnf("[SyncCoordinator] No blockchain client available for FSM monitoring")
		return
	}

	currentState, err := sc.blockchainClient.GetFSMCurrentState(ctx)
	if err != nil {
		sc.logger.Errorf("[SyncCoordinator] Failed to get FSM state: %v", err)
		return
	}

	// Log current FSM state for debugging
	sc.logger.Debugf("[SyncCoordinator] Current FSM state: %v", currentState.String())

	// Handle FSM state transitions
	if sc.handleFSMTransition(currentState) {
		return // Transition handled, no further action needed
	}

	// When FSM is RUNNING, we need to find a new sync peer and trigger catchup
	if *currentState == blockchain_api.FSMStateType_RUNNING {
		sc.handleRunningState(ctx)
	}
}

// handleFSMTransition checks for FSM state transitions and handles them
func (sc *SyncCoordinator) handleFSMTransition(currentState *blockchain_api.FSMStateType) bool {
	// Get previous state for transition detection
	sc.mu.RLock()
	previousState := sc.lastFSMState
	sc.mu.RUnlock()

	// Update last FSM state
	sc.mu.Lock()
	sc.lastFSMState = *currentState
	sc.mu.Unlock()

	// Detect transition from CATCHINGBLOCKS to RUNNING
	if previousState == blockchain_api.FSMStateType_CATCHINGBLOCKS && *currentState == blockchain_api.FSMStateType_RUNNING {
		sc.logger.Infof("[SyncCoordinator] FSM transitioned from CATCHINGBLOCKS to RUNNING")

		// Get current sync peer and check if we should consider this a failure
		sc.mu.Lock()
		currentPeer := sc.currentSyncPeer
		sc.mu.Unlock()

		if currentPeer != "" {
			// Get local height and peer height to determine if this is a failure
			localHeight := sc.getLocalHeightSafe()
			peerInfo, exists := sc.registry.GetPeer(currentPeer)

			if exists && peerInfo.Height > localHeight {
				// Only consider it a failure if we're still behind the sync peer
				sc.logger.Infof("[SyncCoordinator] Sync with peer %s considered failed (local height: %d < peer height: %d)",
					currentPeer, localHeight, peerInfo.Height)

				// Add ban score for catchup failure
				if sc.banManager != nil {
					score, banned := sc.banManager.AddScore(string(currentPeer), ReasonCatchupFailure)
					if banned {
						sc.logger.Warnf("[SyncCoordinator] Peer %s banned after catchup failure (score: %d)", currentPeer, score)
					} else {
						sc.logger.Infof("[SyncCoordinator] Added ban score to peer %s for catchup failure (score: %d)", currentPeer, score)
					}
					// Update the ban status in the registry so the peer selector knows about it
					sc.registry.UpdateBanStatus(currentPeer, score, banned)
				}

				sc.ClearSyncPeer()
				_ = sc.TriggerSync()
				return true // Transition handled
			} else {
				// We've caught up or surpassed the peer, this is success not failure
				sc.logger.Infof("[SyncCoordinator] Sync completed successfully with peer %s (local height: %d, peer height: %d)",
					currentPeer, localHeight, peerInfo.Height)
				// Don't add ban score, just look for a better peer if needed
				_ = sc.TriggerSync()
				return true // Transition handled
			}
		}
	}
	return false // No transition to handle
}

// handleRunningState handles the FSM RUNNING state logic
func (sc *SyncCoordinator) handleRunningState(ctx context.Context) {
	localHeight := sc.getLocalHeightSafe()
	localBlockHash := sc.getCurrentBlockHash(ctx)

	sc.mu.RLock()
	currentSyncPeer := sc.currentSyncPeer
	sc.mu.RUnlock()

	// Evaluate current sync peer
	needNewPeer := sc.evaluateCurrentSyncPeer(ctx, currentSyncPeer, localHeight, localBlockHash)

	if needNewPeer {
		sc.selectAndActivateNewPeer(localHeight, currentSyncPeer)
	}
}

// getLocalHeightSafe safely gets the local blockchain height
func (sc *SyncCoordinator) getLocalHeightSafe() int32 {
	if sc.getLocalHeight != nil {
		return int32(sc.getLocalHeight())
	}
	return 0
}

// getCurrentBlockHash gets the hash of the current chain tip
func (sc *SyncCoordinator) getCurrentBlockHash(ctx context.Context) string {
	if sc.blockchainClient == nil {
		return ""
	}

	tips, err := sc.blockchainClient.GetChainTips(ctx)
	if err != nil {
		sc.logger.Debugf("[SyncCoordinator] Failed to get chain tips: %v", err)
		return ""
	}

	// Find the active chain tip (main chain has branchlen=0 and status="active")
	for _, tip := range tips {
		if tip.Branchlen == 0 && tip.Status == "active" {
			return tip.Hash
		}
	}

	// Fallback to first tip if no active found
	if len(tips) > 0 {
		return tips[0].Hash
	}

	return ""
}

// evaluateCurrentSyncPeer evaluates if the current sync peer is still suitable
func (sc *SyncCoordinator) evaluateCurrentSyncPeer(_ context.Context, currentSyncPeer peer.ID, localHeight int32, localBlockHash string) bool {
	if currentSyncPeer == "" {
		return true // Need new peer
	}

	peerInfo, exists := sc.registry.GetPeer(currentSyncPeer)
	if !exists {
		sc.logger.Infof("[SyncCoordinator] Sync peer %s no longer exists", currentSyncPeer)
		return true // Need new peer
	}

	// Check if we've caught up to or passed this peer
	if localHeight >= peerInfo.Height && peerInfo.Height > 0 {
		sc.logger.Infof("[SyncCoordinator] Caught up to sync peer %s (local: %d, peer: %d)",
			currentSyncPeer, localHeight, peerInfo.Height)
		return true // Need new peer
	}

	return false
}

// selectAndActivateNewPeer selects a new sync peer and activates it
func (sc *SyncCoordinator) selectAndActivateNewPeer(localHeight int32, oldPeer peer.ID) {
	// Clear current sync peer
	sc.ClearSyncPeer()

	// Get all peers
	peers := sc.registry.GetAllPeers()

	// Check URL responsiveness for all peers first
	sc.checkAndUpdateURLResponsiveness(peers)

	// Filter eligible peers
	eligiblePeers := sc.filterEligiblePeers(peers, oldPeer, localHeight)

	if len(eligiblePeers) == 0 {
		sc.logger.Warnf("[SyncCoordinator] No peers ahead of local height %d", localHeight)
		return
	}

	// Select from eligible peers
	criteria := SelectionCriteria{
		LocalHeight: localHeight,
	}

	newSyncPeer := sc.selector.SelectSyncPeer(eligiblePeers, criteria)
	if newSyncPeer == "" || newSyncPeer == oldPeer {
		sc.logger.Warnf("[SyncCoordinator] No suitable new sync peer found (different from %s)", oldPeer)
		sc.logCandidateList(eligiblePeers)
		return
	}

	// Activate the new sync peer
	sc.activateSyncPeer(newSyncPeer, oldPeer)
}

// filterEligiblePeers filters peers that are eligible for syncing
func (sc *SyncCoordinator) filterEligiblePeers(peers []*PeerInfo, oldPeer peer.ID, localHeight int32) []*PeerInfo {
	eligiblePeers := make([]*PeerInfo, 0, len(peers))
	for _, p := range peers {
		// Skip the old peer and peers not ahead of us
		if p.ID == oldPeer || p.Height <= localHeight {
			sc.logger.Debugf("[SyncCoordinator] Skipping peer %s (oldPeer=%v, url=%s, height=%d, local=%d)",
				p.ID, p.ID == oldPeer, p.DataHubURL, p.Height, localHeight)
			continue
		}

		eligiblePeers = append(eligiblePeers, p)
	}
	return eligiblePeers
}

// activateSyncPeer sets and activates a new sync peer
func (sc *SyncCoordinator) activateSyncPeer(newSyncPeer peer.ID, oldPeer peer.ID) {
	// Set the new sync peer
	sc.mu.Lock()
	sc.currentSyncPeer = newSyncPeer
	sc.syncStartTime = time.Now()
	sc.lastSyncTrigger = time.Now()
	sc.mu.Unlock()

	sc.logger.Infof("[SyncCoordinator] Selected new sync peer %s (different from old %s)",
		newSyncPeer, oldPeer)

	// Trigger sync directly (sends to Kafka)
	if err := sc.sendSyncMessage(newSyncPeer); err != nil {
		sc.logger.Errorf("[SyncCoordinator] Failed to trigger sync: %v", err)
	} else {
		sc.logger.Infof("[SyncCoordinator] Triggered sync with peer %s via Kafka", newSyncPeer)
	}
}

// logPeerList logs the list of peers for debugging
func (sc *SyncCoordinator) logPeerList(peers []*PeerInfo) {
	for _, p := range peers {
		sc.logger.Infof("[SyncCoordinator] Peer: %s (url=%s, height=%d, banScore=%d)",
			p.ID, p.DataHubURL, p.Height, p.BanScore)
	}
}

// logCandidateList logs the list of candidate peers that were skipped
func (sc *SyncCoordinator) logCandidateList(candidates []*PeerInfo) {
	for _, p := range candidates {
		sc.logger.Infof("[SyncCoordinator] Candidate skipped: %s (url=%s, height=%d, banScore=%d)",
			p.ID, p.DataHubURL, p.Height, p.BanScore)
	}
}

// periodicEvaluation periodically evaluates sync performance
func (sc *SyncCoordinator) periodicEvaluation(ctx context.Context) {
	defer sc.wg.Done()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-sc.stopCh:
			return
		case <-ticker.C:
			sc.evaluateSyncPeer()
		}
	}
}

// evaluateSyncPeer evaluates current sync peer performance
func (sc *SyncCoordinator) evaluateSyncPeer() {
	sc.mu.RLock()
	currentPeer := sc.currentSyncPeer
	syncDuration := time.Since(sc.syncStartTime)
	sc.mu.RUnlock()

	if currentPeer == "" {
		return
	}

	// Get peer info
	peerInfo, exists := sc.registry.GetPeer(currentPeer)
	if !exists {
		sc.logger.Warnf("[SyncCoordinator] Sync peer %s no longer exists", currentPeer)
		sc.ClearSyncPeer()
		_ = sc.TriggerSync()
		return
	}

	// Check if peer is still healthy
	if !peerInfo.IsHealthy {
		sc.logger.Warnf("[SyncCoordinator] Sync peer %s is unhealthy", currentPeer)
		sc.ClearSyncPeer()
		_ = sc.TriggerSync()
		return
	}

	// Check if we've been syncing too long without progress
	if syncDuration > 5*time.Minute {
		timeSinceLastBlock := time.Since(peerInfo.LastBlockTime)
		if timeSinceLastBlock > 2*time.Minute {
			sc.logger.Warnf("[SyncCoordinator] Sync peer %s inactive for %v", currentPeer, timeSinceLastBlock)
			// Mark peer as unhealthy due to inactivity
			sc.registry.UpdateHealth(currentPeer, false)
			sc.ClearSyncPeer()
			_ = sc.TriggerSync()
			return
		}
	}

	// Check if we've caught up
	if sc.getLocalHeight != nil {
		localHeight := int32(sc.getLocalHeight())
		if localHeight >= peerInfo.Height && peerInfo.Height > 0 {
			sc.logger.Infof("[SyncCoordinator] Caught up to sync peer %s (height %d)",
				currentPeer, localHeight)
			// Don't clear peer yet, but look for better peer
			if betterPeer := sc.selectNewSyncPeer(); betterPeer != currentPeer && betterPeer != "" {
				sc.logger.Infof("[SyncCoordinator] Found better sync peer %s", betterPeer)
				_ = sc.TriggerSync()
			}
		}
	}
}

// UpdatePeerInfo updates peer information
func (sc *SyncCoordinator) UpdatePeerInfo(peerID peer.ID, height int32, blockHash string, dataHubURL string) {
	sc.registry.UpdateHeight(peerID, height, blockHash)
	if dataHubURL != "" {
		sc.registry.UpdateDataHubURL(peerID, dataHubURL)
		// Trigger immediate health check for new DataHub URL
		sc.healthChecker.CheckPeerNow(peerID)
	}
}

// UpdateBanStatus updates ban status from ban manager
func (sc *SyncCoordinator) UpdateBanStatus(peerID peer.ID) {
	if sc.banManager != nil {
		// Use raw string conversion instead of String() method
		peerIDStr := string(peerID)
		score, banned, _ := sc.banManager.GetBanScore(peerIDStr)
		sc.registry.UpdateBanStatus(peerID, score, banned)

		// If sync peer got banned, find new one
		sc.mu.RLock()
		isSyncPeer := sc.currentSyncPeer == peerID
		sc.mu.RUnlock()

		if isSyncPeer && banned {
			sc.logger.Warnf("[SyncCoordinator] Sync peer %s got banned", peerID)
			sc.ClearSyncPeer()
			_ = sc.TriggerSync()
		}
	}
}

// checkURLResponsiveness checks if a peer's DataHub URL is responsive with a short timeout
func (sc *SyncCoordinator) checkURLResponsiveness(url string) bool {
	if url == "" {
		return false
	}

	// Create a client with a very short timeout (2 seconds)
	client := &http.Client{
		Timeout: 2 * time.Second,
	}

	// Try to make a HEAD request to check if the server is responsive
	testURL := fmt.Sprintf("%s/health", url) // Try health endpoint first
	resp, err := client.Head(testURL)
	if err == nil {
		resp.Body.Close()
		return resp.StatusCode < 500 // Consider it responsive if not a server error
	}

	// If health endpoint fails, try the base URL
	resp, err = client.Head(url)
	if err == nil {
		resp.Body.Close()
		return resp.StatusCode < 500
	}

	return false
}

// checkAndUpdateURLResponsiveness checks URL responsiveness for all peers and updates registry
func (sc *SyncCoordinator) checkAndUpdateURLResponsiveness(peers []*PeerInfo) {
	for _, p := range peers {
		// Skip if URL was checked recently (within 30 seconds)
		if time.Since(p.LastURLCheck) < 30*time.Second {
			continue
		}

		if p.DataHubURL != "" {
			responsive := sc.checkURLResponsiveness(p.DataHubURL)
			sc.registry.UpdateURLResponsiveness(p.ID, responsive)

			if !responsive {
				sc.logger.Debugf("[SyncCoordinator] Peer %s URL %s is not responsive", p.ID, p.DataHubURL)
			}
		}
	}
}

// sendSyncTriggerToKafka sends a sync trigger message to Kafka
func (sc *SyncCoordinator) sendSyncTriggerToKafka(syncPeer peer.ID, bestHash string) {
	if sc.blocksKafkaProducerClient == nil || bestHash == "" {
		return
	}

	// Get the peer's DataHub URL if available
	dataHubURL := ""
	if peerInfo, exists := sc.registry.GetPeer(syncPeer); exists {
		dataHubURL = peerInfo.DataHubURL
	}

	// No longer collecting fallback URLs - relying on ban scoring and FSM monitoring instead
	sc.logger.Infof("[sendSyncTriggerToKafka] Sending sync trigger with primary URL %s from peer %s", dataHubURL, syncPeer)

	msg := &kafkamessage.KafkaBlockTopicMessage{
		Hash:   bestHash,
		URL:    dataHubURL,
		PeerId: syncPeer.String(),
	}

	value, err := proto.Marshal(msg)
	if err != nil {
		sc.logger.Errorf("[sendSyncTriggerToKafka] error marshaling sync peer's best block: %v", err)
		return
	}

	sc.blocksKafkaProducerClient.Publish(&kafka.Message{
		Value: value,
	})
	sc.logger.Infof("[sendSyncTriggerToKafka] Sent sync trigger to Kafka for block %s from peer %s", bestHash, syncPeer)
}

// sendSyncMessage sends a sync message to a specific peer
func (sc *SyncCoordinator) sendSyncMessage(peerID peer.ID) error {
	sc.logger.Infof("[sendSyncMessage] Preparing to send sync message to peer %s", peerID)
	// Get peer's best known block hash from registry
	var bestHash string
	if sc.registry != nil {
		if peerInfo, exists := sc.registry.GetPeer(peerID); exists {
			bestHash = peerInfo.BlockHash
			if bestHash != "" {
				sc.logger.Infof("[sendSyncMessage] Found block hash %s for peer %s", bestHash, peerID)
			} else {
				sc.logger.Warnf("[sendSyncMessage] No block hash found in registry for peer %s", peerID)
			}
		} else {
			sc.logger.Errorf("[sendSyncMessage] Peer %s not found in registry", peerID)
			return errors.NewServiceError(fmt.Sprintf("peer %s not found in registry", peerID))
		}
	}

	if bestHash != "" {
		sc.logger.Infof("[sendSyncMessage] Sending sync trigger to Kafka for peer %s with hash %s", peerID, bestHash)
		sc.sendSyncTriggerToKafka(peerID, bestHash)
		return nil
	} else {
		sc.logger.Errorf("[sendSyncMessage] Cannot send sync - no best block hash available for peer %s", peerID)
		return errors.NewServiceError(fmt.Sprintf("no block hash available for peer %s", peerID))
	}
}
