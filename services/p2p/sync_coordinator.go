package p2p

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	kafkamessage "github.com/bsv-blockchain/teranode/util/kafka/kafka_message"
	"github.com/libp2p/go-libp2p/core/peer"
	"google.golang.org/protobuf/proto"
)

// SyncCoordinator orchestrates sync operations
// This is the single point of control for sync decisions
type SyncCoordinator struct {
	logger           ulogger.Logger
	settings         *settings.Settings
	registry         blockchain.PeerRegistryClientI
	selector         *PeerSelector
	blockchainClient blockchain.ClientI

	// Coordinator-scoped context used for the gRPC calls into the registry.
	// Per-RPC contexts are derived from this when needed.
	ctx context.Context

	// Current sync state. currentSyncPeer holds the canonical libp2p ID string.
	mu              sync.RWMutex
	currentSyncPeer string
	syncStartTime   time.Time
	lastSyncTrigger time.Time // Track when we last triggered sync
	lastLocalHeight uint32    // Track last known local height
	lastBlockHash   string    // Track last known block hash

	// Backoff management
	allPeersAttempted       bool      // Flag when all eligible peers have been tried
	lastAllPeersAttemptTime time.Time // When we last exhausted all peers
	backoffMultiplier       int       // Current backoff multiplier (1, 2, 4, 8...)
	maxBackoffMultiplier    int       // Maximum backoff multiplier (e.g., 32)

	// Dependencies for sync operations
	blocksKafkaProducerClient kafka.KafkaAsyncProducerI // Kafka producer for blocks
	getLocalHeight            func() uint32

	// Control
	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewSyncCoordinator creates a new sync coordinator
func NewSyncCoordinator(
	ctx context.Context,
	logger ulogger.Logger,
	settings *settings.Settings,
	registry blockchain.PeerRegistryClientI,
	selector *PeerSelector,
	blockchainClient blockchain.ClientI,
	blocksKafkaProducerClient kafka.KafkaAsyncProducerI,
) *SyncCoordinator {
	return &SyncCoordinator{
		logger:                    logger,
		settings:                  settings,
		registry:                  registry,
		selector:                  selector,
		blockchainClient:          blockchainClient,
		blocksKafkaProducerClient: blocksKafkaProducerClient,
		ctx:                       ctx,
		stopCh:                    make(chan struct{}),
		backoffMultiplier:         1,
		maxBackoffMultiplier:      32, // Max backoff of 64 seconds (32 * 2s)
	}
}

// SetGetLocalHeightCallback sets the local height callback
func (sc *SyncCoordinator) SetGetLocalHeightCallback(getLocalHeight func() uint32) {
	sc.getLocalHeight = getLocalHeight
}

// Constants for monitoring intervals
const (
	fastMonitorInterval = 2 * time.Second  // When actively syncing
	slowMonitorInterval = 15 * time.Second // When caught up
)

// isViableSyncCandidate returns true if a peer passes the unconditional
// viability filters used by the coordinator when deciding whether we're
// caught up and when determining whether all eligible peers have been
// attempted. Keeping this in one place ensures both call sites stay in sync.
//
// These filters — not banned, has a DataHub URL, non-zero advertised height,
// and sufficient reputation — exclude obviously unsuitable peers. They do
// not validate whether a peer's advertised height is truthful: a peer can
// still claim an inflated height while passing them. The HTTP health check
// applied during peer selection (when `settings.P2P.HealthCheckEnabled` is
// true) only confirms that the peer's DataHub endpoint is reachable; it
// does not check the advertised height either. Validation of advertised
// height is handled elsewhere, via catchup validation, reputation
// downgrades after failed catchup, and banning — not by a height-delta
// tolerance here.
func isViableSyncCandidate(p *blockchain.PeerInfo) bool {
	return !p.IsBanned && p.DataHubURL != "" && p.Height != 0 && p.ReputationScore >= 20
}

// listAllPeers returns every peer known to the centralized registry. Errors
// are logged and treated as "no peers" so callers can keep their structure.
func (sc *SyncCoordinator) listAllPeers() []*blockchain.PeerInfo {
	peers, err := sc.registry.ListPeers(sc.ctx, nil, 0, 0, false, false)
	if err != nil {
		sc.logger.Warnf("[SyncCoordinator] ListPeers failed: %v", err)
		return nil
	}
	return peers
}

// getPeer fetches a single peer by libp2p ID from the centralized registry.
func (sc *SyncCoordinator) getPeer(id peer.ID) (*blockchain.PeerInfo, bool) {
	info, found, err := sc.registry.GetPeer(sc.ctx, id.String())
	if err != nil {
		sc.logger.Warnf("[SyncCoordinator] GetPeer failed for %s: %v", id, err)
		return nil, false
	}
	return info, found
}

// isCaughtUp determines if we're caught up with the network
func (sc *SyncCoordinator) isCaughtUp() bool {
	localHeight := sc.getLocalHeightSafe()

	// Get all peers
	peers := sc.listAllPeers()

	// Check if any eligible peer is ahead of us.
	// This must align with sync peer selection criteria; otherwise, a low-quality
	// peer we would never select could cause us to think we're perpetually behind.
	for _, p := range peers {
		// Only consider peers that pass the unconditional viability filters
		// (see isViableSyncCandidate). The HTTP health check, when enabled,
		// only confirms that the peer's DataHub endpoint is reachable.
		// Validation of whether an advertised height is truthful is handled
		// elsewhere via catchup validation, reputation downgrades after
		// failed catchup, and banning, so no extra height-delta tolerance is
		// needed here.
		if !isViableSyncCandidate(p) {
			continue
		}
		if p.Height > localHeight {
			return false // At least one viable peer is ahead
		}
	}

	return true // We're at the same height or ahead of every eligible peer
}

// Start begins the coordinator
func (sc *SyncCoordinator) Start(ctx context.Context) {
	sc.logger.Infof("[SyncCoordinator] Starting sync coordinator")

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
	sc.wg.Wait()
}

// GetCurrentSyncPeer returns the current sync peer (canonical libp2p ID string).
func (sc *SyncCoordinator) GetCurrentSyncPeer() string {
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
		// Check if we've tried all available peers
		sc.checkAllPeersAttempted()
		return nil
	}

	// Record the sync attempt for this peer
	if err := sc.registry.RecordSyncAttempt(sc.ctx, newPeer); err != nil {
		sc.logger.Warnf("[SyncCoordinator] RecordSyncAttempt failed for %s: %v", newPeer, err)
	}

	// Update current sync peer
	sc.mu.Lock()
	oldPeer := sc.currentSyncPeer
	sc.currentSyncPeer = newPeer
	sc.syncStartTime = time.Now()
	sc.lastSyncTrigger = time.Now() // Track when we trigger sync
	sc.mu.Unlock()

	// Reset backoff if we found a peer to sync with
	sc.resetBackoff()

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

// HandlePeerDisconnected handles peer disconnection. peerID is the libp2p peer.ID.
func (sc *SyncCoordinator) HandlePeerDisconnected(peerID peer.ID) {
	idStr := peerID.String()
	if err := sc.registry.RemovePeer(sc.ctx, idStr); err != nil {
		sc.logger.Warnf("[SyncCoordinator] RemovePeer %s failed: %v", idStr, err)
	}

	sc.mu.RLock()
	isSyncPeer := sc.currentSyncPeer == idStr
	sc.mu.RUnlock()

	if isSyncPeer {
		sc.logger.Infof("[SyncCoordinator] Sync peer %s disconnected", idStr)
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

	// Get the failed peer before clearing
	sc.mu.RLock()
	failedPeer := sc.currentSyncPeer
	sc.mu.RUnlock()

	// Record failure for the failed peer BEFORE clearing and triggering sync
	// This ensures reputation is updated so the peer selector won't re-select the same peer
	if failedPeer != "" {
		sc.logger.Infof("[SyncCoordinator] Recording failure for failed peer %s", failedPeer)
		if err := sc.registry.UpdatePeerMetrics(sc.ctx, failedPeer, 0, 0, 0, false, true, false, 0); err != nil {
			sc.logger.Warnf("[SyncCoordinator] UpdatePeerMetrics(failure) for %s: %v", failedPeer, err)
		}
	}

	// Clear current sync peer
	sc.ClearSyncPeer()

	// Trigger new sync
	if err := sc.TriggerSync(); err != nil {
		sc.logger.Errorf("[SyncCoordinator] Failed to trigger sync after failure: %v", err)
	}
}

// selectNewSyncPeer selects a new sync peer based on current criteria.
// The returned ID is a canonical libp2p ID string.
func (sc *SyncCoordinator) selectNewSyncPeer() string {
	// Get local height
	localHeight := uint32(0)
	if sc.getLocalHeight != nil {
		localHeight = sc.getLocalHeight()
	}

	// Get current sync peer to pass as previous peer
	sc.mu.RLock()
	previousPeer := sc.currentSyncPeer
	sc.mu.RUnlock()

	// Build selection criteria
	criteria := SelectionCriteria{
		LocalHeight:         int32(localHeight),
		PreviousPeer:        previousPeer,
		SyncAttemptCooldown: 1 * time.Minute, // Don't retry peers for at least 1 minute
	}

	// Check for forced peer
	if sc.settings.P2P.ForceSyncPeer != "" {
		// Try to decode as a proper peer ID first; on success store its canonical
		// string form, on failure store the raw configured value.
		if forcedPeer, err := peer.Decode(sc.settings.P2P.ForceSyncPeer); err == nil {
			criteria.ForcedPeerID = forcedPeer.String()
			sc.logger.Debugf("[SyncCoordinator] Using forced sync peer %s", criteria.ForcedPeerID)
		} else {
			criteria.ForcedPeerID = sc.settings.P2P.ForceSyncPeer
			sc.logger.Debugf("[SyncCoordinator] Using forced sync peer %s", sc.settings.P2P.ForceSyncPeer)
		}
	}

	// Get all peers and select
	peers := sc.listAllPeers()

	return sc.selector.SelectSyncPeer(peers, criteria)
}

// monitorFSM monitors FSM state changes
func (sc *SyncCoordinator) monitorFSM(ctx context.Context) {
	defer sc.wg.Done()

	sc.logger.Infof("[SyncCoordinator] Starting FSM monitor")
	timer := time.NewTimer(fastMonitorInterval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			sc.logger.Infof("[SyncCoordinator] FSM monitor stopping (context done)")
			return
		case <-sc.stopCh:
			sc.logger.Infof("[SyncCoordinator] FSM monitor stopping (stop requested)")
			return
		case <-timer.C:
			if sc.isCaughtUp() {
				timer.Reset(slowMonitorInterval)
			} else {
				timer.Reset(fastMonitorInterval)
				sc.checkFSMState(ctx)
			}
		}
	}
}

// checkFSMState checks FSM state and triggers sync if needed
func (sc *SyncCoordinator) checkFSMState(ctx context.Context) {
	if sc.blockchainClient == nil {
		sc.logger.Warnf("[SyncCoordinator] No blockchain client available for FSM monitoring")
		return
	}

	// Check if we're in backoff mode
	if sc.checkAndClearExpiredBackoff() {
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
		// Check if we should attempt reputation recovery
		sc.considerReputationRecovery()

		sc.handleRunningState(ctx)
	}
}

// handleFSMTransition checks for FSM state transitions and handles them
func (sc *SyncCoordinator) handleFSMTransition(currentState *blockchain_api.FSMStateType) bool {
	if *currentState == blockchain_api.FSMStateType_RUNNING {
		// Get current sync peer and check if we should consider this a failure
		sc.mu.Lock()
		currentPeer := sc.currentSyncPeer
		sc.mu.Unlock()

		if currentPeer != "" {
			// Get local height and peer height to determine if this is a failure
			localHeight := sc.getLocalHeightSafe()
			peerInfo, exists, err := sc.registry.GetPeer(sc.ctx, currentPeer)
			if err != nil {
				sc.logger.Warnf("[SyncCoordinator] GetPeer %s failed: %v", currentPeer, err)
				return false
			}

			if !exists {
				// Peer no longer exists in registry (likely disconnected)
				sc.logger.Infof("[SyncCoordinator] Sync peer %s no longer in registry, clearing", currentPeer)
				sc.ClearSyncPeer()
				_ = sc.TriggerSync()
				return true // Transition handled
			}

			if peerInfo.Height > localHeight {
				// Only consider it a failure if we're still behind the sync peer
				sc.logger.Infof("[SyncCoordinator] Sync with peer %s considered failed (local height: %d < peer height: %d)",
					currentPeer, localHeight, peerInfo.Height)
				sc.ClearSyncPeer()
				_ = sc.TriggerSync()
				return true // Transition handled
			}
			// We've caught up or surpassed the peer, this is success not failure
			sc.logger.Infof("[SyncCoordinator] Sync completed successfully with peer %s (local height: %d, peer height: %d)",
				currentPeer, localHeight, peerInfo.Height)
			sc.resetBackoff()
			_ = sc.TriggerSync()
			return true // Transition handled
		}
	}
	return false // No transition to handle
}

// handleRunningState handles the FSM RUNNING state logic
func (sc *SyncCoordinator) handleRunningState(_ context.Context) {
	localHeight := sc.getLocalHeightSafe()

	sc.mu.RLock()
	currentSyncPeer := sc.currentSyncPeer
	sc.mu.RUnlock()

	sc.selectAndActivateNewPeer(localHeight, currentSyncPeer)
}

// getLocalHeightSafe safely gets the local blockchain height
func (sc *SyncCoordinator) getLocalHeightSafe() uint32 {
	if sc.getLocalHeight != nil {
		return sc.getLocalHeight()
	}
	return 0
}

// selectAndActivateNewPeer selects a new sync peer and activates it.
// oldPeer is the previously selected peer's canonical libp2p ID string (or empty).
func (sc *SyncCoordinator) selectAndActivateNewPeer(localHeight uint32, oldPeer string) {
	// Clear current sync peer
	sc.ClearSyncPeer()

	// Get all peers
	peers := sc.listAllPeers()

	// Filter eligible peers
	eligiblePeers := sc.filterEligiblePeers(peers, oldPeer, localHeight)

	if len(eligiblePeers) == 0 {
		// No eligible peers - either we're caught up or all peers are filtered
		// (banned, malicious, low reputation, etc.)
		sc.logger.Infof("[SyncCoordinator] No eligible peers found at height %d", localHeight)
		// Enter backoff to prevent busy loop
		sc.enterBackoffMode()
		return
	}

	// Select from eligible peers
	criteria := SelectionCriteria{
		LocalHeight:         int32(localHeight),
		SyncAttemptCooldown: 1 * time.Minute, // Don't retry peers for at least 1 minute
	}

	newSyncPeer := sc.selector.SelectSyncPeer(eligiblePeers, criteria)
	if newSyncPeer == "" || newSyncPeer == oldPeer {
		sc.logger.Warnf("[SyncCoordinator] No suitable new sync peer found (different from %s)", oldPeer)
		sc.logCandidateList(eligiblePeers)
		// Enter backoff mode to prevent busy loop when all peers fail selection
		// (e.g., all health checks fail or peers are on cooldown)
		sc.enterBackoffMode()
		return
	}

	// Activate the new sync peer
	sc.activateSyncPeer(newSyncPeer)
}

// filterEligiblePeers filters peers that are eligible for syncing
func (sc *SyncCoordinator) filterEligiblePeers(peers []*blockchain.PeerInfo, oldPeer string, localHeight uint32) []*blockchain.PeerInfo {
	eligiblePeers := make([]*blockchain.PeerInfo, 0, len(peers))
	for _, p := range peers {
		// Skip the old peer and peers not ahead of us
		if p.ID == oldPeer || p.Height <= localHeight {
			// Only log if this is the old peer (which is more important to know)
			if p.ID == oldPeer {
				sc.logger.Debugf("[SyncCoordinator] Skipping old peer %s (height=%d, local=%d)", p.ID, p.Height, localHeight)
			}
			continue
		}

		eligiblePeers = append(eligiblePeers, p)
	}
	return eligiblePeers
}

// activateSyncPeer sets and activates a new sync peer (canonical libp2p ID string).
func (sc *SyncCoordinator) activateSyncPeer(newSyncPeer string) {
	// Set the new sync peer
	sc.mu.Lock()
	sc.currentSyncPeer = newSyncPeer
	sc.syncStartTime = time.Now()
	sc.lastSyncTrigger = time.Now()
	sc.mu.Unlock()

	// Trigger sync directly (sends to Kafka)
	if err := sc.sendSyncMessage(newSyncPeer); err != nil {
		sc.logger.Errorf("[SyncCoordinator] Failed to trigger sync: %v", err)
	} else {
		sc.logger.Infof("[SyncCoordinator] Triggered sync with peer %s via Kafka", newSyncPeer)
	}
}

// logPeerList logs the list of peers for debugging
func (sc *SyncCoordinator) logPeerList(peers []*blockchain.PeerInfo) {
	for _, p := range peers {
		sc.logger.Infof("[SyncCoordinator] Peer: %s (url=%s, height=%d, banScore=%d)",
			p.ID, p.DataHubURL, p.Height, p.BanScore)
	}
}

// logCandidateList logs the list of candidate peers that were skipped
func (sc *SyncCoordinator) logCandidateList(candidates []*blockchain.PeerInfo) {
	for _, p := range candidates {
		// Include more details about why peer might be skipped
		lastAttemptStr := "never"
		if !p.LastSyncAttempt.IsZero() {
			lastAttemptStr = fmt.Sprintf("%v ago", time.Since(p.LastSyncAttempt).Round(time.Second))
		}
		sc.logger.Infof("[SyncCoordinator] Candidate skipped: %s (height=%d, reputation=%.1f, lastAttempt=%s, url=%s)",
			p.ID, p.Height, p.ReputationScore, lastAttemptStr, p.DataHubURL)
	}
}

// periodicEvaluation periodically evaluates sync performance
func (sc *SyncCoordinator) periodicEvaluation(ctx context.Context) {
	defer sc.wg.Done()

	interval := sc.settings.P2P.SyncCoordinatorPeriodicEvaluationInterval
	if interval <= 0 {
		sc.logger.Warnf("[SyncCoordinator] Invalid periodic evaluation interval %v, using default 30s", interval)
		interval = 30 * time.Second
	}

	ticker := time.NewTicker(interval)
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
	peerInfo, exists, err := sc.registry.GetPeer(sc.ctx, currentPeer)
	if err != nil {
		sc.logger.Warnf("[SyncCoordinator] GetPeer %s failed: %v", currentPeer, err)
		return
	}
	if !exists {
		sc.logger.Warnf("[SyncCoordinator] Sync peer %s no longer exists", currentPeer)
		sc.ClearSyncPeer()
		_ = sc.TriggerSync()
		return
	}

	// Check if peer has low reputation
	if peerInfo.ReputationScore < 20.0 {
		sc.logger.Warnf("[SyncCoordinator] Sync peer %s has low reputation (%.2f)", currentPeer, peerInfo.ReputationScore)
		sc.ClearSyncPeer()
		_ = sc.TriggerSync()
		return
	}

	// Check if we've been syncing too long without progress
	if syncDuration > 5*time.Minute {
		timeSinceLastMessage := time.Since(peerInfo.LastMessageTime)
		if timeSinceLastMessage > 1*time.Minute {
			sc.logger.Warnf("[SyncCoordinator] Sync peer %s inactive for %v", currentPeer, timeSinceLastMessage)
			if err := sc.registry.UpdatePeerMetrics(sc.ctx, currentPeer, 0, 0, 0, false, true, false, 0); err != nil {
				sc.logger.Warnf("[SyncCoordinator] UpdatePeerMetrics(failure) for %s: %v", currentPeer, err)
			}
			sc.ClearSyncPeer()
			_ = sc.TriggerSync()
			return
		}
	}

	// Check if we've caught up
	if sc.getLocalHeight != nil {
		localHeight := int32(sc.getLocalHeight())
		if uint32(localHeight) >= peerInfo.Height && peerInfo.Height > 0 {
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

// UpdatePeerInfo updates peer information in the centralized registry.
func (sc *SyncCoordinator) UpdatePeerInfo(peerID peer.ID, height uint32, blockHash *chainhash.Hash, dataHubURL string) {
	info := &blockchain.PeerInfo{
		ID:               peerID.String(),
		TransportType:    blockchain_api.TransportType_TRANSPORT_HTTP,
		TransportTypeSet: true,
		Height:           height,
		BlockHash:        blockHash,
		DataHubURL:       dataHubURL,
	}
	if err := sc.registry.RegisterPeer(sc.ctx, info); err != nil {
		sc.logger.Warnf("[SyncCoordinator] RegisterPeer %s failed: %v", info.ID, err)
	}
}

// UpdateBanStatus is a legacy entrypoint preserved for callers that previously
// re-synced ban state from the local BanManager into the registry. The
// blockchain-side AddBanScore now writes BanScore/IsBanned atomically, so this
// only needs to react to a peer becoming the sync target.
func (sc *SyncCoordinator) UpdateBanStatus(peerID peer.ID) {
	idStr := peerID.String()

	banned, err := sc.registry.IsPeerBanned(sc.ctx, idStr)
	if err != nil {
		sc.logger.Warnf("[SyncCoordinator] IsPeerBanned %s failed: %v", idStr, err)
		return
	}

	sc.mu.RLock()
	isSyncPeer := sc.currentSyncPeer == idStr
	sc.mu.RUnlock()

	if isSyncPeer && banned {
		sc.logger.Warnf("[SyncCoordinator] Sync peer %s got banned", idStr)
		sc.ClearSyncPeer()
		_ = sc.TriggerSync()
	}
}

// checkAndClearExpiredBackoff checks if we're currently in a backoff period.
// If the backoff has expired, it clears the backoff state and increases the multiplier
// for the next time we exhaust all peers. Returns true if still in backoff.
func (sc *SyncCoordinator) checkAndClearExpiredBackoff() bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if !sc.allPeersAttempted {
		return false // Not in backoff if we haven't tried all peers
	}

	// Calculate backoff duration based on current multiplier
	backoffDuration := time.Duration(sc.backoffMultiplier) * fastMonitorInterval
	timeSinceLastAttempt := time.Since(sc.lastAllPeersAttemptTime)

	if timeSinceLastAttempt < backoffDuration {
		remainingTime := backoffDuration - timeSinceLastAttempt
		sc.logger.Infof("[SyncCoordinator] In backoff period, %v remaining (multiplier: %dx)",
			remainingTime.Round(time.Second), sc.backoffMultiplier)
		return true
	}

	// Backoff period expired; clear backoff state and increase multiplier for next time.
	sc.allPeersAttempted = false
	if sc.backoffMultiplier < sc.maxBackoffMultiplier {
		sc.backoffMultiplier *= 2
	}

	return false
}

// resetBackoff resets the backoff state when sync succeeds
func (sc *SyncCoordinator) resetBackoff() {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if sc.allPeersAttempted {
		sc.logger.Infof("[SyncCoordinator] Resetting backoff state after successful sync")
		sc.allPeersAttempted = false
		sc.backoffMultiplier = 1
		sc.lastAllPeersAttemptTime = time.Time{}
	}
}

// enterBackoffMode marks that all peers have been attempted.
// We enter a backoff period to avoid hammering peers when no eligible peer can be selected.
// We also clear sync attempts so that once backoff expires, peers can be retried immediately.
func (sc *SyncCoordinator) enterBackoffMode() {
	sc.mu.Lock()
	if sc.allPeersAttempted {
		sc.mu.Unlock()
		return
	}

	sc.allPeersAttempted = true
	sc.lastAllPeersAttemptTime = time.Now()

	// Capture for logging while holding the lock
	backoffDuration := time.Duration(sc.backoffMultiplier) * fastMonitorInterval
	currentMultiplier := sc.backoffMultiplier

	sc.mu.Unlock()

	peersCleared, err := sc.registry.ClearAllSyncAttempts(sc.ctx)
	if err != nil {
		sc.logger.Warnf("[SyncCoordinator] ClearAllSyncAttempts failed: %v", err)
	}
	sc.logger.Warnf("[SyncCoordinator] All eligible peers attempted, entering backoff for %v (multiplier: %dx). Cleared sync attempts for %d peers.",
		backoffDuration, currentMultiplier, peersCleared)
}

// checkAllPeersAttempted checks if all eligible peers have been attempted recently
func (sc *SyncCoordinator) checkAllPeersAttempted() {
	// Get all peers and check how many were attempted recently
	peers := sc.listAllPeers()
	localHeight := sc.getLocalHeightSafe()

	eligibleCount := 0
	recentlyAttemptedCount := 0
	syncAttemptCooldown := 1 * time.Minute // Don't retry a peer for at least 1 minute

	for _, p := range peers {
		// Count peers that are viable sync candidates (must match isCaughtUp criteria)
		if !isViableSyncCandidate(p) {
			continue
		}
		if p.Height > localHeight { // Same comparison as isCaughtUp
			eligibleCount++

			// Check if attempted recently
			if !p.LastSyncAttempt.IsZero() &&
				time.Since(p.LastSyncAttempt) < syncAttemptCooldown {
				recentlyAttemptedCount++
			}
		}
	}

	// If all eligible peers were attempted recently, enter backoff
	if eligibleCount > 0 && eligibleCount == recentlyAttemptedCount {
		sc.logger.Warnf("[SyncCoordinator] All %d eligible peers have been attempted recently",
			eligibleCount)
		sc.enterBackoffMode()
	}
}

// considerReputationRecovery checks if any bad peers should have their reputation reset
func (sc *SyncCoordinator) considerReputationRecovery() {
	// Calculate cooldown based on how many times we've been in backoff
	baseCooldown := 5 * time.Minute
	if sc.backoffMultiplier > 1 {
		// Exponentially increase cooldown if we've been in backoff multiple times
		cooldownMultiplier := sc.backoffMultiplier / 2
		if cooldownMultiplier < 1 {
			cooldownMultiplier = 1
		}
		baseCooldown *= time.Duration(cooldownMultiplier)
	}

	peersRecovered, err := sc.registry.ReconsiderBadPeers(sc.ctx, baseCooldown)
	if err != nil {
		sc.logger.Warnf("[SyncCoordinator] ReconsiderBadPeers failed: %v", err)
		return
	}
	if peersRecovered > 0 {
		sc.logger.Infof("[SyncCoordinator] Recovered reputation for %d peers after %v cooldown",
			peersRecovered, baseCooldown)
		// Reset backoff since we have new peers to try
		sc.resetBackoff()
	}
}

// sendSyncTriggerToKafka sends a sync trigger message to Kafka.
// syncPeer is the canonical libp2p ID string.
func (sc *SyncCoordinator) sendSyncTriggerToKafka(syncPeer string, bestHash string) {
	if sc.blocksKafkaProducerClient == nil || bestHash == "" {
		return
	}

	// Get the peer's DataHub URL if available
	dataHubURL := ""
	if peerInfo, exists, err := sc.registry.GetPeer(sc.ctx, syncPeer); err == nil && exists {
		dataHubURL = peerInfo.DataHubURL
	}

	sc.logger.Infof("[sendSyncTriggerToKafka] Sending sync trigger with primary URL %s from peer %s", dataHubURL, syncPeer)

	msg := &kafkamessage.KafkaBlockTopicMessage{
		Hash:   bestHash,
		URL:    dataHubURL,
		PeerId: syncPeer,
	}

	value, err := proto.Marshal(msg)
	if err != nil {
		sc.logger.Errorf("[sendSyncTriggerToKafka] error marshaling sync peer's best block: %v", err)
		return
	}

	sc.blocksKafkaProducerClient.Publish(&kafka.Message{
		Key:   []byte(bestHash),
		Value: value,
	})
	sc.logger.Infof("[sendSyncTriggerToKafka] Sent sync trigger to Kafka for block %s from peer %s", bestHash, syncPeer)
}

// sendSyncMessage sends a sync message to a specific peer (canonical libp2p ID string).
func (sc *SyncCoordinator) sendSyncMessage(peerID string) error {
	sc.logger.Infof("[sendSyncMessage] Preparing to send sync message to peer %s", peerID)

	peerInfo, exists, err := sc.registry.GetPeer(sc.ctx, peerID)
	if err != nil {
		sc.logger.Errorf("[sendSyncMessage] GetPeer %s failed: %v", peerID, err)
		return errors.NewServiceError(fmt.Sprintf("get peer %s: %v", peerID, err))
	}
	if !exists {
		sc.logger.Errorf("[sendSyncMessage] Peer %s not found in registry", peerID)
		return errors.NewServiceError(fmt.Sprintf("peer %s not found in registry", peerID))
	}

	bestHash := ""
	if peerInfo.BlockHash != nil {
		bestHash = peerInfo.BlockHash.String()
		sc.logger.Infof("[sendSyncMessage] Found block hash %s for peer %s", bestHash, peerID)
	} else {
		sc.logger.Warnf("[sendSyncMessage] No block hash found in registry for peer %s", peerID)
	}

	if bestHash != "" {
		sc.logger.Infof("[sendSyncMessage] Sending sync trigger to Kafka for peer %s with hash %s", peerID, bestHash)
		sc.sendSyncTriggerToKafka(peerID, bestHash)
		return nil
	}
	sc.logger.Errorf("[sendSyncMessage] Cannot send sync - no best block hash available for peer %s", peerID)
	return errors.NewServiceError(fmt.Sprintf("no block hash available for peer %s", peerID))
}
