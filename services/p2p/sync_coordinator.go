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
	"github.com/bsv-blockchain/teranode/services/blockchain/work"
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
	mu                         sync.RWMutex
	currentSyncPeer            string
	syncStartTime              time.Time
	lastSyncProgressTime       time.Time
	lastSyncPeerBlocksReceived int64
	lastSyncTrigger            time.Time // Track when we last triggered sync
	lastLocalHeight            uint32    // Track last known local height
	lastBlockHash              string    // Track last known block hash

	// Backoff management
	allPeersAttempted            bool      // Flag when all eligible peers have been tried
	lastAllPeersAttemptTime      time.Time // When we last exhausted all peers
	backoffMultiplier            int       // Current backoff multiplier (1, 2, 4, 8...)
	maxBackoffMultiplier         int       // Maximum backoff multiplier (e.g., 32)
	unprovenProbeBudgetRemaining int       // Remaining advertised/header-only probes in this backoff window
	lastLocalChainWork           []byte    // Last local validated chainwork observed by the coordinator

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
		logger:                       logger,
		settings:                     settings,
		registry:                     registry,
		selector:                     selector,
		blockchainClient:             blockchainClient,
		blocksKafkaProducerClient:    blocksKafkaProducerClient,
		ctx:                          ctx,
		stopCh:                       make(chan struct{}),
		backoffMultiplier:            1,
		maxBackoffMultiplier:         32, // Max backoff of 64 seconds (32 * 2s)
		unprovenProbeBudgetRemaining: maxUnprovenProbeBudget(settings),
	}
}

// SetGetLocalHeightCallback sets the local height callback
func (sc *SyncCoordinator) SetGetLocalHeightCallback(getLocalHeight func() uint32) {
	sc.getLocalHeight = getLocalHeight
}

// Constants for monitoring intervals
const (
	fastMonitorInterval            = 2 * time.Second  // When actively syncing
	slowMonitorInterval            = 15 * time.Second // When caught up
	defaultSyncPeerNoProgressLimit = 5 * time.Minute  // Fallback when p2p_sync_peer_no_progress_timeout is unset

	// syncPeerPreemptionGuardDivisor sets the opportunistic-preemption anti-flap
	// guard to half the no-progress timeout: the incumbent must go this long
	// without delivering a validated block before a higher-work peer may preempt
	// it. Keyed off the configurable timeout (not the periodic-evaluation
	// interval) so an honest peer streaming a large block — which records no
	// validated progress until the body arrives — is not evicted before a
	// realistic delivery window.
	syncPeerPreemptionGuardDivisor = 2
)

// isViableSyncCandidate returns true if a peer passes the unconditional
// viability filters used by the coordinator when deciding whether we're
// caught up and when determining whether all eligible peers have been
// attempted. Keeping this in one place ensures both call sites stay in sync.
//
// These filters — not banned, has a DataHub URL, and sufficient reputation —
// exclude obviously unsuitable peers. They do
// not validate whether a peer's advertised height is truthful: a peer can
// still claim an inflated height while passing them. The HTTP health check
// applied during peer selection (when `settings.P2P.HealthCheckEnabled` is
// true) only confirms that the peer's DataHub endpoint is reachable; it
// does not check the advertised height either. Validation of advertised
// height is handled elsewhere, via catchup validation, reputation
// downgrades after failed catchup, and banning — not by a height-delta
// tolerance here.
func isViableSyncCandidate(p *blockchain.PeerInfo) bool {
	return !p.IsBanned && p.DataHubURL != "" && p.ReputationScore >= 20
}

func maxUnprovenProbeBudget(settings *settings.Settings) int {
	if settings == nil || settings.P2P.MaxUnprovenSyncProbesPerBackoffWindow < 0 {
		return 0
	}
	return settings.P2P.MaxUnprovenSyncProbesPerBackoffWindow
}

func (sc *SyncCoordinator) fullDeliveryFreshnessWindow() time.Duration {
	if sc.settings == nil {
		return 0
	}
	return sc.settings.P2P.FullDeliveryFreshnessWindow
}

// syncPeerNoProgressLimit is the configurable no-progress stall deadline. It
// falls back to defaultSyncPeerNoProgressLimit when unset (nil settings or a
// non-positive value) so existing deployments keep the historical 5-minute
// behaviour exactly.
func (sc *SyncCoordinator) syncPeerNoProgressLimit() time.Duration {
	if sc.settings == nil || sc.settings.P2P.SyncPeerNoProgressTimeout <= 0 {
		return defaultSyncPeerNoProgressLimit
	}
	return sc.settings.P2P.SyncPeerNoProgressTimeout
}

// preemptionProgressGuard is the minimum time the current sync peer must go
// without delivering a validated block before opportunistic preemption toward a
// higher-work peer is allowed. It is a fraction of the configurable no-progress
// timeout so it always fires before the hard no-progress eviction, and scales
// with the operator's timeout rather than the periodic-evaluation interval.
func (sc *SyncCoordinator) preemptionProgressGuard() time.Duration {
	return sc.syncPeerNoProgressLimit() / syncPeerPreemptionGuardDivisor
}

func peerHasValidatedWork(p *blockchain.PeerInfo) bool {
	return p != nil && p.ValidatedBlockHash != nil && len(p.ValidatedChainWork) > 0
}

func peerAheadByValidatedWork(p *blockchain.PeerInfo, localChainWork []byte) bool {
	return peerHasValidatedWork(p) && len(localChainWork) > 0 &&
		!peerInFullStoragePenalty(p, time.Now()) &&
		chainWorkGreater(p.ValidatedChainWork, localChainWork)
}

// peerInFullStoragePenalty reports whether the peer is inside an active
// block-incomplete / full-storage penalty window. During the window the peer's
// validated chainwork does not confer top-tier "ahead by validated work"
// eligibility: a peer whose header work was credited before delivery but that
// then failed to serve a full block body is demoted to the advertised-probe tier
// (which is budget-gated) rather than pinning the sync slot.
func peerInFullStoragePenalty(p *blockchain.PeerInfo, now time.Time) bool {
	return p != nil && !p.FullStoragePenaltyUntil.IsZero() && now.Before(p.FullStoragePenaltyUntil)
}

func chainWorkGreater(a, b []byte) bool {
	// Delegates to the shared big-endian comparison in services/blockchain/work,
	// which compares the byte slices as unsigned big integers, treating empty/nil as
	// zero work. Most callers pass real, positive chainwork, but some legitimately pass
	// an empty operand (e.g. resetProbeBudgetIfLocalChainWorkAdvanced on its first call,
	// where lastLocalChainWork is still empty); the empty==zero handling covers those.
	return work.CompareChainWork(a, b) > 0
}

// BlocksReceived and LastBlockTime are written by ReportValidBlock through
// RecordBlockReceived after block validation, not by block announcements.
func peerHasRecentFullBlockDelivery(p *blockchain.PeerInfo, now time.Time, window time.Duration) bool {
	if p == nil || p.BlocksReceived <= 0 || p.LastBlockTime.IsZero() {
		return false
	}
	if window <= 0 {
		return true
	}
	if p.LastBlockTime.After(now) {
		return true
	}
	return now.Sub(p.LastBlockTime) <= window
}

func (sc *SyncCoordinator) peerHasRecentFullBlockDelivery(p *blockchain.PeerInfo, now time.Time) bool {
	return peerHasRecentFullBlockDelivery(p, now, sc.fullDeliveryFreshnessWindow())
}

func peerEligibleForAdvertisedProbe(p *blockchain.PeerInfo, localHeight uint32) bool {
	return isViableSyncCandidate(p) && p.Height > localHeight && p.BlockHash != nil
}

func (sc *SyncCoordinator) peerEligibleForAdvertisedProbe(p *blockchain.PeerInfo, localHeight uint32) bool {
	return peerEligibleForAdvertisedProbe(p, localHeight)
}

func (sc *SyncCoordinator) isUnprovenProbeCandidate(p *blockchain.PeerInfo, now time.Time) bool {
	return !sc.peerHasRecentFullBlockDelivery(p, now)
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

func (sc *SyncCoordinator) getLocalTipWorkSafe(ctx context.Context) (uint32, []byte, bool) {
	if sc.blockchainClient == nil {
		return sc.getLocalHeightSafe(), nil, false
	}
	_, meta, err := sc.blockchainClient.GetBestBlockHeader(ctx)
	if err != nil {
		sc.logger.Warnf("[SyncCoordinator] GetBestBlockHeader failed: %v", err)
		return sc.getLocalHeightSafe(), nil, false
	}
	if meta == nil || len(meta.ChainWork) == 0 {
		return sc.getLocalHeightSafe(), nil, false
	}
	chainWork := append([]byte(nil), meta.ChainWork...)
	return meta.Height, chainWork, true
}

func (sc *SyncCoordinator) refreshProbeBudgetFromLocalTip(ctx context.Context) {
	_, chainWork, ok := sc.getLocalTipWorkSafe(ctx)
	if ok {
		sc.resetProbeBudgetIfLocalChainWorkAdvanced(chainWork)
	}
}

func (sc *SyncCoordinator) resetProbeBudgetIfLocalChainWorkAdvanced(chainWork []byte) {
	if len(chainWork) == 0 {
		return
	}

	sc.mu.Lock()
	defer sc.mu.Unlock()
	if chainWorkGreater(chainWork, sc.lastLocalChainWork) {
		sc.lastLocalChainWork = append([]byte(nil), chainWork...)
		sc.unprovenProbeBudgetRemaining = maxUnprovenProbeBudget(sc.settings)
	}
	// The no-progress stall deadline (lastSyncProgressTime) is intentionally NOT
	// refreshed here. Local best-tip chainwork advances from ordinary block gossip
	// delivered by any peer, so refreshing the deadline on a local-tip advance would
	// let a stalled sync peer keep its slot for as long as the network produces
	// blocks. The deadline is refreshed solely from peer-attributable delivery
	// progress in recordSyncPeerBlockProgress.
}

func (sc *SyncCoordinator) hasUnprovenProbeBudget() bool {
	return sc.unprovenProbeBudgetRemainingValue() > 0
}

func (sc *SyncCoordinator) unprovenProbeBudgetRemainingValue() int {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return sc.unprovenProbeBudgetRemaining
}

func (sc *SyncCoordinator) currentSyncPeerLocked() string {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return sc.currentSyncPeer
}

// isCaughtUp determines if we're caught up with the network
func (sc *SyncCoordinator) isCaughtUp() bool {
	localHeight := sc.getLocalHeightSafe()
	if tipHeight, chainWork, ok := sc.getLocalTipWorkSafe(sc.ctx); ok {
		localHeight = tipHeight
		peers := sc.listAllPeers()
		for _, p := range peers {
			if isViableSyncCandidate(p) && peerAheadByValidatedWork(p, chainWork) {
				return false
			}
		}
		if sc.currentSyncPeerLocked() != "" {
			return false
		}
		// A viable advertised-ahead peer means we are NOT caught up, regardless of
		// the unproven-probe budget. Probe *activation* stays budget-gated (see
		// filterEligiblePeersWithTip and claimSelectedSyncPeer), but the caught-up
		// determination must not hide an ahead peer: reporting caught-up on an
		// exhausted budget lets monitorFSM back off to slowMonitorInterval, which
		// stops calling checkFSMState and makes checkAndClearExpiredBackoff — the
		// only budget/backoff refill — unreachable, wedging the node as caught-up.
		for _, p := range peers {
			if sc.peerEligibleForAdvertisedProbe(p, localHeight) {
				return false
			}
		}
		return true
	}

	// Fallback path taken when local tip work is unavailable (transient
	// GetBestBlockHeader error / empty ChainWork). As above, a viable
	// advertised-ahead peer keeps us not-caught-up regardless of the probe budget;
	// probe activation remains budget-gated at the point of selection.
	peers := sc.listAllPeers()
	if sc.currentSyncPeerLocked() != "" {
		return false
	}

	for _, p := range peers {
		if sc.peerEligibleForAdvertisedProbe(p, localHeight) {
			return false
		}
	}

	return true
}

// Start begins the coordinator
func (sc *SyncCoordinator) Start(ctx context.Context) {
	sc.logger.Infof("[SyncCoordinator] Starting sync coordinator")

	// A zero unproven-probe budget disables cold-start probing entirely. That is a
	// legitimate "validated-work peers only" choice, but with no forced peer it can
	// silently wedge bootstrap until a validated-ahead peer appears, so warn rather
	// than let it fail quietly.
	if maxUnprovenProbeBudget(sc.settings) == 0 &&
		(sc.settings == nil || sc.settings.P2P.ForceSyncPeer == "") {
		sc.logger.Warnf("[SyncCoordinator] p2p_max_unproven_sync_probes_per_backoff_window is 0: unproven peers are never probed; a cold-start node with no validated-work peer and no forced sync peer cannot bootstrap until a validated-ahead peer appears")
	}

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
	sc.clearSyncPeerIfCurrent("")
}

func (sc *SyncCoordinator) clearSyncPeerIfCurrent(peerID string) bool {
	sc.mu.Lock()
	oldPeer := sc.currentSyncPeer
	if peerID != "" && oldPeer != peerID {
		sc.mu.Unlock()
		return false
	}
	sc.currentSyncPeer = ""
	sc.syncStartTime = time.Time{}
	sc.lastSyncProgressTime = time.Time{}
	sc.lastSyncPeerBlocksReceived = 0
	sc.mu.Unlock()

	if oldPeer != "" {
		sc.logger.Infof("[SyncCoordinator] Cleared sync peer %s", oldPeer)
	}
	return oldPeer != ""
}

// recordSyncPeerBlockProgress refreshes the no-progress stall deadline only when
// the sync peer has delivered a new fully-validated block. blocksReceived is the
// peer's BlocksReceived counter, which is incremented by ReportValidBlock after a
// block body is delivered and validated locally. Keying the deadline off block
// delivery (rather than validated header chainwork) ensures a peer that only
// serves headers — header work is credited before any block body arrives — still
// times out, closing the header-only-non-deliverer griefing window.
func (sc *SyncCoordinator) recordSyncPeerBlockProgress(peerID string, blocksReceived int64, now time.Time) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if sc.currentSyncPeer != peerID {
		return
	}
	if blocksReceived > sc.lastSyncPeerBlocksReceived {
		sc.lastSyncPeerBlocksReceived = blocksReceived
		sc.lastSyncProgressTime = now
	}
}

func (sc *SyncCoordinator) syncPeerNoProgressTimedOut(now time.Time) (string, time.Duration, bool) {
	sc.mu.RLock()
	currentPeer := sc.currentSyncPeer
	lastProgress := sc.lastSyncProgressTime
	if lastProgress.IsZero() {
		lastProgress = sc.syncStartTime
	}
	sc.mu.RUnlock()

	if currentPeer == "" || lastProgress.IsZero() {
		return currentPeer, 0, false
	}

	progressAge := now.Sub(lastProgress)
	return currentPeer, progressAge, progressAge > sc.syncPeerNoProgressLimit()
}

func (sc *SyncCoordinator) clearNoProgressSyncPeer(peerID string, progressAge time.Duration) bool {
	sc.logger.Warnf("[SyncCoordinator] Sync peer %s made no validated progress for %v", peerID, progressAge.Round(time.Second))
	if err := sc.registry.RecordSyncAttempt(sc.ctx, peerID); err != nil {
		sc.logger.Warnf("[SyncCoordinator] RecordSyncAttempt(no-progress) failed for %s: %v", peerID, err)
	}
	if !sc.clearSyncPeerIfCurrent(peerID) {
		return false
	}
	_ = sc.TriggerSync()
	return true
}

// TriggerSync triggers a new sync operation
func (sc *SyncCoordinator) TriggerSync() error {
	sc.logger.Debugf("[SyncCoordinator] Sync triggered")

	localHeight := sc.getLocalHeightSafe()
	oldPeer := sc.currentSyncPeerLocked()
	if oldPeer == "" && len(sc.listAllPeers()) == 0 {
		sc.checkAllPeersAttempted()
		return nil
	}
	return sc.selectAndActivateNewPeer(localHeight, oldPeer)
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
	localHeight, localChainWork, localWorkOK := sc.getLocalTipWorkSafe(sc.ctx)
	previousPeer := sc.currentSyncPeerLocked()

	peers := sc.listAllPeers()
	eligiblePeers := sc.filterEligiblePeersWithTip(peers, previousPeer, localHeight, localChainWork, localWorkOK)
	if sc.settings != nil && sc.settings.P2P.ForceSyncPeer != "" {
		eligiblePeers = peers
	}

	return sc.selectSyncPeerFromCandidates(eligiblePeers, localHeight, localChainWork, previousPeer)
}

func (sc *SyncCoordinator) selectionCriteria(localHeight uint32, localChainWork []byte, previousPeer string) SelectionCriteria {
	unprovenProbeBudgetRemaining := sc.unprovenProbeBudgetRemainingValue()
	criteria := SelectionCriteria{
		LocalHeight:                  int32(localHeight),
		LocalChainWork:               localChainWork,
		AllowAdvertisedProbe:         unprovenProbeBudgetRemaining > 0,
		UnprovenProbeBudgetRemaining: unprovenProbeBudgetRemaining,
		FullDeliveryFreshnessWindow:  sc.fullDeliveryFreshnessWindow(),
		PreviousPeer:                 previousPeer,
		SyncAttemptCooldown:          1 * time.Minute, // Don't retry peers for at least 1 minute
	}
	// Check for forced peer
	if sc.settings != nil && sc.settings.P2P.ForceSyncPeer != "" {
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
	return criteria
}

func (sc *SyncCoordinator) selectSyncPeerFromCandidates(peers []*blockchain.PeerInfo, localHeight uint32, localChainWork []byte, previousPeer string) string {
	criteria := sc.selectionCriteria(localHeight, localChainWork, previousPeer)
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
			sc.refreshProbeBudgetFromLocalTip(ctx)
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
	sc.refreshProbeBudgetFromLocalTip(ctx)

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
		sc.mu.RLock()
		currentPeer := sc.currentSyncPeer
		sc.mu.RUnlock()

		if currentPeer != "" {
			_, localChainWork, localWorkOK := sc.getLocalTipWorkSafe(sc.ctx)
			if localWorkOK {
				sc.resetProbeBudgetIfLocalChainWorkAdvanced(localChainWork)
			}
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

			now := time.Now()
			sc.recordSyncPeerBlockProgress(currentPeer, peerInfo.BlocksReceived, now)
			if stalledPeer, progressAge, timedOut := sc.syncPeerNoProgressTimedOut(now); timedOut && stalledPeer == currentPeer {
				return sc.clearNoProgressSyncPeer(currentPeer, progressAge)
			}

			if !localWorkOK || !peerHasValidatedWork(peerInfo) {
				sc.logger.Debugf("[SyncCoordinator] Deferring sync-peer transition for %s until validated chainwork is available", currentPeer)
				return false
			}

			if peerAheadByValidatedWork(peerInfo, localChainWork) {
				sc.logger.Infof("[SyncCoordinator] Sync with peer %s considered failed; peer still has higher validated work",
					currentPeer)
				sc.ClearSyncPeer()
				_ = sc.TriggerSync()
				return true // Transition handled
			}
			// We've caught up or surpassed the peer, this is success not failure
			sc.logger.Infof("[SyncCoordinator] Sync completed successfully with peer %s by validated work", currentPeer)
			sc.resetBackoff()
			sc.ClearSyncPeer()
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

	if err := sc.selectAndActivateNewPeer(localHeight, currentSyncPeer); err != nil {
		sc.logger.Warnf("[SyncCoordinator] selectAndActivateNewPeer failed: %v", err)
	}
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
func (sc *SyncCoordinator) selectAndActivateNewPeer(localHeight uint32, oldPeer string) error {
	if oldPeer != "" {
		sc.logger.Debugf("[SyncCoordinator] Sync peer %s already active; skipping new activation", oldPeer)
		return nil
	}
	sc.refreshProbeBudgetFromLocalTip(sc.ctx)
	tipHeight, localChainWork, localWorkOK := sc.getLocalTipWorkSafe(sc.ctx)
	if localWorkOK {
		localHeight = tipHeight
	}

	// Get all peers
	peers := sc.listAllPeers()

	// Filter eligible peers
	eligiblePeers := sc.filterEligiblePeersWithTip(peers, oldPeer, localHeight, localChainWork, localWorkOK)
	if sc.settings != nil && sc.settings.P2P.ForceSyncPeer != "" {
		eligiblePeers = peers
	}

	if len(eligiblePeers) == 0 {
		// No eligible peers - either we're caught up or all peers are filtered
		// (banned, malicious, low reputation, etc.)
		sc.logger.Infof("[SyncCoordinator] No eligible peers found at height %d", localHeight)
		// Enter backoff to prevent busy loop
		sc.enterBackoffMode()
		return nil
	}

	// Select from eligible peers
	newSyncPeer := sc.selectSyncPeerFromCandidates(eligiblePeers, localHeight, localChainWork, oldPeer)
	if newSyncPeer == "" {
		sc.logger.Warnf("[SyncCoordinator] No suitable new sync peer found (different from %s)", oldPeer)
		sc.logCandidateList(eligiblePeers)
		// Enter backoff mode to prevent busy loop when all peers fail selection
		// (e.g., all health checks fail or peers are on cooldown)
		sc.enterBackoffMode()
		return nil
	}

	selectedPeer := findPeerByID(peers, newSyncPeer)
	if selectedPeer == nil {
		sc.logger.Warnf("[SyncCoordinator] Selected sync peer %s no longer exists", newSyncPeer)
		return nil
	}
	// A forced peer (operator override) and a peer that is ahead by locally-validated
	// work are both exempt from the unproven-probe budget: the filter stage already
	// admits them unconditionally, so the claim stage must not re-gate and reject them.
	exemptFromProbeBudget := (sc.settings != nil && sc.settings.P2P.ForceSyncPeer != "") ||
		(localWorkOK && peerAheadByValidatedWork(selectedPeer, localChainWork))
	if !sc.claimSelectedSyncPeer(newSyncPeer, selectedPeer, time.Now(), exemptFromProbeBudget) {
		if sc.currentSyncPeerLocked() == "" {
			sc.logger.Warnf("[SyncCoordinator] Unproven sync probe budget exhausted before activating %s", newSyncPeer)
			sc.enterBackoffMode()
		} else {
			sc.logger.Debugf("[SyncCoordinator] Sync peer already claimed before activating %s", newSyncPeer)
		}
		return nil
	}
	if err := sc.registry.RecordSyncAttempt(sc.ctx, newSyncPeer); err != nil {
		sc.logger.Warnf("[SyncCoordinator] RecordSyncAttempt failed for %s: %v", newSyncPeer, err)
	}

	if err := sc.sendSyncMessage(newSyncPeer); err != nil {
		sc.logger.Errorf("[SyncCoordinator] Failed to trigger sync: %v", err)
		return err
	}
	sc.logger.Infof("[SyncCoordinator] Triggered sync with peer %s via Kafka", newSyncPeer)
	return nil
}

func (sc *SyncCoordinator) filterEligiblePeersWithTip(peers []*blockchain.PeerInfo, oldPeer string, localHeight uint32, localChainWork []byte, localWorkOK bool) []*blockchain.PeerInfo {
	validatedPeers := make([]*blockchain.PeerInfo, 0, len(peers))
	for _, p := range peers {
		if p.ID == oldPeer || !isViableSyncCandidate(p) {
			if p.ID == oldPeer {
				sc.logger.Debugf("[SyncCoordinator] Skipping old peer %s", p.ID)
			}
			continue
		}
		if localWorkOK && peerAheadByValidatedWork(p, localChainWork) {
			validatedPeers = append(validatedPeers, peerForSelector(p, localHeight))
		}
	}
	if len(validatedPeers) > 0 {
		return validatedPeers
	}

	if !sc.hasUnprovenProbeBudget() {
		return nil
	}

	eligiblePeers := make([]*blockchain.PeerInfo, 0, len(peers))
	for _, p := range peers {
		if p.ID == oldPeer {
			continue
		}
		if sc.peerEligibleForAdvertisedProbe(p, localHeight) {
			eligiblePeers = append(eligiblePeers, p)
		}
	}
	return eligiblePeers
}

func (sc *SyncCoordinator) claimSelectedSyncPeer(newSyncPeer string, peerInfo *blockchain.PeerInfo, now time.Time, exemptFromProbeBudget bool) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if sc.currentSyncPeer != "" {
		return false
	}
	if !exemptFromProbeBudget && sc.isUnprovenProbeCandidate(peerInfo, now) {
		if sc.unprovenProbeBudgetRemaining <= 0 {
			return false
		}
		sc.unprovenProbeBudgetRemaining--
	}
	sc.currentSyncPeer = newSyncPeer
	sc.syncStartTime = now
	sc.lastSyncProgressTime = now
	if peerInfo != nil {
		sc.lastSyncPeerBlocksReceived = peerInfo.BlocksReceived
	} else {
		sc.lastSyncPeerBlocksReceived = 0
	}
	sc.lastSyncTrigger = now
	return true
}

func findPeerByID(peers []*blockchain.PeerInfo, id string) *blockchain.PeerInfo {
	for _, p := range peers {
		if p.ID == id {
			return p
		}
	}
	return nil
}

func peerForSelector(p *blockchain.PeerInfo, localHeight uint32) *blockchain.PeerInfo {
	if p.Height > localHeight {
		return p
	}
	candidate := *p
	if localHeight < ^uint32(0) {
		candidate.Height = localHeight + 1
	}
	return &candidate
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
	now := time.Now()
	sc.mu.RLock()
	currentPeer := sc.currentSyncPeer
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

	_, localChainWork, localWorkOK := sc.getLocalTipWorkSafe(sc.ctx)
	if localWorkOK {
		sc.resetProbeBudgetIfLocalChainWorkAdvanced(localChainWork)
	}
	sc.recordSyncPeerBlockProgress(currentPeer, peerInfo.BlocksReceived, now)
	stalledPeer, progressAge, timedOut := sc.syncPeerNoProgressTimedOut(now)
	if timedOut && stalledPeer == currentPeer {
		sc.clearNoProgressSyncPeer(currentPeer, progressAge)
		return
	}

	// Check if we've caught up
	if !localWorkOK || !peerHasValidatedWork(peerInfo) {
		sc.logger.Debugf("[SyncCoordinator] Skipping caught-up evaluation for %s until validated chainwork is available", currentPeer)
		return
	}
	if !peerAheadByValidatedWork(peerInfo, localChainWork) {
		sc.logger.Infof("[SyncCoordinator] Caught up to sync peer %s by validated work", currentPeer)
		// Don't clear peer yet, but look for better peer.
		if betterPeer := sc.selectNewSyncPeer(); betterPeer != currentPeer && betterPeer != "" {
			sc.logger.Infof("[SyncCoordinator] Found better sync peer %s", betterPeer)
			sc.ClearSyncPeer()
			_ = sc.TriggerSync()
		}
		return
	}

	// The peer is still ahead by validated work but not timed out. Opportunistically
	// preempt it toward a strictly-higher-work peer so a top-ranked peer that drips
	// just enough validated blocks to dodge the no-progress deadline cannot hold the
	// slot and starve faster honest peers. Guards:
	//   - progressAge must exceed preemptionProgressGuard() (a fraction of the
	//     no-progress timeout), so a peer actively delivering a large block — which
	//     records no validated progress until the body lands — is not evicted early;
	//   - the candidate must have strictly greater validated work than the incumbent
	//     (not merely greater than local); the comparison is a strict zero-margin ">",
	//     which is inherently non-thrashing since equal work never preempts.
	// Residual (by design): a sole top-work slow-dripper — one with no higher-work
	// rival to preempt it — is not caught here and is evicted only by the hard
	// no-progress timeout (syncPeerNoProgressTimedOut).
	if progressAge <= sc.preemptionProgressGuard() {
		return
	}
	candidate := sc.selectNewSyncPeer()
	if candidate == "" || candidate == currentPeer {
		return
	}
	candInfo, exists, err := sc.registry.GetPeer(sc.ctx, candidate)
	if err != nil || !exists {
		return
	}
	if chainWorkGreater(candInfo.ValidatedChainWork, peerInfo.ValidatedChainWork) {
		// Activate the chosen candidate atomically rather than clear-then-reselect. A
		// clear followed by TriggerSync re-runs selection with oldPeer="", so the
		// proven-first sort would re-pin the stalled incumbent and reset its progress
		// clock, defeating eviction; the atomic swap moves the slot to the candidate we
		// already picked and never leaves the node peerless if state moved on under us.
		if !sc.preemptSyncPeer(currentPeer, candidate, candInfo, now) {
			return
		}
		sc.logger.Infof("[SyncCoordinator] Preempted sync peer %s for higher-work peer %s after %v without validated progress",
			currentPeer, candidate, progressAge.Round(time.Second))
		// Bench the incumbent on the sync-attempt cooldown (mirrors clearNoProgressSyncPeer)
		// so it is not immediately reselected if the candidate later clears; then record
		// the candidate's attempt exactly as the normal activation path does.
		if err := sc.registry.RecordSyncAttempt(sc.ctx, currentPeer); err != nil {
			sc.logger.Warnf("[SyncCoordinator] RecordSyncAttempt failed for benched peer %s: %v", currentPeer, err)
		}
		if err := sc.registry.RecordSyncAttempt(sc.ctx, candidate); err != nil {
			sc.logger.Warnf("[SyncCoordinator] RecordSyncAttempt failed for %s: %v", candidate, err)
		}
		if err := sc.sendSyncMessage(candidate); err != nil {
			sc.logger.Errorf("[SyncCoordinator] Failed to trigger preemptive sync: %v", err)
		}
	}
}

// preemptSyncPeer atomically moves the sync slot from the stalled incumbent to the
// already-chosen higher-work candidate. It returns false (leaving the incumbent in
// place) if the slot moved on under us, so preemption never leaves the node peerless.
// The candidate has strictly greater validated work than the incumbent, which is
// itself ahead of local by validated work, so the candidate is validated-ahead and is
// not an unproven probe (no budget charge — see peerAheadByValidatedWork). The
// incumbent's progress clock is not touched: the slot moves to the new peer, whose
// clock starts fresh here.
func (sc *SyncCoordinator) preemptSyncPeer(incumbent, candidate string, candInfo *blockchain.PeerInfo, now time.Time) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if sc.currentSyncPeer != incumbent {
		return false
	}
	sc.currentSyncPeer = candidate
	sc.syncStartTime = now
	sc.lastSyncProgressTime = now
	if candInfo != nil {
		sc.lastSyncPeerBlocksReceived = candInfo.BlocksReceived
	} else {
		sc.lastSyncPeerBlocksReceived = 0
	}
	sc.lastSyncTrigger = now
	return true
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
	sc.unprovenProbeBudgetRemaining = maxUnprovenProbeBudget(sc.settings)
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
	sc.unprovenProbeBudgetRemaining = maxUnprovenProbeBudget(sc.settings)
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
	localHeight, localChainWork, localWorkOK := sc.getLocalTipWorkSafe(sc.ctx)
	probeBudgetAvailable := sc.hasUnprovenProbeBudget()

	eligibleCount := 0
	recentlyAttemptedCount := 0
	syncAttemptCooldown := 1 * time.Minute // Don't retry a peer for at least 1 minute

	for _, p := range peers {
		if !isViableSyncCandidate(p) {
			continue
		}
		eligibleByValidatedWork := localWorkOK && peerAheadByValidatedWork(p, localChainWork)
		eligibleByAdvertisedProbe := probeBudgetAvailable && sc.peerEligibleForAdvertisedProbe(p, localHeight)
		if eligibleByValidatedWork || eligibleByAdvertisedProbe {
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
