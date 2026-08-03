package p2p

import (
	"context"
	"math/rand/v2"
	"net/http"
	"sort"
	"time"

	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/work"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/health"
)

// SelectionCriteria defines criteria for peer selection
type SelectionCriteria struct {
	LocalHeight                  int32
	LocalChainWork               []byte
	AllowAdvertisedProbe         bool
	UnprovenProbeBudgetRemaining int
	FullDeliveryFreshnessWindow  time.Duration
	ForcedPeerID                 string        // If set, only this peer (canonical libp2p ID string) will be selected
	PreviousPeer                 string        // The previously selected peer (canonical libp2p ID string), if any
	SyncAttemptCooldown          time.Duration // Cooldown period before retrying a peer
}

// PeerSelector handles peer selection logic.
// It is stateless; selection among candidates that are equal on every merit
// criterion is randomized so a Sybil attacker cannot capture selection by
// grinding peer IDs.
type PeerSelector struct {
	logger   ulogger.Logger
	settings *settings.Settings
	randIntN func(n int) int // injectable for tests; must return a uniform value in [0, n)
}

// NewPeerSelector creates a new peer selector
func NewPeerSelector(logger ulogger.Logger, settings *settings.Settings) *PeerSelector {
	return &PeerSelector{
		logger:   logger,
		settings: settings,
		// math/rand/v2 uses a per-thread ChaCha8 generator seeded from OS
		// entropy; a remote attacker cannot observe or predict the stream, so
		// crypto/rand is unnecessary for this tiebreak.
		randIntN: rand.IntN,
	}
}

// randomIndex returns a uniform value in [0, n), falling back to the package
// default source if the selector was constructed without one.
func (ps *PeerSelector) randomIndex(n int) int {
	if ps.randIntN == nil {
		return rand.IntN(n)
	}
	return ps.randIntN(n)
}

// SelectSyncPeer selects the best peer for syncing using two-phase selection:
// Phase 1: Try to select from full nodes (nodes with complete block data)
// Phase 2: If no full nodes and fallback enabled, select from non-full nodes
// Ties among equally ranked candidates are broken randomly, never by peer ID.
// May perform HTTP health checks when P2P.HealthCheckEnabled is set.
// Peer IDs are canonical libp2p ID strings.
func (ps *PeerSelector) SelectSyncPeer(peers []*blockchain.PeerInfo, criteria SelectionCriteria) string {
	// Handle forced peer - always select it if it exists, regardless of
	// eligibility. This deliberately skips the blacklist check in isEligible
	// too, but a blacklisted DataHub URL is still refused downstream:
	// sendSyncTriggerToKafka drops the trigger rather than handing the URL to
	// block validation, so forcing a peer cannot override the blacklist.
	if criteria.ForcedPeerID != "" {
		for _, p := range peers {
			if p.ID == criteria.ForcedPeerID {
				ps.logger.Infof("[PeerSelector] Using forced peer %s", p.ID)
				return p.ID
			}
		}
		ps.logger.Infof("[PeerSelector] Forced peer %s not connected", criteria.ForcedPeerID)
		return ""
	}

	// PHASE 1: Try to select from full nodes
	fullNodeCandidates := ps.getFullNodeCandidates(peers, criteria)
	if len(fullNodeCandidates) > 0 {
		selected := ps.selectFromCandidates(fullNodeCandidates, criteria, true)
		if selected != "" {
			ps.logger.Infof("[PeerSelector] Selected FULL node %s", selected)
			return selected
		}
	}

	// PHASE 2: Fall back to pruned nodes if enabled (enabled by default if settings is nil)
	allowFallback := true // Default: allow fallback
	if ps.settings != nil {
		allowFallback = ps.settings.P2P.AllowPrunedNodeFallback
	}

	if allowFallback {
		ps.logger.Infof("[PeerSelector] No full nodes available, attempting pruned node fallback")
		prunedCandidates := ps.getPrunedNodeCandidates(peers, criteria)
		if len(prunedCandidates) > 0 {
			selected := ps.selectFromCandidates(prunedCandidates, criteria, false)
			if selected != "" {
				ps.logger.Warnf("[PeerSelector] Selected PRUNED node %s", selected)
				return selected
			}
		}
	} else {
		ps.logger.Infof("[PeerSelector] No full nodes available and pruned node fallback disabled")
	}

	ps.logger.Debugf("[PeerSelector] No suitable sync peer found")
	return ""
}

// getFullNodeCandidates returns eligible full nodes that are ahead by validated
// work or eligible for a bounded advertised probe.
func (ps *PeerSelector) getFullNodeCandidates(peers []*blockchain.PeerInfo, criteria SelectionCriteria) []*blockchain.PeerInfo {
	var candidates []*blockchain.PeerInfo
	now := time.Now()
	for _, p := range peers {
		if ps.isEligibleFullNode(p, criteria, now) {
			candidates = append(candidates, p)
			ps.logger.Debugf("[PeerSelector] Full node candidate: %s (validated_height=%d, advertised_height=%d, mode=%s)",
				p.ID, p.ValidatedHeight, p.Height, p.Storage)
		}
	}
	return candidates
}

// getPrunedNodeCandidates returns eligible non-full nodes that are ahead by
// validated work or eligible for a bounded advertised probe.
func (ps *PeerSelector) getPrunedNodeCandidates(peers []*blockchain.PeerInfo, criteria SelectionCriteria) []*blockchain.PeerInfo {
	var candidates []*blockchain.PeerInfo
	now := time.Now()
	for _, p := range peers {
		// Only include if eligible but NOT a full node
		if ps.isEligible(p, criteria) && !ps.isEffectiveFullNode(p, now) {
			candidates = append(candidates, p)
			ps.logger.Debugf("[PeerSelector] Pruned node candidate: %s (validated_height=%d, advertised_height=%d, mode=%s)",
				p.ID, p.ValidatedHeight, p.Height, p.Storage)
		}
	}
	return candidates
}

// comparePeerCandidates orders two candidates by merit: proven recent full-block
// delivery, validated chain work, reputation, response time, ban score, then
// validated height. Returns a negative value when a ranks before b, positive
// when b ranks before a, and 0 when they are equal on every criterion. There is
// deliberately no peer-ID tiebreak: peer IDs are attacker-grindable, so ties
// are resolved by random selection in selectFromCandidates instead.
func (ps *PeerSelector) comparePeerCandidates(a, b *blockchain.PeerInfo, now time.Time, freshnessWindow time.Duration) int {
	aProven := peerHasRecentFullBlockDelivery(a, now, freshnessWindow)
	bProven := peerHasRecentFullBlockDelivery(b, now, freshnessWindow)
	if aProven != bProven {
		if aProven {
			return -1
		}
		return 1
	}
	if cmp := compareChainWork(a.ValidatedChainWork, b.ValidatedChainWork); cmp != 0 {
		return -cmp
	}
	if a.ReputationScore != b.ReputationScore {
		if a.ReputationScore > b.ReputationScore {
			return -1
		}
		return 1
	}
	aHasTime := a.AvgResponseTimeMs > 0
	bHasTime := b.AvgResponseTimeMs > 0
	if aHasTime != bHasTime {
		if aHasTime {
			return -1
		}
		return 1
	}
	if aHasTime && bHasTime && a.AvgResponseTimeMs != b.AvgResponseTimeMs {
		if a.AvgResponseTimeMs < b.AvgResponseTimeMs {
			return -1
		}
		return 1
	}
	if a.BanScore != b.BanScore {
		if a.BanScore < b.BanScore {
			return -1
		}
		return 1
	}
	if a.ValidatedHeight != b.ValidatedHeight {
		if a.ValidatedHeight > b.ValidatedHeight {
			return -1
		}
		return 1
	}
	return 0
}

// selectFromCandidates selects the best peer from a list of candidates
// using validation-gated delivery evidence and locally validated work.
// Candidates that tie on every merit criterion form the top band, and the
// winner is drawn uniformly at random from that band so an attacker cannot
// deterministically capture selection by grinding peer IDs.
// The candidates slice is consumed: it is reordered and may be filtered in
// place, so callers must not reuse it afterwards.
func (ps *PeerSelector) selectFromCandidates(candidates []*blockchain.PeerInfo, criteria SelectionCriteria, isFullNode bool) string {
	if len(candidates) == 0 {
		return ""
	}

	// Rotate off the previously selected peer whenever an alternative exists,
	// so a tied previous peer cannot be re-drawn from the top band.
	if len(candidates) > 1 && criteria.PreviousPeer != "" {
		filtered := candidates[:0]
		for _, c := range candidates {
			if c.ID != criteria.PreviousPeer {
				filtered = append(filtered, c)
			}
		}
		if len(filtered) > 0 && len(filtered) < len(candidates) {
			ps.logger.Debugf("[PeerSelector] Excluding previous peer %s from selection", criteria.PreviousPeer)
			candidates = filtered
		}
	}

	now := time.Now()
	sort.Slice(candidates, func(i, j int) bool {
		return ps.comparePeerCandidates(candidates[i], candidates[j], now, criteria.FullDeliveryFreshnessWindow) < 0
	})

	topBandSize := 1
	for topBandSize < len(candidates) &&
		ps.comparePeerCandidates(candidates[0], candidates[topBandSize], now, criteria.FullDeliveryFreshnessWindow) == 0 {
		topBandSize++
	}

	selectedIndex := 0
	if topBandSize > 1 {
		selectedIndex = ps.randomIndex(topBandSize)
	}

	selected := candidates[selectedIndex]
	nodeType := "FULL"
	if !isFullNode {
		nodeType = "PRUNED"
	}
	ps.logger.Infof("[PeerSelector] Selected %s node peer %s (validated_height=%d, advertised_height=%d, banScore=%d, avgResponseTimeMs=%d) from %d candidates (topBandSize=%d, index=%d)",
		nodeType, selected.ID, selected.ValidatedHeight, selected.Height, selected.BanScore, selected.AvgResponseTimeMs, len(candidates), topBandSize, selectedIndex)

	for i := 0; i < len(candidates) && i < 3; i++ {
		ps.logger.Debugf("[PeerSelector] Candidate %d: %s (validated_height=%d, advertised_height=%d, banScore=%d, avgResponseTimeMs=%d, mode=%s, url=%s)",
			i+1, candidates[i].ID, candidates[i].ValidatedHeight, candidates[i].Height, candidates[i].BanScore, candidates[i].AvgResponseTimeMs, candidates[i].Storage, candidates[i].DataHubURL)
	}

	return selected.ID
}

// isEligible checks if a peer meets selection criteria
func (ps *PeerSelector) isEligible(p *blockchain.PeerInfo, criteria SelectionCriteria) bool {
	// Always exclude banned peers
	if p.IsBanned {
		ps.logger.Debugf("[PeerSelector] Peer %s is banned (score: %d)", p.ID, p.BanScore)
		return false
	}

	// Check DataHub URL requirement - this protects against listen-only nodes
	if p.DataHubURL == "" {
		ps.logger.Debugf("[PeerSelector] Peer %s has no DataHub URL (listen-only node)", p.ID)
		return false
	}

	// Enforce the operator blacklist at the point of use: a URL stored in the
	// registry before its host was blacklisted must never be selected for
	// sync/catchup (gossip-time checks cannot evict already-stored URLs).
	if ps.settings != nil && isBaseURLBlacklisted(p.DataHubURL, ps.settings.SubtreeValidation.BlacklistedBaseURLs) {
		ps.logger.Debugf("[PeerSelector] Peer %s has blacklisted DataHub URL %s", p.ID, p.DataHubURL)
		return false
	}

	// Check reputation threshold - peers with very low reputation should not be selected
	if p.ReputationScore < 20.0 {
		ps.logger.Debugf("[PeerSelector] Peer %s has very low reputation %.2f (below threshold 20.0)", p.ID, p.ReputationScore)
		return false
	}

	if !ps.isEligibleByProgress(p, criteria) {
		ps.logger.Debugf("[PeerSelector] Peer %s has no validated-work/probe eligibility", p.ID)
		return false
	}

	// Check sync attempt cooldown BEFORE health check (avoids re-checking failed peers)
	if criteria.SyncAttemptCooldown > 0 && !p.LastSyncAttempt.IsZero() {
		timeSinceLastAttempt := time.Since(p.LastSyncAttempt)
		if timeSinceLastAttempt < criteria.SyncAttemptCooldown {
			ps.logger.Debugf("[PeerSelector] Peer %s attempted recently (%v ago, cooldown: %v)",
				p.ID, timeSinceLastAttempt.Round(time.Second), criteria.SyncAttemptCooldown)
			return false
		}
	}

	// Check HTTP availability if enabled
	// Note: Health check failures are NOT recorded as sync attempts - they're filtered out early.
	// The caller (SyncCoordinator) will record sync attempt after selecting the peer.
	if ps.settings != nil && ps.settings.P2P.HealthCheckEnabled {
		ps.logger.Debugf("[PeerSelector] Checking availability for peer %s", p.ID)

		isHealthy, err := checkPeerAvailability(context.Background(), p.DataHubURL)

		if !isHealthy {
			ps.logger.Debugf("[PeerSelector] Peer %s is unhealthy: %v", p.ID, err)
			return false
		}
	}

	return true
}

// isEligibleFullNode checks if a peer is eligible as a full node for catchup
// Only peers explicitly announcing as "full" are considered full nodes
func (ps *PeerSelector) isEligibleFullNode(p *blockchain.PeerInfo, criteria SelectionCriteria, now time.Time) bool {
	if !ps.isEligible(p, criteria) {
		return false // Must pass basic eligibility first
	}

	return ps.isEffectiveFullNode(p, now)
}

func (ps *PeerSelector) isEffectiveFullNode(p *blockchain.PeerInfo, now time.Time) bool {
	return p.Storage == "full" && (p.FullStoragePenaltyUntil.IsZero() || !now.Before(p.FullStoragePenaltyUntil))
}

func (ps *PeerSelector) isEligibleByProgress(p *blockchain.PeerInfo, criteria SelectionCriteria) bool {
	if peerAheadByValidatedWork(p, criteria.LocalChainWork) {
		return true
	}
	if !criteria.AllowAdvertisedProbe || criteria.UnprovenProbeBudgetRemaining <= 0 {
		return false
	}
	// Unproven/header-only churn is bounded by the coordinator budget: for a
	// Sybil set of size N, at most min(N, budget) peers can be activated before
	// the backoff window is entered or honored. With the default budget this is
	// min(N, 3).
	return peerEligibleForAdvertisedProbe(p, localHeightFromCriteria(criteria))
}

func localHeightFromCriteria(criteria SelectionCriteria) uint32 {
	if criteria.LocalHeight <= 0 {
		return 0
	}
	return uint32(criteria.LocalHeight)
}

func compareChainWork(a, b []byte) int {
	return work.CompareChainWork(a, b)
}

// checkPeerAvailability tests if a peer's DataHub URL is reachable via HTTP.
// DataHubURL already includes /api/v1 prefix, so we just append the endpoint path.
// Uses existing util/health infrastructure with built-in 2s timeout.
func checkPeerAvailability(ctx context.Context, dataHubURL string) (bool, error) {
	if dataHubURL == "" {
		return false, nil
	}

	// DataHubURL format: "https://host/api/v1"
	// Append /bestblockheader to get full endpoint path
	checker := health.CheckHTTPServer(dataHubURL, "/bestblockheader")

	statusCode, _, err := checker(ctx, false)

	// Only accept 200 OK - API endpoints should return exactly 200
	return statusCode == http.StatusOK, err
}
