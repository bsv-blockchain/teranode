package p2p

import (
	"context"
	"math/rand/v2"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/work"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/health"
)

const (
	// peerHealthCheckConcurrency bounds how many peer availability probes run at once.
	peerHealthCheckConcurrency = 8

	// peerHealthCheckBudget caps the total wall-clock time one selection spends on
	// availability probes; probes still pending when it expires resolve as unhealthy.
	peerHealthCheckBudget = 5 * time.Second

	// peerHealthCacheTTL is how long a probe result is reused before re-probing.
	// It spans the full-node and pruned-node passes of one selection as well as
	// closely spaced selection ticks.
	peerHealthCacheTTL = 10 * time.Second
)

// peerHealthCacheEntry is a cached availability probe result.
type peerHealthCacheEntry struct {
	healthy   bool
	checkedAt time.Time
}

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
// Selection among candidates that are equal on every merit criterion is
// randomized so a Sybil attacker cannot capture selection by grinding peer
// IDs. When settings.P2P.HealthCheckEnabled is set it probes candidate DataHub
// URLs over HTTP (bounded concurrency, overall deadline) and keeps a short-TTL
// cache of those probe results, so it is neither pure nor stateless.
type PeerSelector struct {
	logger   ulogger.Logger
	settings *settings.Settings
	randIntN func(n int) int // injectable for tests; must return a uniform value in [0, n)

	// checkHealth probes a peer's DataHub URL; overridable in tests.
	checkHealth func(ctx context.Context, dataHubURL string) (bool, error)

	// healthCheckBudget caps the total wall-clock time one selection spends on
	// availability probes; peerHealthCheckBudget by default, overridable in tests.
	healthCheckBudget time.Duration

	healthMu    sync.Mutex
	healthCache map[string]peerHealthCacheEntry
}

// NewPeerSelector creates a new peer selector
func NewPeerSelector(logger ulogger.Logger, settings *settings.Settings) *PeerSelector {
	return &PeerSelector{
		logger:   logger,
		settings: settings,
		// math/rand/v2 uses a per-thread ChaCha8 generator seeded from OS
		// entropy; a remote attacker cannot observe or predict the stream, so
		// crypto/rand is unnecessary for this tiebreak.
		randIntN:          rand.IntN,
		checkHealth:       checkPeerAvailability,
		healthCheckBudget: peerHealthCheckBudget,
		healthCache:       make(map[string]peerHealthCacheEntry),
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
// When settings.P2P.HealthCheckEnabled is set, candidate DataHub URLs are
// probed over HTTP before either phase: concurrently (bounded by
// peerHealthCheckConcurrency), full-node tier first so slow pruned peers
// cannot starve a full node's probe, under both ctx and an overall deadline
// (peerHealthCheckBudget), with results cached for peerHealthCacheTTL so the
// two phases and closely spaced calls do not re-probe the same URL.
// Peer IDs are canonical libp2p ID strings.
func (ps *PeerSelector) SelectSyncPeer(ctx context.Context, peers []*blockchain.PeerInfo, criteria SelectionCriteria) string {
	// Handle forced peer - always select it if it exists, regardless of
	// eligibility. This deliberately skips the blacklist check in
	// isEligibleBasic too, but a blacklisted DataHub URL is still refused
	// downstream: sendSyncTriggerToKafka drops the trigger rather than handing
	// the URL to block validation, so forcing a peer cannot override the
	// blacklist.
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

	// Basic (network-free) eligibility is computed once per selection and
	// shared by the availability pre-pass and both candidate passes below.
	basicEligible := ps.basicEligibleByID(peers, criteria)

	// Probe availability of every basic-eligible candidate once, up front, so
	// the eligibility checks below do no network I/O.
	healthByURL := ps.checkCandidatesHealth(ctx, peers, basicEligible)

	// PHASE 1: Try to select from full nodes
	fullNodeCandidates := ps.getFullNodeCandidates(peers, basicEligible, healthByURL)
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
		prunedCandidates := ps.getPrunedNodeCandidates(peers, basicEligible, healthByURL)
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
func (ps *PeerSelector) getFullNodeCandidates(peers []*blockchain.PeerInfo, basicEligible map[string]struct{}, healthByURL map[string]bool) []*blockchain.PeerInfo {
	var candidates []*blockchain.PeerInfo
	now := time.Now()
	for _, p := range peers {
		if ps.isEligibleFullNode(p, now, basicEligible, healthByURL) {
			candidates = append(candidates, p)
			ps.logger.Debugf("[PeerSelector] Full node candidate: %s (validated_height=%d, advertised_height=%d, mode=%s)",
				p.ID, p.ValidatedHeight, p.Height, p.Storage)
		}
	}
	return candidates
}

// getPrunedNodeCandidates returns eligible non-full nodes that are ahead by
// validated work or eligible for a bounded advertised probe.
func (ps *PeerSelector) getPrunedNodeCandidates(peers []*blockchain.PeerInfo, basicEligible map[string]struct{}, healthByURL map[string]bool) []*blockchain.PeerInfo {
	var candidates []*blockchain.PeerInfo
	now := time.Now()
	for _, p := range peers {
		// Only include if eligible but NOT a full node
		if ps.isEligible(p, basicEligible, healthByURL) && !ps.isEffectiveFullNode(p, now) {
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

// basicEligibleByID runs isEligibleBasic over every peer once and returns the
// set of eligible peer IDs. Computed once per selection so the availability
// pre-pass and both candidate passes share one result (and one round of
// per-criterion debug logging) instead of re-deriving it per pass. Eligibility
// is therefore fixed at selection start: a peer whose cooldown expires while
// probes run stays ineligible until the next selection.
func (ps *PeerSelector) basicEligibleByID(peers []*blockchain.PeerInfo, criteria SelectionCriteria) map[string]struct{} {
	eligible := make(map[string]struct{}, len(peers))
	for _, p := range peers {
		if ps.isEligibleBasic(p, criteria) {
			eligible[p.ID] = struct{}{}
		}
	}
	return eligible
}

// isEligibleBasic checks every selection criterion that needs no network I/O.
// Callers on the selection path should consult the per-selection set from
// basicEligibleByID instead of calling this repeatedly.
func (ps *PeerSelector) isEligibleBasic(p *blockchain.PeerInfo, criteria SelectionCriteria) bool {
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

	return true
}

// isEligible checks if a peer meets selection criteria. basicEligible is the
// per-selection result of basicEligibleByID; healthByURL is the availability
// pre-pass result from checkCandidatesHealth, nil meaning health checking is
// disabled.
func (ps *PeerSelector) isEligible(p *blockchain.PeerInfo, basicEligible map[string]struct{}, healthByURL map[string]bool) bool {
	if _, ok := basicEligible[p.ID]; !ok {
		return false
	}

	// Check HTTP availability if enabled
	// Note: Health check failures are NOT recorded as sync attempts - they're filtered out early.
	// The caller (SyncCoordinator) will record sync attempt after selecting the peer.
	if healthByURL != nil {
		healthy, probed := healthByURL[p.DataHubURL]

		// Defensive: the pre-pass probes every basic-eligible peer's URL, so
		// this should always hit; if it ever doesn't, treat the peer as
		// unavailable this round rather than probing serially.
		if !probed {
			ps.logger.Debugf("[PeerSelector] Peer %s was not probed this selection, skipping", p.ID)
			return false
		}

		if !healthy {
			ps.logger.Debugf("[PeerSelector] Peer %s is unhealthy", p.ID)
			return false
		}
	}

	return true
}

// checkCandidatesHealth probes the DataHub URL of every basic-eligible peer and
// returns availability keyed by URL, or nil when health checking is disabled.
// URLs are deduplicated and enqueued full-node tier first, so the shared
// worker pool and probe budget serve the preferred tier before pruned or
// header-only peers: a queue of slow pruned peers can no longer exhaust the
// budget ahead of a healthy full node and demote it for the tick. Probes run
// on a worker pool of at most peerHealthCheckConcurrency goroutines under ctx
// plus the overall healthCheckBudget deadline, and results are cached for
// peerHealthCacheTTL. A probe cut short by ctx or the budget counts as
// unhealthy for this selection but is not cached, so a slow peer is not
// remembered as down. Concurrent calls may probe the same URL twice (the
// cache only dedupes completed probes); the write-back keeps whichever
// probe completed later, so a slower round cannot overwrite a fresher entry
// with a staler one. Today every caller serialises behind
// SyncCoordinator.decisionMu.
func (ps *PeerSelector) checkCandidatesHealth(ctx context.Context, peers []*blockchain.PeerInfo, basicEligible map[string]struct{}) map[string]bool {
	if ps.settings == nil || !ps.settings.P2P.HealthCheckEnabled {
		return nil
	}

	// Collect the distinct DataHub URLs of basic-eligible peers, full-node
	// tier ahead of pruned tier. A URL shared by peers of both tiers is
	// classified by the first peer seen with it.
	var fullURLs, prunedURLs []string
	seen := make(map[string]struct{}, len(peers))
	now := time.Now()

	for _, p := range peers {
		if _, dup := seen[p.DataHubURL]; dup {
			continue
		}

		if _, ok := basicEligible[p.ID]; !ok {
			continue
		}

		seen[p.DataHubURL] = struct{}{}
		if ps.isEffectiveFullNode(p, now) {
			fullURLs = append(fullURLs, p.DataHubURL)
		} else {
			prunedURLs = append(prunedURLs, p.DataHubURL)
		}
	}
	urls := make([]string, 0, len(fullURLs)+len(prunedURLs))
	urls = append(urls, fullURLs...)
	urls = append(urls, prunedURLs...)

	results := make(map[string]bool, len(urls))
	pending := make([]string, 0, len(urls))

	ps.healthMu.Lock()
	for _, url := range urls {
		if entry, ok := ps.healthCache[url]; ok && now.Sub(entry.checkedAt) < peerHealthCacheTTL {
			results[url] = entry.healthy
			continue
		}

		pending = append(pending, url)
	}
	ps.healthMu.Unlock()

	if len(pending) == 0 {
		return results
	}

	budget := ps.healthCheckBudget
	if budget <= 0 {
		budget = peerHealthCheckBudget
	}

	probeCtx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()

	type probeResult struct {
		healthy   bool
		cacheable bool
		checkedAt time.Time
	}

	probeResults := make([]probeResult, len(pending))

	work := make(chan int, len(pending))
	for i := range pending {
		work <- i
	}
	close(work)

	var wg sync.WaitGroup

	for range min(peerHealthCheckConcurrency, len(pending)) {
		wg.Go(func() {
			for i := range work {
				url := pending[i]

				healthy, err := ps.checkHealth(probeCtx, url)
				if err != nil {
					if probeCtx.Err() != nil {
						ps.logger.Debugf("[PeerSelector] Availability probe for %s aborted: %v", url, err)
					} else {
						ps.logger.Debugf("[PeerSelector] Availability probe for %s failed: %v", url, err)
					}
				}

				// Don't cache a negative result caused by ctx or the budget expiring:
				// the peer wasn't actually probed to completion. Stamp each result
				// as its own probe completes, not when the whole round ends, so a
				// fast probe's cache entry doesn't inherit the slowest probe's
				// finish time and outlive the TTL.
				probeResults[i] = probeResult{
					healthy:   healthy,
					cacheable: healthy || probeCtx.Err() == nil,
					checkedAt: time.Now(),
				}
			}
		})
	}

	wg.Wait()

	pruneCutoff := time.Now()

	ps.healthMu.Lock()
	for i, r := range probeResults {
		results[pending[i]] = r.healthy
		if !r.cacheable {
			continue
		}
		// Keep the newer entry if a concurrent round already wrote one, so a
		// slower round cannot overwrite a fresher result with a staler one.
		if existing, ok := ps.healthCache[pending[i]]; !ok || r.checkedAt.After(existing.checkedAt) {
			ps.healthCache[pending[i]] = peerHealthCacheEntry{healthy: r.healthy, checkedAt: r.checkedAt}
		}
	}
	// Drop expired entries so churning peer URLs don't grow the cache unboundedly.
	for url, entry := range ps.healthCache {
		if pruneCutoff.Sub(entry.checkedAt) >= peerHealthCacheTTL {
			delete(ps.healthCache, url)
		}
	}
	ps.healthMu.Unlock()

	return results
}

// isEligibleFullNode checks if a peer is eligible as a full node for catchup
// Only peers explicitly announcing as "full" are considered full nodes
func (ps *PeerSelector) isEligibleFullNode(p *blockchain.PeerInfo, now time.Time, basicEligible map[string]struct{}, healthByURL map[string]bool) bool {
	if !ps.isEligible(p, basicEligible, healthByURL) {
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
