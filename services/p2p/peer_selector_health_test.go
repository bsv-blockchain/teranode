package p2p

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

func newHealthCheckSelectorForTest() *PeerSelector {
	return NewPeerSelector(ulogger.TestLogger{}, &settings.Settings{
		P2P: settings.P2PSettings{
			AllowPrunedNodeFallback:     true,
			FullDeliveryFreshnessWindow: 24 * time.Hour,
			HealthCheckEnabled:          true,
		},
	})
}

// countingHealthStub records probe invocations per URL and returns the
// configured health status (unlisted URLs are healthy).
type countingHealthStub struct {
	mu        sync.Mutex
	calls     map[string]int
	unhealthy map[string]bool
}

func newCountingHealthStub(unhealthyURLs ...string) *countingHealthStub {
	unhealthy := make(map[string]bool, len(unhealthyURLs))
	for _, u := range unhealthyURLs {
		unhealthy[u] = true
	}
	return &countingHealthStub{calls: make(map[string]int), unhealthy: unhealthy}
}

func (s *countingHealthStub) check(_ context.Context, url string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls[url]++
	return !s.unhealthy[url], nil
}

func (s *countingHealthStub) callsFor(url string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls[url]
}

func (s *countingHealthStub) totalCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	total := 0
	for _, n := range s.calls {
		total += n
	}
	return total
}

func TestPeerSelector_HealthChecks_FilterUnhealthyPeers(t *testing.T) {
	ps := newHealthCheckSelectorForTest()
	peerA := newPeer("peer-a", 100, "full", 50, 0)
	peerB := newPeer("peer-b", 100, "full", 50, 0)

	stub := newCountingHealthStub(peerA.DataHubURL)
	ps.checkHealth = stub.check

	got := ps.SelectSyncPeer(context.Background(), []*blockchain.PeerInfo{peerA, peerB}, advertisedProbeCriteria(50))
	require.Equal(t, "peer-b", got)
}

func TestPeerSelector_HealthChecks_ProbeEachURLOnceAcrossBothPasses(t *testing.T) {
	ps := newHealthCheckSelectorForTest()
	full := newPeer("peer-full", 100, "full", 50, 0)
	pruned := newPeer("peer-pruned", 100, "pruned", 50, 0)

	// Both peers unhealthy: the full pass yields nothing, forcing the pruned
	// fallback pass, which previously re-probed every peer.
	stub := newCountingHealthStub(full.DataHubURL, pruned.DataHubURL)
	ps.checkHealth = stub.check

	got := ps.SelectSyncPeer(context.Background(), []*blockchain.PeerInfo{full, pruned}, advertisedProbeCriteria(50))
	require.Equal(t, "", got)
	require.Equal(t, 1, stub.callsFor(full.DataHubURL))
	require.Equal(t, 1, stub.callsFor(pruned.DataHubURL))
}

func TestPeerSelector_HealthChecks_CachedAcrossCalls(t *testing.T) {
	ps := newHealthCheckSelectorForTest()
	// Pin the tiebreak: peerA and peerB tie on every merit criterion, and the
	// test needs both selections to pick the same peer to compare them.
	ps.randIntN = func(int) int { return 0 }
	peerA := newPeer("peer-a", 100, "full", 50, 0)
	peerB := newPeer("peer-b", 100, "full", 50, 0)
	peers := []*blockchain.PeerInfo{peerA, peerB}

	stub := newCountingHealthStub()
	ps.checkHealth = stub.check

	first := ps.SelectSyncPeer(context.Background(), peers, advertisedProbeCriteria(50))
	require.NotEqual(t, "", first)
	require.Equal(t, 2, stub.totalCalls())

	second := ps.SelectSyncPeer(context.Background(), peers, advertisedProbeCriteria(50))
	require.Equal(t, first, second)
	require.Equal(t, 2, stub.totalCalls(), "second selection within the TTL must not re-probe")
}

func TestPeerSelector_HealthChecks_UnhealthyResultCachedAcrossCalls(t *testing.T) {
	ps := newHealthCheckSelectorForTest()
	peerA := newPeer("peer-a", 100, "full", 50, 0)
	peers := []*blockchain.PeerInfo{peerA}

	// The probe completes (no ctx expiry) with an unhealthy verdict, so it is
	// cacheable and must not be re-probed within the TTL.
	stub := newCountingHealthStub(peerA.DataHubURL)
	ps.checkHealth = stub.check

	require.Empty(t, ps.SelectSyncPeer(context.Background(), peers, advertisedProbeCriteria(50)))
	require.Equal(t, 1, stub.callsFor(peerA.DataHubURL))

	require.Empty(t, ps.SelectSyncPeer(context.Background(), peers, advertisedProbeCriteria(50)))
	require.Equal(t, 1, stub.callsFor(peerA.DataHubURL), "a completed unhealthy probe must be served from cache within the TTL")
}

func TestPeerSelector_HealthChecks_DisabledSkipsProbes(t *testing.T) {
	ps := NewPeerSelector(ulogger.TestLogger{}, &settings.Settings{
		P2P: settings.P2PSettings{
			AllowPrunedNodeFallback:     true,
			FullDeliveryFreshnessWindow: 24 * time.Hour,
			HealthCheckEnabled:          false,
		},
	})

	stub := newCountingHealthStub()
	ps.checkHealth = stub.check

	got := ps.SelectSyncPeer(context.Background(), []*blockchain.PeerInfo{newPeer("peer-a", 100, "full", 50, 0)}, advertisedProbeCriteria(50))
	require.Equal(t, "peer-a", got, "selection must still work with health checking disabled")
	require.Zero(t, stub.totalCalls(), "no probes may run when health checking is disabled")
}

func TestPeerSelector_HealthChecks_SharedURLProbedOnce(t *testing.T) {
	ps := newHealthCheckSelectorForTest()
	peerA := newPeer("peer-a", 100, "full", 50, 0)
	peerB := newPeer("peer-b", 100, "full", 50, 0)
	peerB.DataHubURL = peerA.DataHubURL

	stub := newCountingHealthStub()
	ps.checkHealth = stub.check

	got := ps.SelectSyncPeer(context.Background(), []*blockchain.PeerInfo{peerA, peerB}, advertisedProbeCriteria(50))
	require.NotEmpty(t, got)
	require.Equal(t, 1, stub.totalCalls(), "peers sharing one DataHub URL must be probed once")
}

func TestPeerSelector_HealthChecks_ExpiredEntriesPruned(t *testing.T) {
	ps := newHealthCheckSelectorForTest()
	peerA := newPeer("peer-a", 100, "full", 50, 0)
	peerB := newPeer("peer-b", 100, "full", 50, 0)

	stub := newCountingHealthStub()
	ps.checkHealth = stub.check

	ps.SelectSyncPeer(context.Background(), []*blockchain.PeerInfo{peerA}, advertisedProbeCriteria(50))

	// Age peerA's entry past the TTL, then run a selection that no longer
	// offers its URL; the probe round must prune the stale entry so churning
	// peer URLs cannot grow the cache unboundedly.
	ps.healthMu.Lock()
	entry := ps.healthCache[peerA.DataHubURL]
	entry.checkedAt = entry.checkedAt.Add(-2 * peerHealthCacheTTL)
	ps.healthCache[peerA.DataHubURL] = entry
	ps.healthMu.Unlock()

	ps.SelectSyncPeer(context.Background(), []*blockchain.PeerInfo{peerB}, advertisedProbeCriteria(50))

	ps.healthMu.Lock()
	_, stillCached := ps.healthCache[peerA.DataHubURL]
	ps.healthMu.Unlock()
	require.False(t, stillCached, "expired entry for a churned peer URL must be pruned")
}

func TestPeerSelector_HealthChecks_CacheEntriesStampedPerProbe(t *testing.T) {
	ps := newHealthCheckSelectorForTest()
	fast := newPeer("peer-fast", 100, "full", 50, 0)
	slow := newPeer("peer-slow", 100, "full", 50, 0)

	// One fast and one slow probe in the same round: each cache entry must
	// carry its own probe's completion time, not the round's end time, or the
	// fast entry would outlive its TTL by the slow probe's duration.
	const slowDelay = 250 * time.Millisecond
	ps.checkHealth = func(ctx context.Context, url string) (bool, error) {
		if url == slow.DataHubURL {
			select {
			case <-time.After(slowDelay):
			case <-ctx.Done():
			}
		}
		return true, nil
	}

	ps.SelectSyncPeer(context.Background(), []*blockchain.PeerInfo{fast, slow}, advertisedProbeCriteria(50))

	ps.healthMu.Lock()
	fastEntry, fastCached := ps.healthCache[fast.DataHubURL]
	slowEntry, slowCached := ps.healthCache[slow.DataHubURL]
	ps.healthMu.Unlock()

	require.True(t, fastCached)
	require.True(t, slowCached)
	gap := slowEntry.checkedAt.Sub(fastEntry.checkedAt)
	require.GreaterOrEqual(t, gap, slowDelay/2,
		"the fast probe's entry must be stamped at its own completion, not at the end of the round")
}

func TestPeerSelector_HealthChecks_ConcurrentSelectionsAreSafe(t *testing.T) {
	ps := newHealthCheckSelectorForTest()

	peers := make([]*blockchain.PeerInfo, 0, 8)
	for i := range 8 {
		peers = append(peers, newPeer(fmt.Sprintf("peer-%d", i), 100, "full", 50, 0))
	}

	ps.checkHealth = func(ctx context.Context, _ string) (bool, error) {
		select {
		case <-time.After(10 * time.Millisecond):
		case <-ctx.Done():
		}
		return true, nil
	}

	// The godoc permits concurrent SelectSyncPeer calls (double-probing at
	// worst); drive several rounds from several goroutines so healthMu and the
	// cache write-back are exercised across rounds under the race detector.
	var emptySelections atomic.Int32
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 5 {
				if ps.SelectSyncPeer(context.Background(), peers, advertisedProbeCriteria(50)) == "" {
					emptySelections.Add(1)
				}
			}
		})
	}
	wg.Wait()

	require.Zero(t, emptySelections.Load(), "every concurrent selection must find a healthy peer")
}

func TestPeerSelector_HealthChecks_CacheExpires(t *testing.T) {
	ps := newHealthCheckSelectorForTest()
	peerA := newPeer("peer-a", 100, "full", 50, 0)
	peers := []*blockchain.PeerInfo{peerA}

	stub := newCountingHealthStub()
	ps.checkHealth = stub.check

	ps.SelectSyncPeer(context.Background(), peers, advertisedProbeCriteria(50))
	require.Equal(t, 1, stub.totalCalls())

	// Age the cached entry past the TTL; the next selection must re-probe.
	ps.healthMu.Lock()
	entry := ps.healthCache[peerA.DataHubURL]
	entry.checkedAt = entry.checkedAt.Add(-peerHealthCacheTTL)
	ps.healthCache[peerA.DataHubURL] = entry
	ps.healthMu.Unlock()

	ps.SelectSyncPeer(context.Background(), peers, advertisedProbeCriteria(50))
	require.Equal(t, 2, stub.totalCalls())
}

func TestPeerSelector_HealthChecks_RunConcurrently(t *testing.T) {
	ps := newHealthCheckSelectorForTest()

	const numPeers = 3 * peerHealthCheckConcurrency
	const probeDelay = 100 * time.Millisecond

	peers := make([]*blockchain.PeerInfo, 0, numPeers)
	for i := range numPeers {
		peers = append(peers, newPeer(fmt.Sprintf("peer-%d", i), 100, "full", 50, 0))
	}

	var inFlight, maxInFlight atomic.Int32
	ps.checkHealth = func(ctx context.Context, _ string) (bool, error) {
		cur := inFlight.Add(1)
		defer inFlight.Add(-1)
		for {
			prev := maxInFlight.Load()
			if cur <= prev || maxInFlight.CompareAndSwap(prev, cur) {
				break
			}
		}
		select {
		case <-time.After(probeDelay):
		case <-ctx.Done():
		}
		return true, nil
	}

	start := time.Now()
	got := ps.SelectSyncPeer(context.Background(), peers, advertisedProbeCriteria(50))
	elapsed := time.Since(start)

	require.NotEqual(t, "", got)
	require.Greater(t, maxInFlight.Load(), int32(1), "probes must overlap, not run serially")
	require.LessOrEqual(t, maxInFlight.Load(), int32(peerHealthCheckConcurrency), "in-flight probes must respect the concurrency bound")
	require.Less(t, elapsed, time.Duration(numPeers)*probeDelay, "selection must be faster than serial probing")
}

func TestPeerSelector_HealthChecks_FullNodeNotStarvedBySlowPrunedPeers(t *testing.T) {
	ps := newHealthCheckSelectorForTest()
	ps.healthCheckBudget = 200 * time.Millisecond

	// Enough slow pruned peers to keep every worker busy for the whole budget,
	// all ordered ahead of the single healthy full node in the peer slice.
	// Full-node-tier URLs must be probed first, so the full node still gets a
	// worker while the budget is alive and wins Phase 1.
	peers := make([]*blockchain.PeerInfo, 0, 2*peerHealthCheckConcurrency+1)
	for i := range 2 * peerHealthCheckConcurrency {
		peers = append(peers, newPeer(fmt.Sprintf("peer-pruned-%d", i), 100, "pruned", 50, 0))
	}
	full := newPeer("peer-full", 100, "full", 50, 0)
	peers = append(peers, full)

	ps.checkHealth = func(ctx context.Context, url string) (bool, error) {
		// Like the real probe, fail immediately once the budget has expired.
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if url == full.DataHubURL {
			return true, nil
		}
		<-ctx.Done()
		return false, ctx.Err()
	}

	got := ps.SelectSyncPeer(context.Background(), peers, advertisedProbeCriteria(50))
	require.Equal(t, "peer-full", got, "a healthy full node must not be starved of its probe by slow pruned peers")
}

func TestPeerSelector_HealthChecks_HonorCallerContext(t *testing.T) {
	ps := newHealthCheckSelectorForTest()
	peerA := newPeer("peer-a", 100, "full", 50, 0)

	ps.checkHealth = func(ctx context.Context, _ string) (bool, error) {
		<-ctx.Done()
		return false, ctx.Err()
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	got := ps.SelectSyncPeer(ctx, []*blockchain.PeerInfo{peerA}, advertisedProbeCriteria(50))
	elapsed := time.Since(start)

	require.Equal(t, "", got)
	require.Less(t, elapsed, time.Second, "a cancelled context must abort probes immediately")
}

func TestPeerSelector_HealthChecks_BudgetExpiryNotCached(t *testing.T) {
	ps := newHealthCheckSelectorForTest()
	ps.healthCheckBudget = 50 * time.Millisecond
	peerA := newPeer("peer-a", 100, "full", 50, 0)
	peers := []*blockchain.PeerInfo{peerA}

	// The probe outlives the overall budget; the derived probe context must
	// cut it short and the peer must count as unhealthy for this selection.
	ps.checkHealth = func(ctx context.Context, _ string) (bool, error) {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(10 * time.Second):
			return true, nil
		}
	}

	start := time.Now()
	got := ps.SelectSyncPeer(context.Background(), peers, advertisedProbeCriteria(50))
	require.Equal(t, "", got)
	require.Less(t, time.Since(start), 5*time.Second, "budget expiry must bound the probe phase")

	// The budget-aborted probe must not have been cached as unhealthy.
	ps.healthCheckBudget = peerHealthCheckBudget
	stub := newCountingHealthStub()
	ps.checkHealth = stub.check

	got = ps.SelectSyncPeer(context.Background(), peers, advertisedProbeCriteria(50))
	require.Equal(t, "peer-a", got)
	require.Equal(t, 1, stub.totalCalls())
}

func TestPeerSelector_HealthChecks_RealHTTPEndpoints(t *testing.T) {
	// This is the only health test that exercises the real probe client rather than a
	// stubbed checkHealth. That client refuses loopback targets, since a peer-supplied URL
	// must never resolve internally, and httptest only ever listens on 127.0.0.1 - so the
	// guard is disabled here exactly as the test daemons do it.
	allowLoopbackProbes(t)

	healthyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/bestblockheader" {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer healthyServer.Close()

	unhealthyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer unhealthyServer.Close()

	ps := newHealthCheckSelectorForTest()
	healthyPeer := newPeer("peer-healthy", 100, "full", 50, 0)
	healthyPeer.DataHubURL = healthyServer.URL
	unhealthyPeer := newPeer("peer-unhealthy", 100, "full", 50, 0)
	unhealthyPeer.DataHubURL = unhealthyServer.URL

	got := ps.SelectSyncPeer(context.Background(), []*blockchain.PeerInfo{healthyPeer, unhealthyPeer}, advertisedProbeCriteria(50))
	require.Equal(t, "peer-healthy", got)
}
