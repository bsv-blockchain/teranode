package blockvalidation

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/jellydator/ttlcache/v3"
	"github.com/stretchr/testify/require"
)

// countingMaliciousP2PClient counts IsPeerMalicious RPCs, answering from a
// fixed verdict map (default false) or with a fixed error.
type countingMaliciousP2PClient struct {
	maliciousAbortP2PClient

	rpcCalls  int
	malicious map[string]bool
	err       error
}

func (m *countingMaliciousP2PClient) IsPeerMalicious(_ context.Context, peerID string) (bool, string, error) {
	m.rpcCalls++

	if m.err != nil {
		return false, "", m.err
	}

	return m.malicious[peerID], "", nil
}

func newMaliciousCacheTestServer(client P2PClientI) *Server {
	return &Server{
		logger:    ulogger.TestLogger{},
		p2pClient: client,
		peerMaliciousCache: ttlcache.New[string, bool](
			ttlcache.WithTTL[string, bool](peerMaliciousCacheTTL),
			ttlcache.WithDisableTouchOnHit[string, bool](),
		),
	}
}

func TestIsPeerMalicious_CachesVerdictPerPeer(t *testing.T) {
	client := &countingMaliciousP2PClient{malicious: map[string]bool{"peer-bad": true}}
	u := newMaliciousCacheTestServer(client)

	ctx := context.Background()

	require.True(t, u.isPeerMalicious(ctx, "peer-bad"))
	require.True(t, u.isPeerMalicious(ctx, "peer-bad"))
	require.True(t, u.isPeerMalicious(ctx, "peer-bad"))
	require.Equal(t, 1, client.rpcCalls, "repeat checks within the TTL must be served from cache")

	require.False(t, u.isPeerMalicious(ctx, "peer-good"))
	require.False(t, u.isPeerMalicious(ctx, "peer-good"))
	require.Equal(t, 2, client.rpcCalls, "each distinct peer costs exactly one RPC per TTL window")
}

func TestIsPeerMalicious_CachesErrorFallback(t *testing.T) {
	client := &countingMaliciousP2PClient{err: errors.NewServiceError("p2p unavailable")}
	u := newMaliciousCacheTestServer(client)

	ctx := context.Background()

	require.False(t, u.isPeerMalicious(ctx, "peer-x"), "errors must fail open")
	require.False(t, u.isPeerMalicious(ctx, "peer-x"))
	require.Equal(t, 1, client.rpcCalls, "a degraded p2p service must be asked once per TTL, not per message")
}

func TestIsPeerMalicious_ExpiredVerdictIsRefetched(t *testing.T) {
	client := &countingMaliciousP2PClient{}
	u := &Server{
		logger:    ulogger.TestLogger{},
		p2pClient: client,
		peerMaliciousCache: ttlcache.New[string, bool](
			ttlcache.WithTTL[string, bool](time.Nanosecond),
			ttlcache.WithDisableTouchOnHit[string, bool](),
		),
	}

	ctx := context.Background()

	require.False(t, u.isPeerMalicious(ctx, "peer-x"))
	time.Sleep(time.Millisecond)

	client.malicious = map[string]bool{"peer-x": true}
	require.True(t, u.isPeerMalicious(ctx, "peer-x"), "an expired verdict must be refetched, so a fresh ban takes effect")
	require.Equal(t, 2, client.rpcCalls)
}

func TestIsPeerMalicious_SelfReportInvalidatesCachedVerdict(t *testing.T) {
	client := &countingMaliciousP2PClient{}
	u := newMaliciousCacheTestServer(client)

	ctx := context.Background()

	require.False(t, u.isPeerMalicious(ctx, "peer-x"))

	// The node itself reports the peer malicious mid-catchup; the cached
	// not-malicious verdict must not outlive that report.
	client.malicious = map[string]bool{"peer-x": true}
	u.reportCatchupMalicious(ctx, "peer-x", "served garbage")

	require.True(t, u.isPeerMalicious(ctx, "peer-x"), "a self-reported malicious peer must be refused immediately")
	require.Equal(t, 2, client.rpcCalls)
}

func TestIsPeerMalicious_NilCacheDegradesToUncached(t *testing.T) {
	client := &countingMaliciousP2PClient{}
	u := &Server{logger: ulogger.TestLogger{}, p2pClient: client}

	ctx := context.Background()

	require.False(t, u.isPeerMalicious(ctx, "peer-x"))
	require.False(t, u.isPeerMalicious(ctx, "peer-x"))
	require.Equal(t, 2, client.rpcCalls, "Server literals without the cache keep the old per-call behavior")

	require.False(t, u.isPeerMalicious(ctx, ""), "empty peer ID short-circuits")
	require.Equal(t, 2, client.rpcCalls)
}
