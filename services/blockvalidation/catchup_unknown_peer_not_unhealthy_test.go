package blockvalidation

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jellydator/ttlcache/v3"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// unhealthyStubP2PClient is a full P2PClientI implementation (embeds subtreeAttributionP2PClient's
// no-op methods) whose IsPeerUnhealthy answer is fixed by the test, so isPeerBad's decision can be
// driven directly through the interface rather than by asserting on the p2p_api response struct.
type unhealthyStubP2PClient struct {
	subtreeAttributionP2PClient
	isUnhealthy bool
	reason      string
	unknown     bool
}

func (u *unhealthyStubP2PClient) IsPeerUnhealthy(_ context.Context, _ string) (bool, string, float32, bool, error) {
	return u.isUnhealthy, u.reason, 0, u.unknown, nil
}

// TestIsPeerBad_UnknownPeerFailsOpen pins the fix: a peer absent from the registry (unknown=true)
// must NOT be treated as bad, only a peer the registry actually reports unhealthy. Driven entirely
// through isPeerBad via P2PClientI, not by asserting on the IsPeerUnhealthyResponse struct.
func TestIsPeerBad_UnknownPeerFailsOpen(t *testing.T) {
	initPrometheusMetrics()

	t.Run("unknown peer: not bad, gate counter fires with reason unknown", func(t *testing.T) {
		stub := &unhealthyStubP2PClient{isUnhealthy: true, reason: "unknown peer", unknown: true}
		u := &Server{logger: ulogger.TestLogger{}, p2pClient: stub}

		before := testutil.ToFloat64(prometheusCatchupPeerHealthGate.WithLabelValues("unknown"))

		bad, reason := u.isPeerBad("some-peer")

		require.False(t, bad, "a peer merely unknown to the registry must fail open, not be refused")
		require.Equal(t, "", reason)
		require.Equal(t, before+1, testutil.ToFloat64(prometheusCatchupPeerHealthGate.WithLabelValues("unknown")), "the unknown path must be observable even though it does not refuse the peer")
	})

	t.Run("genuinely unhealthy peer (low reputation): still refused", func(t *testing.T) {
		stub := &unhealthyStubP2PClient{isUnhealthy: true, reason: "low reputation score: 12.34", unknown: false}
		u := &Server{logger: ulogger.TestLogger{}, p2pClient: stub}

		bad, reason := u.isPeerBad("some-peer")

		require.True(t, bad, "a peer the registry reports unhealthy must still be refused; the unknown-peer fix must not disable the real check")
		require.Equal(t, "low reputation score: 12.34", reason)
	})

	t.Run("healthy peer: not bad", func(t *testing.T) {
		stub := &unhealthyStubP2PClient{isUnhealthy: false, unknown: false}
		u := &Server{logger: ulogger.TestLogger{}, p2pClient: stub}

		bad, reason := u.isPeerBad("some-peer")

		require.False(t, bad)
		require.Equal(t, "", reason)
	})
}

// TestProcessCatchupChItem_UnknownPeerIsNotRefusedAsCatchupSource is the end-to-end regression test:
// before the fix, a peer absent from the registry was refused as a catchup source (IsUnhealthy=true,
// "unknown peer"), which skipped catchupFunc entirely and fell through to the alternative-peer walk.
// With the fix, catchupFunc must run for an unknown peer exactly like a healthy one.
func TestProcessCatchupChItem_UnknownPeerIsNotRefusedAsCatchupSource(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.CatchupMaxAttemptsPerBlock = 3
	tSettings.BlockValidation.MaxCorruptAttemptsPerBlock = 3

	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	realClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	// unknown=true, IsUnhealthy=true: exactly what the p2p server returns today for a peer absent
	// from the registry (services/p2p/handle_catchup_metrics.go's not-found branch).
	p2pStub := &unhealthyStubP2PClient{isUnhealthy: true, reason: "unknown peer", unknown: true}

	prev := chainhash.HashH([]byte("unknown-peer-not-refused-prev"))
	mr := chainhash.HashH([]byte("unknown-peer-not-refused-merkle"))
	block := &model.Block{Header: &model.BlockHeader{HashPrevBlock: &prev, HashMerkleRoot: &mr}}

	const unknownPeer = "12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ"

	catchupCalls := 0

	u := &Server{
		settings:            tSettings,
		logger:              ulogger.TestLogger{},
		blockchainClient:    realClient,
		p2pClient:           p2pStub,
		processBlockNotify:  ttlcache.New[chainhash.Hash, bool](),
		catchupAlternatives: ttlcache.New[chainhash.Hash, []processBlockCatchup](),
		blockCatchupAttempts: ttlcache.New[chainhash.Hash, int](
			ttlcache.WithTTL[chainhash.Hash, int](10*time.Minute),
			ttlcache.WithDisableTouchOnHit[chainhash.Hash, int](),
		),
		blockPolicyDeclineAttempts: ttlcache.New[blockAttemptKey, int](
			ttlcache.WithTTL[blockAttemptKey, int](10*time.Minute),
			ttlcache.WithDisableTouchOnHit[blockAttemptKey, int](),
		),
	}
	u.catchupFunc = func(_ context.Context, _ *model.Block, _, _ string) error {
		catchupCalls++
		return nil
	}

	u.processCatchupChItem(ctx, processBlockCatchup{block: block, peerID: unknownPeer, baseURL: "http://unknown-peer.example"})

	require.Equal(t, 1, catchupCalls, "catchup must run for a peer merely unknown to the registry, exactly like a healthy one")
}
