package blockvalidation

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/p2p"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jellydator/ttlcache/v3"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// lowReputationStubP2PClient answers the catchup health gate with a fixed
// verdict and counts GetPeersForCatchup, so a test can see how many times the
// alternative-peer walk ran. The reason text is free; the gate must decide on
// the cause alone.
type lowReputationStubP2PClient struct {
	subtreeAttributionP2PClient
	unhealthyCause  p2p.PeerHealthCause
	unhealthyReason string
	score           float32
	malicious       bool
	peerLookups     int
}

func (c *lowReputationStubP2PClient) IsPeerUnhealthy(_ context.Context, _ string) (p2p.PeerHealth, error) {
	return p2p.PeerHealth{
		Unhealthy:       c.unhealthyCause != p2p.PeerHealthCauseUnspecified || c.unhealthyReason != "",
		Reason:          c.unhealthyReason,
		Cause:           c.unhealthyCause,
		ReputationScore: c.score,
	}, nil
}

func (c *lowReputationStubP2PClient) IsPeerMalicious(_ context.Context, _ string) (bool, string, error) {
	return c.malicious, "", nil
}

func (c *lowReputationStubP2PClient) GetPeersForCatchup(_ context.Context) ([]*p2p.PeerInfo, error) {
	c.peerLookups++
	return nil, nil
}

func newLowReputationCatchupServer(t *testing.T, p2pClient P2PClientI) *Server {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.CatchupMaxAttemptsPerBlock = 3
	tSettings.BlockValidation.MaxCorruptAttemptsPerBlock = 3

	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	realClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	return &Server{
		settings:            tSettings,
		logger:              ulogger.TestLogger{},
		blockchainClient:    realClient,
		p2pClient:           p2pClient,
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
}

func lowReputationTestBlock(seed string) *model.Block {
	prev := chainhash.HashH([]byte(seed + "-prev"))
	mr := chainhash.HashH([]byte(seed + "-merkle"))

	return &model.Block{Header: &model.BlockHeader{HashPrevBlock: &prev, HashMerkleRoot: &mr}}
}

// TestProcessCatchupChItem_OnlyPeerAheadWithLowReputationStillCatchesUp is the
// regression test for a node that stopped advancing: its only peer ahead was
// refused as a catchup source for its health record alone, no alternative
// could serve the block, and the item was dropped without ever trying the one
// peer that had it. A peer refused only for its health record (not malicious,
// not banned) must be tried once no alternative succeeds.
func TestProcessCatchupChItem_OnlyPeerAheadWithLowReputationStillCatchesUp(t *testing.T) {
	initPrometheusMetrics()

	const primaryPeer = "12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ"

	for reason, cause := range map[string]p2p.PeerHealthCause{
		"low reputation score: 19.00": p2p.PeerHealthCauseLowReputation,
		"low success rate: 25.00%":    p2p.PeerHealthCauseLowSuccessRate,
	} {
		t.Run(reason, func(t *testing.T) {
			stub := &lowReputationStubP2PClient{unhealthyCause: cause, unhealthyReason: reason, score: 19}
			u := newLowReputationCatchupServer(t, stub)

			var catchupPeers []string
			u.catchupFunc = func(_ context.Context, _ *model.Block, peerID, _ string) error {
				catchupPeers = append(catchupPeers, peerID)
				return nil
			}

			u.processCatchupChItem(context.Background(), processBlockCatchup{block: lowReputationTestBlock(reason), peerID: primaryPeer, baseURL: "http://peer.example"})

			require.Equal(t, []string{primaryPeer}, catchupPeers, "the only peer ahead must be tried when it is refused only for its health record")
			require.Equal(t, 1, stub.peerLookups, "alternatives must be tried before falling back to the refused peer")
		})
	}
}

// TestProcessCatchupChItem_RefusalOutsideHealthRecordIsNotFallenBackTo keeps
// the fail-open narrow: a peer flagged malicious is still refused outright,
// even when it is also low on reputation and no alternative exists, and so is
// a peer the registry could not assess, and an unhealthy verdict with no cause
// (a p2p service that predates causes).
func TestProcessCatchupChItem_RefusalOutsideHealthRecordIsNotFallenBackTo(t *testing.T) {
	initPrometheusMetrics()

	for name, stub := range map[string]*lowReputationStubP2PClient{
		"malicious":      {unhealthyCause: p2p.PeerHealthCauseLowReputation, unhealthyReason: "low reputation score: 5.00", score: 5, malicious: true},
		"invalid peer":   {unhealthyCause: p2p.PeerHealthCauseNotAssessable, unhealthyReason: "invalid peer ID"},
		"not assessable": {unhealthyCause: p2p.PeerHealthCauseNotAssessable, unhealthyReason: "unable to determine peer health"},
		"no cause":       {unhealthyReason: "low reputation score: 19.00", score: 19},
	} {
		t.Run(name, func(t *testing.T) {
			u := newLowReputationCatchupServer(t, stub)

			catchupCalls := 0
			u.catchupFunc = func(_ context.Context, _ *model.Block, _, _ string) error {
				catchupCalls++
				return nil
			}

			u.processCatchupChItem(context.Background(), processBlockCatchup{block: lowReputationTestBlock(name), peerID: "12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ", baseURL: "http://peer.example"})

			require.Equal(t, 0, catchupCalls, "only a peer refused for its health record may be fallen back to")
		})
	}
}

// TestProcessCatchupChItem_FailedLowReputationFallbackIsBounded checks that a
// fallback that fails goes through the normal failure path: it counts toward
// CatchupMaxAttemptsPerBlock, so a peer that cannot serve the block is not
// retried in a tight loop, and the alternative walk the gate already ran is
// not repeated.
func TestProcessCatchupChItem_FailedLowReputationFallbackIsBounded(t *testing.T) {
	initPrometheusMetrics()

	stub := &lowReputationStubP2PClient{unhealthyCause: p2p.PeerHealthCauseLowReputation, unhealthyReason: "low reputation score: 19.00", score: 19}
	u := newLowReputationCatchupServer(t, stub)

	u.catchupFunc = func(_ context.Context, _ *model.Block, _, _ string) error {
		return errors.NewNetworkError("peer could not serve the block")
	}

	block := lowReputationTestBlock("bounded")
	u.processCatchupChItem(context.Background(), processBlockCatchup{block: block, peerID: "12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ", baseURL: "http://peer.example"})

	item := u.blockCatchupAttempts.Get(*block.Hash())
	require.NotNil(t, item, "a failed fallback must be counted toward the per-block attempt cap")
	require.Equal(t, 1, item.Value())
	require.Equal(t, 1, stub.peerLookups, "the alternative walk must not run twice in one cycle")
}

// TestProcessCatchupChItem_RegistryMaliciousVerdictBeatsStaleMaliciousCache is
// the stale-cache case: the registry's health verdict names the peer malicious
// or banned, but isPeerMalicious answers false, as it does for up to its cache
// TTL after a not-malicious answer was cached in the window before a malicious
// report landed, or after an IsPeerMalicious error was cached as false. The
// gate must refuse on the registry verdict and never fall back to the peer.
func TestProcessCatchupChItem_RegistryMaliciousVerdictBeatsStaleMaliciousCache(t *testing.T) {
	initPrometheusMetrics()

	const primaryPeer = "12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ"

	for name, cause := range map[string]p2p.PeerHealthCause{
		"malicious": p2p.PeerHealthCauseMalicious,
		"banned":    p2p.PeerHealthCauseBanned,
	} {
		t.Run(name, func(t *testing.T) {
			stub := &lowReputationStubP2PClient{unhealthyCause: cause, unhealthyReason: "low reputation score: 5.00", score: 5}
			u := newLowReputationCatchupServer(t, stub)
			u.peerMaliciousCache = ttlcache.New[string, bool](ttlcache.WithTTL[string, bool](time.Minute))
			u.peerMaliciousCache.Set(primaryPeer, false, ttlcache.DefaultTTL)

			catchupCalls := 0
			u.catchupFunc = func(_ context.Context, _ *model.Block, _, _ string) error {
				catchupCalls++
				return nil
			}

			block := lowReputationTestBlock("stale-cache-" + name)
			u.processBlockNotify.Set(*block.Hash(), true, ttlcache.DefaultTTL)

			before := testutil.ToFloat64(prometheusCatchupPeerHealthGate.WithLabelValues("malicious"))

			u.processCatchupChItem(context.Background(), processBlockCatchup{block: block, peerID: primaryPeer, baseURL: "http://peer.example"})

			require.Equal(t, 0, catchupCalls, "a peer the registry names %s must never be fallen back to, whatever the malicious cache says", name)
			require.Nil(t, u.processBlockNotify.Get(*block.Hash()), "the refusal must clear the marker so another peer's announcement can retry the block")
			require.Equal(t, before+1, testutil.ToFloat64(prometheusCatchupPeerHealthGate.WithLabelValues("malicious")), "the refusal must be counted as malicious, not low_reputation")
		})
	}
}

// TestProcessCatchupChItem_FallbackDecisionIgnoresReasonText pins that the
// fallback is decided on the typed cause and not on the reason text: rewording
// the text the p2p service builds must neither switch the fallback off for a
// low-reputation peer nor switch it on for a verdict without that cause.
func TestProcessCatchupChItem_FallbackDecisionIgnoresReasonText(t *testing.T) {
	initPrometheusMetrics()

	const primaryPeer = "12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ"

	cases := map[string]struct {
		stub         *lowReputationStubP2PClient
		wantFallback bool
	}{
		"low reputation cause, reworded text": {
			stub:         &lowReputationStubP2PClient{unhealthyCause: p2p.PeerHealthCauseLowReputation, unhealthyReason: "reputation 19.0 is under the floor", score: 19},
			wantFallback: true,
		},
		"low success rate cause, no text": {
			stub:         &lowReputationStubP2PClient{unhealthyCause: p2p.PeerHealthCauseLowSuccessRate, score: 30},
			wantFallback: true,
		},
		"other cause, low reputation text": {
			stub:         &lowReputationStubP2PClient{unhealthyCause: p2p.PeerHealthCauseNotAssessable, unhealthyReason: "low reputation score: 19.00", score: 19},
			wantFallback: false,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			u := newLowReputationCatchupServer(t, tc.stub)

			catchupCalls := 0
			u.catchupFunc = func(_ context.Context, _ *model.Block, _, _ string) error {
				catchupCalls++
				return nil
			}

			u.processCatchupChItem(context.Background(), processBlockCatchup{block: lowReputationTestBlock(name), peerID: primaryPeer, baseURL: "http://peer.example"})

			wantCalls := 0
			if tc.wantFallback {
				wantCalls = 1
			}
			require.Equal(t, wantCalls, catchupCalls)
		})
	}
}
