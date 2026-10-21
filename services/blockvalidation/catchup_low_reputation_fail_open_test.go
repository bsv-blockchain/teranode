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
	"github.com/stretchr/testify/require"
)

// lowReputationStubP2PClient answers the catchup health gate with a fixed
// verdict and counts GetPeersForCatchup, so a test can see how many times the
// alternative-peer walk ran.
type lowReputationStubP2PClient struct {
	subtreeAttributionP2PClient
	unhealthyReason string
	malicious       bool
	peerLookups     int
}

func (c *lowReputationStubP2PClient) IsPeerUnhealthy(_ context.Context, _ string) (bool, string, float32, bool, error) {
	return c.unhealthyReason != "", c.unhealthyReason, 20, false, nil
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

	for _, reason := range []string{"low reputation score: 20.00", "low success rate: 25.00%"} {
		t.Run(reason, func(t *testing.T) {
			stub := &lowReputationStubP2PClient{unhealthyReason: reason}
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
// a peer the registry could not assess.
func TestProcessCatchupChItem_RefusalOutsideHealthRecordIsNotFallenBackTo(t *testing.T) {
	initPrometheusMetrics()

	for name, stub := range map[string]*lowReputationStubP2PClient{
		"malicious":      {unhealthyReason: "low reputation score: 5.00", malicious: true},
		"invalid peer":   {unhealthyReason: "invalid peer ID"},
		"not assessable": {unhealthyReason: "unable to determine peer health"},
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

	stub := &lowReputationStubP2PClient{unhealthyReason: "low reputation score: 20.00"}
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
