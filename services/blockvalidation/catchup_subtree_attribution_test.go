package blockvalidation

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	p2pconstants "github.com/bsv-blockchain/teranode/interfaces/p2p"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/p2p"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/jarcoal/httpmock"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

// banScoreStrike is one AddBanScore call, with the reason, so a test can assert WHICH peer was
// struck and FOR WHAT — the reason is what maps to concrete penalty points in the registry.
type banScoreStrike struct {
	peerID string
	reason string
}

// subtreeAttributionP2PClient is a full P2PClientI stub that both serves a catchup peer list (so
// DistributeSubtreesAcrossPeers can hand a subtree to a non-primary peer) and records the strikes
// that land. AddBanScore and GetPeersForCatchup are on the SAME interface, so one instance can be
// assigned to both Server.p2pClient (which drives the distribution) and
// Server.blockValidation.p2pClient (which receives the strike) — wire both, or the strike silently
// no-ops on a nil client and the test passes for the wrong reason.
type subtreeAttributionP2PClient struct {
	peers []*p2p.PeerInfo

	mu sync.Mutex
	// strikes records AddBanScore. catchupFailures and maliciousReports exist so a test can assert
	// the ABSENCE of a reputation charge or a malicious report, which is not observable any other
	// way — see TestProcessCatchupChItem_PolicyDeclineIsTerminal.
	strikes          []banScoreStrike
	catchupFailures  map[string]int
	maliciousReports map[string]int
}

func (c *subtreeAttributionP2PClient) AddBanScore(_ context.Context, peerID string, reason string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.strikes = append(c.strikes, banScoreStrike{peerID: peerID, reason: reason})

	return nil
}

func (c *subtreeAttributionP2PClient) struck() []banScoreStrike {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]banScoreStrike, len(c.strikes))
	copy(out, c.strikes)

	return out
}

func (c *subtreeAttributionP2PClient) GetPeersForCatchup(_ context.Context) ([]*p2p.PeerInfo, error) {
	return c.peers, nil
}

func (c *subtreeAttributionP2PClient) charged(peerID string) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.catchupFailures[peerID]
}

func (c *subtreeAttributionP2PClient) totalCharges() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	total := 0
	for _, n := range c.catchupFailures {
		total += n
	}

	return total
}

func (c *subtreeAttributionP2PClient) totalMaliciousReports() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	total := 0
	for _, n := range c.maliciousReports {
		total += n
	}

	return total
}

// The rest of P2PClientI is unused by these tests, but must exist: a nil embedded interface would
// panic on the reputation bookkeeping the fetch path does around a failure, "failing" the test for
// a reason that says nothing about attribution.
func (c *subtreeAttributionP2PClient) RecordCatchupAttempt(_ context.Context, _ string) error {
	return nil
}
func (c *subtreeAttributionP2PClient) RecordCatchupSuccess(_ context.Context, _ string, _ int64) error {
	return nil
}
func (c *subtreeAttributionP2PClient) RecordCatchupFailure(_ context.Context, peerID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.catchupFailures == nil {
		c.catchupFailures = make(map[string]int)
	}
	c.catchupFailures[peerID]++

	return nil
}
func (c *subtreeAttributionP2PClient) RecordCatchupFailureWithKind(_ context.Context, peerID, _, _ string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.catchupFailures == nil {
		c.catchupFailures = make(map[string]int)
	}
	c.catchupFailures[peerID]++

	return nil
}
func (c *subtreeAttributionP2PClient) RecordCatchupMalicious(_ context.Context, peerID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.maliciousReports == nil {
		c.maliciousReports = make(map[string]int)
	}
	c.maliciousReports[peerID]++

	return nil
}
func (c *subtreeAttributionP2PClient) UpdateCatchupError(_ context.Context, _ string, _ string) error {
	return nil
}
func (c *subtreeAttributionP2PClient) GetPeer(_ context.Context, _ string) (*p2p.PeerInfo, error) {
	return nil, nil
}
func (c *subtreeAttributionP2PClient) ReportValidBlock(_ context.Context, _ string, _ string) error {
	return nil
}
func (c *subtreeAttributionP2PClient) ReportValidBlockHeaders(_ context.Context, _ string, _ int64) error {
	return nil
}
func (c *subtreeAttributionP2PClient) ReportValidSubtree(_ context.Context, _ string, _ string) error {
	return nil
}
func (c *subtreeAttributionP2PClient) ReportValidatedChainProgress(_ context.Context, _ string, _ uint32, _ string, _ []byte) error {
	return nil
}
func (c *subtreeAttributionP2PClient) IsPeerMalicious(_ context.Context, _ string) (bool, string, error) {
	return false, "", nil
}
func (c *subtreeAttributionP2PClient) IsPeerUnhealthy(_ context.Context, _ string) (bool, string, float32, error) {
	return false, "", 0, nil
}
func (c *subtreeAttributionP2PClient) RecordBytesDownloaded(_ context.Context, _ string, _ uint64) error {
	return nil
}

// TestFetchSubtreeDataForBlock_StrikesTheServingPeerNotThePrimary drives the parallel-fetch
// distribution end to end and pins the attribution fix (bitcoin-sv/teranode#4692).
//
// Before the fix, fetchAndStoreSubtree stored a peer's subtree node bytes under the hash they were
// REQUESTED under without checking they hash to it. Under parallel fetch a non-primary peer can be
// assigned a subtree, so its doctored bytes landed on disk under an honest filename,
// findLocalSubtreeFile short-circuited to them on retry, and the fault only surfaced later as a
// whole-body merkle mismatch — charged to the catch-up PRIMARY, which had nothing to do with it.
// The check now runs where the bytes arrive, which is the one place attribution is unambiguous.
//
// Fixture: a block with two subtrees. DistributeSubtreesAcrossPeers always puts the primary at
// peers[0] and assigns round-robin by index, so with two peers subtree 0 -> A (primary) and
// subtree 1 -> B. That index mapping is what makes the assertion deterministic, so it is asserted
// (via each endpoint's request count) rather than assumed.
//
// B is only returned to the distribution if the fixture satisfies every GetPeersAtMaxHeight filter.
// Two of them a zero-value p2p.PeerInfo silently fails: DataHubURL must be non-empty, and
// ReputationScore must be >= 20 (it defaults to 0). Without those, the distribution collapses to
// the primary alone, both subtrees come from A, and the test passes green while proving nothing —
// hence the explicit values below, set well clear of the threshold.
//
// Mutation proof: delete the root check in fetchAndStoreSubtree and B's bytes are stored, no strike
// lands anywhere, and the misattribution reappears later as a whole-body merkle mismatch charged to
// A — the reported path, now red.
func TestFetchSubtreeDataForBlock_StrikesTheServingPeerNotThePrimary(t *testing.T) {
	suite := NewCatchupTestSuite(t)
	defer suite.Cleanup()

	const (
		primaryPeerID  = "12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ"
		primaryBaseURL = "http://peer-a:8000"
		servingBaseURL = "http://peer-b:8000"
	)

	servingPeerID := peer.ID("peer-b-serving-the-doctored-bytes").String()
	require.NotEqual(t, primaryPeerID, servingPeerID)

	// CreateTestTransactionChainWithCount returns count-1 transactions (a coinbase plus count-2
	// chained spends), so 6 yields txs[0..4]: four for subtree 0 and one more for subtree 1.
	txs := transactions.CreateTestTransactionChainWithCount(t, 6)

	// Subtree 0: honest, served by A. Built the way fetchAndStoreSubtree builds it from node bytes,
	// so the root it computes matches the hash the block names.
	subtreeA, err := subtreepkg.NewIncompleteTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, subtreeA.AddCoinbaseNode())
	require.NoError(t, subtreeA.AddNode(*txs[1].TxIDChainHash(), 1, 11))
	require.NoError(t, subtreeA.AddNode(*txs[2].TxIDChainHash(), 2, 12))
	require.NoError(t, subtreeA.AddNode(*txs[3].TxIDChainHash(), 3, 13))
	hashA := subtreeA.RootHash()

	var nodeBytesA []byte
	nodeBytesA = append(nodeBytesA, subtreepkg.CoinbasePlaceholderHashValue[:]...)
	nodeBytesA = append(nodeBytesA, txs[1].TxIDChainHash()[:]...)
	nodeBytesA = append(nodeBytesA, txs[2].TxIDChainHash()[:]...)
	nodeBytesA = append(nodeBytesA, txs[3].TxIDChainHash()[:]...)

	subtreeDataA := subtreepkg.NewSubtreeData(subtreeA)
	require.NoError(t, subtreeDataA.AddTx(txs[0], 0))
	require.NoError(t, subtreeDataA.AddTx(txs[1], 1))
	require.NoError(t, subtreeDataA.AddTx(txs[2], 2))
	require.NoError(t, subtreeDataA.AddTx(txs[3], 3))
	subtreeDataBytesA, err := subtreeDataA.Serialize()
	require.NoError(t, err)

	// Subtree 1: a legitimate second subtree hash the primary named in its block message. B is
	// asked for it and answers with subtree 0's node bytes — a valid subtree, but not THIS one.
	subtreeB, err := subtreepkg.NewIncompleteTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, subtreeB.AddNode(*txs[4].TxIDChainHash(), 4, 14))
	require.NoError(t, subtreeB.AddNode(chainhash.HashH([]byte("subtree-b-second-node")), 5, 15))
	hashB := subtreeB.RootHash()
	require.False(t, hashA.IsEqual(hashB), "the two subtrees must be distinct for the fixture to mean anything")

	httpmock.ActivateNonDefault(util.HTTPClient())
	defer httpmock.DeactivateAndReset()

	subtreeURLA := fmt.Sprintf("%s/subtree/%s", primaryBaseURL, hashA.String())
	subtreeDataURLA := fmt.Sprintf("%s/subtree_data/%s", primaryBaseURL, hashA.String())
	subtreeURLB := fmt.Sprintf("%s/subtree/%s", servingBaseURL, hashB.String())

	httpmock.RegisterResponder("GET", subtreeURLA, httpmock.NewBytesResponder(200, nodeBytesA))
	httpmock.RegisterResponder("GET", subtreeDataURLA, httpmock.NewBytesResponder(200, subtreeDataBytesA))

	// B answers only once A's honest blob is observable in the store. The two fetches share an
	// errgroup context, so B's failure cancels A's: without this ordering, whether A's blob has
	// landed by the time the group returns is a race and the "honest half of a mixed fetch is
	// unaffected" assertion below would flake on a loaded box. A fixed sleep is not an ordering
	// primitive, so wait on the actual condition — bounded, so a wiring mistake fails loudly with
	// the flag below instead of hanging. require must not be called here: this runs on the fetch
	// goroutine, not the test goroutine.
	var bWaitTimedOut atomic.Bool

	httpmock.RegisterResponder("GET", subtreeURLB, func(*http.Request) (*http.Response, error) {
		deadline := time.Now().Add(20 * time.Second)

		for {
			landed, existsErr := suite.Server.subtreeStore.Exists(context.Background(), hashA[:], fileformat.FileTypeSubtreeToCheck)
			if existsErr == nil && landed {
				return httpmock.NewBytesResponse(200, nodeBytesA), nil
			}

			if time.Now().After(deadline) {
				bWaitTimedOut.Store(true)

				return nil, errors.NewError("fixture: timed out waiting for peer A's subtree blob to land")
			}

			time.Sleep(2 * time.Millisecond)
		}
	})

	fake := &subtreeAttributionP2PClient{
		peers: []*p2p.PeerInfo{
			{
				ID:              peer.ID("peer-b-serving-the-doctored-bytes"),
				DataHubURL:      servingBaseURL,
				Height:          1000,
				IsBanned:        false,
				ReputationScore: 50, // must be >= 20 or GetPeersAtMaxHeight drops B silently
			},
		},
	}

	suite.Server.settings.BlockValidation.CatchupParallelFetchEnabled = true
	suite.Server.p2pClient = fake
	suite.Server.blockValidation.p2pClient = fake

	block := &model.Block{
		Height:   100,
		Subtrees: []*chainhash.Hash{hashA, hashB},
	}

	contributing, freshlyWritten, fetchErr := suite.Server.fetchSubtreeDataForBlock(suite.Ctx, block, primaryPeerID, primaryBaseURL)

	require.False(t, bWaitTimedOut.Load(),
		"the fixture's ordering gate timed out: peer A's blob never landed, so nothing below is meaningful")

	require.Error(t, fetchErr, "a subtree whose bytes do not hash to the requested hash must fail the fetch")
	require.Contains(t, fetchErr.Error(), servingPeerID, "the error must name the peer that served the bytes")

	// fetchSubtreeDataForBlock discards both return maps on any error, so the honest half is
	// asserted against the store below rather than through them. The per-pair freshness bookkeeping
	// is pinned where it is observable: TestFetchAndStoreSubtree/RejectsMismatchedRoot.
	require.Nil(t, contributing)
	require.Nil(t, freshlyWritten)

	// The distribution really did split the two subtrees across the two peers. Asserted, not
	// assumed: if B were never returned by the peer filters, A would have served both and the
	// strike assertions below would be vacuous.
	counts := httpmock.GetCallCountInfo()
	require.Equal(t, 1, counts["GET "+subtreeURLA], "subtree 0 must be fetched from the primary exactly once")
	// Twice for B: the wrong-root rejection is cache-bypass retryable, so tryPeerForSubtree spends
	// exactly one cache-busted retry against the same peer before giving up
	// (bitcoin-sv/teranode#4692). Both requests hit the same responder — httpmock counts the
	// cache-busted URL under the same key — and the retry serves the same doctored bytes, so the
	// verdict and the single strike below are unchanged.
	require.Equal(t, 2, counts["GET "+subtreeURLB],
		"subtree 1 must be fetched from the non-primary peer twice: the initial attempt plus one cache-busted retry")

	// THE FIX: exactly one strike, and it lands on B, for serving a corrupt block body.
	strikes := fake.struck()
	require.Len(t, strikes, 1, "exactly one strike must land, got: %+v", strikes)
	require.Equal(t, servingPeerID, strikes[0].peerID, "the strike must land on the peer that served the bytes, not the catch-up primary")
	require.NotEqual(t, primaryPeerID, strikes[0].peerID)
	require.Equal(t, p2pconstants.ReasonCorruptBlockBody.String(), strikes[0].reason)

	// B's bytes never landed, so a retry cannot short-circuit to them via findLocalSubtreeFile.
	stored, err := suite.Server.subtreeStore.Exists(suite.Ctx, hashB[:], fileformat.FileTypeSubtreeToCheck)
	require.NoError(t, err)
	require.False(t, stored, "rejected bytes must not be stored under the hash they do not match")

	// The honest half of a mixed fetch is unaffected: A's blob landed, so one peer serving doctored
	// bytes does not cost the work the honest peers already did.
	storedA, err := suite.Server.subtreeStore.Exists(suite.Ctx, hashA[:], fileformat.FileTypeSubtreeToCheck)
	require.NoError(t, err)
	require.True(t, storedA, "the honest peer's subtree must still be stored")
}
