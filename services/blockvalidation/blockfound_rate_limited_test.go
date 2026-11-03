package blockvalidation

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/bsv-blockchain/teranode/services/p2p"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jellydator/ttlcache/v3"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

// catchupErrorRecordingP2PClient adds an UpdateCatchupError count to subtreeAttributionP2PClient,
// whose charged(peerID) already counts RecordCatchupFailureWithKind (where reportCatchupFailure
// routes). Together they observe both halves of the blockFound fetch-failure charge.
type catchupErrorRecordingP2PClient struct {
	subtreeAttributionP2PClient
	catchupErrors map[string][]string
}

func (c *catchupErrorRecordingP2PClient) UpdateCatchupError(_ context.Context, peerID string, errorMsg string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.catchupErrors == nil {
		c.catchupErrors = make(map[string][]string)
	}
	c.catchupErrors[peerID] = append(c.catchupErrors[peerID], errorMsg)

	return nil
}

func (c *catchupErrorRecordingP2PClient) recordedErrors(peerID string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]string(nil), c.catchupErrors[peerID]...)
}

// countingPeer is an httptest peer that counts the GETs it served. Only GETs are counted: a local
// process may probe loopback test servers with HEAD /.
type countingPeer struct {
	url  string
	gets atomic.Int64
}

func newCountingPeer(t *testing.T, handler http.HandlerFunc) *countingPeer {
	t.Helper()

	p := &countingPeer{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			p.gets.Add(1)
		}
		handler(w, r)
	}))
	t.Cleanup(srv.Close)
	p.url = srv.URL

	return p
}

func newStatusPeer(t *testing.T, status int) *countingPeer {
	return newCountingPeer(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(status) })
}

// newBlockFoundFetchServer builds a Server with just enough wired for the blockFound fetch paths: a
// sqlitememory chain that has none of the announced blocks, the per-peer 429 allowance cache, and a
// recording p2p client.
func newBlockFoundFetchServer(t *testing.T) (*Server, *catchupErrorRecordingP2PClient) {
	t.Helper()

	initPrometheusMetrics()

	origSSRF := util.SSRFProtectionEnabled()
	util.SetSSRFProtection(false)
	t.Cleanup(func() { util.SetSSRFProtection(origSSRF) })

	tSettings := test.CreateBaseTestSettings(t)

	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	bv := &BlockValidation{
		logger:           ulogger.TestLogger{},
		settings:         tSettings,
		blockchainClient: blockchainClient,
		blockExistsCache: expiringmap.New[chainhash.Hash, bool](time.Minute),
	}
	t.Cleanup(bv.blockExistsCache.Stop)

	p2pClient := &catchupErrorRecordingP2PClient{}

	u := &Server{
		logger:                   ulogger.TestLogger{},
		settings:                 tSettings,
		blockValidation:          bv,
		p2pClient:                p2pClient,
		stats:                    gocore.NewStat("test"),
		announcedBlockRateLimits: newAnnouncedBlockRateLimitCache(),
		blockPriorityQueue:       NewBlockPriorityQueue(ulogger.TestLogger{}),
		forkManager:              NewForkManager(ulogger.TestLogger{}, tSettings),
		blockFoundCh:             make(chan processBlockFound, 10),
	}

	return u, p2pClient
}

// announce runs addBlockToPriorityQueue for hash announced by "announcer" at baseURL and returns
// the error delivered on errCh.
func announce(t *testing.T, u *Server, hash *chainhash.Hash, baseURL string) error {
	t.Helper()

	errCh := make(chan error, 1)

	u.addBlockToPriorityQueue(context.Background(), processBlockFound{
		hash:    hash,
		baseURL: baseURL,
		peerID:  "announcer",
		errCh:   errCh,
	})

	select {
	case err := <-errCh:
		return err
	default:
		t.Fatal("addBlockToPriorityQueue returned without delivering a result on errCh")
		return nil
	}
}

func testHash(seed string) *chainhash.Hash {
	h := chainhash.HashH([]byte(seed))
	return &h
}

// TestAddBlockToPriorityQueue_RateLimitedFetchDoesNotChargeAnnouncer: the peer's /block/:hash
// shares its per-IP rate-limit bucket with the catch-up routes this node's own fan-out drains, so an
// occasional 429 on the blockFound fetch is our collision and must not cost the announcer reputation.
func TestAddBlockToPriorityQueue_RateLimitedFetchDoesNotChargeAnnouncer(t *testing.T) {
	u, p2pClient := newBlockFoundFetchServer(t)
	announcer := newStatusPeer(t, http.StatusTooManyRequests)

	fetchErr := announce(t, u, testHash("blockfound-rate-limited"), announcer.url)

	require.ErrorIs(t, fetchErr, errors.ErrServiceRateLimited, "the 429 must reach errCh as ErrServiceRateLimited through fetchSingleBlock's wrapping")
	require.Equal(t, int64(1), announcer.gets.Load(), "the blockFound fetch must not retry")
	require.Equal(t, 0, p2pClient.charged("announcer"), "a 429 within the allowance must not record a catchup failure against the announcer")
	require.Empty(t, p2pClient.recordedErrors("announcer"), "a 429 within the allowance must not set a catchup error on the announcer")
}

// TestAddBlockToPriorityQueue_RateLimitedFetchChargesAnnouncerPastAllowance: the announcer chooses
// the baseURL, so a 429 alone proves nothing about our fan-out. Past the per-peer allowance a 429
// is charged like any other failure, so a peer cannot answer 429 on purpose to announce for free.
func TestAddBlockToPriorityQueue_RateLimitedFetchChargesAnnouncerPastAllowance(t *testing.T) {
	u, p2pClient := newBlockFoundFetchServer(t)
	announcer := newStatusPeer(t, http.StatusTooManyRequests)

	for i := 0; i < announcedBlockRateLimitAllowance; i++ {
		require.ErrorIs(t, announce(t, u, testHash(fmt.Sprintf("free-%d", i)), announcer.url), errors.ErrServiceRateLimited)
	}

	require.Equal(t, 0, p2pClient.charged("announcer"), "429s within the allowance must not be charged")

	fetchErr := announce(t, u, testHash("over-allowance"), announcer.url)

	require.ErrorIs(t, fetchErr, errors.ErrServiceRateLimited)
	require.Equal(t, 1, p2pClient.charged("announcer"), "the first 429 past the allowance must be charged once")
	require.Equal(t, []string{fetchErr.Error()}, p2pClient.recordedErrors("announcer"))
}

// TestAddBlockToPriorityQueue_RateLimitAllowanceIsPerPeer: one peer spending its allowance must not
// spend another's.
func TestAddBlockToPriorityQueue_RateLimitAllowanceIsPerPeer(t *testing.T) {
	u, _ := newBlockFoundFetchServer(t)

	for i := 0; i < announcedBlockRateLimitAllowance; i++ {
		require.False(t, u.spendAnnouncedBlockRateLimitAllowance("noisy"))
	}

	require.True(t, u.spendAnnouncedBlockRateLimitAllowance("noisy"), "noisy has spent its allowance")
	require.False(t, u.spendAnnouncedBlockRateLimitAllowance("quiet"), "quiet has its own allowance")
}

// TestAddBlockToPriorityQueue_NonRateLimitedFetchErrorChargesAnnouncer pins that the 429 exemption is
// narrow: any other failed /block/:hash response, such as a misconfigured asset server answering 401
// or a 500, still charges the announcer exactly once, with the fetch error as the recorded reason.
func TestAddBlockToPriorityQueue_NonRateLimitedFetchErrorChargesAnnouncer(t *testing.T) {
	for _, status := range []int{http.StatusInternalServerError, http.StatusUnauthorized} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			u, p2pClient := newBlockFoundFetchServer(t)
			announcer := newStatusPeer(t, status)

			fetchErr := announce(t, u, testHash("non-rate-limited"), announcer.url)

			require.Error(t, fetchErr)
			require.NotErrorIs(t, fetchErr, errors.ErrServiceRateLimited)
			require.Equal(t, int64(1), announcer.gets.Load(), "the blockFound fetch must not retry")
			require.Equal(t, 1, p2pClient.charged("announcer"), "a non-429 fetch failure must still record one catchup failure")
			require.Equal(t, []string{fetchErr.Error()}, p2pClient.recordedErrors("announcer"), "a non-429 fetch failure must still set one catchup error")
		})
	}
}

// TestAddBlockToPriorityQueue_TransportErrorChargesAnnouncer: a baseURL nothing listens on is the
// announcer's own advertised endpoint, so the failure is charged.
func TestAddBlockToPriorityQueue_TransportErrorChargesAnnouncer(t *testing.T) {
	u, p2pClient := newBlockFoundFetchServer(t)

	srv := httptest.NewServer(http.NotFoundHandler())
	deadURL := srv.URL
	srv.Close()

	fetchErr := announce(t, u, testHash("transport-error"), deadURL)

	require.Error(t, fetchErr)
	require.Equal(t, 1, p2pClient.charged("announcer"))
	require.Equal(t, []string{fetchErr.Error()}, p2pClient.recordedErrors("announcer"))
}

// TestFetchAnnouncedBlock_DeadlineChargesAnnouncer: the fetch budget is detached from any caller
// context, so a deadline means the announcer's endpoint used the whole budget. It is charged:
// exempting it would let a peer that stalls every fetch hold a worker without cost.
func TestFetchAnnouncedBlock_DeadlineChargesAnnouncer(t *testing.T) {
	u, p2pClient := newBlockFoundFetchServer(t)

	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	announcer := newCountingPeer(t, func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	})

	fetchCtx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_, _, err := u.fetchAnnouncedBlock(context.Background(), fetchCtx, processBlockFound{
		hash:    testHash("stalled"),
		baseURL: announcer.url,
		peerID:  "announcer",
	})

	require.Error(t, err)
	require.Equal(t, 1, p2pClient.charged("announcer"), "a fetch that runs out the budget must be charged")
	require.Len(t, p2pClient.recordedErrors("announcer"), 1)
}

// tipPeer is a p2p registry entry advertising tip as its best block, served at dataHubURL.
func tipPeer(id string, dataHubURL string, tip *chainhash.Hash) *p2p.PeerInfo {
	return &p2p.PeerInfo{
		ID:              peer.ID(id),
		DataHubURL:      dataHubURL,
		Height:          1000,
		BlockHash:       tip,
		ReputationScore: 50,
	}
}

// TestFetchAnnouncedBlock_RateLimitedFallsBackToTipMatchingPeer: a 429 from the announcer must not
// drop the block when another peer advertises the same hash as its tip. The block is fetched there
// and the source handed on is the peer that served it.
func TestFetchAnnouncedBlock_RateLimitedFallsBackToTipMatchingPeer(t *testing.T) {
	u, p2pClient := newBlockFoundFetchServer(t)

	blocks := testhelpers.CreateTestBlockChain(t, 2)
	block := blocks[1]
	blockBytes, err := block.Bytes()
	require.NoError(t, err)

	announcer := newStatusPeer(t, http.StatusTooManyRequests)
	alt := newCountingPeer(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(blockBytes) })
	stale := newStatusPeer(t, http.StatusNotFound)

	p2pClient.peers = []*p2p.PeerInfo{
		tipPeer("announcer", announcer.url, block.Hash()),
		tipPeer("stale-tip", stale.url, testHash("some-other-tip")),
		tipPeer("alt", alt.url, block.Hash()),
	}

	fetched, source, err := u.fetchAnnouncedBlock(context.Background(), context.Background(), processBlockFound{
		hash:    block.Hash(),
		baseURL: announcer.url,
		peerID:  peer.ID("announcer").String(),
	})

	require.NoError(t, err)
	require.Equal(t, block.Hash(), fetched.Hash())
	require.Equal(t, peer.ID("alt").String(), source.peerID, "the block must be attributed to the peer that served it")
	require.Equal(t, alt.url, source.baseURL, "later subtree fetches must go to the peer that served the block")
	require.Equal(t, int64(1), announcer.gets.Load(), "the announcer must not be asked twice")
	require.Equal(t, int64(0), stale.gets.Load(), "a peer whose tip is a different block must not be asked")
	require.Equal(t, int64(1), alt.gets.Load())
	require.Equal(t, 0, p2pClient.totalCharges(), "neither the rate-limited announcer within its allowance nor the alternative may be charged")
}

// TestFetchAnnouncedBlock_NoFallbackWithoutTipMatch: no peer advertises the hash, so nothing is
// asked. This is what stops a flood of made-up hashes from being amplified onto honest peers.
func TestFetchAnnouncedBlock_NoFallbackWithoutTipMatch(t *testing.T) {
	u, p2pClient := newBlockFoundFetchServer(t)

	announcer := newStatusPeer(t, http.StatusTooManyRequests)
	other := newStatusPeer(t, http.StatusNotFound)

	p2pClient.peers = []*p2p.PeerInfo{tipPeer("other", other.url, testHash("other-tip"))}

	_, _, err := u.fetchAnnouncedBlock(context.Background(), context.Background(), processBlockFound{
		hash:    testHash("made-up"),
		baseURL: announcer.url,
		peerID:  "announcer",
	})

	require.ErrorIs(t, err, errors.ErrServiceRateLimited, "with no alternative the announcer's 429 is the result")
	require.Equal(t, int64(0), other.gets.Load())
}

// TestFetchAnnouncedBlock_NoFallbackPastAllowance: once an announcer is past its 429 allowance it is
// treated as abusing the exemption, so its 429s cost this node no peer lookup or alternative fetch.
func TestFetchAnnouncedBlock_NoFallbackPastAllowance(t *testing.T) {
	u, p2pClient := newBlockFoundFetchServer(t)

	hash := testHash("past-allowance")
	announcer := newStatusPeer(t, http.StatusTooManyRequests)
	alt := newStatusPeer(t, http.StatusNotFound)

	p2pClient.peers = []*p2p.PeerInfo{tipPeer("alt", alt.url, hash)}

	for i := 0; i < announcedBlockRateLimitAllowance; i++ {
		require.False(t, u.spendAnnouncedBlockRateLimitAllowance("announcer"))
	}

	_, _, err := u.fetchAnnouncedBlock(context.Background(), context.Background(), processBlockFound{
		hash:    hash,
		baseURL: announcer.url,
		peerID:  "announcer",
	})

	require.ErrorIs(t, err, errors.ErrServiceRateLimited)
	require.Equal(t, 1, p2pClient.charged("announcer"))
	require.Equal(t, int64(0), alt.gets.Load(), "a charged 429 must not trigger the alternative fetch")
}

// TestFetchAnnouncedBlock_NonRateLimitedDoesNotFallBack: only a 429 says the announcer may have the
// block but is refusing us for now; any other failure is the announcer's, and is not retried
// elsewhere.
func TestFetchAnnouncedBlock_NonRateLimitedDoesNotFallBack(t *testing.T) {
	u, p2pClient := newBlockFoundFetchServer(t)

	hash := testHash("server-error")
	announcer := newStatusPeer(t, http.StatusInternalServerError)
	alt := newStatusPeer(t, http.StatusNotFound)

	p2pClient.peers = []*p2p.PeerInfo{tipPeer("alt", alt.url, hash)}

	_, _, err := u.fetchAnnouncedBlock(context.Background(), context.Background(), processBlockFound{
		hash:    hash,
		baseURL: announcer.url,
		peerID:  "announcer",
	})

	require.Error(t, err)
	require.Equal(t, int64(0), alt.gets.Load())
}

// TestFetchAnnouncedBlock_AlternativeFailureIsNotCharged: this node chose the alternative, not the
// other way around, so its failure to serve a block it did not announce to us is not charged.
func TestFetchAnnouncedBlock_AlternativeFailureIsNotCharged(t *testing.T) {
	u, p2pClient := newBlockFoundFetchServer(t)

	hash := testHash("alt-fails")
	announcer := newStatusPeer(t, http.StatusTooManyRequests)
	alt := newStatusPeer(t, http.StatusInternalServerError)

	p2pClient.peers = []*p2p.PeerInfo{tipPeer("alt", alt.url, hash)}

	_, _, err := u.fetchAnnouncedBlock(context.Background(), context.Background(), processBlockFound{
		hash:    hash,
		baseURL: announcer.url,
		peerID:  "announcer",
	})

	require.ErrorIs(t, err, errors.ErrServiceRateLimited, "the announcer's error is the one returned")
	require.Equal(t, int64(1), alt.gets.Load())
	require.Equal(t, 0, p2pClient.charged(peer.ID("alt").String()))
}

// TestProcessBlockFoundChannel_CatchupBranchFetchFailureChargesAnnouncer: the backed-up-queue branch
// fetches the same /block/:hash, so it charges by the same rule. Before, it charged nothing, so
// whether a peer paid for a broken endpoint depended on this node's queue depth.
func TestProcessBlockFoundChannel_CatchupBranchFetchFailureChargesAnnouncer(t *testing.T) {
	u, p2pClient := newBlockFoundFetchServer(t)
	u.settings.BlockValidation.UseCatchupWhenBehind = true

	// len(blockFoundCh) > 3 selects the shouldConsiderCatchup branch.
	for i := 0; i < 4; i++ {
		u.blockFoundCh <- processBlockFound{}
	}

	announcer := newStatusPeer(t, http.StatusInternalServerError)
	errCh := make(chan error, 1)

	err := u.processBlockFoundChannel(context.Background(), processBlockFound{
		hash:    testHash("catchup-branch"),
		baseURL: announcer.url,
		peerID:  "announcer",
		errCh:   errCh,
	})

	require.Error(t, err)
	require.Equal(t, int64(1), announcer.gets.Load(), "the catchup branch must have done the fetch")
	require.Error(t, <-errCh)
	require.Equal(t, 1, p2pClient.charged("announcer"))
	require.Len(t, p2pClient.recordedErrors("announcer"), 1)
}

// TestRecordCatchupPeerFailure_RateLimitedIsRecorded pins the mid-run half of the catch-up 429 rule:
// a peer that runs out of 429 retries during a run is recorded in failedPeers, so the
// releaseCatchupLock drain charges it if the run fails. That charge is the rotation signal; only a
// 429 as the run's terminal error is exempt (isLocalCatchupFault).
func TestRecordCatchupPeerFailure_RateLimitedIsRecorded(t *testing.T) {
	u := &Server{activeCatchupCtx: &CatchupContext{}}

	u.recordCatchupPeerFailure("secondary", errors.NewServiceRateLimitedError("subtree fetch rate limited"))

	require.Contains(t, u.activeCatchupCtx.failedPeers, "secondary")
}

// rateLimitedAnnouncementWithAlternative wires a Server whose announcer answers 429 for a block
// that another peer, "alt", serves and advertises as its tip. The block's parent is not in the
// chain, so both blockFound paths hand it to catchupCh, where the source it names can be read.
func rateLimitedAnnouncementWithAlternative(t *testing.T) (*Server, *chainhash.Hash, *countingPeer, *countingPeer) {
	t.Helper()

	u, p2pClient := newBlockFoundFetchServer(t)
	u.catchupCh = make(chan processBlockCatchup, 1)
	u.processBlockNotify = ttlcache.New[chainhash.Hash, bool]()
	u.catchupAlternatives = ttlcache.New[chainhash.Hash, []processBlockCatchup]()

	blocks := testhelpers.CreateTestBlockChain(t, 2)
	block := blocks[1]
	blockBytes, err := block.Bytes()
	require.NoError(t, err)

	announcer := newStatusPeer(t, http.StatusTooManyRequests)
	alt := newCountingPeer(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(blockBytes) })

	p2pClient.peers = []*p2p.PeerInfo{tipPeer("alt", alt.url, block.Hash())}

	return u, block.Hash(), announcer, alt
}

// requireCatchupSource reads the block handed to catchupCh and checks it names alt as its source.
func requireCatchupSource(t *testing.T, u *Server, hash *chainhash.Hash, alt *countingPeer) {
	t.Helper()

	select {
	case c := <-u.catchupCh:
		require.Equal(t, hash, c.block.Hash())
		require.Equal(t, peer.ID("alt").String(), c.peerID, "catch-up must be attributed to the peer that served the block")
		require.Equal(t, alt.url, c.baseURL, "catch-up must run against the peer that served the block, not the announcer that answered 429")
	case <-time.After(5 * time.Second):
		t.Fatal("block was not handed to catchupCh")
	}
}

// TestAddBlockToPriorityQueue_RateLimitedFallbackHandsOnAlternativeSource pins that
// addBlockToPriorityQueue uses the source fetchAnnouncedBlock returns: the block came from the
// alternative, so what follows the fetch must go there too.
func TestAddBlockToPriorityQueue_RateLimitedFallbackHandsOnAlternativeSource(t *testing.T) {
	u, hash, announcer, alt := rateLimitedAnnouncementWithAlternative(t)

	require.NoError(t, announce(t, u, hash, announcer.url))
	requireCatchupSource(t, u, hash, alt)
	require.Equal(t, int64(1), alt.gets.Load())
}

// TestProcessBlockFoundChannel_CatchupBranchRateLimitedFallbackHandsOnAlternativeSource is the same
// check for the backed-up-queue branch of processBlockFoundChannel.
func TestProcessBlockFoundChannel_CatchupBranchRateLimitedFallbackHandsOnAlternativeSource(t *testing.T) {
	u, hash, announcer, alt := rateLimitedAnnouncementWithAlternative(t)
	u.settings.BlockValidation.UseCatchupWhenBehind = true

	// len(blockFoundCh) > 3 selects the shouldConsiderCatchup branch.
	for i := 0; i < 4; i++ {
		u.blockFoundCh <- processBlockFound{}
	}

	errCh := make(chan error, 1)

	err := u.processBlockFoundChannel(context.Background(), processBlockFound{
		hash:    hash,
		baseURL: announcer.url,
		peerID:  peer.ID("announcer").String(),
		errCh:   errCh,
	})

	require.NoError(t, err)
	require.NoError(t, <-errCh)
	requireCatchupSource(t, u, hash, alt)
	require.Equal(t, int64(1), alt.gets.Load())
}

// TestProcessBlockFoundChannel_CatchupBranchFallThroughDoesNotRefetch: a chain-extending block in
// the backed-up-queue branch falls through to the priority queue. That path must queue the block
// already fetched rather than fetch it again: a second fetch would go to the alternative and charge
// it if it failed, and a 429 there could fall back onto the original announcer as an uncharged
// alternative.
func TestProcessBlockFoundChannel_CatchupBranchFallThroughDoesNotRefetch(t *testing.T) {
	u, p2pClient := newBlockFoundFetchServer(t)
	u.settings.BlockValidation.UseCatchupWhenBehind = true
	u.blockClassifier = NewBlockClassifier(ulogger.TestLogger{}, 10, u.blockValidation.blockchainClient)

	// Built on genesis, the sqlitememory chain's tip, so it classifies as chain-extending.
	block := testhelpers.CreateTestBlocksWithPrev(t, 1, u.settings.ChainCfgParams.GenesisHash)[0]
	blockBytes, err := block.Bytes()
	require.NoError(t, err)

	announcer := newStatusPeer(t, http.StatusTooManyRequests)
	alt := newCountingPeer(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(blockBytes) })

	p2pClient.peers = []*p2p.PeerInfo{
		tipPeer("announcer", announcer.url, block.Hash()),
		tipPeer("alt", alt.url, block.Hash()),
	}

	// len(blockFoundCh) > 3 selects the shouldConsiderCatchup branch.
	for i := 0; i < 4; i++ {
		u.blockFoundCh <- processBlockFound{}
	}

	errCh := make(chan error, 1)

	err = u.processBlockFoundChannel(context.Background(), processBlockFound{
		hash:    block.Hash(),
		baseURL: announcer.url,
		peerID:  peer.ID("announcer").String(),
		errCh:   errCh,
	})

	require.NoError(t, err)
	require.NoError(t, <-errCh)
	require.Equal(t, int64(1), announcer.gets.Load(), "the announcer must be asked once")
	require.Equal(t, int64(1), alt.gets.Load(), "the block already fetched from the alternative must not be fetched again")

	queued, _, ok := u.blockPriorityQueue.Peek()
	require.True(t, ok, "a chain-extending block must reach the priority queue")
	require.Equal(t, block.Hash(), queued.hash)
	require.Equal(t, peer.ID("alt").String(), queued.peerID)
	require.Equal(t, alt.url, queued.baseURL)
	require.Equal(t, 0, p2pClient.totalCharges())
}

// TestFetchAnnouncedBlock_NoFallbackWithoutPeerID: an announcement with no peer ID has no
// allowance to spend, so its 429s are never charged. Falling back on them would put load on other
// peers without bound.
func TestFetchAnnouncedBlock_NoFallbackWithoutPeerID(t *testing.T) {
	u, p2pClient := newBlockFoundFetchServer(t)

	hash := testHash("no-peer-id")
	announcer := newStatusPeer(t, http.StatusTooManyRequests)
	alt := newStatusPeer(t, http.StatusNotFound)

	p2pClient.peers = []*p2p.PeerInfo{tipPeer("alt", alt.url, hash)}

	_, _, err := u.fetchAnnouncedBlock(context.Background(), context.Background(), processBlockFound{
		hash:    hash,
		baseURL: announcer.url,
	})

	require.ErrorIs(t, err, errors.ErrServiceRateLimited)
	require.Equal(t, int64(0), alt.gets.Load(), "a 429 with no peer ID must not trigger the alternative fetch")
}

// TestFetchAnnouncedBlock_CallerCtxDoneIsNotCharged: a done caller ctx is this node shutting down,
// so a failure that would otherwise be charged is not.
func TestFetchAnnouncedBlock_CallerCtxDoneIsNotCharged(t *testing.T) {
	u, p2pClient := newBlockFoundFetchServer(t)
	announcer := newStatusPeer(t, http.StatusInternalServerError)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, err := u.fetchAnnouncedBlock(ctx, context.Background(), processBlockFound{
		hash:    testHash("shutdown-500"),
		baseURL: announcer.url,
		peerID:  "announcer",
	})

	require.Error(t, err)
	require.Equal(t, int64(1), announcer.gets.Load())
	require.Equal(t, 0, p2pClient.charged("announcer"), "a failure while this node shuts down must not be charged")
	require.Empty(t, p2pClient.recordedErrors("announcer"))
}

// TestFetchAnnouncedBlock_CallerCtxDoneDoesNotFallBack: nothing is retried elsewhere once the
// caller's ctx is done.
func TestFetchAnnouncedBlock_CallerCtxDoneDoesNotFallBack(t *testing.T) {
	u, p2pClient := newBlockFoundFetchServer(t)

	hash := testHash("shutdown-429")
	announcer := newStatusPeer(t, http.StatusTooManyRequests)
	alt := newStatusPeer(t, http.StatusNotFound)

	p2pClient.peers = []*p2p.PeerInfo{tipPeer("alt", alt.url, hash)}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, err := u.fetchAnnouncedBlock(ctx, context.Background(), processBlockFound{
		hash:    hash,
		baseURL: announcer.url,
		peerID:  "announcer",
	})

	require.ErrorIs(t, err, errors.ErrServiceRateLimited)
	require.Equal(t, int64(0), alt.gets.Load(), "a done caller ctx must not trigger the alternative fetch")
}
