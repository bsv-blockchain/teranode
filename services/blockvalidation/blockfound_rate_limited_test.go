package blockvalidation

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/bsv-blockchain/teranode/util/test"
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

// runBlockFoundFetchWithStatus announces a block the local chain does not have from a peer whose
// /block/:hash answers status, runs addBlockToPriorityQueue, and returns what the peer was charged,
// the error delivered on errCh and the number of GETs the peer served. Only GETs are counted: a
// local process may probe loopback test servers with HEAD /.
func runBlockFoundFetchWithStatus(t *testing.T, status int) (*catchupErrorRecordingP2PClient, error, int64) {
	t.Helper()

	initPrometheusMetrics()

	origSSRF := util.SSRFProtectionEnabled()
	util.SetSSRFProtection(false)
	t.Cleanup(func() { util.SetSSRFProtection(origSSRF) })

	var gets atomic.Int64

	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			gets.Add(1)
		}
		w.WriteHeader(status)
	}))
	t.Cleanup(peer.Close)

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
		logger:          ulogger.TestLogger{},
		settings:        tSettings,
		blockValidation: bv,
		p2pClient:       p2pClient,
		stats:           gocore.NewStat("test"),
	}

	hash := chainhash.HashH([]byte("blockfound-rate-limited"))
	errCh := make(chan error, 1)

	u.addBlockToPriorityQueue(context.Background(), processBlockFound{
		hash:    &hash,
		baseURL: peer.URL,
		peerID:  "announcer",
		errCh:   errCh,
	})

	var fetchErr error
	select {
	case fetchErr = <-errCh:
	default:
		t.Fatal("addBlockToPriorityQueue returned without delivering the fetch error on errCh")
	}

	return p2pClient, fetchErr, gets.Load()
}

// TestAddBlockToPriorityQueue_RateLimitedFetchDoesNotChargeAnnouncer: the peer's /block/:hash
// shares its per-IP rate-limit bucket with the catch-up routes this node's own fan-out drains, so a
// 429 on the blockFound fetch is our collision and must not cost the announcer reputation.
func TestAddBlockToPriorityQueue_RateLimitedFetchDoesNotChargeAnnouncer(t *testing.T) {
	p2pClient, fetchErr, gets := runBlockFoundFetchWithStatus(t, http.StatusTooManyRequests)

	require.ErrorIs(t, fetchErr, errors.ErrServiceRateLimited, "the 429 must reach errCh as ErrServiceRateLimited through fetchSingleBlock's wrapping")
	require.Equal(t, int64(1), gets, "the blockFound fetch must not retry")
	require.Equal(t, 0, p2pClient.charged("announcer"), "a 429 must not record a catchup failure against the announcer")
	require.Empty(t, p2pClient.recordedErrors("announcer"), "a 429 must not set a catchup error on the announcer")
}

// TestAddBlockToPriorityQueue_NonRateLimitedFetchErrorChargesAnnouncer pins that the 429 exemption is
// narrow: any other failed /block/:hash response, such as a misconfigured asset server answering 401
// or a 500, still charges the announcer exactly once.
func TestAddBlockToPriorityQueue_NonRateLimitedFetchErrorChargesAnnouncer(t *testing.T) {
	for _, status := range []int{http.StatusInternalServerError, http.StatusUnauthorized} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			p2pClient, fetchErr, gets := runBlockFoundFetchWithStatus(t, status)

			require.Error(t, fetchErr)
			require.NotErrorIs(t, fetchErr, errors.ErrServiceRateLimited)
			require.Equal(t, int64(1), gets, "the blockFound fetch must not retry")
			require.Equal(t, 1, p2pClient.charged("announcer"), "a non-429 fetch failure must still record one catchup failure")

			recorded := p2pClient.recordedErrors("announcer")
			require.Len(t, recorded, 1, "a non-429 fetch failure must still set one catchup error")
			require.Contains(t, recorded[0], strconv.Itoa(status))
		})
	}
}
