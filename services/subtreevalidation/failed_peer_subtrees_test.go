package subtreevalidation

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jarcoal/httpmock"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// newFailedPeerSubtreeTestServer returns a RUNNING server built by New, with the negative cache
// TTL set to ttl, so every test also covers the wiring of the cache in New.
func newFailedPeerSubtreeTestServer(t *testing.T, ttl time.Duration) *Server {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.SubtreeValidation.FailedSubtreeNegativeCacheTTL = ttl
	tSettings.SubtreeValidation.QuorumPath = t.TempDir()
	tSettings.SubtreeValidation.TxMetaCacheEnabled = false
	tSettings.Kafka.InvalidSubtrees = ""

	InitPrometheusMetrics()

	running := blockchain.FSMStateRUNNING
	blockchainClient := &blockchain.Mock{}
	blockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&running, nil)
	blockchainClient.On("Subscribe", mock.Anything, mock.Anything).Return(make(chan *blockchain_api.Notification), nil)
	blockchainClient.On("GetBestBlockHeader", mock.Anything).Return(model.GenesisBlockHeader, &model.BlockHeaderMeta{Height: 1}, nil)
	blockchainClient.On("GetBlockHeaderIDs", mock.Anything, mock.Anything, mock.Anything).Return([]uint32{1}, nil)

	utxoStore, err := nullstore.NewNullStore()
	require.NoError(t, err)

	server, err := New(newCancelCtx(t), ulogger.TestLogger{}, tSettings, memory.New(), memory.New(), utxoStore,
		&validator.MockValidator{}, blockchainClient, nil, nil, nil, nil)
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, server.Stop(context.Background()))
	})

	return server
}

// activateFailingPeers sends every peer fetch to the httpmock transport, which answers 500, and
// returns a subtree hash with the URL of a peer that announces it.
func activateFailingPeers(t *testing.T) (chainhash.Hash, *url.URL) {
	t.Helper()

	httpmock.ActivateNonDefault(util.HTTPClient())
	t.Cleanup(httpmock.DeactivateAndReset)
	httpmock.RegisterResponder("GET", `=~.*`, httpmock.NewBytesResponder(http.StatusInternalServerError, nil))

	baseURL, err := url.Parse("http://peer.invalid")
	require.NoError(t, err)

	return chainhash.HashH([]byte("peer subtree")), baseURL
}

// A peer subtree that failed validation is not fetched or validated again when it is
// re-announced with the same URL within the TTL, and nothing holds its quorum lock meanwhile.
func TestSubtreesHandler_SkipsASubtreeThatRecentlyFailed(t *testing.T) {
	server := newFailedPeerSubtreeTestServer(t, time.Minute)

	hash, baseURL := activateFailingPeers(t)

	err := server.subtreesHandler(context.Background(), time.Now(), &hash, baseURL, "peer-1")
	require.Error(t, err, "the first announcement must be validated and fail")
	require.False(t, errors.Is(err, errors.ErrContextCanceled))

	fetches := httpmock.GetTotalCallCount()
	require.Positive(t, fetches, "the first announcement must have fetched from the peer")

	code, failed := server.recentlyFailedPeerSubtree(&hash, baseURL.String())
	require.True(t, failed, "the failure must be in the negative cache")
	require.NotEmpty(t, code)

	before := testutil.ToFloat64(prometheusRecentlyFailedPeerSubtreesSkipped)

	err = server.subtreesHandler(context.Background(), time.Now(), &hash, baseURL, "peer-1")
	require.NoError(t, err, "a re-announcement with the same URL within the TTL is skipped, not an error")
	require.Equal(t, fetches, httpmock.GetTotalCallCount(), "a skipped announcement must not fetch from the peer")
	require.Equal(t, before+1, testutil.ToFloat64(prometheusRecentlyFailedPeerSubtreesSkipped))

	gotLock, exists, releaseLock, err := server.quorum.TryLockIfFileNotExists(context.Background(), &hash, fileformat.FileTypeSubtree)
	require.NoError(t, err)
	require.True(t, gotLock, "a skipped announcement must not hold the quorum lock")
	require.False(t, exists, "a failed subtree must not have been stored")
	releaseLock()
}

// A failure from one announcer's URL says nothing about the subtree, so another announcer of
// the same hash, with its own URL, is still fetched and validated.
func TestSubtreesHandler_ValidatesTheSameSubtreeFromAnotherURL(t *testing.T) {
	server := newFailedPeerSubtreeTestServer(t, time.Minute)

	hash, badURL := activateFailingPeers(t)

	goodURL, err := url.Parse("http://good-peer.invalid")
	require.NoError(t, err)

	require.Error(t, server.subtreesHandler(context.Background(), time.Now(), &hash, badURL, "peer-bad"))

	_, failed := server.recentlyFailedPeerSubtree(&hash, badURL.String())
	require.True(t, failed, "the failure from the first peer's URL must be kept")

	fetches := httpmock.GetTotalCallCount()
	before := testutil.ToFloat64(prometheusRecentlyFailedPeerSubtreesSkipped)

	err = server.subtreesHandler(context.Background(), time.Now(), &hash, goodURL, "peer-good")
	require.Error(t, err, "the announcement from another URL must be validated, not skipped")
	require.Greater(t, httpmock.GetTotalCallCount(), fetches, "the subtree must be fetched from the other announcer's URL")
	require.Equal(t, before, testutil.ToFloat64(prometheusRecentlyFailedPeerSubtreesSkipped), "nothing must be skipped")
}

// The cache keeps at most failedPeerSubtreeCacheMaxSize entries, so a flood of distinct hashes
// that fail cannot grow it without bound; the oldest entry is dropped first.
func TestRecordFailedPeerSubtree_IsBounded(t *testing.T) {
	server := newFailedPeerSubtreeTestServer(t, time.Minute)

	const baseURL = "http://peer.invalid"

	first := chainhash.HashH([]byte("subtree 0"))
	server.recordFailedPeerSubtree(context.Background(), &first, baseURL, errors.NewSubtreeNotFoundError("not found"))

	// The oldest entry is found by insertion time; keep it strictly older than the rest.
	time.Sleep(time.Millisecond)

	for i := 1; i <= failedPeerSubtreeCacheMaxSize; i++ {
		hash := chainhash.HashH([]byte(fmt.Sprintf("subtree %d", i)))
		server.recordFailedPeerSubtree(context.Background(), &hash, baseURL, errors.NewSubtreeNotFoundError("not found"))
	}

	require.Equal(t, failedPeerSubtreeCacheMaxSize, server.failedPeerSubtrees.Len())

	_, failed := server.recentlyFailedPeerSubtree(&first, baseURL)
	require.False(t, failed, "the oldest entry must be dropped when the cache is full")

	last := chainhash.HashH([]byte(fmt.Sprintf("subtree %d", failedPeerSubtreeCacheMaxSize)))
	_, failed = server.recentlyFailedPeerSubtree(&last, baseURL)
	require.True(t, failed, "the newest entry must be kept")
}

// Once the TTL has passed the subtree is validated again.
func TestSubtreesHandler_ValidatesAgainAfterTheTTL(t *testing.T) {
	server := newFailedPeerSubtreeTestServer(t, 50*time.Millisecond)

	hash, baseURL := activateFailingPeers(t)

	require.Error(t, server.subtreesHandler(context.Background(), time.Now(), &hash, baseURL, "peer-1"))

	fetches := httpmock.GetTotalCallCount()

	require.Eventually(t, func() bool {
		_, failed := server.recentlyFailedPeerSubtree(&hash, baseURL.String())
		return !failed
	}, 5*time.Second, 10*time.Millisecond, "the entry must expire")

	require.Error(t, server.subtreesHandler(context.Background(), time.Now(), &hash, baseURL, "peer-1"), "after the TTL the subtree is validated again")
	require.Greater(t, httpmock.GetTotalCallCount(), fetches, "the second validation must fetch from the peer again")
}

// With the TTL at 0 the cache is off: every announcement is validated.
func TestSubtreesHandler_NegativeCacheOff(t *testing.T) {
	server := newFailedPeerSubtreeTestServer(t, 0)
	require.Nil(t, server.failedPeerSubtrees)

	hash, baseURL := activateFailingPeers(t)

	require.Error(t, server.subtreesHandler(context.Background(), time.Now(), &hash, baseURL, "peer-1"))

	fetches := httpmock.GetTotalCallCount()

	require.Error(t, server.subtreesHandler(context.Background(), time.Now(), &hash, baseURL, "peer-1"))
	require.Greater(t, httpmock.GetTotalCallCount(), fetches, "with the cache off every announcement is fetched")

	_, failed := server.recentlyFailedPeerSubtree(&hash, baseURL.String())
	require.False(t, failed)
}

// A cancellation is not a verdict on the subtree and is not kept.
func TestRecordFailedPeerSubtree_DoesNotKeepACancellation(t *testing.T) {
	server := newFailedPeerSubtreeTestServer(t, time.Minute)

	const baseURL = "http://peer.invalid"

	hash := chainhash.HashH([]byte("peer subtree"))

	server.recordFailedPeerSubtree(context.Background(), &hash, baseURL, errors.NewProcessingError("fetch failed", errors.NewContextCanceledError("ctx")))

	_, failed := server.recentlyFailedPeerSubtree(&hash, baseURL)
	require.False(t, failed, "a wrapped cancellation must not be kept")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	server.recordFailedPeerSubtree(ctx, &hash, baseURL, errors.NewStorageError("store read"))

	_, failed = server.recentlyFailedPeerSubtree(&hash, baseURL)
	require.False(t, failed, "a failure under an ended context must not be kept")

	server.recordFailedPeerSubtree(context.Background(), &hash, baseURL, errors.NewStorageError("store read"))

	code, failed := server.recentlyFailedPeerSubtree(&hash, baseURL)
	require.True(t, failed)
	require.Equal(t, errors.ERR_STORAGE_ERROR.String(), code)
}

// The replay of a subtree kept during a catchup consults the cache too and counts no outcome.
func TestReplaySubtree_SkipsASubtreeThatRecentlyFailed(t *testing.T) {
	server := newFailedPeerSubtreeTestServer(t, time.Minute)

	hash, baseURL := activateFailingPeers(t)

	server.recordFailedPeerSubtree(context.Background(), &hash, baseURL.String(), errors.NewTxInvalidError("bad tx"))

	outcome, err := server.replaySubtree(context.Background(), skippedSubtree{hash: hash, baseURL: baseURL, peerID: "peer-1", recordedAt: time.Now()})
	require.NoError(t, err)
	require.Empty(t, outcome)
	require.Zero(t, httpmock.GetTotalCallCount(), "a replay of a recently failed subtree must not fetch")
}
