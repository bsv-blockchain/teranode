package subtreevalidation

import (
	"context"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
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

// newFailedPeerSubtreeTestServer returns a RUNNING server whose peer-subtree fetches go to the
// httpmock transport, with the negative cache set to ttl.
func newFailedPeerSubtreeTestServer(t *testing.T, ttl time.Duration) *Server {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.SubtreeValidation.FailedSubtreeNegativeCacheTTL = ttl

	InitPrometheusMetrics()

	running := blockchain.FSMStateRUNNING
	blockchainClient := &blockchain.Mock{}
	blockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&running, nil)

	utxoStore, err := nullstore.NewNullStore()
	require.NoError(t, err)

	subtreeStore := memory.New()

	q, err := NewQuorum(ulogger.TestLogger{}, subtreeStore, t.TempDir(), WithTimeout(quorumOpTimeout))
	require.NoError(t, err)

	server := &Server{
		logger:             ulogger.TestLogger{},
		settings:           tSettings,
		quorum:             q,
		blockchainClient:   blockchainClient,
		subtreeStore:       subtreeStore,
		utxoStore:          utxoStore,
		validatorClient:    &validator.MockValidator{},
		failedPeerSubtrees: newFailedPeerSubtreeCache(ttl),
	}

	blockIDs := map[uint32]bool{}
	server.currentBlockIDsMap.Store(&blockIDs)
	server.bestBlockHeaderMeta.Store(&model.BlockHeaderMeta{Height: 1})

	t.Cleanup(func() {
		if server.failedPeerSubtrees != nil {
			server.failedPeerSubtrees.Stop()
		}
	})

	return server
}

// A peer subtree that failed validation is not fetched or validated again when it is
// re-announced within the TTL, and nothing holds its quorum lock meanwhile.
func TestSubtreesHandler_SkipsASubtreeThatRecentlyFailed(t *testing.T) {
	server := newFailedPeerSubtreeTestServer(t, time.Minute)

	httpmock.ActivateNonDefault(util.HTTPClient())
	defer httpmock.DeactivateAndReset()
	httpmock.RegisterResponder("GET", `=~.*`, httpmock.NewBytesResponder(http.StatusInternalServerError, nil))

	hash := chainhash.HashH([]byte("peer subtree"))
	baseURL, err := url.Parse("http://peer.invalid")
	require.NoError(t, err)

	err = server.subtreesHandler(context.Background(), time.Now(), &hash, baseURL, "peer-1")
	require.Error(t, err, "the first announcement must be validated and fail")
	require.False(t, errors.Is(err, errors.ErrContextCanceled))

	fetches := httpmock.GetTotalCallCount()
	require.Positive(t, fetches, "the first announcement must have fetched from the peer")

	code, failed := server.recentlyFailedPeerSubtree(&hash)
	require.True(t, failed, "the failure must be in the negative cache")
	require.NotEmpty(t, code)

	before := testutil.ToFloat64(prometheusRecentlyFailedPeerSubtreesSkipped)

	err = server.subtreesHandler(context.Background(), time.Now(), &hash, baseURL, "peer-2")
	require.NoError(t, err, "a re-announcement within the TTL is skipped, not an error")
	require.Equal(t, fetches, httpmock.GetTotalCallCount(), "a skipped announcement must not fetch from the peer")
	require.Equal(t, before+1, testutil.ToFloat64(prometheusRecentlyFailedPeerSubtreesSkipped))

	gotLock, exists, releaseLock, err := server.quorum.TryLockIfFileNotExists(context.Background(), &hash, fileformat.FileTypeSubtree)
	require.NoError(t, err)
	require.True(t, gotLock, "a skipped announcement must not hold the quorum lock")
	require.False(t, exists, "a failed subtree must not have been stored")
	releaseLock()
}

// Once the TTL has passed the subtree is validated again.
func TestSubtreesHandler_ValidatesAgainAfterTheTTL(t *testing.T) {
	server := newFailedPeerSubtreeTestServer(t, 50*time.Millisecond)

	httpmock.ActivateNonDefault(util.HTTPClient())
	defer httpmock.DeactivateAndReset()
	httpmock.RegisterResponder("GET", `=~.*`, httpmock.NewBytesResponder(http.StatusInternalServerError, nil))

	hash := chainhash.HashH([]byte("peer subtree"))
	baseURL, err := url.Parse("http://peer.invalid")
	require.NoError(t, err)

	require.Error(t, server.subtreesHandler(context.Background(), time.Now(), &hash, baseURL, "peer-1"))

	fetches := httpmock.GetTotalCallCount()

	require.Eventually(t, func() bool {
		_, failed := server.recentlyFailedPeerSubtree(&hash)
		return !failed
	}, 5*time.Second, 10*time.Millisecond, "the entry must expire")

	require.Error(t, server.subtreesHandler(context.Background(), time.Now(), &hash, baseURL, "peer-1"), "after the TTL the subtree is validated again")
	require.Greater(t, httpmock.GetTotalCallCount(), fetches, "the second validation must fetch from the peer again")
}

// With the TTL at 0 the cache is off: every announcement is validated.
func TestSubtreesHandler_NegativeCacheOff(t *testing.T) {
	server := newFailedPeerSubtreeTestServer(t, 0)
	require.Nil(t, server.failedPeerSubtrees)

	httpmock.ActivateNonDefault(util.HTTPClient())
	defer httpmock.DeactivateAndReset()
	httpmock.RegisterResponder("GET", `=~.*`, httpmock.NewBytesResponder(http.StatusInternalServerError, nil))

	hash := chainhash.HashH([]byte("peer subtree"))
	baseURL, err := url.Parse("http://peer.invalid")
	require.NoError(t, err)

	require.Error(t, server.subtreesHandler(context.Background(), time.Now(), &hash, baseURL, "peer-1"))

	fetches := httpmock.GetTotalCallCount()

	require.Error(t, server.subtreesHandler(context.Background(), time.Now(), &hash, baseURL, "peer-1"))
	require.Greater(t, httpmock.GetTotalCallCount(), fetches, "with the cache off every announcement is fetched")

	_, failed := server.recentlyFailedPeerSubtree(&hash)
	require.False(t, failed)
}

// A cancellation is not a verdict on the subtree and is not kept.
func TestRecordFailedPeerSubtree_DoesNotKeepACancellation(t *testing.T) {
	server := newFailedPeerSubtreeTestServer(t, time.Minute)

	hash := chainhash.HashH([]byte("peer subtree"))

	server.recordFailedPeerSubtree(context.Background(), &hash, errors.NewProcessingError("fetch failed", errors.NewContextCanceledError("ctx")))

	_, failed := server.recentlyFailedPeerSubtree(&hash)
	require.False(t, failed, "a wrapped cancellation must not be kept")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	server.recordFailedPeerSubtree(ctx, &hash, errors.NewStorageError("store read"))

	_, failed = server.recentlyFailedPeerSubtree(&hash)
	require.False(t, failed, "a failure under an ended context must not be kept")

	server.recordFailedPeerSubtree(context.Background(), &hash, errors.NewStorageError("store read"))

	code, failed := server.recentlyFailedPeerSubtree(&hash)
	require.True(t, failed)
	require.Equal(t, errors.ERR_STORAGE_ERROR.String(), code)
}

// The replay of a subtree kept during a catchup consults the cache too and counts no outcome.
func TestReplaySubtree_SkipsASubtreeThatRecentlyFailed(t *testing.T) {
	server := newFailedPeerSubtreeTestServer(t, time.Minute)

	httpmock.ActivateNonDefault(util.HTTPClient())
	defer httpmock.DeactivateAndReset()
	httpmock.RegisterResponder("GET", `=~.*`, httpmock.NewBytesResponder(http.StatusInternalServerError, nil))

	hash := chainhash.HashH([]byte("peer subtree"))
	baseURL, err := url.Parse("http://peer.invalid")
	require.NoError(t, err)

	server.recordFailedPeerSubtree(context.Background(), &hash, errors.NewTxInvalidError("bad tx"))

	outcome, err := server.replaySubtree(context.Background(), skippedSubtree{hash: hash, baseURL: baseURL, peerID: "peer-1", recordedAt: time.Now()})
	require.NoError(t, err)
	require.Empty(t, outcome)
	require.Zero(t, httpmock.GetTotalCallCount(), "a replay of a recently failed subtree must not fetch")
}
