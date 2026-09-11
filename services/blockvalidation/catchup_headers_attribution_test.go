package blockvalidation

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/catchup"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/require"
)

func newHeaderAttributionServer(t *testing.T) (*Server, blockchainstore.Store, *failureCountingP2PClient, *model.Block, func()) {
	t.Helper()
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.CatchupOperationTimeout = 30
	tSettings.BlockValidation.CatchupIterationTimeout = 10
	tSettings.BlockValidation.CatchupMaxRetries = 1
	store, err := blockchainstore.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)
	closeStore := sync.OnceFunc(func() { require.NoError(t, store.Close(context.Background())) })
	t.Cleanup(closeStore)
	client, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, store, nil, nil)
	require.NoError(t, err)
	cache := expiringmap.New[chainhash.Hash, bool](time.Minute)
	t.Cleanup(cache.Stop)
	recorder := &failureCountingP2PClient{}
	server := &Server{
		logger:              ulogger.TestLogger{},
		settings:            tSettings,
		blockchainClient:    client,
		p2pClient:           recorder,
		peerCircuitBreakers: catchup.NewPeerCircuitBreakers(catchup.DefaultCircuitBreakerConfig()),
		blockValidation: &BlockValidation{
			blockchainClient: client,
			blockExistsCache: cache,
		},
	}
	return server, store, recorder, testhelpers.CreateTestBlockChain(t, 2)[1], closeStore
}

func TestCatchupHeaders_FailureAttribution(t *testing.T) {
	for _, tc := range []struct {
		name         string
		wantFailures int
	}{
		{name: "parent cancellation"},
		{name: "parent deadline"},
		{name: "wrapped local cancellation with live parent"},
		{name: "peer iteration timeout", wantFailures: 1},
		{name: "peer HTTP failure", wantFailures: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server, _, recorder, block, _ := newHeaderAttributionServer(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.name == "parent deadline" {
				var deadlineCancel context.CancelFunc
				ctx, deadlineCancel = context.WithTimeout(ctx, time.Second)
				defer deadlineCancel()
			}
			if tc.name == "peer iteration timeout" {
				server.settings.BlockValidation.CatchupIterationTimeout = 1
			}
			httpmock.ActivateNonDefault(util.HTTPClient())
			defer httpmock.DeactivateAndReset()
			var requests atomic.Int64
			httpmock.RegisterNoResponder(func(req *http.Request) (*http.Response, error) {
				requests.Add(1)
				switch tc.name {
				case "parent cancellation":
					cancel()
					return nil, req.Context().Err()
				case "parent deadline", "peer iteration timeout":
					<-req.Context().Done()
					return nil, req.Context().Err()
				case "wrapped local cancellation with live parent":
					return nil, fmt.Errorf("local request canceled: %w", context.Canceled) //nolint:forbidigo // Exercise a foreign HTTP error wrapper, not a teranode error.
				default:
					return httpmock.NewStringResponse(http.StatusNotFound, "missing headers"), nil
				}
			})

			_, _, err := server.catchupGetBlockHeaders(ctx, block, "peer-headers", "http://headers-peer")
			require.Error(t, err)
			require.Positive(t, requests.Load(), "exercise HTTP failure after real local chain lookups")
			_, failures, _, _ := server.peerCircuitBreakers.GetBreaker("peer-headers").GetStats()
			require.Equal(t, tc.wantFailures, failures, "only peer faults affect the breaker")
			require.Equal(t, tc.wantFailures, recorder.failures, "breaker and reputation must agree")
			if tc.wantFailures == 0 {
				require.True(t, errors.IsLocalError(err), "local cancellation must survive the returned error: %v", err)
			} else {
				require.False(t, errors.IsLocalError(err))
				require.True(t, catchupFailureAlreadyReported(err))
			}
			if tc.name == "wrapped local cancellation with live parent" || tc.wantFailures > 0 {
				require.NoError(t, ctx.Err())
			}
		})
	}
}

func TestCatchupHeaders_LocalLookupFailureDoesNotPenalizePeer(t *testing.T) {
	for _, stage := range []string{"block existence", "best header", "block locator"} {
		t.Run(stage, func(t *testing.T) {
			server, store, recorder, block, closeStore := newHeaderAttributionServer(t)
			if stage != "block existence" {
				// Cache the real absence result so the next lookup reaches GetBestBlockHeader.
				exists, err := store.GetBlockExists(context.Background(), block.Hash())
				require.NoError(t, err)
				require.False(t, exists)
			}
			if stage == "block locator" {
				_, _, err := store.GetBestBlockHeader(context.Background())
				require.NoError(t, err)
			}
			closeStore()
			httpmock.ActivateNonDefault(util.HTTPClient())
			defer httpmock.DeactivateAndReset()
			httpmock.RegisterNoResponder(httpmock.NewStringResponder(http.StatusOK, ""))

			_, _, err := server.catchupGetBlockHeaders(context.Background(), block, "peer-headers", "http://headers-peer")
			require.Error(t, err)
			if stage == "block locator" {
				require.ErrorContains(t, err, "failed to get block locator")
			} else if stage == "best header" {
				require.ErrorContains(t, err, "failed to get best block header")
			} else {
				require.ErrorContains(t, err, "failed to check if block exists")
			}
			require.Zero(t, httpmock.GetTotalCallCount(), "local lookup failed before any peer request")
			_, failures, _, _ := server.peerCircuitBreakers.GetBreaker("peer-headers").GetStats()
			require.Zero(t, failures)
			require.Zero(t, recorder.failures)
		})
	}
}

func TestCatchupHeaders_HalfOpenLocalCancellationReleasesProbe(t *testing.T) {
	server, _, recorder, block, _ := newHeaderAttributionServer(t)
	config := catchup.DefaultCircuitBreakerConfig()
	config.Timeout = -time.Second // Make the open breaker immediately ready for its probe.
	server.peerCircuitBreakers = catchup.NewPeerCircuitBreakers(config)
	breaker := server.peerCircuitBreakers.GetBreaker("peer-headers")
	breaker.RecordFailure(5)
	_, failuresBefore, successesBefore, lastFailureBefore := breaker.GetStats()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	httpmock.ActivateNonDefault(util.HTTPClient())
	defer httpmock.DeactivateAndReset()
	httpmock.RegisterNoResponder(func(req *http.Request) (*http.Response, error) {
		cancel()
		return nil, req.Context().Err()
	})

	_, _, err := server.catchupGetBlockHeaders(ctx, block, "peer-headers", "http://headers-peer")
	require.Error(t, err)
	require.True(t, errors.IsLocalError(err))
	require.Zero(t, recorder.failures)
	state, failures, successes, lastFailure := breaker.GetStats()
	require.Equal(t, catchup.StateHalfOpen, state)
	require.Equal(t, failuresBefore, failures)
	require.Equal(t, successesBefore, successes)
	require.Equal(t, lastFailureBefore, lastFailure)
	require.True(t, breaker.CanCall(), "local cancellation must release the half-open probe")
}

func TestCatchupHeaders_HalfOpenEmptyResponseReleasesProbe(t *testing.T) {
	server, _, _, block, _ := newHeaderAttributionServer(t)
	config := catchup.DefaultCircuitBreakerConfig()
	config.Timeout = -time.Second
	server.peerCircuitBreakers = catchup.NewPeerCircuitBreakers(config)
	breaker := server.peerCircuitBreakers.GetBreaker("peer-headers")
	breaker.RecordFailure(5)
	httpmock.ActivateNonDefault(util.HTTPClient())
	defer httpmock.DeactivateAndReset()
	httpmock.RegisterNoResponder(httpmock.NewStringResponder(http.StatusOK, ""))

	_, _, err := server.catchupGetBlockHeaders(context.Background(), block, "peer-headers", "http://headers-peer")
	require.ErrorContains(t, err, "no headers received from peer")
	require.Positive(t, httpmock.GetTotalCallCount())
	require.True(t, breaker.CanCall(), "an exit without a recorded outcome must release the probe")
}

func TestCatchupHeaders_HalfOpenSuccessDoesNotReleaseAnotherProbe(t *testing.T) {
	server, _, _, block, _ := newHeaderAttributionServer(t)
	config := catchup.DefaultCircuitBreakerConfig()
	config.Timeout = -time.Second
	config.MaxHalfOpenRequests = 2
	server.peerCircuitBreakers = catchup.NewPeerCircuitBreakers(config)
	breaker := server.peerCircuitBreakers.GetBreaker("peer-headers")
	breaker.RecordFailure(5)
	require.True(t, breaker.CanCall(), "another request holds the first probe")
	httpmock.ActivateNonDefault(util.HTTPClient())
	defer httpmock.DeactivateAndReset()
	httpmock.RegisterNoResponder(httpmock.NewBytesResponder(http.StatusOK, block.Header.Bytes()))

	_, _, err := server.catchupGetBlockHeaders(context.Background(), block, "peer-headers", "http://headers-peer")
	require.NoError(t, err)
	require.Equal(t, catchup.StateHalfOpen, breaker.GetState(), "one success does not finish recovery")
	require.True(t, breaker.CanCall(), "successful header fetch released its own probe")
	require.False(t, breaker.CanCall(), "success must not also cancel and release the other request's probe")
}
