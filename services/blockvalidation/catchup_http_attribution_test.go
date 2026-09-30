package blockvalidation

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/require"
)

func TestCatchupHeaders_MalformedHTTPRemainsPeerFailure(t *testing.T) {
	server, _, recorder, block, _ := newHeaderAttributionServer(t)
	protected := util.SSRFProtectionEnabled()
	util.SetSSRFProtection(false)
	t.Cleanup(func() { util.SetSSRFProtection(protected) })
	var requests atomic.Int32
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		conn, rw, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		defer conn.Close()
		requests.Add(1)
		_, _ = fmt.Fprint(rw, "HTTP/1.1 200 OK\r\ncontext canceled forged-peer-text\r\n\r\n")
		_ = rw.Flush()
	}))
	t.Cleanup(peer.Close)

	ctx := context.Background()
	_, _, err := server.catchupGetBlockHeaders(ctx, block, "bad-headers", peer.URL)
	require.Error(t, err)
	require.Positive(t, requests.Load())
	require.NotContains(t, err.Error(), "forged-peer-text")
	require.False(t, isLocalCatchupFault(err))
	require.False(t, shouldStopPeerFailover(ctx, err))
	require.True(t, catchupFailureAlreadyReported(err))
	_, failures, _, _ := server.peerCircuitBreakers.GetBreaker("bad-headers").GetStats()
	require.Equal(t, 1, failures)
	require.Equal(t, 1, recorder.failures)
}

func TestCatchupBlocks_ExhaustedRateLimitChargesPeer(t *testing.T) {
	server, _, recorder, block, _ := newHeaderAttributionServer(t)
	server.settings.BlockValidation.PerPeerFetchRate = 0
	protected := util.SSRFProtectionEnabled()
	util.SetSSRFProtection(false)
	t.Cleanup(func() { util.SetSSRFProtection(protected) })
	var requests atomic.Int32
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	t.Cleanup(peer.Close)

	ctx := context.Background()
	_, err := server.fetchBlocksBatch(ctx, block.Hash(), 1, "limited-peer", peer.URL)
	require.Error(t, err)
	require.Greater(t, requests.Load(), int32(1), "back off and retry before declaring the peer unavailable")
	require.False(t, errors.IsTransientLocalError(err))
	require.False(t, isLocalCatchupFault(err), "exhaustion must reach the peer-failure/rotation path")
	require.False(t, shouldStopPeerFailover(ctx, err))
	catchupCtx := &CatchupContext{blockUpTo: block, peerID: "limited-peer", baseURL: peer.URL, startTime: time.Now()}
	server.activeCatchupCtx = catchupCtx
	server.releaseCatchupLock(catchupCtx, &err)
	require.Equal(t, "network_error", server.previousCatchupAttempt.ErrorType)
	// Release records the diagnostic; the coordinator's terminal failure path
	// records the reputation charge through this shared helper.
	server.reportCatchupFailureForError(ctx, "limited-peer", err)
	require.Equal(t, 1, recorder.failures, "exhaustion must charge the failed source once")
}
