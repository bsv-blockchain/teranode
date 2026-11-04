package blockvalidation

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/require"
)

// registryBackedRecorder forwards UpdateCatchupError into a real peer registry, the
// way the p2p service does, so a test sees what the registry stores and the Asset
// /peers endpoint serves.
type registryBackedRecorder struct {
	catchupReportRecorder

	registry *blockchain.CentralizedPeerRegistry
}

func (m *registryBackedRecorder) UpdateCatchupError(ctx context.Context, peerID, errorMsg string) error {
	m.registry.RecordCatchupError(peerID, errorMsg)

	return m.catchupReportRecorder.UpdateCatchupError(ctx, peerID, errorMsg)
}

// TestReleaseCatchupLock_DoesNotDiscloseFetchDetail covers the two regression cases of
// bitcoin-sv/teranode#4855 from a real failing fetch to what is stored. A peer chooses
// the URL catchup fetches from, so the failure can be a response from, or a dial to, a
// service inside this node's network. Neither the peer registry's LastCatchupError
// (served by /peers) nor the previous-attempt error message (served by /catchup/status)
// may carry any byte of that response body or the address that was dialled.
func TestReleaseCatchupLock_DoesNotDiscloseFetchDetail(t *testing.T) {
	const peerID = "peer-steered"

	// The httptest servers and the closed port are on loopback, which the SSRF guard
	// refuses; the guard's own refusal is the third case below.
	origSSRF := util.SSRFProtectionEnabled()
	t.Cleanup(func() { util.SetSSRFProtection(origSSRF) })

	run := func(t *testing.T, ssrfProtection bool, fetchURL string) (stored string, previous string) {
		t.Helper()

		util.SetSSRFProtection(ssrfProtection)

		_, fetchErr := util.DoHTTPRequest(context.Background(), fetchURL)
		require.Error(t, fetchErr)

		registry := blockchain.NewCentralizedPeerRegistry(blockchain.DefaultBanConfig())
		registry.Register(&blockchain.PeerInfo{ID: peerID})

		rec := &registryBackedRecorder{registry: registry}
		u := &Server{logger: ulogger.TestLogger{}, p2pClient: rec}

		catchupCtx := &CatchupContext{
			blockUpTo: newReleaseCatchupBlock(t),
			baseURL:   fetchURL,
			peerID:    peerID,
			startTime: time.Now(),
		}

		err := error(errors.NewServiceError("[catchup] failed to fetch headers", fetchErr))
		u.releaseCatchupLock(catchupCtx, &err)

		require.NotEmpty(t, rec.errorMessagesReported(), "the peer error must still be reported")
		require.NotNil(t, u.previousCatchupAttempt)

		info, ok := registry.Get(peerID)
		require.True(t, ok)

		return info.LastCatchupError, u.previousCatchupAttempt.ErrorMessage
	}

	t.Run("error body", func(t *testing.T) {
		const secret = "SECRET-4855-internal-admin-token"

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"token":"` + secret + `"}`))
		}))
		t.Cleanup(srv.Close)

		stored, previous := run(t, false, srv.URL+"/headers")

		require.NotEmpty(t, stored)
		require.NotContains(t, stored, secret)
		require.NotContains(t, previous, secret)
		require.Contains(t, stored, "returned status code [500]", "the diagnostic must survive")
	})

	t.Run("dial failure", func(t *testing.T) {
		// A port that was just closed, so the dial is refused and Go's error names the
		// address it tried.
		l, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)

		_, port, err := net.SplitHostPort(l.Addr().String())
		require.NoError(t, err)
		require.NoError(t, l.Close())

		stored, previous := run(t, false, "http://localhost:"+port+"/headers")

		require.Contains(t, stored, "connection refused", "the diagnostic must survive")
		requireNoLoopbackLiteral(t, stored)
		requireNoLoopbackLiteral(t, previous)
	})

	t.Run("ssrf refusal names the resolved address", func(t *testing.T) {
		// The guard refuses after resolving, and says what the name resolved to - the
		// very fact a peer probing internal hostnames wants to learn.
		stored, previous := run(t, true, "http://localhost:1/headers")

		require.Contains(t, stored, "SSRF dial check", "the diagnostic must survive")
		requireNoLoopbackLiteral(t, stored)
		requireNoLoopbackLiteral(t, previous)
	})
}

func requireNoLoopbackLiteral(t *testing.T, s string) {
	t.Helper()

	require.NotContains(t, s, "127.0.0.1")
	require.NotContains(t, s, "[::1]")
	require.NotContains(t, s, " ::1")
}
