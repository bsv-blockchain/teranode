package httpimpl

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/bsv-blockchain/teranode/services/blockvalidation"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestGetCatchupStatusPreviousAttemptProjection pins, through the handler, that
// the previous attempt's peer_url and error_message follow the detail switches
// while the rest of the attempt stays public.
func TestGetCatchupStatusPreviousAttemptProjection(t *testing.T) {
	status := &blockvalidation.CatchupStatus{
		IsCatchingUp: true,
		PeerID:       "peer-1",
		PeerURL:      "http://10.0.0.4:8000",
		PreviousAttempt: &blockvalidation.PreviousAttempt{
			PeerID:       "peer-2",
			PeerURL:      "http://10.0.0.5:8000",
			ErrorMessage: "dial tcp 10.0.0.5:8000: connection refused",
			ErrorType:    "network",
		},
	}

	serve := func(t *testing.T, publicDetail bool) map[string]interface{} {
		t.Helper()

		httpServer, mockRepo, c, rec := GetMockHTTP(t, nil)
		httpServer.settings.Asset.PublicPeersDetail = publicDetail
		httpServer.settings.Asset.PublicErrorDetail = publicDetail

		bv := &blockvalidation.Mock{}
		bv.On("GetCatchupStatus", mock.Anything).Return(status, nil)
		mockRepo.On("GetBlockvalidationClient").Return(bv)

		require.NoError(t, httpServer.GetCatchupStatus(c))
		require.Equal(t, http.StatusOK, rec.Code)

		var body map[string]interface{}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))

		return body
	}

	t.Run("defaults keep every field", func(t *testing.T) {
		body := serve(t, true)
		attempt := body["previous_attempt"].(map[string]interface{})

		require.Equal(t, "http://10.0.0.4:8000", body["peer_url"])
		require.Equal(t, "http://10.0.0.5:8000", attempt["peer_url"])
		require.Equal(t, "dial tcp 10.0.0.5:8000: connection refused", attempt["error_message"])
		require.Equal(t, "network", attempt["error_type"])
	})

	t.Run("switches off withhold peer URLs and the raw error", func(t *testing.T) {
		body := serve(t, false)
		attempt := body["previous_attempt"].(map[string]interface{})

		require.NotContains(t, body, "peer_url")
		require.NotContains(t, attempt, "peer_url")
		require.NotContains(t, attempt, "error_message")
		require.Equal(t, "peer-2", attempt["peer_id"])
		require.Equal(t, "network", attempt["error_type"])
	})
}
