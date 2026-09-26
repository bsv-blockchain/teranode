package httpimpl

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	aero "github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

// serveWithErrorHandler runs one request through the central error handler with
// the supplied handler error and returns the status and decoded JSON body.
func serveWithErrorHandler(t *testing.T, tSettings *settings.Settings, handlerErr error) (int, map[string]interface{}) {
	t.Helper()

	e := echo.New()
	e.HTTPErrorHandler = customHTTPErrorHandler(ulogger.TestLogger{}, tSettings)
	e.GET("/boom", func(_ echo.Context) error { return handlerErr })

	req := httptest.NewRequest(http.MethodGet, "/boom", nil)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	body := map[string]interface{}{}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))

	return rec.Code, body
}

// TestCustomHTTPErrorHandler_PublicErrorDetail pins the public projection of
// error bodies: verbose by default, generic-plus-correlation-id once the
// operator turns the detail off.
func TestCustomHTTPErrorHandler_PublicErrorDetail(t *testing.T) {
	const backendDetail = "dial tcp 10.0.0.4:3000: connection refused"

	t.Run("detail on returns the backend text verbatim", func(t *testing.T) {
		tSettings := &settings.Settings{}
		tSettings.Asset.PublicErrorDetail = true

		code, body := serveWithErrorHandler(t, tSettings,
			echo.NewHTTPError(http.StatusInternalServerError, backendDetail))

		require.Equal(t, http.StatusInternalServerError, code)
		require.Equal(t, backendDetail, body["message"])
		require.NotContains(t, body, "correlation_id")
	})

	t.Run("detail off replaces a 5xx body with a correlation id", func(t *testing.T) {
		tSettings := &settings.Settings{}

		code, body := serveWithErrorHandler(t, tSettings,
			echo.NewHTTPError(http.StatusInternalServerError, backendDetail))

		require.Equal(t, http.StatusInternalServerError, code)
		require.NotContains(t, body["message"], "10.0.0.4")
		require.NotContains(t, body["message"], "dial tcp")
		require.Equal(t, genericServerErrorMessage, body["message"])

		correlationID, ok := body["correlation_id"].(string)
		require.True(t, ok, "a redacted 5xx body must carry a correlation id")
		require.NotEmpty(t, correlationID)
	})

	t.Run("detail off keeps the client-facing part of a 4xx body", func(t *testing.T) {
		tSettings := &settings.Settings{}

		wrapped := errors.NewInvalidArgumentError("invalid block hash format",
			errors.NewStorageError("aerospike 10.0.0.4:3000 key not found"))

		code, body := serveWithErrorHandler(t, tSettings,
			echo.NewHTTPError(http.StatusBadRequest, wrapped.Error()))

		require.Equal(t, http.StatusBadRequest, code)
		require.Contains(t, body["message"], "invalid block hash format")
		require.NotContains(t, body["message"], "10.0.0.4")
		require.NotContains(t, body["message"], "->")
	})
}

// TestHealthResponse pins the /health projection against both settings.
func TestHealthResponse(t *testing.T) {
	const details = `{"status":"503","dependencies":[{"resource":"UtxoStore","error":"dial tcp 10.0.0.4:3000"}]}`

	t.Run("defaults preserve the verbose body and the 200 status", func(t *testing.T) {
		tSettings := &settings.Settings{}
		tSettings.Asset.PublicHealthDetail = true

		status, body := healthResponse(tSettings, http.StatusServiceUnavailable, details)

		require.Equal(t, http.StatusOK, status)
		require.Equal(t, details, body)
	})

	t.Run("detail off drops the dependency inventory", func(t *testing.T) {
		tSettings := &settings.Settings{}

		status, body := healthResponse(tSettings, http.StatusServiceUnavailable, details)

		require.Equal(t, http.StatusOK, status)
		require.NotContains(t, body, "UtxoStore")
		require.NotContains(t, body, "10.0.0.4")
		require.Equal(t, healthStatusUnavailable, body)
	})

	t.Run("strict status propagates the computed code", func(t *testing.T) {
		tSettings := &settings.Settings{}
		tSettings.Asset.HealthStrictStatus = true

		status, _ := healthResponse(tSettings, http.StatusServiceUnavailable, details)
		require.Equal(t, http.StatusServiceUnavailable, status)

		status, body := healthResponse(tSettings, http.StatusOK, details)
		require.Equal(t, http.StatusOK, status)
		require.Equal(t, healthStatusOK, body)
	})
}

// TestCatchupStatusProjection pins that the catchup status drops the wrapped
// backend error text and the selected peer URLs once detail is turned off.
func TestCatchupStatusProjection(t *testing.T) {
	const backendDetail = "STORAGE_ERROR (69): fetch failed -> dial tcp 10.0.0.4:8000"

	t.Run("detail off drops error_message and peer_url", func(t *testing.T) {
		tSettings := &settings.Settings{}

		body := map[string]interface{}{}
		attempt := map[string]interface{}{}

		applyCatchupStatusProjection(tSettings, body, attempt, "http://10.0.0.4:8000", "http://10.0.0.5:8000", backendDetail)

		require.NotContains(t, body, "peer_url")
		require.NotContains(t, attempt, "peer_url")
		require.NotContains(t, attempt, "error_message")
	})

	t.Run("defaults keep today's fields", func(t *testing.T) {
		tSettings := &settings.Settings{}
		tSettings.Asset.PublicPeersDetail = true
		tSettings.Asset.PublicErrorDetail = true

		body := map[string]interface{}{}
		attempt := map[string]interface{}{}

		applyCatchupStatusProjection(tSettings, body, attempt, "http://10.0.0.4:8000", "http://10.0.0.5:8000", backendDetail)

		require.Equal(t, "http://10.0.0.4:8000", body["peer_url"])
		require.Equal(t, "http://10.0.0.5:8000", attempt["peer_url"])
		require.Equal(t, backendDetail, attempt["error_message"])
	})
}

// TestPeerProjection pins that the minimal peer shape drops ban, reputation and
// error state while keeping the fields catchup peer selection needs.
func TestPeerProjection(t *testing.T) {
	peer := &blockchain.PeerInfo{
		ID:               "peer-1",
		ClientName:       "teranode/1.2.3",
		Height:           42,
		DataHubURL:       "http://peer-1:8000",
		NetworkAddress:   "10.0.0.9:8333",
		BanScore:         77,
		IsBanned:         true,
		IsConnected:      true,
		ReputationScore:  0.25,
		MaliciousCount:   3,
		LastCatchupError: "dial tcp 10.0.0.9:8000: connection refused",
	}

	minimal := minimalPeerInfoToResponse(peer)

	encoded, err := json.Marshal(minimal)
	require.NoError(t, err)

	js := string(encoded)
	require.Contains(t, js, "peer-1")
	require.Contains(t, js, "http://peer-1:8000")
	require.NotContains(t, js, "ban_score")
	require.NotContains(t, js, "is_banned")
	require.NotContains(t, js, "reputation")
	require.NotContains(t, js, "malicious")
	require.NotContains(t, js, "last_catchup_error")
	require.NotContains(t, js, "connection refused")
}

// TestNewAerospikeRecordNilNode pins that a record with no owning node is
// serialized instead of panicking on a public, unauthenticated route.
func TestNewAerospikeRecordNilNode(t *testing.T) {
	key, err := aero.NewKey("test", "txmeta", []byte("abc"))
	require.NoError(t, err)

	record := &aero.Record{
		Key:        key,
		Node:       nil,
		Bins:       aero.BinMap{"fee": 1},
		Generation: 7,
	}

	require.NotPanics(t, func() {
		out := newAerospikeRecord(record)
		require.Empty(t, out.Node)
		require.EqualValues(t, 7, out.Generation)
	})
}

// TestGetPeers_MinimalShapeWhenDetailOff drives the handler end to end to check
// that the redacted shape is what actually reaches the wire.
func TestGetPeers_MinimalShapeWhenDetailOff(t *testing.T) {
	reg := blockchain.NewCentralizedPeerRegistry(blockchain.DefaultBanConfig())
	reg.Register(&blockchain.PeerInfo{
		ID:               "12D3KooWGRUEbFsXTBnpVRHtE3ZBSbSMd4x8hs9NfCVCNhqTFPHb",
		TransportType:    blockchain_api.TransportType_TRANSPORT_HTTP,
		TransportTypeSet: true,
		DataHubURL:       "http://198.51.100.4:8090",
		Height:           912350,
		BanScore:         91,
		IsBanned:         true,
		LastCatchupError: "dial tcp 10.0.0.9:8000: connection refused",
	})

	h := newPeersHandler(t, blockchain.NewLocalPeerRegistryClient(reg))
	h.settings.Asset.PublicPeersDetail = false

	req := httptest.NewRequest(http.MethodGet, "/api/v1/peers", nil)
	rec := httptest.NewRecorder()
	require.NoError(t, h.GetPeers(h.e.NewContext(req, rec)))

	require.Equal(t, http.StatusOK, rec.Code)

	js := rec.Body.String()
	require.Contains(t, js, "http://198.51.100.4:8090")
	require.NotContains(t, js, "ban_score")
	require.NotContains(t, js, "is_banned")
	require.NotContains(t, js, "connection refused")
}
