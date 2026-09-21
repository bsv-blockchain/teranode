package httpimpl

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/asset/repository"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

// failingListPeers wraps a registry client and fails only ListPeers, so the
// handler's error path can be exercised against an otherwise real client.
type failingListPeers struct {
	blockchain.PeerRegistryClientI
}

func (f failingListPeers) ListPeers(_ context.Context, _ *blockchain_api.TransportType,
	_ float64, _ uint32, _, _ bool) ([]*blockchain.PeerInfo, error) {
	return nil, errors.NewServiceError("registry unreachable")
}

func newPeersHandler(t *testing.T, registry blockchain.PeerRegistryClientI) *HTTP {
	t.Helper()

	return &HTTP{
		logger:     ulogger.TestLogger{},
		settings:   &settings.Settings{},
		repository: &repository.Repository{PeerRegistryClient: registry},
		e:          echo.New(),
		startTime:  time.Now(),
	}
}

func callGetPeers(t *testing.T, h *HTTP) (int, PeersResponse) {
	t.Helper()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/peers", nil)
	rec := httptest.NewRecorder()

	require.NoError(t, h.GetPeers(h.e.NewContext(req, rec)))

	var body PeersResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))

	return rec.Code, body
}

// TestGetPeers_ReturnsBothTransports checks the endpoint serves libp2p and
// wire-protocol peers from one registry read, each tagged with its transport.
func TestGetPeers_ReturnsBothTransports(t *testing.T) {
	reg := blockchain.NewCentralizedPeerRegistry(blockchain.DefaultBanConfig())

	hash, err := chainhash.NewHashFromStr(
		"0000000000000000000000000000000000000000000000000000000000000001")
	require.NoError(t, err)

	reg.Register(&blockchain.PeerInfo{
		ID:               "12D3KooWGRUEbFsXTBnpVRHtE3ZBSbSMd4x8hs9NfCVCNhqTFPHb",
		TransportType:    blockchain_api.TransportType_TRANSPORT_HTTP,
		TransportTypeSet: true,
		DataHubURL:       "http://198.51.100.4:8090",
		Height:           912350,
		BlockHash:        hash,
	})
	reg.Register(&blockchain.PeerInfo{
		ID:               "legacy:203.0.113.7:8333",
		TransportType:    blockchain_api.TransportType_TRANSPORT_WIRE_PROTOCOL,
		TransportTypeSet: true,
		NetworkAddress:   "203.0.113.7:8333",
		ClientName:       "/Bitcoin SV:1.0.16/",
		Height:           912344,
		Legacy: &blockchain.LegacyPeerInfo{
			Inbound:         true,
			ProtocolVersion: 70016,
			IsSyncPeer:      true,
			TimeConnected:   time.Unix(1750000000, 0).UTC(),
		},
	})

	code, body := callGetPeers(t, newPeersHandler(t, blockchain.NewLocalPeerRegistryClient(reg)))

	require.Equal(t, http.StatusOK, code)
	require.Equal(t, 2, body.Count)
	require.Len(t, body.Peers, 2)

	byTransport := map[string]PeerInfoResponse{}
	for _, p := range body.Peers {
		byTransport[p.Transport] = p
	}

	libp2p, ok := byTransport["libp2p"]
	require.True(t, ok, "a libp2p peer must be served")
	require.Nil(t, libp2p.Legacy, "a libp2p peer carries no legacy block")
	require.Equal(t, "http://198.51.100.4:8090", libp2p.DataHubURL)
	require.Equal(t, hash.String(), libp2p.BlockHash)

	legacy, ok := byTransport["legacy"]
	require.True(t, ok, "a wire-protocol peer must be served")
	require.Equal(t, "legacy:203.0.113.7:8333", legacy.ID)
	require.Equal(t, "203.0.113.7:8333", legacy.NetworkAddress)
	require.NotNil(t, legacy.Legacy)
	require.True(t, legacy.Legacy.Inbound)
	require.True(t, legacy.Legacy.IsSyncPeer)
	require.Equal(t, uint32(70016), legacy.Legacy.ProtocolVersion)

	// Unset catchup timestamps must serialise as 0, not as the year-1 sentinel.
	require.Zero(t, legacy.CatchupLastAttempt)
	require.Zero(t, libp2p.CatchupLastAttempt)
}

// TestGetPeers_EmptyRegistry checks the endpoint serves an empty list rather
// than a null, so the dashboard can iterate it unconditionally.
func TestGetPeers_EmptyRegistry(t *testing.T) {
	reg := blockchain.NewCentralizedPeerRegistry(blockchain.DefaultBanConfig())

	code, body := callGetPeers(t, newPeersHandler(t, blockchain.NewLocalPeerRegistryClient(reg)))

	require.Equal(t, http.StatusOK, code)
	require.Zero(t, body.Count)
	require.NotNil(t, body.Peers)
	require.Empty(t, body.Peers)
}

// TestGetPeers_NoRegistryClient checks a node without a reachable registry
// reports 503 and an empty list instead of panicking.
func TestGetPeers_NoRegistryClient(t *testing.T) {
	code, body := callGetPeers(t, newPeersHandler(t, nil))

	require.Equal(t, http.StatusServiceUnavailable, code)
	require.Zero(t, body.Count)
	require.Empty(t, body.Peers)
}

// TestGetPeers_ListPeersError checks a registry failure reports 500 and an empty
// list rather than a partial or null payload.
func TestGetPeers_ListPeersError(t *testing.T) {
	reg := blockchain.NewCentralizedPeerRegistry(blockchain.DefaultBanConfig())
	client := failingListPeers{blockchain.NewLocalPeerRegistryClient(reg)}

	code, body := callGetPeers(t, newPeersHandler(t, client))

	require.Equal(t, http.StatusInternalServerError, code)
	require.Zero(t, body.Count)
	require.Empty(t, body.Peers)
}
