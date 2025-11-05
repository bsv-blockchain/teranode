package p2p

import (
	"context"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/settings"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// Test helpers and mocks for the new architecture

// MockBanEventHandler for testing ban events
type MockBanEventHandler struct {
	mock.Mock
}

func (m *MockBanEventHandler) OnPeerBanned(peerID string, until time.Time, reason string) {
	m.Called(peerID, until, reason)
}

// MockHTTPClient for testing HTTP requests
type MockHTTPClient struct {
	mock.Mock
}

func (m *MockHTTPClient) Do(req *http.Request) (*http.Response, error) {
	args := m.Called(req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*http.Response), args.Error(1)
}

// CreateTestSettings creates test settings with sensible defaults
func CreateTestSettings() *settings.Settings {
	s := &settings.Settings{
		P2P: settings.P2PSettings{
			BanThreshold: 100,
			BanDuration:  24 * time.Hour,
			DisableNAT:   true, // Disable NAT in tests to prevent data races in libp2p
		},
	}
	return s
}

// CreateTestPeerInfo creates a test peer with specified attributes
func CreateTestPeerInfo(id peer.ID, height int32, healthy bool, banned bool, dataHubURL string) *PeerInfo {
	return &PeerInfo{
		ID:              id,
		Height:          height,
		BlockHash:       "test-hash",
		DataHubURL:      dataHubURL,
		IsHealthy:       healthy,
		IsBanned:        banned,
		BanScore:        0,
		ConnectedAt:     time.Now(),
		BytesReceived:   0,
		LastBlockTime:   time.Now(),
		LastMessageTime: time.Now(),
		URLResponsive:   dataHubURL != "",
		LastURLCheck:    time.Now(),
		LastHealthCheck: time.Now(),
		Storage:         "full", // Default test peers to full nodes
	}
}

// CreateTestLogger creates a test logger
func CreateTestLogger(t *testing.T) ulogger.Logger {
	return ulogger.New("test")
}

// CreateTestContext creates a test context with timeout
func CreateTestContext(timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), timeout)
}

// GenerateTestPeerIDs generates a list of test peer IDs
func GenerateTestPeerIDs(count int) []peer.ID {
	var ids []peer.ID
	for i := 0; i < count; i++ {
		// Use simple string IDs for testing
		id := peer.ID(string(rune('A' + i)))
		ids = append(ids, id)
	}
	return ids
}

// CreateTestPeerInfoList creates a list of test peers with varied attributes
func CreateTestPeerInfoList(count int) []*PeerInfo {
	peers := make([]*PeerInfo, count)
	ids := GenerateTestPeerIDs(count)

	for i := 0; i < count; i++ {
		peers[i] = &PeerInfo{
			ID:              ids[i],
			Height:          int32(100 + i*10),
			BlockHash:       "test-hash",
			DataHubURL:      "",
			IsHealthy:       i%2 == 0,     // Every other peer is healthy
			IsBanned:        i >= count-2, // Last two peers are banned
			BanScore:        i * 10,
			ConnectedAt:     time.Now().Add(-time.Duration(i) * time.Minute),
			BytesReceived:   uint64(i * 1000),
			LastBlockTime:   time.Now(),
			LastMessageTime: time.Now(),
			URLResponsive:   false,
			LastURLCheck:    time.Now(),
			LastHealthCheck: time.Now(),
			Storage:         "full", // Default test peers to full nodes
		}
	}

	// Give some peers DataHub URLs
	if count > 2 {
		peers[0].DataHubURL = "http://peer0.test"
		peers[0].URLResponsive = true
	}
	if count > 3 {
		peers[1].DataHubURL = "http://peer1.test"
		peers[1].URLResponsive = false // Has URL but not responsive
	}

	return peers
}

// TestBlockchainSetup holds test blockchain components
type TestBlockchainSetup struct {
	Client blockchain.ClientI
	Store  blockchain_store.Store
	Ctx    context.Context
	Cancel context.CancelFunc
}

// SetupTestBlockchain creates a real blockchain client with in-memory store for testing
func SetupTestBlockchain(t *testing.T) *TestBlockchainSetup {
	// Create context
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

	// Create in-memory store
	storeURL, err := url.Parse("sqlitememory://")
	require.NoError(t, err)

	// Create test settings
	tSettings := &settings.Settings{
		ChainCfgParams: &chaincfg.RegressionNetParams,
		P2P: settings.P2PSettings{
			BanThreshold: 100,
			BanDuration:  24 * time.Hour,
			DisableNAT:   true, // Disable NAT in tests to prevent data races in libp2p
		},
	}

	// Create blockchain store
	logger := ulogger.New("test-blockchain")
	blockchainStore, err := blockchain_store.NewStore(logger, storeURL, tSettings)
	require.NoError(t, err)

	// Create LocalClient for testing
	blockchainClient, err := blockchain.NewLocalClient(logger, tSettings, blockchainStore, nil, nil)
	require.NoError(t, err)

	return &TestBlockchainSetup{
		Client: blockchainClient,
		Store:  blockchainStore,
		Ctx:    ctx,
		Cancel: cancel,
	}
}

// SetFSMState sets the FSM to a specific state for testing
func (tbs *TestBlockchainSetup) SetFSMState(t *testing.T, state blockchain_api.FSMStateType) {
	// LocalClient returns RUNNING by default in GetFSMCurrentState
	// For testing purposes, we can simulate state transitions
	switch state {
	case blockchain_api.FSMStateType_RUNNING:
		// LocalClient defaults to RUNNING
		err := tbs.Client.Run(tbs.Ctx, "test")
		require.NoError(t, err)
	case blockchain_api.FSMStateType_CATCHINGBLOCKS:
		// LocalClient doesn't fully implement CatchUpBlocks
		// but we can use it for testing sync coordinator logic
		err := tbs.Client.Run(tbs.Ctx, "test")
		require.NoError(t, err)
	default:
		// IDLE is the default state
		err := tbs.Client.Idle(tbs.Ctx)
		require.NoError(t, err)
	}
}

// Cleanup cleans up test resources
func (tbs *TestBlockchainSetup) Cleanup() {
	if tbs.Cancel != nil {
		tbs.Cancel()
	}
}
