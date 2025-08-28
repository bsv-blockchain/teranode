package blockvalidation

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/services/blockvalidation/catchup"
	"github.com/bitcoin-sv/teranode/services/blockvalidation/testhelpers"
	"github.com/bitcoin-sv/teranode/services/validator"
	blobmemory "github.com/bitcoin-sv/teranode/stores/blob/memory"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/ulogger"
	testutil "github.com/bitcoin-sv/teranode/util/test"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/jellydator/ttlcache/v3"
	"github.com/ordishs/go-utils/expiringmap"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

// CatchupTestSuite provides a complete test environment for catchup tests
type CatchupTestSuite struct {
	T              *testing.T
	Ctx            context.Context
	Cancel         context.CancelFunc
	Server         *Server // Direct access to Server
	MockBlockchain *blockchain.Mock
	MockUTXOStore  *utxo.MockUtxostore
	MockValidator  *validator.MockValidatorClient
	HttpMock       *testhelpers.HTTPMockSetup
	Config         *testhelpers.CatchupServerConfig
	CleanupFuncs   []func()
	Logger         ulogger.Logger
}

// NewCatchupTestSuite creates a new test suite with default configuration
func NewCatchupTestSuite(t *testing.T) *CatchupTestSuite {
	config := testhelpers.DefaultCatchupServerConfig()
	return NewCatchupTestSuiteWithConfig(t, config)
}

// NewCatchupTestSuiteWithConfig creates a new test suite with custom configuration
func NewCatchupTestSuiteWithConfig(t *testing.T, config *testhelpers.CatchupServerConfig) *CatchupTestSuite {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

	suite := &CatchupTestSuite{
		T:            t,
		Ctx:          ctx,
		Cancel:       cancel,
		Config:       config,
		CleanupFuncs: make([]func(), 0),
		Logger:       ulogger.TestLogger{},
	}

	// Setup mocks
	suite.setupMocks()

	// Create server
	suite.createServer(t)

	return suite
}

// setupMocks initializes all mock objects
func (s *CatchupTestSuite) setupMocks() {
	s.MockBlockchain = &blockchain.Mock{}
	s.MockUTXOStore = &utxo.MockUtxostore{}
	s.MockValidator = &validator.MockValidatorClient{UtxoStore: s.MockUTXOStore}
	s.HttpMock = testhelpers.NewHTTPMockSetup(s.T)
}

// createServer creates the Server instance with all dependencies
func (s *CatchupTestSuite) createServer(t *testing.T) {
	// Initialize metrics for tests
	initPrometheusMetrics()

	// Create settings from config
	tSettings := testutil.CreateBaseTestSettings(t)
	if s.Config != nil {
		tSettings.BlockValidation.SecretMiningThreshold = uint32(s.Config.SecretMiningThreshold)
		tSettings.BlockValidation.CatchupMaxRetries = s.Config.MaxRetries
		tSettings.BlockValidation.CatchupIterationTimeout = 5 // Default
		tSettings.BlockValidation.CatchupOperationTimeout = s.Config.CatchupOperationTimeout
	}

	// Create BlockValidation instance
	bv := &BlockValidation{
		logger:                        s.Logger,
		settings:                      tSettings,
		blockchainClient:              s.MockBlockchain,
		blockHashesCurrentlyValidated: txmap.NewSwissMap(0),
		blockExists:                   expiringmap.New[chainhash.Hash, bool](120 * time.Minute),
		bloomFilterStats:              model.NewBloomStats(),
		utxoStore:                     s.MockUTXOStore,
		validatorClient:               s.MockValidator,
		lastValidatedBlocks:           expiringmap.New[chainhash.Hash, *model.Block](2 * time.Minute),
		recentBlocksBloomFilters:      txmap.NewSyncedMap[chainhash.Hash, *model.BlockBloomFilter](100),
		subtreeStore:                  blobmemory.New(),
		blockBloomFiltersBeingCreated: txmap.NewSwissMap(0),
	}

	// Create circuit breakers
	var circuitBreakers *catchup.PeerCircuitBreakers
	if s.Config != nil && s.Config.CircuitBreakerConfig != nil {
		circuitBreakers = catchup.NewPeerCircuitBreakers(*s.Config.CircuitBreakerConfig)
	} else {
		circuitBreakers = catchup.NewPeerCircuitBreakers(catchup.DefaultCircuitBreakerConfig())
	}

	// Create server
	s.Server = &Server{
		logger:               s.Logger,
		settings:             tSettings,
		blockFoundCh:         make(chan processBlockFound, 10),
		catchupCh:            make(chan processBlockCatchup, 10),
		validatorClient:      s.MockValidator,
		blockValidation:      bv,
		blockchainClient:     s.MockBlockchain,
		utxoStore:            s.MockUTXOStore,
		subtreeStore:         bv.subtreeStore,
		processSubtreeNotify: ttlcache.New[chainhash.Hash, bool](),
		stats:                gocore.NewStat("test"),
		peerMetrics: &catchup.CatchupMetrics{
			PeerMetrics: make(map[string]*catchup.PeerCatchupMetrics),
		},
		peerCircuitBreakers: circuitBreakers,
		headerChainCache:    catchup.NewHeaderChainCache(s.Logger),
		isCatchingUp:        atomic.Bool{},
		catchupAttempts:     atomic.Int64{},
		catchupSuccesses:    atomic.Int64{},
		catchupStatsMu:      sync.RWMutex{},
	}

	// Add cleanup for channels
	s.AddCleanup(func() {
		close(s.Server.blockFoundCh)
		close(s.Server.catchupCh)
	})
}

// Cleanup should be called with defer in every test
func (s *CatchupTestSuite) Cleanup() {
	// Run cleanup functions in reverse order
	for i := len(s.CleanupFuncs) - 1; i >= 0; i-- {
		s.CleanupFuncs[i]()
	}

	if s.HttpMock != nil {
		s.HttpMock.Deactivate()
	}

	if s.Cancel != nil {
		s.Cancel()
	}

	s.MockBlockchain.AssertExpectations(s.T)
	s.MockUTXOStore.AssertExpectations(s.T)
	s.Server.subtreeStore = nil // Clear the subtree store to release resources
}

// AddCleanup registers a cleanup function to run during Cleanup
func (s *CatchupTestSuite) AddCleanup(fn func()) {
	s.CleanupFuncs = append(s.CleanupFuncs, fn)
}

// RequireNoError is a helper that uses require.NoError
func (s *CatchupTestSuite) RequireNoError(err error, msgAndArgs ...interface{}) {
	require.NoError(s.T, err, msgAndArgs...)
}

// NewMockBuilder creates a mock builder for this suite
func (s *CatchupTestSuite) NewMockBuilder() *testhelpers.MockBuilder {
	return &testhelpers.MockBuilder{
		// Pass necessary fields from suite
		// This will be implemented in testhelpers
	}
}

// SetupScenario configures the suite for a specific test scenario
func SetupScenario(t *testing.T, scenario testhelpers.TestScenario) *CatchupTestSuite {
	config := testhelpers.GetScenarioConfig(scenario)

	suite := NewCatchupTestSuiteWithConfig(t, config.ServerConfig)

	// Run scenario-specific setup
	if config.SetupFunc != nil {
		// Convert the testhelpers.CatchupTestSuite to our CatchupTestSuite
		// For now, we'll need to pass the necessary fields
		helperSuite := &testhelpers.CatchupTestSuite{
			T:              t,
			Ctx:            suite.Ctx,
			Cancel:         suite.Cancel,
			MockBlockchain: suite.MockBlockchain,
			MockUTXOStore:  suite.MockUTXOStore,
			HttpMock:       suite.HttpMock,
			Config:         config.ServerConfig,
			Logger:         suite.Logger,
		}
		config.SetupFunc(helperSuite)
	}

	return suite
}

// AssertCircuitBreakerState checks the circuit breaker state for a peer
func (s *CatchupTestSuite) AssertCircuitBreakerState(peerURL string, expectedState catchup.CircuitBreakerState) {
	breaker := s.Server.peerCircuitBreakers.GetBreaker(peerURL)
	require.NotNil(s.T, breaker, "Circuit breaker not found for peer %s", peerURL)

	state, _, _, _ := breaker.GetStats()
	require.Equal(s.T, expectedState, state, "Circuit breaker state mismatch for %s", peerURL)
}
