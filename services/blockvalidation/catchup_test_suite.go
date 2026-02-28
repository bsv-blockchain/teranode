package blockvalidation

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/catchup"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/bsv-blockchain/teranode/services/validator"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	testutil "github.com/bsv-blockchain/teranode/util/test"
	"github.com/jellydator/ttlcache/v3"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/mock"
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

	// Provide a permissive default for Spend to avoid unexpected calls from concurrent validation goroutines.
	s.MockUTXOStore.On("Spend", mock.Anything, mock.Anything, mock.Anything).Return([]*utxo.Spend{}, nil).Maybe()
}

// createServer creates the Server instance with all dependencies
func (s *CatchupTestSuite) createServer(t *testing.T) {
	// Initialize metrics for tests
	initPrometheusMetrics()

	// Create settings from config
	tSettings := testutil.CreateBaseTestSettings(t)
	if s.Config != nil {
		tSettings.BlockValidation.SecretMiningThreshold = uint32(s.Config.SecretMiningThreshold)
		// Use CatchupOperationTimeout as iteration timeout if set, otherwise default to 5
		if s.Config.CatchupOperationTimeout > 0 {
			tSettings.BlockValidation.CatchupIterationTimeout = s.Config.CatchupOperationTimeout
		} else {
			tSettings.BlockValidation.CatchupIterationTimeout = 5
			tSettings.BlockValidation.CatchupOperationTimeout = s.Config.CatchupOperationTimeout
		}
	}

	// Create BlockValidation instance
	bv := &BlockValidation{
		logger:                        s.Logger,
		settings:                      tSettings,
		blockchainClient:              s.MockBlockchain,
		blockHashesCurrentlyValidated: txmap.NewSwissMap(0),
		blockExistsCache:              expiringmap.New[chainhash.Hash, bool](120 * time.Minute),
		utxoStore:                     s.MockUTXOStore,
		validatorClient:               s.MockValidator,
		lastValidatedBlocks:           expiringmap.New[chainhash.Hash, *model.Block](2 * time.Minute),
		subtreeStore:                  blobmemory.New(),
		blocksCurrentlyValidating:     txmap.NewSyncedMap[chainhash.Hash, *validationResult](),
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
		logger:              s.Logger,
		settings:            tSettings,
		blockFoundCh:        make(chan processBlockFound, 10),
		catchupCh:           make(chan processBlockCatchup, 10),
		validatorClient:     s.MockValidator,
		blockValidation:     bv,
		blockchainClient:    s.MockBlockchain,
		utxoStore:           s.MockUTXOStore,
		subtreeStore:        bv.subtreeStore,
		processBlockNotify:  ttlcache.New[chainhash.Hash, bool](),
		catchupAlternatives: ttlcache.New[chainhash.Hash, []processBlockCatchup](),
		stats:               gocore.NewStat("test"),
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
	// Stop the processBlockNotify cache if it was created and started
	if s.Server != nil && s.Server.processBlockNotify != nil {
		// Use a goroutine with timeout to prevent blocking forever
		// if the cache was never started
		done := make(chan struct{})
		go func() {
			s.Server.processBlockNotify.Stop()
			close(done)
		}()

		select {
		case <-done:
			// Successfully stopped
		case <-time.After(100 * time.Millisecond):
			// Timeout - cache was likely never started
		}
	}

	// Stop expiring map background goroutines
	if s.Server != nil && s.Server.blockValidation != nil {
		s.Server.blockValidation.blockExistsCache.Stop()
		s.Server.blockValidation.lastValidatedBlocks.Stop()
	}

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
func (s *CatchupTestSuite) AssertCircuitBreakerState(peerID string, expectedState catchup.CircuitBreakerState) {
	breaker := s.Server.peerCircuitBreakers.GetBreaker(peerID)
	require.NotNil(s.T, breaker, "Circuit breaker not found for peer %s", peerID)

	state, _, _, _ := breaker.GetStats()
	require.Equal(s.T, expectedState, state, "Circuit breaker state mismatch for %s", peerID)
}
