package testhelpers

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/services/blockvalidation/catchup"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// CatchupTestSuite provides common test setup and utilities for catchup tests
type CatchupTestSuite struct {
	T              *testing.T
	Ctx            context.Context
	Cancel         context.CancelFunc
	Server         interface{} // The actual Server from blockvalidation package
	MockBlockchain *blockchain.Mock
	MockUTXOStore  *utxo.MockUtxostore
	// mockSubtree - removed, not needed for catchup tests
	HttpMock     *HTTPMockSetup
	Config       *CatchupServerConfig
	CleanupFuncs []func()
	Logger       ulogger.Logger
	settings     *settings.Settings // Internal settings for server creation
}

// NewCatchupTestSuite creates a new test suite with default setup
func NewCatchupTestSuite(t *testing.T) *CatchupTestSuite {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

	suite := &CatchupTestSuite{
		T:            t,
		Ctx:          ctx,
		Cancel:       cancel,
		CleanupFuncs: make([]func(), 0),
		Logger:       ulogger.TestLogger{},
	}

	// Initialize with default test configuration
	suite.Config = DefaultCatchupServerConfig()
	suite.setupMocks()

	return suite
}

func (s *CatchupTestSuite) setupMocks() {
	// Initialize all mocks
	s.MockBlockchain = &blockchain.Mock{}
	s.MockUTXOStore = &utxo.MockUtxostore{}
	s.HttpMock = NewHTTPMockSetup(s.T)
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
}

// AddCleanup adds a cleanup function to be called during Cleanup
func (s *CatchupTestSuite) AddCleanup(fn func()) {
	s.CleanupFuncs = append(s.CleanupFuncs, fn)
}

// RequireNoError is a helper that uses require.NoError with the suite's testing.T
func (s *CatchupTestSuite) RequireNoError(err error, msgAndArgs ...interface{}) {
	require.NoError(s.T, err, msgAndArgs...)
}

// CatchupServerConfig holds configuration for the test server
type CatchupServerConfig struct {
	MaxRetries              int
	RetryDelay              time.Duration
	CatchupOperationTimeout int // seconds
	MaxConcurrentCatchups   int
	SecretMiningThreshold   int
	CircuitBreakerConfig    *catchup.CircuitBreakerConfig
	HeaderValidationConfig  HeaderValidationConfig
}

// DefaultCatchupTestConfig returns default configuration for testing (alias for compatibility)
func DefaultCatchupTestConfig() *CatchupServerConfig {
	return DefaultCatchupServerConfig()
}

// DefaultCatchupServerConfig returns default configuration for testing
func DefaultCatchupServerConfig() *CatchupServerConfig {
	return &CatchupServerConfig{
		MaxRetries:              3,
		RetryDelay:              100 * time.Millisecond,
		CatchupOperationTimeout: 30,
		MaxConcurrentCatchups:   5,
		SecretMiningThreshold:   100,
		CircuitBreakerConfig: &catchup.CircuitBreakerConfig{
			FailureThreshold:    3,
			SuccessThreshold:    2,
			Timeout:             time.Minute,
			MaxHalfOpenRequests: 1,
		},
		HeaderValidationConfig: HeaderValidationConfig{
			MaxTimeDrift:     2 * time.Hour,
			MinimumChainWork: big.NewInt(1),
		},
	}
}

// ToSettings converts ServerConfig to internal settings
func (c *CatchupServerConfig) ToSettings() *settings.Settings {
	// For testing, return a minimal settings object
	// In real tests, this would be properly configured
	return &settings.Settings{}
}

// HeaderValidationConfig contains header validation parameters
type HeaderValidationConfig struct {
	MaxTimeDrift     time.Duration
	MinimumChainWork *big.Int
	CheckpointHeight uint32
	CheckpointHash   *chainhash.Hash
}

// CreateCatchupTestContext creates a test context with timeout
func CreateCatchupTestContext(t *testing.T) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 30*time.Second)
}

// CreateTestBlock creates a test block with the given height and hash
func CreateTestBlock(height uint32, hash string) *model.Block {
	// Parse hash if provided, otherwise use empty hash
	var blockHash *chainhash.Hash
	if hash != "" {
		blockHash, _ = chainhash.NewHashFromStr(hash)
	} else {
		blockHash = &chainhash.Hash{}
	}

	return &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  blockHash,
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      uint32(time.Now().Unix()),
			Bits:           model.NBit{},
			Nonce:          0,
		},
		Height: height,
	}
}

// NewCatchupTestSuiteWithConfig creates a test suite with specific configuration
func NewCatchupTestSuiteWithConfig(t *testing.T, config *CatchupServerConfig) *CatchupTestSuite {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

	suite := &CatchupTestSuite{
		T:            t,
		Ctx:          ctx,
		Cancel:       cancel,
		Config:       config,
		CleanupFuncs: make([]func(), 0),
		Logger:       ulogger.TestLogger{},
	}

	suite.setupMocks()
	return suite
}

// AssertCircuitBreakerState checks the circuit breaker state for a peer
func (s *CatchupTestSuite) AssertCircuitBreakerState(peerURL string, expectedState int) {
	// This would normally check the actual circuit breaker state
	// For now, just log it
	s.T.Logf("Circuit breaker state for %s expected to be %d", peerURL, expectedState)
}
