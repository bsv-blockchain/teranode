package factory

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// MockUTXOStore implements the utxo.Store interface for testing
type MockUTXOStore struct {
	mock.Mock
}

func (m *MockUTXOStore) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	args := m.Called(ctx, checkLiveness)
	return args.Int(0), args.String(1), args.Error(2)
}

func (m *MockUTXOStore) SetBlockHeight(height uint32) error {
	args := m.Called(height)
	return args.Error(0)
}

func (m *MockUTXOStore) SetMedianBlockTime(time uint32) error {
	args := m.Called(time)
	return args.Error(0)
}

func (m *MockUTXOStore) GetBlockHeight() uint32 {
	args := m.Called()
	return args.Get(0).(uint32)
}

func (m *MockUTXOStore) GetMedianBlockTime() uint32 {
	args := m.Called()
	return args.Get(0).(uint32)
}

func (m *MockUTXOStore) GetBlockState() utxo.BlockState {
	return utxo.BlockState{
		Height:     m.GetBlockHeight(),
		MedianTime: m.GetMedianBlockTime(),
	}
}

// Implement remaining interface methods as no-ops for testing
func (m *MockUTXOStore) Create(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...utxo.CreateOption) (*meta.Data, error) {
	return nil, nil
}

func (m *MockUTXOStore) Get(ctx context.Context, hash *chainhash.Hash, fields ...fields.FieldName) (*meta.Data, error) {
	return nil, nil
}

func (m *MockUTXOStore) Delete(ctx context.Context, hash *chainhash.Hash) error {
	return nil
}

func (m *MockUTXOStore) GetSpend(ctx context.Context, spend *utxo.Spend) (*utxo.SpendResponse, error) {
	return nil, nil
}

func (m *MockUTXOStore) GetMeta(ctx context.Context, hash *chainhash.Hash, data *meta.Data) error {
	return nil
}

func (m *MockUTXOStore) Spend(ctx context.Context, tx *bt.Tx, blockHeight uint32, ignoreFlags ...utxo.IgnoreFlags) ([]*utxo.Spend, error) {
	return nil, nil
}

func (m *MockUTXOStore) Unspend(ctx context.Context, spends []*utxo.Spend, flagAsLocked ...bool) error {
	return nil
}

func (m *MockUTXOStore) SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, minedBlockInfo utxo.MinedBlockInfo) (map[chainhash.Hash][]uint32, error) {
	return nil, nil
}

func (m *MockUTXOStore) GetUnminedTxIterator(bool) (utxo.UnminedTxIterator, error) {
	return nil, nil
}

func (m *MockUTXOStore) QueryOldUnminedTransactions(ctx context.Context, cutoffBlockHeight uint32) ([]chainhash.Hash, error) {
	return nil, nil
}

func (m *MockUTXOStore) PreserveTransactions(ctx context.Context, txIDs []chainhash.Hash, preserveUntilHeight uint32) error {
	return nil
}

func (m *MockUTXOStore) ProcessExpiredPreservations(ctx context.Context, currentHeight uint32) error {
	return nil
}

func (m *MockUTXOStore) BatchDecorate(ctx context.Context, unresolvedMetaDataSlice []*utxo.UnresolvedMetaData, fields ...fields.FieldName) error {
	return nil
}

func (m *MockUTXOStore) PreviousOutputsDecorate(ctx context.Context, tx *bt.Tx) error {
	return nil
}

func (m *MockUTXOStore) FreezeUTXOs(ctx context.Context, spends []*utxo.Spend, tSettings *settings.Settings) error {
	return nil
}

func (m *MockUTXOStore) UnFreezeUTXOs(ctx context.Context, spends []*utxo.Spend, tSettings *settings.Settings) error {
	return nil
}

func (m *MockUTXOStore) ReAssignUTXO(ctx context.Context, utxo *utxo.Spend, newUtxo *utxo.Spend, tSettings *settings.Settings) error {
	return nil
}

func (m *MockUTXOStore) GetCounterConflicting(ctx context.Context, txHash chainhash.Hash) ([]chainhash.Hash, error) {
	return nil, nil
}

func (m *MockUTXOStore) GetConflictingChildren(ctx context.Context, txHash chainhash.Hash) ([]chainhash.Hash, error) {
	return nil, nil
}

func (m *MockUTXOStore) SetConflicting(ctx context.Context, txHashes []chainhash.Hash, value bool) ([]*utxo.Spend, []chainhash.Hash, error) {
	return nil, nil, nil
}

func (m *MockUTXOStore) SetLocked(ctx context.Context, txHashes []chainhash.Hash, value bool) error {
	return nil
}

func (m *MockUTXOStore) MarkTransactionsOnLongestChain(ctx context.Context, txHashes []chainhash.Hash, onLongestChain bool) error {
	args := m.Called(ctx, txHashes, onLongestChain)
	return args.Error(0)
}

// TestNewStore_UnknownScheme tests handling of unknown database scheme
func TestNewStore_UnknownScheme(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	// Set unknown scheme
	unknownURL, err := url.Parse("unknown://localhost:3000/test")
	require.NoError(t, err)
	tSettings.UtxoStore.UtxoStore = unknownURL

	store, err := NewStore(ctx, logger, tSettings, "test-source")

	assert.Nil(t, store)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "utxostore: unknown scheme: unknown")
}

// TestNewStore_NoPortSpecified tests URL without port (empty port case)
func TestNewStore_NoPortSpecified(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	// Register mock database initializer
	mockStore := &MockUTXOStore{}
	availableDatabases["memory"] = func(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, url *url.URL) (utxo.Store, error) {
		return mockStore, nil
	}
	defer delete(availableDatabases, "memory") // Clean up after test

	// Create URL without port - this should succeed
	noPortURL, err := url.Parse("memory://localhost/test")
	require.NoError(t, err)
	require.Equal(t, "", noPortURL.Port(), "Expected no port")
	tSettings.UtxoStore.UtxoStore = noPortURL

	store, err := NewStore(ctx, logger, tSettings, "test-source", false)

	assert.NotNil(t, store)
	assert.NoError(t, err)
}

// TestNewStore_WithMemoryScheme tests memory database creation
func TestNewStore_WithMemoryScheme(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	// Register mock database initializer
	mockStore := &MockUTXOStore{}
	availableDatabases["memory"] = func(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, url *url.URL) (utxo.Store, error) {
		return mockStore, nil
	}
	defer delete(availableDatabases, "memory") // Clean up after test

	// Set memory scheme
	memoryURL, err := url.Parse("memory://localhost:3000/test")
	require.NoError(t, err)
	tSettings.UtxoStore.UtxoStore = memoryURL

	// Disable blockchain listener for this test
	store, err := NewStore(ctx, logger, tSettings, "test-source", false)

	assert.NotNil(t, store)
	assert.NoError(t, err)
	assert.Equal(t, mockStore, store)
}

// TestNewStore_WithLogging tests store creation with logging enabled
func TestNewStore_WithLogging(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	// Register mock database initializer
	mockStore := &MockUTXOStore{}
	availableDatabases["memory"] = func(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, url *url.URL) (utxo.Store, error) {
		return mockStore, nil
	}
	defer delete(availableDatabases, "memory") // Clean up after test

	// Set memory scheme with logging
	memoryURL, err := url.Parse("memory://localhost:3000/test?logging=true")
	require.NoError(t, err)
	tSettings.UtxoStore.UtxoStore = memoryURL

	// Disable blockchain listener for this test
	store, err := NewStore(ctx, logger, tSettings, "test-source", false)

	assert.NotNil(t, store)
	assert.NoError(t, err)
	// Store should be wrapped with logger, so it won't be equal to mockStore directly
	assert.NotEqual(t, mockStore, store)
}

// TestNewStore_DatabaseInitError tests handling of database initialization errors
func TestNewStore_DatabaseInitError(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	// Register failing database initializer
	initError := errors.NewProcessingError("database connection failed")
	availableDatabases["failing"] = func(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, url *url.URL) (utxo.Store, error) {
		return nil, initError
	}
	defer delete(availableDatabases, "failing") // Clean up after test

	// Set failing scheme
	failingURL, err := url.Parse("failing://localhost:3000/test")
	require.NoError(t, err)
	tSettings.UtxoStore.UtxoStore = failingURL

	store, err := NewStore(ctx, logger, tSettings, "test-source", false)

	assert.Nil(t, store)
	assert.Error(t, err)
	assert.Equal(t, initError, err)
}

// TestNewStore_EmptyPort tests handling of URL without port
func TestNewStore_EmptyPort(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	// Register mock database initializer
	mockStore := &MockUTXOStore{}
	availableDatabases["memory"] = func(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, url *url.URL) (utxo.Store, error) {
		return mockStore, nil
	}
	defer delete(availableDatabases, "memory") // Clean up after test

	// Set memory scheme without port
	memoryURL, err := url.Parse("memory://localhost/test")
	require.NoError(t, err)
	tSettings.UtxoStore.UtxoStore = memoryURL

	// Disable blockchain listener for this test
	store, err := NewStore(ctx, logger, tSettings, "test-source", false)

	assert.NotNil(t, store)
	assert.NoError(t, err)
}

// TestNewStore_BlockchainClientError tests handling of blockchain client creation error
func TestNewStore_BlockchainClientError(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	// Register mock database initializer
	mockStore := &MockUTXOStore{}
	mockStore.On("SetBlockHeight", mock.Anything).Return(nil).Maybe()
	mockStore.On("SetMedianBlockTime", mock.Anything).Return(nil).Maybe()

	availableDatabases["memory"] = func(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, url *url.URL) (utxo.Store, error) {
		return mockStore, nil
	}
	defer delete(availableDatabases, "memory") // Clean up after test

	// Set invalid blockchain settings to cause blockchain client creation error
	// blockchain.NewClient requires GRPCAddress to be set, so we clear it to trigger an error
	originalGRPCAddress := tSettings.BlockChain.GRPCAddress
	tSettings.BlockChain.GRPCAddress = ""
	defer func() { tSettings.BlockChain.GRPCAddress = originalGRPCAddress }()

	// Set memory scheme
	memoryURL, err := url.Parse("memory://localhost:3000/test")
	require.NoError(t, err)
	tSettings.UtxoStore.UtxoStore = memoryURL

	// Enable blockchain listener (default behavior)
	store, err := NewStore(ctx, logger, tSettings, "test-source")

	assert.Nil(t, store)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no blockchain_grpcAddress setting found")
}

// TestNewStore_DefaultBlockchainListenerBehavior tests default blockchain listener behavior
func TestNewStore_DefaultBlockchainListenerBehavior(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	// Register mock database initializer
	mockStore := &MockUTXOStore{}
	availableDatabases["memory"] = func(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, url *url.URL) (utxo.Store, error) {
		return mockStore, nil
	}
	defer delete(availableDatabases, "memory") // Clean up after test

	// Set invalid blockchain settings to ensure we hit blockchain client creation
	// blockchain.NewClient requires GRPCAddress to be set, so we clear it to trigger an error
	originalGRPCAddress := tSettings.BlockChain.GRPCAddress
	tSettings.BlockChain.GRPCAddress = ""
	defer func() { tSettings.BlockChain.GRPCAddress = originalGRPCAddress }()

	// Set memory scheme
	memoryURL, err := url.Parse("memory://localhost:3000/test")
	require.NoError(t, err)
	tSettings.UtxoStore.UtxoStore = memoryURL

	// Test default behavior (should try to start blockchain listener)
	store, err := NewStore(ctx, logger, tSettings, "test-source")

	assert.Nil(t, store)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no blockchain_grpcAddress setting found")
}

// TestNewStore_MultipleStartBlockchainListenerParams tests multiple parameters for blockchain listener
func TestNewStore_MultipleStartBlockchainListenerParams(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	// Register mock database initializer
	mockStore := &MockUTXOStore{}
	availableDatabases["memory"] = func(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, url *url.URL) (utxo.Store, error) {
		return mockStore, nil
	}
	defer delete(availableDatabases, "memory") // Clean up after test

	// Set memory scheme
	memoryURL, err := url.Parse("memory://localhost:3000/test")
	require.NoError(t, err)
	tSettings.UtxoStore.UtxoStore = memoryURL

	// Test with multiple parameters - only first one should be used
	store, err := NewStore(ctx, logger, tSettings, "test-source", false, true, false)

	assert.NotNil(t, store)
	assert.NoError(t, err)
	assert.Equal(t, mockStore, store)
}

// TestAvailableDatabases_GlobalVariable tests the global availableDatabases variable
func TestAvailableDatabases_GlobalVariable(t *testing.T) {
	// Record initial state
	initialLen := len(availableDatabases)

	// Test that we can add to it
	testFunc := func(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, url *url.URL) (utxo.Store, error) {
		return nil, nil
	}

	availableDatabases["test"] = testFunc
	assert.Equal(t, initialLen+1, len(availableDatabases))
	assert.NotNil(t, availableDatabases["test"])

	// Clean up
	delete(availableDatabases, "test")
	assert.Equal(t, initialLen, len(availableDatabases))
}
