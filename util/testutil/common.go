package testutil

import (
	"context"
	"net/url"
	"testing"

	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/blob/memory"
	blockchainstore "github.com/bitcoin-sv/teranode/stores/blockchain"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/stores/utxo/sql"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// CommonTestSetup provides the standard test context used across services
type CommonTestSetup struct {
	Ctx      context.Context
	Logger   ulogger.Logger
	Settings *settings.Settings
}

// NewCommonTestSetup creates the basic test infrastructure used by most service tests
func NewCommonTestSetup(t *testing.T) *CommonTestSetup {
	return &CommonTestSetup{
		Ctx:      context.Background(),
		Logger:   ulogger.NewErrorTestLogger(t),
		Settings: test.CreateBaseTestSettings(t),
	}
}

// NewMemoryBlobStore creates a memory blob store - commonly used pattern
func NewMemoryBlobStore() *memory.Memory {
	return memory.New()
}

// NewSQLiteMemoryUTXOStore creates the standard SQLite memory UTXO store used in most tests
func NewSQLiteMemoryUTXOStore(ctx context.Context, logger ulogger.Logger, settings *settings.Settings, t *testing.T) utxo.Store {
	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	var utxoStore utxo.Store
	utxoStore, err = sql.New(ctx, logger, settings, utxoStoreURL)
	require.NoError(t, err)

	return utxoStore
}

// NewMemorySQLiteBlockchainClient creates a real blockchain client with memory SQLite store
// This replaces the need for mock blockchain clients in most tests
func NewMemorySQLiteBlockchainClient(logger ulogger.Logger, settings *settings.Settings, t *testing.T) blockchain.ClientI {
	storeURL, err := url.Parse("sqlitememory://")
	require.NoError(t, err)

	var blockchainStore blockchainstore.Store
	blockchainStore, err = blockchainstore.NewStore(logger, storeURL, settings)
	require.NoError(t, err)

	var blockchainClient blockchain.ClientI
	blockchainClient, err = blockchain.NewLocalClient(logger, blockchainStore, nil, nil)
	require.NoError(t, err)

	return blockchainClient
}

// AssertHealthResponse provides common health check assertions that handle both strict and flexible status codes
func AssertHealthResponse(t *testing.T, status int, msg string, err error, expectStatus int, expectErr bool) {
	if expectErr {
		require.Error(t, err)
	} else {
		require.NoError(t, err)
	}

	if expectStatus == -1 {
		// Flexible status check - accept 200 or 503
		require.True(t, status == 200 || status == 503, "Expected status 200 or 503, got %d", status)
	} else {
		require.Equal(t, expectStatus, status)
	}

	require.NotEmpty(t, msg)
}
