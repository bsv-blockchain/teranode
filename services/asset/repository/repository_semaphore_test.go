package repository_test

import (
	"context"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/services/asset/repository"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestSemaphoreUnlimitedByDefault verifies that when concurrency is 0 (default),
// no semaphore is created and operations proceed without limits
func TestSemaphoreUnlimitedByDefault(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	settings := test.CreateBaseTestSettings(t)

	// Default settings should have all concurrency values at 0 (unlimited)
	require.Equal(t, 0, settings.Asset.ConcurrencyGetTransaction)
	require.Equal(t, 0, settings.Asset.ConcurrencyGetSubtreeData)

	txStore := getMemoryStore(t)
	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, settings, utxoStoreURL)
	require.NoError(t, err)

	blockchainStore, err := blockchain_store.NewStore(logger, &url.URL{Scheme: "sqlitememory"}, settings)
	require.NoError(t, err)

	blockchainClient, err := blockchain.NewLocalClient(logger, settings, blockchainStore, nil, nil)
	require.NoError(t, err)

	repo, err := repository.NewRepository(
		logger,
		settings,
		utxoStore,
		txStore,
		blockchainClient,
		nil,
		txStore,
		txStore,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, repo)

	// Verify we can make multiple concurrent calls without blocking
	// This should complete quickly since there's no semaphore
	concurrency := 100
	var wg sync.WaitGroup
	wg.Add(concurrency)

	start := time.Now()
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			// Call a method that would use semaphore if configured
			hash := chainhash.HashH([]byte("test"))
			_, _ = repo.GetTransaction(ctx, &hash)
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	// All calls should complete quickly (well under 1 second) since there's no throttling
	require.Less(t, elapsed, 1*time.Second, "Unlimited concurrency should complete quickly")
}

// TestSemaphoreLimitsEnforced verifies that configured concurrency limits are respected
func TestSemaphoreLimitsEnforced(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	settings := test.CreateBaseTestSettings(t)

	// Set a low concurrency limit
	settings.Asset.ConcurrencyGetTransaction = 2

	txStore := getMemoryStore(t)
	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, settings, utxoStoreURL)
	require.NoError(t, err)

	blockchainStore, err := blockchain_store.NewStore(logger, &url.URL{Scheme: "sqlitememory"}, settings)
	require.NoError(t, err)

	blockchainClient, err := blockchain.NewLocalClient(logger, settings, blockchainStore, nil, nil)
	require.NoError(t, err)

	repo, err := repository.NewRepository(
		logger,
		settings,
		utxoStore,
		txStore,
		blockchainClient,
		nil,
		txStore,
		txStore,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, repo)

	// With a limit of 2, launching many concurrent requests should work
	// but may take longer than unlimited. The key is that it doesn't deadlock.
	concurrency := 10
	var wg sync.WaitGroup
	wg.Add(concurrency)

	start := time.Now()
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			hash := chainhash.HashH([]byte("test"))
			_, _ = repo.GetTransaction(ctx, &hash)
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	// All calls should complete (no deadlock) but may take longer than unlimited
	// This is mainly a smoke test to ensure semaphore doesn't cause deadlocks
	require.Less(t, elapsed, 5*time.Second, "Semaphore should not cause excessive delays or deadlocks")
}

// TestSemaphoreContextCancellation verifies proper handling of context cancellation
func TestSemaphoreContextCancellation(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	settings := test.CreateBaseTestSettings(t)

	// Set a very low limit to force blocking
	settings.Asset.ConcurrencyGetTransaction = 1

	txStore := getMemoryStore(t)
	ctx := context.Background()

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, settings, utxoStoreURL)
	require.NoError(t, err)

	blockchainStore, err := blockchain_store.NewStore(logger, &url.URL{Scheme: "sqlitememory"}, settings)
	require.NoError(t, err)

	blockchainClient, err := blockchain.NewLocalClient(logger, settings, blockchainStore, nil, nil)
	require.NoError(t, err)

	repo, err := repository.NewRepository(
		logger,
		settings,
		utxoStore,
		txStore,
		blockchainClient,
		nil,
		txStore,
		txStore,
		nil,
	)
	require.NoError(t, err)

	// Create a context that we'll cancel
	cancelCtx, cancel := context.WithCancel(ctx)

	// Cancel immediately
	cancel()

	// Try to acquire semaphore with canceled context
	hash := chainhash.HashH([]byte("test"))
	_, err = repo.GetTransaction(cancelCtx, &hash)

	// Should get context canceled error
	require.Error(t, err)
	require.Contains(t, err.Error(), "canceled", "Expected context canceled error")
}

// TestSemaphorePerMethodIndependence verifies that different methods have independent semaphores
func TestSemaphorePerMethodIndependence(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	settings := test.CreateBaseTestSettings(t)

	// Set limit on one method but not another
	settings.Asset.ConcurrencyGetTransaction = 1
	settings.Asset.ConcurrencyGetTransactionMeta = 0 // Unlimited

	txStore := getMemoryStore(t)
	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, settings, utxoStoreURL)
	require.NoError(t, err)

	blockchainStore, err := blockchain_store.NewStore(logger, &url.URL{Scheme: "sqlitememory"}, settings)
	require.NoError(t, err)

	blockchainClient, err := blockchain.NewLocalClient(logger, settings, blockchainStore, nil, nil)
	require.NoError(t, err)

	repo, err := repository.NewRepository(
		logger,
		settings,
		utxoStore,
		txStore,
		blockchainClient,
		nil,
		txStore,
		txStore,
		nil,
	)
	require.NoError(t, err)

	// Both GetTransaction (limited) and GetTransactionMeta (unlimited) should work
	// GetTransactionMeta calls should not be blocked by GetTransaction's semaphore
	hash := chainhash.HashH([]byte("test"))

	var wg sync.WaitGroup
	wg.Add(2)

	// Call GetTransaction (will acquire semaphore)
	go func() {
		defer wg.Done()
		_, _ = repo.GetTransaction(ctx, &hash)
	}()

	// Call GetTransactionMeta concurrently (should not block)
	go func() {
		defer wg.Done()
		_, _ = repo.GetTransactionMeta(ctx, &hash)
	}()

	wg.Wait()

	// Test passes if both complete without deadlock
}

// TestSemaphoreNumCPU verifies that -1 value uses runtime.NumCPU()
func TestSemaphoreNumCPU(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	settings := test.CreateBaseTestSettings(t)

	// Set to -1 to use NumCPU()
	settings.Asset.ConcurrencyGetTransaction = -1

	txStore := getMemoryStore(t)
	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, settings, utxoStoreURL)
	require.NoError(t, err)

	blockchainStore, err := blockchain_store.NewStore(logger, &url.URL{Scheme: "sqlitememory"}, settings)
	require.NoError(t, err)

	blockchainClient, err := blockchain.NewLocalClient(logger, settings, blockchainStore, nil, nil)
	require.NoError(t, err)

	repo, err := repository.NewRepository(
		logger,
		settings,
		utxoStore,
		txStore,
		blockchainClient,
		nil,
		txStore,
		txStore,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, repo)

	// We can't directly verify the semaphore size, but we can verify
	// that the repository was created successfully with -1 config
	// The actual NumCPU() value will be logged during initialization
}
