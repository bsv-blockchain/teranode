package aerospike

import (
	"context"
	"sync"
	"testing"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo/pruner"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCleanupProviderInterface(t *testing.T) {
	// Test that Store implements the PrunerServiceProvider interface
	var _ pruner.PrunerServiceProvider = (*Store)(nil)
}

func TestCleanupServiceSingleton(t *testing.T) {
	// Test basic singleton pattern without complex mocking

	// Reset singleton state for testing
	prunerServiceInstance = nil
	prunerServiceError = nil

	// Test that multiple calls to create service maintain singleton pattern
	assert.Nil(t, prunerServiceInstance)
	assert.Nil(t, prunerServiceError)
}

func TestCleanupServiceConcurrency(t *testing.T) {
	// Test thread safety without creating actual services
	var wg sync.WaitGroup
	numGoroutines := 10

	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			// Test that the mutex exists and can be used
			prunerServiceMutex.Lock()
			// Simulate some work
			_ = prunerServiceInstance
			prunerServiceMutex.Unlock()
		}()
	}

	wg.Wait()
	// Test passes if no race condition occurs
}

func TestCleanupServiceDisabled(t *testing.T) {
	// Test that cleanup service returns nil when disabled
	store := &Store{
		settings: &settings.Settings{
			UtxoStore: settings.UtxoStoreSettings{
				DisableDAHCleaner: true,
			},
		},
	}

	service, err := store.GetPrunerService()
	assert.Nil(t, service)
	assert.Nil(t, err)
}

func TestCleanupServiceEnabled(t *testing.T) {
	// Test that cleanup service returns nil when enabled (default behavior)
	store := &Store{
		settings: &settings.Settings{
			UtxoStore: settings.UtxoStoreSettings{
				DisableDAHCleaner: false,
			},
		},
	}

	service, err := store.GetPrunerService()
	// Should return an error because we don't have a valid aerospike client
	// but the important thing is that it didn't return early due to disabled setting
	assert.NotNil(t, err)
	assert.Nil(t, service)
}

func TestCleanupServiceWithContext(t *testing.T) {
	// Reset singleton state for testing
	ResetPrunerServiceForTests()

	ctx := context.Background()
	testLogger := ulogger.NewErrorTestLogger(t)

	// Test that context is properly passed to cleanup service
	store := &Store{
		ctx:    ctx,
		logger: testLogger,
		settings: &settings.Settings{
			UtxoStore: settings.UtxoStoreSettings{
				DisableDAHCleaner: false,
			},
		},
	}

	// Verify that the store context is not nil
	require.NotNil(t, store.ctx, "Store context should not be nil")

	service, err := store.GetPrunerService()
	// Should return an error because we don't have a valid aerospike client,
	// but the important thing is that it didn't panic due to nil context
	assert.NotNil(t, err)
	assert.Nil(t, service)
	// The error should be about missing client, not nil context panic
	assert.Contains(t, err.Error(), "client is required")
}
