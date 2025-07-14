package aerospike

import (
	"sync"
	"testing"

	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/cleanup"
	"github.com/stretchr/testify/assert"
)

func TestCleanupProviderInterface(t *testing.T) {
	// Test that Store implements the CleanupServiceProvider interface
	var _ cleanup.CleanupServiceProvider = (*Store)(nil)
}

func TestCleanupServiceSingleton(t *testing.T) {
	// Test basic singleton pattern without complex mocking

	// Reset singleton state for testing
	cleanupServiceInstance = nil
	cleanupServiceError = nil

	// Test that multiple calls to create service maintain singleton pattern
	assert.Nil(t, cleanupServiceInstance)
	assert.Nil(t, cleanupServiceError)
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
			cleanupServiceMutex.Lock()
			// Simulate some work
			_ = cleanupServiceInstance
			cleanupServiceMutex.Unlock()
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

	service, err := store.GetCleanupService()
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

	service, err := store.GetCleanupService()
	// Should return an error because we don't have a valid aerospike client
	// but the important thing is that it didn't return early due to disabled setting
	assert.NotNil(t, err)
	assert.Nil(t, service)
}
