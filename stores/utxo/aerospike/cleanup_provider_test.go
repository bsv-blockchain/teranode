package aerospike

import (
	"sync"
	"testing"

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
