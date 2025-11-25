package file

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSemaphoreDefaults verifies that the default semaphores are created in init()
func TestSemaphoreDefaults(t *testing.T) {
	// Verify the semaphores exist and were initialized
	require.NotNil(t, readSemaphore, "readSemaphore should be initialized")
	require.NotNil(t, writeSemaphore, "writeSemaphore should be initialized")

	// Note: golang.org/x/sync/semaphore.Weighted doesn't expose capacity,
	// so we can't verify the capacity directly. Checking non-nil is sufficient.
}
