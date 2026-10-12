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

// TestAppliedDefaultsBeforeInit pins that the recorded limits describe the
// concurrency genuinely in force before InitSemaphores runs — the defaults the
// semaphores in init() were built with. Reporting zeros here would name a
// concurrency that applies nowhere, and a repeat call to InitSemaphores returns
// these values.
//
// Nothing else in this package calls InitSemaphores, so this holds whatever
// order the tests run in.
func TestAppliedDefaultsBeforeInit(t *testing.T) {
	require.Equal(t, defaultReadLimit, applied.Read)
	require.Equal(t, defaultWriteLimit, applied.Write)
	require.False(t, applied.Clamped, "no clamp can have happened before InitSemaphores")
}
