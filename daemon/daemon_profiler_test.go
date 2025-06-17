package daemon

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDatadogProfiler tests the datadogProfiler function to ensure it starts the profiler without panicking and returns a valid stop function.
func TestDatadogProfiler(t *testing.T) {
	// Ensure the profiler starts without panicking and returns a valid stop function.
	require.NotPanics(t, func() {
		stopFunc := datadogProfiler()
		require.NotNil(t, stopFunc)

		// Call the returned stop function and make sure that it, too, doesn't panic.
		require.NotPanics(t, func() {
			stopFunc()
		})
	})
}
