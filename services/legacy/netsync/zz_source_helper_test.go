package netsync

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// readManagerSource returns the sync manager's source, for the small number of
// assertions that are about the shape of the block loop rather than its
// behaviour. Building that loop needs a dispatcher, park workers and a live block
// queue, and a test that assembled all three would be exercising those instead of
// the one property it means to pin.
func readManagerSource(t *testing.T) string {
	t.Helper()

	b, err := os.ReadFile("manager.go")
	require.NoError(t, err, "the test runs in the package directory, so the source is beside it")

	return string(b)
}
