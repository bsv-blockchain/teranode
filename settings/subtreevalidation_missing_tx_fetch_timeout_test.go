package settings

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestMissingTransactionsFetchTimeoutDefault pins the default at 10m: the
// announcement path already shares one streaming window per batch today
// (http_streaming_timeout, 10m in settings.conf), and this is the first
// per-batch cap on the block-validation path, which previously had none
// short of the much larger 30m gRPC deadline on the whole call. A shorter
// default (e.g. 5m) would tighten today's announcement-path window and could
// fail a slow but honest peer mid-batch.
func TestMissingTransactionsFetchTimeoutDefault(t *testing.T) {
	require.Equal(t, 10*time.Minute, NewSettings().SubtreeValidation.MissingTransactionsFetchTimeout)

	// NewSettings reads settings.conf, so the line above can't see the Go fallback
	// that a missing or non-positive value uses. Pin the constant itself.
	require.Equal(t, 10*time.Minute, DefaultMissingTransactionsFetchTimeout)
}
