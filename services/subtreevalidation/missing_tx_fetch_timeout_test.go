package subtreevalidation

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/stretchr/testify/require"
)

// TestMissingTransactionsFetchTimeoutFallback pins that an unset, zero or
// negative value, and a nil settings object, fall back to the setting's own
// default rather than a separate hard-coded value that can drift from it.
func TestMissingTransactionsFetchTimeoutFallback(t *testing.T) {
	require.Equal(t, settings.DefaultMissingTransactionsFetchTimeout, missingTransactionsFetchTimeout(nil))

	for _, configured := range []time.Duration{0, -time.Second} {
		s := &settings.Settings{}
		s.SubtreeValidation.MissingTransactionsFetchTimeout = configured

		require.Equal(t, settings.DefaultMissingTransactionsFetchTimeout, missingTransactionsFetchTimeout(s), "configured %s", configured)
	}
}
