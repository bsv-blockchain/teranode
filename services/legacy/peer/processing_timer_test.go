package peer

import (
	"testing"

	"github.com/bsv-blockchain/go-wire"
	"github.com/stretchr/testify/require"
)

// TestShouldArmProcessingTimer covers the per-message processing-watchdog gate:
// with prefetch enabled the watchdog is skipped only for block messages (which
// legitimately park in AcquireBlockPrefetch under budget backpressure); every
// other case still arms.
func TestShouldArmProcessingTimer(t *testing.T) {
	tests := []struct {
		name            string
		cmd             string
		prefetchEnabled bool
		want            bool
	}{
		{"prefetch on, block msg -> not armed", wire.CmdBlock, true, false},
		{"prefetch on, tx msg -> armed", wire.CmdTx, true, true},
		{"prefetch off, block msg -> armed", wire.CmdBlock, false, true},
		{"prefetch off, tx msg -> armed", wire.CmdTx, false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, shouldArmProcessingTimer(tt.cmd, tt.prefetchEnabled))
		})
	}
}
