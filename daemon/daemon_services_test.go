package daemon

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Each service that checks whether a parent is mined, or builds a validator that does, starts the
// lane index on its own; a process running none of them, such as an RPC-only one, does not.
func TestNeedsLaneReader(t *testing.T) {
	require.False(t, needsLaneReader(false, false, false, false, false), "none of the five, as an RPC-only process")

	for i, name := range []string{"subtree validation", "block validation", "validator", "propagation", "legacy"} {
		flags := make([]bool, 5)
		flags[i] = true

		require.True(t, needsLaneReader(flags[0], flags[1], flags[2], flags[3], flags[4]), name)
	}
}
