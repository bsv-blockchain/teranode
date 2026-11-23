package daemon

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Subtree validation, block validation and the validator each start the lane index on their own.
// A service that hands its transactions to a validator (propagation, legacy, RPC) starts it only
// when that validator runs in-process; a process running none of these does not.
func TestNeedsLaneReader(t *testing.T) {
	require.False(t, needsLaneReader(false, false, false, false, false), "none of them")
	require.False(t, needsLaneReader(false, false, false, false, true), "a local validator setting alone")
	require.False(t, needsLaneReader(false, false, false, true, false), "a validator client with a remote validator")
	require.True(t, needsLaneReader(false, false, false, true, true), "a validator client with a local validator")

	for i, name := range []string{"subtree validation", "block validation", "validator"} {
		flags := make([]bool, 3)
		flags[i] = true

		require.True(t, needsLaneReader(flags[0], flags[1], flags[2], false, false), name)
	}
}
