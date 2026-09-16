package utxo

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestFreezeWindowActiveAt pins the half-open semantics of the alert system's
// enforceAtHeight window (issue #1422). Every backend evaluates the same predicate —
// the SQL store calls this function directly, and teranode.lua's freezeWindowActiveAt is
// a line-for-line mirror — because a node that disagreed by one block about when a freeze
// starts or ends would disagree with the fleet about a block for exactly that long.
func TestFreezeWindowActiveAt(t *testing.T) {
	tests := []struct {
		name        string
		from        uint32
		until       uint32
		blockHeight uint32
		want        bool
	}{
		// A recorded window bites for [from, until) and nothing outside it.
		{name: "well below the window", from: 500, until: 600, blockHeight: 1, want: false},
		{name: "one block below the start", from: 500, until: 600, blockHeight: 499, want: false},
		{name: "the start height itself", from: 500, until: 600, blockHeight: 500, want: true},
		{name: "inside the window", from: 500, until: 600, blockHeight: 550, want: true},
		{name: "the last enforced height", from: 500, until: 600, blockHeight: 599, want: true},
		{name: "the stop height itself", from: 500, until: 600, blockHeight: 600, want: false},
		{name: "well above the window", from: 500, until: 600, blockHeight: 1_000_000, want: false},

		// No window at all is "enforce from genesis, forever" — the meaning of every
		// freeze written before windows existed, so an upgrade changes nothing for them.
		{name: "no window at height zero", from: 0, until: 0, blockHeight: 0, want: true},
		{name: "no window at height one", from: 0, until: 0, blockHeight: 1, want: true},
		{name: "no window far up the chain", from: 0, until: 0, blockHeight: 1_000_000, want: true},

		// A zero stop means the window never ends.
		{name: "open ended below the start", from: 500, until: 0, blockHeight: 499, want: false},
		{name: "open ended at the start", from: 500, until: 0, blockHeight: 500, want: true},
		{name: "open ended far above the start", from: 500, until: 0, blockHeight: 1_000_000, want: true},

		// A zero start with a stop is "from genesis until".
		{name: "from genesis until, inside", from: 0, until: 600, blockHeight: 1, want: true},
		{name: "from genesis until, at stop", from: 0, until: 600, blockHeight: 600, want: false},

		// A window one block wide still covers exactly one block.
		{name: "single block window, at it", from: 500, until: 501, blockHeight: 500, want: true},
		{name: "single block window, past it", from: 500, until: 501, blockHeight: 501, want: false},

		// An empty or inverted range covers nothing. The alert service rejects these
		// before they reach a store, but the predicate must not invent enforcement.
		{name: "empty range", from: 500, until: 500, blockHeight: 500, want: false},
		{name: "inverted range", from: 600, until: 500, blockHeight: 550, want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, FreezeWindowActiveAt(tc.from, tc.until, tc.blockHeight),
				"window [%d, %d) at block height %d", tc.from, tc.until, tc.blockHeight)
		})
	}
}
