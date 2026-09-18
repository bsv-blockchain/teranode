package utxo

import (
	"math"
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

	// The stored encoding of an empty interval must be inert at every height, including
	// the extremes, or a coin an authority meant to leave alone would be consensus-frozen.
	for _, h := range []uint32{0, 1, 500, 1_000_000, FreezeWindowNever - 1} {
		require.False(t, FreezeWindowActiveAt(FreezeWindowNever, FreezeWindowNever, h),
			"FreezeWindowNever must never be active (height %d)", h)
	}
}

// TestNormalizeFreezeWindow pins the one place SV Node's interval semantics meet the
// stored encoding. On the wire stop is an exclusive end and 0 is "empty"; in the store 0
// is "unbounded" because that is what every pre-window freeze meant. Getting this wrong
// in either direction turns an unfreeze into a permanent freeze or vice versa.
func TestNormalizeFreezeWindow(t *testing.T) {
	tests := []struct {
		name            string
		start, stop     uint64
		wantFrom        uint32
		wantUntil       uint32
		enforcesNothing bool
	}{
		{name: "the pre-window unfreeze idiom (0, 0) is an empty interval", start: 0, stop: 0, wantFrom: FreezeWindowNever, wantUntil: FreezeWindowNever, enforcesNothing: true},
		{name: "stop equal to start is empty", start: 100, stop: 100, wantFrom: FreezeWindowNever, wantUntil: FreezeWindowNever, enforcesNothing: true},
		{name: "stop below start is empty", start: 200, stop: 100, wantFrom: FreezeWindowNever, wantUntil: FreezeWindowNever, enforcesNothing: true},
		{name: "a zero stop with a positive start is empty, not open-ended", start: 500, stop: 0, wantFrom: FreezeWindowNever, wantUntil: FreezeWindowNever, enforcesNothing: true},
		{name: "a bounded interval is stored as is", start: 500, stop: 600, wantFrom: 500, wantUntil: 600},
		{name: "from genesis until a height", start: 0, stop: 600, wantFrom: 0, wantUntil: 600},
		{name: "a one-block interval", start: 500, stop: 501, wantFrom: 500, wantUntil: 501},
		{name: "a stop beyond uint32 clamps to effectively unbounded", start: 500, stop: math.MaxUint64, wantFrom: 500, wantUntil: math.MaxUint32},
		{name: "a start beyond uint32 is never reached", start: math.MaxUint64 - 1, stop: math.MaxUint64, wantFrom: math.MaxUint32, wantUntil: math.MaxUint32},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			from, until, enforcesNothing := NormalizeFreezeWindow(tc.start, tc.stop)
			require.Equal(t, tc.wantFrom, from, "from")
			require.Equal(t, tc.wantUntil, until, "until")
			require.Equal(t, tc.enforcesNothing, enforcesNothing, "enforcesNothing")

			// Whatever was stored, an empty wire interval must never be consensus-active.
			if tc.enforcesNothing {
				for _, h := range []uint32{0, uint32(tc.start), 1_000_000} {
					require.False(t, FreezeWindowActiveAt(from, until, h), "empty interval active at %d", h)
				}
			}
		})
	}
}

// TestFreezePolicyActiveAt pins policyExpiresWithConsensus: the policy tier lifts once
// the consensus window has ended — at its stop, or immediately for an empty interval —
// only when the alert said so, and an open-ended window never lifts it.
func TestFreezePolicyActiveAt(t *testing.T) {
	tests := []struct {
		name          string
		from, until   uint32
		policyExpires bool
		blockHeight   uint32
		want          bool
	}{
		{name: "flag clear: still frozen after the window", from: 500, until: 600, policyExpires: false, blockHeight: 600, want: true},
		{name: "flag clear: still frozen far after the window", from: 500, until: 600, policyExpires: false, blockHeight: 1_000_000, want: true},
		{name: "flag set: frozen on the last enforced height", from: 500, until: 600, policyExpires: true, blockHeight: 599, want: true},
		{name: "flag set: lifted at the stop height", from: 500, until: 600, policyExpires: true, blockHeight: 600, want: false},
		{name: "flag set: lifted after the stop height", from: 500, until: 600, policyExpires: true, blockHeight: 601, want: false},
		{name: "flag set but no end: never lifts", from: 500, until: 0, policyExpires: true, blockHeight: 1_000_000, want: true},
		{name: "flag set, legacy always-window: never lifts", from: 0, until: 0, policyExpires: true, blockHeight: 1_000_000, want: true},
		{name: "empty interval with flag set: lifted immediately", from: FreezeWindowNever, until: FreezeWindowNever, policyExpires: true, blockHeight: 1, want: false},
		{name: "empty interval with flag clear: still frozen", from: FreezeWindowNever, until: FreezeWindowNever, policyExpires: false, blockHeight: 1, want: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, FreezePolicyActiveAt(tc.from, tc.until, tc.policyExpires, tc.blockHeight))
		})
	}
}
