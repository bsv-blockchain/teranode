package utxo

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCreateOptionsDoNotAllocate pins the option constructors on the
// SpendAndCreate hot path to zero allocations.
//
// Why this is worth a test rather than a comment. Each of these used to return
// a fresh closure over its argument, which is one heap allocation per call, and
// applyOneWave builds the list once per TRANSACTION. A CPU profile taken on
// Hetzner mainnet during block 760431 charged 14.77 core-seconds to
// runtime.newobject underneath WithIgnoreLocked alone, in a 25-second window.
// That is not the cost of an allocation; it is the cost of allocating while the
// collector is pinned at GOMEMLIMIT, where every allocation first pays off
// sweep debt through runtime.deductSweepCredit.
//
// AllocsPerRun is used rather than a heap reading because it counts a locally
// produced quantity. An assertion against a share of total heap passes alone
// and fails in the suite, since the test binary is shared.
//
// The options MUST be consumed by escapeOptions below rather than discarded.
// The first version of this test wrote `_ = WithIgnoreLocked(true)`, and it
// passed against the unfixed code: with the result thrown away the closure
// never escapes, so the compiler stack-allocates it and there is nothing to
// count. At the real call site the option goes into a variadic slice passed to
// an interface method, which is what forces it onto the heap.
// optionSink is written by escapeOptions so the compiler cannot prove the
// options are dead. Package-level, because a local would let escape analysis
// see the whole lifetime and stack-allocate the closure again.
var optionSink CreateOption

// escapeOption makes the option outlive the call, as passing it to the store
// does, in a function the inliner will not fold into its caller.
//
// It takes ONE option rather than a variadic slice. A variadic call allocates
// the slice itself, which would be counted here and would keep every case at
// one allocation however the constructors are written; the slice is a real cost
// but it is one per SpendAndCreate call, not one per option.
//
//go:noinline
func escapeOption(opt CreateOption) {
	optionSink = opt
}

func TestCreateOptionsDoNotAllocate(t *testing.T) {
	cases := []struct {
		name string
		call func()
	}{
		{"WithIgnoreLocked/true", func() { escapeOption(WithIgnoreLocked(true)) }},
		{"WithIgnoreLocked/false", func() { escapeOption(WithIgnoreLocked(false)) }},
		{"WithIgnoreConflicting", func() { escapeOption(WithIgnoreConflicting(true)) }},
		{"WithSkipUTXOHashCheck", func() { escapeOption(WithSkipUTXOHashCheck(true)) }},
		{"WithSkipExtendedInputs", func() { escapeOption(WithSkipExtendedInputs(false)) }},
		{"WithLocked", func() { escapeOption(WithLocked(true)) }},
		{"WithFrozen", func() { escapeOption(WithFrozen(false)) }},
		{"WithConflicting", func() { escapeOption(WithConflicting(true)) }},
		{"WithCreateOnly", func() { escapeOption(WithCreateOnly()) }},
		{"WithSpendOnly", func() { escapeOption(WithSpendOnly()) }},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := testing.AllocsPerRun(100, c.call)
			require.Zero(t, got, "%s allocates %.0f time(s) per call; it is called once per transaction on the apply path", c.name, got)
		})
	}
}

// TestCreateOptionsStillSetWhatTheySay is the other half: a constructor that
// allocates nothing but sets the wrong field would pass the test above. Both
// values of every boolean option are checked, because the fix replaces one
// closure over a variable with a choice between two fixed closures, and picking
// the wrong branch is exactly how that goes wrong.
func TestCreateOptionsStillSetWhatTheySay(t *testing.T) {
	for _, b := range []bool{true, false} {
		var o CreateOptions

		WithIgnoreLocked(b)(&o)
		require.Equal(t, b, o.IgnoreFlags.IgnoreLocked)

		WithIgnoreConflicting(b)(&o)
		require.Equal(t, b, o.IgnoreFlags.IgnoreConflicting)

		WithSkipUTXOHashCheck(b)(&o)
		require.Equal(t, b, o.IgnoreFlags.SkipUTXOHashCheck)

		WithSkipExtendedInputs(b)(&o)
		require.Equal(t, b, o.SkipExtendedInputs)

		WithLocked(b)(&o)
		require.Equal(t, b, o.Locked)

		WithFrozen(b)(&o)
		require.Equal(t, b, o.Frozen)

		WithConflicting(b)(&o)
		require.Equal(t, b, o.Conflicting)
	}

	var o CreateOptions

	WithCreateOnly()(&o)
	require.True(t, o.CreateOnly)

	WithSpendOnly()(&o)
	require.True(t, o.SpendOnly)
}
