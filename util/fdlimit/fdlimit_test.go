package fdlimit

import (
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

// stubRlimit swaps the rlimit read for a fake and restores the real one when the
// test ends. The limits worth testing are ones this process cannot actually
// hold — a hard limit below the current soft limit cannot be undone once
// lowered — so they are posed rather than applied.
func stubRlimit(t *testing.T, soft uint64, err error) {
	t.Helper()

	real := get

	t.Cleanup(func() { get = real })

	get = func() (uint64, error) { return soft, err }
}

// TestBudgetReserve pins the reserve across the whole range, including the
// boundary where the proportional reserve of a small limit hands over to the
// fixed Headroom. The budget must never exceed the limit it came from, and must
// never decrease as the limit grows.
func TestBudgetReserve(t *testing.T) {
	tests := []struct {
		name string
		soft uint64
		want uint64
	}{
		{"no limit at all yields no budget", 0, 0},
		{"a limit of one reserves nothing it cannot spare", 1, 1},
		{"a tiny limit reserves half", 2, 1},
		{"a limit just below Headroom reserves half of itself", 511, 256},
		{"a limit equal to Headroom reserves half, not all of it", 512, 256},
		{"just above Headroom the reserve is still half", 513, 257},
		{"the last limit where half is the smaller reserve", 1023, 512},
		{"at twice Headroom the two reserves agree", 1024, 512},
		{"the last limit where the two reserves still agree", 1025, 513},
		{"an ample limit reserves exactly Headroom", 100_000, 100_000 - Headroom},
	}

	var prev uint64

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stubRlimit(t, tt.soft, nil)

			budget, err := Budget()
			require.NoError(t, err)
			require.Equal(t, tt.want, budget)
			require.LessOrEqual(t, budget, tt.soft, "the budget must fit under the limit it came from")
		})

		require.GreaterOrEqual(t, tt.want, prev, "the budget must not shrink as the limit grows")

		prev = tt.want
	}
}

// TestBudgetReportsAnUnreadableLimit covers the platforms with no RLIMIT_NOFILE,
// where the caller must be able to tell "unknown" apart from a real budget and
// carry on rather than refuse to run.
func TestBudgetReportsAnUnreadableLimit(t *testing.T) {
	stubRlimit(t, 0, errors.NewProcessingError("no limit on this platform"))

	budget, err := Budget()
	require.Error(t, err)
	require.Zero(t, budget, "an unreadable limit must not look like a real budget")
}

// TestBudgetReadsTheRealLimit exercises the platform wrapper itself rather than
// a stub, so a broken syscall shim cannot hide behind the synthetic tests above.
func TestBudgetReadsTheRealLimit(t *testing.T) {
	soft, err := getRlimit()
	require.NoError(t, err, "this platform should expose RLIMIT_NOFILE")
	require.Positive(t, soft, "a running process holds at least one descriptor")

	budget, err := Budget()
	require.NoError(t, err)
	require.LessOrEqual(t, budget, soft)
}
