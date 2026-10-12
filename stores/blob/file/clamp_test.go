package file

import (
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

// TestResolveConcurrency pins the behaviour that replaced refusing to start:
// when the configured concurrency exceeds the descriptors the OS leaves spare,
// it is scaled down to fit rather than aborting the node (issue 1431).
//
// It lives in its own function precisely so it can be tested. InitSemaphores is
// guarded by a sync.Once and runs once per process, so driving these cases
// through it would need one process each.
func TestResolveConcurrency(t *testing.T) {
	// The configured defaults throughout: a 3:1 read/write split totalling 1024.
	const (
		configuredRead  = 768
		configuredWrite = 256
	)

	tests := []struct {
		name            string
		useSystemLimits bool
		budget          uint64
		limitErr        error
		wantRead        int
		wantWrite       int
		wantClamped     bool
		wantTotalAtMost int
	}{
		{
			name:            "scales proportionally and uses the budget exactly",
			useSystemLimits: true,
			budget:          512,
			// Exact values, not just "read exceeds write": half the budget at a
			// 3:1 split is 384/128. A weaker check passes even if the scaling is
			// dropped altogether.
			wantRead: 384, wantWrite: 128, wantClamped: true, wantTotalAtMost: 512,
		},
		{
			name:            "a limit at or below the headroom still clamps",
			useSystemLimits: true,
			budget:          256,
			// The case that once escaped entirely: a host whose whole open-file
			// limit sits at or below the reserve used to produce a budget of zero,
			// which read as "no limit worth fitting under" and skipped the clamp —
			// running 1024 concurrent operations against 512 descriptors.
			wantRead: 192, wantWrite: 64, wantClamped: true, wantTotalAtMost: 256,
		},
		{
			name:            "the smallest budget that fits one of each",
			useSystemLimits: true,
			budget:          2,
			wantRead:        MinSemaphoreLimit, wantWrite: MinSemaphoreLimit,
			wantClamped: true, wantTotalAtMost: 2,
		},
		{
			name:            "a single-descriptor budget costs one descriptor of reserve",
			useSystemLimits: true,
			budget:          1,
			// The floor of one read and one write wins over the budget, so this is
			// the one case where the total overshoots. It borrows a descriptor
			// from the reserve fdlimit holds back, which is the price of keeping
			// the store able to do anything at all.
			wantRead: MinSemaphoreLimit, wantWrite: MinSemaphoreLimit,
			wantClamped: true, wantTotalAtMost: 2,
		},
		{
			name:            "a zero budget still yields a usable store",
			useSystemLimits: true,
			budget:          0,
			wantRead:        MinSemaphoreLimit, wantWrite: MinSemaphoreLimit,
			wantClamped: true, wantTotalAtMost: 2,
		},
		{
			name:            "an ample budget is left alone",
			useSystemLimits: true,
			budget:          100_000,
			wantRead:        configuredRead, wantWrite: configuredWrite,
			wantClamped: false, wantTotalAtMost: 1024,
		},
		{
			name:            "a budget exactly equal to the total is not clamped",
			useSystemLimits: true,
			budget:          1024,
			// It fits exactly; reducing it would be wrong.
			wantRead: configuredRead, wantWrite: configuredWrite,
			wantClamped: false, wantTotalAtMost: 1024,
		},
		{
			name:            "the operator can opt out",
			useSystemLimits: false,
			budget:          512,
			// useSystemLimits=false means "use the explicit values regardless of
			// system limits", accepting the risk of EMFILE. The configured numbers
			// must stand even though the budget cannot cover them.
			wantRead: configuredRead, wantWrite: configuredWrite,
			wantClamped: false, wantTotalAtMost: 1024,
		},
		{
			name:            "an unreadable limit leaves concurrency alone",
			useSystemLimits: true,
			budget:          0,
			limitErr:        errors.NewProcessingError("no limit on this platform"),
			// Windows and friends: there is no limit to fit under, and a zero
			// budget here means "unknown", not "no descriptors". This is why the
			// error must be distinguishable from a genuine budget of zero above.
			wantRead: configuredRead, wantWrite: configuredWrite,
			wantClamped: false, wantTotalAtMost: 1024,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := resolveConcurrency(configuredRead, configuredWrite, tt.useSystemLimits, tt.budget, tt.limitErr)

			require.Equal(t, tt.wantRead, got.Read)
			require.Equal(t, tt.wantWrite, got.Write)
			require.Equal(t, tt.wantClamped, got.Clamped)
			require.LessOrEqual(t, got.Read+got.Write, tt.wantTotalAtMost)
			require.GreaterOrEqual(t, got.Read, MinSemaphoreLimit, "the store must keep at least one read permit")
			require.GreaterOrEqual(t, got.Write, MinSemaphoreLimit, "the store must keep at least one write permit")
		})
	}
}
