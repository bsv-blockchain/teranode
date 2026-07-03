package aerospike

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/stretchr/testify/require"
)

// TestBatcherWaitForSumsBudget pins the leak-guard formula: the guard must be
// initial-attempt (TotalTimeout) + overload budget + 30s grace. A regression to
// the weaker max(TotalTimeout, budget)+30s would abandon a batch while the
// overload-retry wrapper is still working — the legacy-sync stall this guard
// caused at 40s. Exact-equality asserts catch a silent switch back to max().
func TestBatcherWaitForSumsBudget(t *testing.T) {
	const budget = 2 * time.Minute // aerospike_overload_retry_max_elapsed default

	tests := []struct {
		name         string
		totalTimeout time.Duration
		budget       time.Duration
		want         time.Duration
	}{
		// docker.m's 10s. max() would give 2m30s, not 2m40s — this pins the sum.
		{"docker.m small TotalTimeout still covers full budget", 10 * time.Second, budget, 10*time.Second + budget + 30*time.Second},
		{"large TotalTimeout", 90 * time.Second, budget, 90*time.Second + budget + 30*time.Second},
		{"global 5m default", 5 * time.Minute, budget, 5*time.Minute + budget + 30*time.Second},
		// TotalTimeout <= 0 falls back to a 2m floor before adding the budget.
		{"zero TotalTimeout uses fallback floor", 0, budget, 2*time.Minute + budget + 30*time.Second},
		{"negative TotalTimeout uses fallback floor", -5 * time.Second, budget, 2*time.Minute + budget + 30*time.Second},
		// Overload retry disabled: behaviour is unchanged (TotalTimeout + grace).
		{"disabled overload retry adds nothing", 10 * time.Second, 0, 10*time.Second + 30*time.Second},
		{"negative budget adds nothing", 10 * time.Second, -1 * time.Minute, 10*time.Second + 30*time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, batcherWaitFor(tt.totalTimeout, tt.budget))
		})
	}
}

// TestBatcherWaitOutlastsOverloadBudget is the invariant guard: for every
// context that configures overload retries, the leak guard must strictly
// outlast the overload-retry budget, so a future settings.conf edit cannot
// silently reintroduce a guard shorter than the budget (the 40s trap).
func TestBatcherWaitOutlastsOverloadBudget(t *testing.T) {
	for _, ctx := range []string{"docker.m", "docker"} {
		t.Run(ctx, func(t *testing.T) {
			s := settings.NewSettings(ctx)

			budget := s.Aerospike.OverloadRetryMaxElapsed
			require.Positive(t, budget, "context expected to enable overload retries")

			totalTimeout := batchPolicyTotalTimeout(t, s)
			guard := batcherWaitFor(totalTimeout, budget)

			require.Greater(t, guard, budget,
				"leak guard %s must outlast overload budget %s (TotalTimeout=%s) so it cannot abort a batch the retry layer would still complete",
				guard, budget, totalTimeout)
		})
	}
}

// batchPolicyTotalTimeout reads TotalTimeout from the context's configured
// aerospike_batchPolicy URL. It parses the URL directly rather than going
// through util.GetAerospikeBatchPolicy, whose value is only populated once a
// live Aerospike client is constructed (absent in a unit test).
func batchPolicyTotalTimeout(t *testing.T, s *settings.Settings) time.Duration {
	t.Helper()

	require.NotNil(t, s.Aerospike.BatchPolicyURL, "no aerospike_batchPolicy configured")

	raw := s.Aerospike.BatchPolicyURL.Query().Get("TotalTimeout")
	require.NotEmpty(t, raw, "aerospike_batchPolicy has no TotalTimeout")

	d, err := time.ParseDuration(raw)
	require.NoError(t, err, "TotalTimeout %q is not a valid duration", raw)

	return d
}
