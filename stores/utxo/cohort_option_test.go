package utxo

import (
	"testing"

	"github.com/bsv-blockchain/teranode/util/cohort"
	"github.com/stretchr/testify/require"
)

// TestWithCohort checks the create option lands on CreateOptions and that the
// default, with no option supplied, is cohort.Unset — the value every caller
// gets while the issue-556 feature flag is off.
func TestWithCohort(t *testing.T) {
	t.Run("default is unset", func(t *testing.T) {
		options, err := ParseCreateOptions()
		require.NoError(t, err)
		require.Equal(t, cohort.Unset, options.Cohort)
	})

	t.Run("stamped", func(t *testing.T) {
		options, err := ParseCreateOptions(WithCohort(cohort.ID(1_700_000_000)))
		require.NoError(t, err)
		require.Equal(t, cohort.ID(1_700_000_000), options.Cohort)
	})

	t.Run("last option wins", func(t *testing.T) {
		options, err := ParseCreateOptions(
			WithCohort(cohort.BornMined),
			WithCohort(cohort.Historical),
		)
		require.NoError(t, err)
		require.Equal(t, cohort.Historical, options.Cohort)
	})

	t.Run("composes with the other create options", func(t *testing.T) {
		options, err := ParseCreateOptions(
			WithLocked(true),
			WithCohort(cohort.ID(1_700_000_000)),
			WithConflicting(true),
		)
		require.NoError(t, err)
		require.Equal(t, cohort.ID(1_700_000_000), options.Cohort)
		require.True(t, options.Locked)
		require.True(t, options.Conflicting)
	})
}
