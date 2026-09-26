package settings_test

import (
	"testing"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/stretchr/testify/require"
)

// The struct tag is documentation; only the getInt call in settings.go loads a value. This
// test fails if either setting is declared but never loaded (the field would read 0, not 1).
func TestQuickWindowSettingsAreLoaded(t *testing.T) {
	t.Setenv("blockvalidation_quick_window_blocks", "3")
	t.Setenv("blockvalidation_quick_window_budget_mib", "256")

	s := settings.NewSettings("test")
	require.Equal(t, 3, s.BlockValidation.QuickWindowBlocks)
	require.Equal(t, 256, s.BlockValidation.QuickWindowBudgetMiB)
}

func TestQuickWindowSettingsDefaults(t *testing.T) {
	s := settings.NewSettings("test")
	require.Equal(t, 1, s.BlockValidation.QuickWindowBlocks)
	require.Equal(t, 0, s.BlockValidation.QuickWindowBudgetMiB)
}

// QuickWindowConfiguredDepth is the one depth rule both block validation and legacy sync call,
// so its arithmetic is pinned here rather than in either service's own test.
func TestQuickWindowConfiguredDepth(t *testing.T) {
	cases := []struct {
		name             string
		blocks           int
		skipLock         bool
		maxBlocksBehind  int
		expectedDepth    int
		expectClampGiven bool
	}{
		{name: "off", blocks: 0, skipLock: true, maxBlocksBehind: 20, expectedDepth: 0},
		{name: "negative reads as off", blocks: -3, skipLock: true, maxBlocksBehind: 20, expectedDepth: 0},
		{name: "one", blocks: 1, skipLock: true, maxBlocksBehind: 20, expectedDepth: 1},
		{name: "one without skip lock stays one, no clamp reported", blocks: 1, skipLock: false, maxBlocksBehind: 20, expectedDepth: 1},
		{name: "four without skip lock is forced to one", blocks: 4, skipLock: false, maxBlocksBehind: 20, expectedDepth: 1, expectClampGiven: true},
		{name: "four under an allowance of twenty is uncapped", blocks: 4, skipLock: true, maxBlocksBehind: 20, expectedDepth: 4},
		{name: "twenty under an allowance of forty is uncapped", blocks: 20, skipLock: true, maxBlocksBehind: 40, expectedDepth: 20},
		{name: "twenty under an allowance of twenty is capped at ten", blocks: 20, skipLock: true, maxBlocksBehind: 20, expectedDepth: 10, expectClampGiven: true},
		{name: "an allowance of one floors the cap at one", blocks: 4, skipLock: true, maxBlocksBehind: 1, expectedDepth: 1, expectClampGiven: true},
		{name: "an allowance of zero floors the cap at one", blocks: 4, skipLock: true, maxBlocksBehind: 0, expectedDepth: 1, expectClampGiven: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := &settings.BlockValidationSettings{
				QuickWindowBlocks:            tc.blocks,
				QuickValidateSkipUtxoLock:    tc.skipLock,
				MaxBlocksBehindBlockAssembly: tc.maxBlocksBehind,
			}

			depth, reasons := s.QuickWindowConfiguredDepth()
			require.Equal(t, tc.expectedDepth, depth)

			if tc.expectClampGiven {
				require.NotEmpty(t, reasons, "a clamp must say why")
				return
			}

			require.Empty(t, reasons, "nothing was clamped, so there is nothing to explain")
		})
	}
}
