package settings

import (
	"testing"

	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

func TestPrunerSkipDuringCatchupLoaderReadsOverride(t *testing.T) {
	const key = "pruner_skipDuringCatchup"
	// Use a dedicated context so ambient settings cannot hide the override.
	const testContext = "pruner_fsm_loader_test"
	overrideKey := key + "." + testContext
	config := gocore.Config(testContext)

	for _, tc := range []struct {
		value string
		want  bool
	}{
		{"true", true},
		{"false", false},
	} {
		t.Run(tc.value, func(t *testing.T) {
			previous := config.Set(overrideKey, tc.value)
			t.Cleanup(func() { config.Set(overrideKey, previous) })
			require.Equal(t, tc.want, NewSettings(testContext).Pruner.SkipDuringCatchup,
				"loader must read the configured pruning admission guard")
		})
	}
}
