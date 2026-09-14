package settings

import (
	"testing"
	"time"

	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

func TestUnminedRecoveryIntervalLoadsConfiguration(t *testing.T) {
	const testContext = "assembly_recovery_loader_test"
	const key = "blockassembly_unminedRecoveryInterval." + testContext
	config := gocore.Config(testContext)
	for _, tc := range []struct {
		value string
		want  time.Duration
	}{
		{"2h", 2 * time.Hour},
		{"30m", 30 * time.Minute},
		{"0s", 0},
		{"-1s", -time.Second},
	} {
		t.Run(tc.value, func(t *testing.T) {
			previous := config.Set(key, tc.value)
			t.Cleanup(func() { config.Set(key, previous) })
			require.Equal(t, tc.want, NewSettings(testContext).BlockAssembly.UnminedRecoveryInterval)
		})
	}
}
