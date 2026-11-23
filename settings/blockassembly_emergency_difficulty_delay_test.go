package settings

import (
	"testing"
	"time"

	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

func TestEmergencyDifficultyDelay_LoaderReadsKey(t *testing.T) {
	const key = "blockassembly_emergencyDifficultyDelay"

	require.Zero(t, NewSettings().BlockAssembly.EmergencyDifficultyDelay, "the delay must ship at zero so a node with no configuration keeps its behaviour")

	gocore.Config().Set(key, "2m")
	t.Cleanup(func() { gocore.Config().Set(key, "") })

	require.Equal(t, 2*time.Minute, NewSettings().BlockAssembly.EmergencyDifficultyDelay, "the loader must read %s", key)
}
