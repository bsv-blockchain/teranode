package settings

import (
	"testing"
	"time"

	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

func TestCheckLaneLeaseRefusesStopNotShorterThanClose(t *testing.T) {
	s := BlockChainSettings{LaneLeaseRenewInterval: 30 * time.Second, LaneLeaseStopAfter: 3 * time.Minute, LaneLeaseCloseAfter: 3 * time.Minute, LaneGrantDuration: 2 * time.Minute}
	require.Error(t, s.CheckLaneLease())

	s.LaneLeaseStopAfter = 2 * time.Minute
	require.NoError(t, s.CheckLaneLease())
}

func TestCheckLaneLeaseRefusesRenewNotShorterThanStop(t *testing.T) {
	s := BlockChainSettings{LaneLeaseRenewInterval: 2 * time.Minute, LaneLeaseStopAfter: 2 * time.Minute, LaneLeaseCloseAfter: 3 * time.Minute, LaneGrantDuration: 2 * time.Minute}
	require.Error(t, s.CheckLaneLease())
}

// A grant duration of zero would leave every grant after its first position.
func TestCheckLaneLeaseRefusesANonPositiveGrantDuration(t *testing.T) {
	s := BlockChainSettings{LaneLeaseRenewInterval: 30 * time.Second, LaneLeaseStopAfter: 2 * time.Minute, LaneLeaseCloseAfter: 3 * time.Minute}
	require.Error(t, s.CheckLaneLease())

	s.LaneGrantDuration = 2 * time.Minute
	require.NoError(t, s.CheckLaneLease())
}

func TestCheckLaneMigrationAcceptsOffAndShadowOnly(t *testing.T) {
	require.NoError(t, CheckLaneMigration(LaneMigrationOff))
	require.NoError(t, CheckLaneMigration(LaneMigrationShadow))

	for _, m := range []LaneMigration{LaneMigrationRows, LaneMigrationLanes, "on", ""} {
		require.Error(t, CheckLaneMigration(m), "%q", m)
	}

	require.False(t, LaneMigrationOff.Stamps())
	require.True(t, LaneMigrationShadow.Stamps())
	require.True(t, LaneMigrationRows.Stamps())
	require.True(t, LaneMigrationLanes.Stamps())
}

func TestLaneLeaseDefaults(t *testing.T) {
	s := NewSettings()

	require.Equal(t, 30*time.Second, s.BlockChain.LaneLeaseRenewInterval)
	require.Equal(t, 2*time.Minute, s.BlockChain.LaneLeaseStopAfter)
	require.Equal(t, 3*time.Minute, s.BlockChain.LaneLeaseCloseAfter)
	require.Equal(t, uint64(10_000), s.BlockChain.LaneGrantMin)
	require.Equal(t, 2*time.Minute, s.BlockChain.LaneGrantDuration)
	require.Equal(t, LaneMigrationOff, s.UtxoStore.LaneMigration)
	require.NoError(t, s.BlockChain.CheckLaneLease())
}

// TestLaneMigrationIsWired proves the loader reads the key: a struct tag alone is not wiring.
func TestLaneMigrationIsWired(t *testing.T) {
	gocore.Config().Set("utxostore_laneMigration", "shadow")
	t.Cleanup(func() { gocore.Config().Set("utxostore_laneMigration", "") })

	require.Equal(t, LaneMigrationShadow, NewSettings().UtxoStore.LaneMigration)
}
