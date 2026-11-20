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

	// An unknown value never stamps, so code that checks Stamps before CheckLaneMigration still
	// writes nothing for a typo.
	require.False(t, LaneMigration("on").Stamps())
	require.False(t, LaneMigration("").Stamps())
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

// setConfigForTest sets a config key and, when the test ends, puts back what was there: the old
// value, or no value at all if the key was missing, so the default applies again.
func setConfigForTest(t *testing.T, key, value string) {
	t.Helper()

	old, had := gocore.Config().Get(key)

	gocore.Config().Set(key, value)

	t.Cleanup(func() {
		if had {
			gocore.Config().Set(key, old)
		} else {
			gocore.Config().Unset(key)
		}
	})
}

// TestLaneMigrationIsWired proves the loader reads the key: a struct tag alone is not wiring.
func TestLaneMigrationIsWired(t *testing.T) {
	setConfigForTest(t, "utxostore_laneMigration", "shadow")

	require.Equal(t, LaneMigrationShadow, NewSettings().UtxoStore.LaneMigration)
}

// Each lane lease setting is read from its own key. Every value differs from every default and
// from the others, so a field read from the wrong key shows.
func TestLaneLeaseSettingsAreWired(t *testing.T) {
	setConfigForTest(t, "blockchain_laneLeaseRenewInterval", "7s")
	setConfigForTest(t, "blockchain_laneLeaseStopAfter", "11s")
	setConfigForTest(t, "blockchain_laneLeaseCloseAfter", "13s")
	setConfigForTest(t, "blockchain_laneGrantMin", "123")
	setConfigForTest(t, "blockchain_laneGrantDuration", "17s")

	s := NewSettings().BlockChain

	require.Equal(t, 7*time.Second, s.LaneLeaseRenewInterval)
	require.Equal(t, 11*time.Second, s.LaneLeaseStopAfter)
	require.Equal(t, 13*time.Second, s.LaneLeaseCloseAfter)
	require.Equal(t, uint64(123), s.LaneGrantMin)
	require.Equal(t, 17*time.Second, s.LaneGrantDuration)
	require.NoError(t, s.CheckLaneLease())
}

// A grant of no positions is useless, so a configured minimum of zero is raised to one.
func TestLaneGrantMinHasAFloorOfOne(t *testing.T) {
	setConfigForTest(t, "blockchain_laneGrantMin", "0")

	require.Equal(t, uint64(1), NewSettings().BlockChain.LaneGrantMin)
}

// CheckLaneLease at the edge of each rule: the value on the line is refused and one step inside is
// accepted.
func TestCheckLaneLeaseBoundaries(t *testing.T) {
	valid := BlockChainSettings{
		LaneLeaseRenewInterval: 30 * time.Second,
		LaneLeaseStopAfter:     2 * time.Minute,
		LaneLeaseCloseAfter:    3 * time.Minute,
		LaneGrantDuration:      2 * time.Minute,
	}
	require.NoError(t, valid.CheckLaneLease())

	tests := []struct {
		name   string
		change func(*BlockChainSettings)
		refuse bool
	}{
		{"renew zero", func(s *BlockChainSettings) { s.LaneLeaseRenewInterval = 0 }, true},
		{"renew negative", func(s *BlockChainSettings) { s.LaneLeaseRenewInterval = -time.Second }, true},
		{"renew one nanosecond", func(s *BlockChainSettings) { s.LaneLeaseRenewInterval = time.Nanosecond }, false},
		{"stop equals close", func(s *BlockChainSettings) { s.LaneLeaseStopAfter = s.LaneLeaseCloseAfter }, true},
		{"stop one nanosecond before close", func(s *BlockChainSettings) { s.LaneLeaseStopAfter = s.LaneLeaseCloseAfter - time.Nanosecond }, false},
		{"grant duration zero", func(s *BlockChainSettings) { s.LaneGrantDuration = 0 }, true},
		{"grant duration one nanosecond", func(s *BlockChainSettings) { s.LaneGrantDuration = time.Nanosecond }, false},
		{"renew equals stop", func(s *BlockChainSettings) { s.LaneLeaseRenewInterval = s.LaneLeaseStopAfter }, true},
		{"renew one nanosecond before stop", func(s *BlockChainSettings) { s.LaneLeaseRenewInterval = s.LaneLeaseStopAfter - time.Nanosecond }, false},
		{"grant minimum at the documented cap", func(s *BlockChainSettings) { s.LaneGrantMin = 1 << 40 }, false},
		{"grant minimum one above the documented cap", func(s *BlockChainSettings) { s.LaneGrantMin = 1<<40 + 1 }, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := valid
			test.change(&s)

			if test.refuse {
				require.Error(t, s.CheckLaneLease())
			} else {
				require.NoError(t, s.CheckLaneLease())
			}
		})
	}
}
