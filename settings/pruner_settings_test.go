package settings

import (
	"testing"
	"time"

	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

// TestPrunerSettings_LoaderReadsUnwiredKeys guards a bug where these keys
// carried `key:` and `default:` struct tags and appeared in the reference
// docs, but no getBool call populated the field in the Pruner settings
// block, so setting the key in settings.conf did nothing and the field
// stayed at the Go zero value forever.
//
// A default-value assertion cannot catch a relapse on its own: both fields
// default to false, which is also the Go zero value, so the loader could be
// entirely disconnected and the test would still pass. Only an override
// proves the key is actually read.
func TestPrunerSettings_LoaderReadsUnwiredKeys(t *testing.T) {
	cases := []struct {
		key      string
		override string
		check    func(t *testing.T, s *Settings)
	}{
		{
			key:      "pruner_skipDuringCatchup",
			override: "true",
			check: func(t *testing.T, s *Settings) {
				require.True(t, s.Pruner.SkipDuringCatchup,
					"loader must read pruner_skipDuringCatchup; otherwise catchup-skip is unconfigurable")
			},
		},
		{
			key:      "pruner_skipProcessExpiredPreservations",
			override: "true",
			check: func(t *testing.T, s *Settings) {
				require.True(t, s.Pruner.SkipProcessExpiredPreservations,
					"loader must read pruner_skipProcessExpiredPreservations; otherwise the Phase 1b kill-switch is unconfigurable")
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.key, func(t *testing.T) {
			gocore.Config().Set(tc.key, tc.override)
			t.Cleanup(func() { gocore.Config().Set(tc.key, "") })

			tc.check(t, NewSettings())
		})
	}
}

// TestPrunerSettings_UnwiredKeysDefaultToFalse pins the other half of wiring
// these keys: with no override, both must default to false, preserving
// today's behaviour for every deployment that never sets them.
func TestPrunerSettings_UnwiredKeysDefaultToFalse(t *testing.T) {
	for _, key := range []string{"pruner_skipDuringCatchup", "pruner_skipProcessExpiredPreservations"} {
		gocore.Config().Set(key, "")
	}

	s := NewSettings()

	require.False(t, s.Pruner.SkipDuringCatchup)
	require.False(t, s.Pruner.SkipProcessExpiredPreservations)
}

// TestPrunerSettings_BacklogDefaults pins the backlog-monitor defaults. They are
// the mainnet-oriented values from the design spec; changing one changes when
// every node alerts, so it must be a deliberate edit to this test.
func TestPrunerSettings_BacklogDefaults(t *testing.T) {
	keys := []string{
		"pruner_backlogMonitorEnabled", "pruner_backlogMaxLagBlocks", "pruner_backlogMaxIncompleteRuns",
		"pruner_backlogHeadroomWarnRatio", "pruner_backlogHeadroomWindow", "pruner_backlogFailReadiness",
		"pruner_backlogLogRepeatInterval",
	}
	for _, k := range keys {
		gocore.Config().Set(k, "")
	}

	p := NewSettings().Pruner

	require.True(t, p.BacklogMonitorEnabled)
	require.Equal(t, uint32(3), p.BacklogMaxLagBlocks)
	require.Equal(t, 2, p.BacklogMaxIncompleteRuns)
	require.InDelta(t, 0.8, p.BacklogHeadroomWarnRatio, 1e-9)
	require.Equal(t, 6, p.BacklogHeadroomWindow)
	require.True(t, p.BacklogFailReadiness)
	require.Equal(t, 5*time.Minute, p.BacklogLogRepeatInterval)
}

// TestPrunerSettings_BacklogOverrides proves every key is actually read by the
// loader (a default-only test cannot catch an unwired key; see
// TestPrunerSettings_LoaderReadsUnwiredKeys).
func TestPrunerSettings_BacklogOverrides(t *testing.T) {
	overrides := map[string]string{
		"pruner_backlogMonitorEnabled":    "false",
		"pruner_backlogMaxLagBlocks":      "7",
		"pruner_backlogMaxIncompleteRuns": "4",
		"pruner_backlogHeadroomWarnRatio": "0.65",
		"pruner_backlogHeadroomWindow":    "10",
		"pruner_backlogFailReadiness":     "false",
		"pruner_backlogLogRepeatInterval": "90s",
	}
	for k, v := range overrides {
		gocore.Config().Set(k, v)
		key := k
		t.Cleanup(func() { gocore.Config().Set(key, "") })
	}

	p := NewSettings().Pruner

	require.False(t, p.BacklogMonitorEnabled)
	require.Equal(t, uint32(7), p.BacklogMaxLagBlocks)
	require.Equal(t, 4, p.BacklogMaxIncompleteRuns)
	require.InDelta(t, 0.65, p.BacklogHeadroomWarnRatio, 1e-9)
	require.Equal(t, 10, p.BacklogHeadroomWindow)
	require.False(t, p.BacklogFailReadiness)
	require.Equal(t, 90*time.Second, p.BacklogLogRepeatInterval)
}
