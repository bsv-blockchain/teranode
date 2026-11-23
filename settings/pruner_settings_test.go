package settings

import (
	"testing"

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
