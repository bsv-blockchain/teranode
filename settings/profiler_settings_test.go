package settings

import (
	"testing"

	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

// TestProfilerContentionSettings_LoaderReadsBothKeys guards
// BlockProfileRate/MutexProfileFraction against the field-exists-but-loader-
// never-reads-it bug: each field has a `key:` tag, but if NewSettings() does
// not call getInt for it the value stays at Go zero regardless of what's
// configured, silently leaving contention profiling permanently disabled.
//
// Default 0 == Go zero, so a default-value assertion alone would pass
// spuriously even with a missing loader entry. The honest test is: set a
// non-zero override, call NewSettings(), assert the field changed.
func TestProfilerContentionSettings_LoaderReadsBothKeys(t *testing.T) {
	def := NewSettings()
	require.Equal(t, 0, def.BlockProfileRate,
		"default BlockProfileRate must be 0 (disabled); block profiling must not run by default in prod")
	require.Equal(t, 0, def.MutexProfileFraction,
		"default MutexProfileFraction must be 0 (disabled); mutex profiling must not run by default in prod")

	type kv struct {
		key      string
		override string
		check    func(t *testing.T, s *Settings)
	}

	cases := []kv{
		{
			key:      "profiler_blockProfileRate",
			override: "1000000",
			check:    func(t *testing.T, s *Settings) { require.Equal(t, 1000000, s.BlockProfileRate) },
		},
		{
			key:      "profiler_mutexProfileFraction",
			override: "100",
			check:    func(t *testing.T, s *Settings) { require.Equal(t, 100, s.MutexProfileFraction) },
		},
	}

	for _, tc := range cases {
		t.Run(tc.key, func(t *testing.T) {
			gocore.Config().Set(tc.key, tc.override)
			t.Cleanup(func() { gocore.Config().Set(tc.key, "") })

			s := NewSettings()
			tc.check(t, s)
		})
	}
}
