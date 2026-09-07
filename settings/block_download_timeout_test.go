package settings

import (
	"testing"
	"time"

	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

// TestBlockDownloadTimeout_LoaderReadsAllKeys guards the three DetectStalling
// percentages against the field-exists-but-loader-never-reads-it bug: a `key:`
// tag is documentation only, so without a getInt64 call in NewSettings the field
// stays at the Go zero value.
//
// A zero here is not a harmless default. The svp2p per-block download timeout
// multiplies the block interval by these percentages, so a base of 0 gives every
// peer holding a block a window of zero and disconnects it on the first stall
// check. The defaults must be SVNode's own (validation.h:177-185), because the
// point of carrying them from settings is that out of the box the behavior is
// SVNode's, and only an operator moves it.
func TestBlockDownloadTimeout_LoaderReadsAllKeys(t *testing.T) {
	tests := []struct {
		key     string
		want    int64
		read    func(*Settings) int64
		probe   string
		wantHit int64
	}{
		{
			key:     "legacy_blockDownloadTimeoutBasePercent",
			want:    100,
			read:    func(s *Settings) int64 { return s.Legacy.BlockDownloadTimeoutBasePercent },
			probe:   "321",
			wantHit: 321,
		},
		{
			key:     "legacy_blockDownloadTimeoutBaseIBDPercent",
			want:    600,
			read:    func(s *Settings) int64 { return s.Legacy.BlockDownloadTimeoutBaseIBDPercent },
			probe:   "1234",
			wantHit: 1234,
		},
		{
			key:     "legacy_blockDownloadTimeoutPerPeerPercent",
			want:    50,
			read:    func(s *Settings) int64 { return s.Legacy.BlockDownloadTimeoutPerPeerPercent },
			probe:   "77",
			wantHit: 77,
		},
	}

	ctx := gocore.Config().GetContext()

	// The parallel-fetch pair is checked the same way below; they share this
	// test because they share the failure mode.

	for _, tc := range tests {
		t.Run(tc.key, func(t *testing.T) {
			// Default contract, only under a context that carries no .conf
			// override for these keys.
			if ctx == "" || ctx == "dev" {
				require.Equal(t, tc.want, tc.read(NewSettings()),
					"%s must default to the SVNode value", tc.key)
			}

			// Loader wiring, under every context: gocore resolves key.<context>
			// ahead of the base key, so set at the precedence that wins.
			winKey := tc.key
			if ctx != "" {
				winKey = tc.key + "." + ctx
			}

			gocore.Config().Set(winKey, tc.probe)
			t.Cleanup(func() { gocore.Config().Set(winKey, "") })

			require.Equal(t, tc.wantHit, tc.read(NewSettings()),
				"loader must read %s under context %q", tc.key, ctx)
		})
	}
}

// TestBlockDownloadParallelFetch_LoaderReadsAllKeys is the same guard for the
// parallel-fetch fuse and cap. A zero fuse would race every block the instant it
// was requested; a zero cap would stop the walk from ever racing one, since the
// cap is compared against a staller count of at least one.
func TestBlockDownloadParallelFetch_LoaderReadsAllKeys(t *testing.T) {
	ctx := gocore.Config().GetContext()

	winKey := func(key string) string {
		if ctx == "" {
			return key
		}

		return key + "." + ctx
	}

	t.Run("legacy_blockDownloadSlowFetchTimeout", func(t *testing.T) {
		const key = "legacy_blockDownloadSlowFetchTimeout"

		if ctx == "" || ctx == "dev" {
			require.Equal(t, 30*time.Second, NewSettings().Legacy.BlockDownloadSlowFetchTimeout)
		}

		gocore.Config().Set(winKey(key), "90s")
		t.Cleanup(func() { gocore.Config().Set(winKey(key), "") })

		require.Equal(t, 90*time.Second, NewSettings().Legacy.BlockDownloadSlowFetchTimeout,
			"loader must read %s under context %q", key, ctx)
	})

	t.Run("legacy_blockDownloadMaxParallelFetch", func(t *testing.T) {
		const key = "legacy_blockDownloadMaxParallelFetch"

		if ctx == "" || ctx == "dev" {
			require.Equal(t, 3, NewSettings().Legacy.BlockDownloadMaxParallelFetch)
		}

		gocore.Config().Set(winKey(key), "5")
		t.Cleanup(func() { gocore.Config().Set(winKey(key), "") })

		require.Equal(t, 5, NewSettings().Legacy.BlockDownloadMaxParallelFetch,
			"loader must read %s under context %q", key, ctx)
	})
}
