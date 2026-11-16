package settings

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBelowCheckpointRouteDocNoStalePhrases guards the operator text for the
// below-checkpoint route and the legacy per-peer download depth against the
// drift that made both describe code that no longer exists. The two route
// longdescs said false meant "the existing inline netsync pipeline", which was
// deleted, and recommended staying off pending a soak; the depth longdesc said
// the block-size ladder narrows the multi-peer fan-out, which block_scheduler.go
// says in so many words it never does, and that the setting is not read with
// multi-peer download off, when the admission budget and the peer package's
// download-timeout budget read it in every mode. Same shape as
// TestOptimisticMiningDocNoStalePhrases.
func TestBelowCheckpointRouteDocNoStalePhrases(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	require.True(t, ok, "unable to determine test file location")

	settingsDir := filepath.Dir(thisFile)
	repoRoot := filepath.Dir(settingsDir)

	read := func(parts ...string) string {
		b, err := os.ReadFile(filepath.Join(parts...))
		require.NoError(t, err, "reading %s", filepath.Join(parts...))

		return string(b)
	}

	blockvalidationGo := read(settingsDir, "blockvalidation_settings.go")
	legacyGo := read(settingsDir, "legacy_settings.go")
	blockvalidationMd := read(repoRoot, "docs", "references", "settings", "services", "blockvalidation_settings.md")
	legacyMd := read(repoRoot, "docs", "references", "settings", "services", "legacy_settings.md")
	settingsConf := read(repoRoot, "settings.conf")

	stalePhrases := []string{
		"existing inline netsync pipeline",
		"Leave false until a controlled soak",
		"throughput gain and UTXO-set equivalence",
		"collapses back to one peer",
		"lowers it further for large blocks",
		"is applied on top as a ceiling",
		"Only applies while legacy_multiPeerBlockDownload is on",
	}

	for name, text := range map[string]string{
		"blockvalidation_settings.go": blockvalidationGo,
		"legacy_settings.go":          legacyGo,
		"blockvalidation_settings.md": blockvalidationMd,
		"legacy_settings.md":          legacyMd,
	} {
		for _, phrase := range stalePhrases {
			require.False(t, strings.Contains(text, phrase), "stale below-checkpoint or download-depth prose is back in %s: %q", name, phrase)
		}
	}

	for _, key := range []string{"blockvalidation_outpoint_only_below_checkpoint", "blockvalidation_legacy_unified_below_checkpoint"} {
		require.True(t, strings.Contains(blockvalidationMd, key), "the settings reference must have a row for the route key %s", key)
		require.True(t, strings.Contains(settingsConf, key+" "), "settings.conf must carry the route key %s so an operator can see it exists", key)
	}

	require.True(t, strings.Contains(legacyGo, "parkBackstopBytes"), "the depth longdesc must name the disk backstop that actually brakes the download")
}
