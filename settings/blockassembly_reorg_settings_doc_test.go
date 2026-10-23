package settings

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBlockAssemblyReorgSettingsDocNoStalePhrases guards the operator text for
// blockassembly_maxBlockReorgCatchup and blockassembly_maxBlockReorgRollback.
// Both longdescs described a reorg depth limit. The code applies none: the
// catchup value only sets the initial capacity of two BlockAssembler maps, and
// the rollback value is read by nothing. Same shape as
// TestBelowCheckpointRouteDocNoStalePhrases.
func TestBlockAssemblyReorgSettingsDocNoStalePhrases(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	require.True(t, ok, "unable to determine test file location")

	settingsDir := filepath.Dir(thisFile)
	repoRoot := filepath.Dir(settingsDir)

	read := func(parts ...string) string {
		b, err := os.ReadFile(filepath.Join(parts...))
		require.NoError(t, err, "reading %s", filepath.Join(parts...))

		return string(b)
	}

	blockassemblyGo := read(settingsDir, "blockassembly_settings.go")
	blockassemblyMd := read(repoRoot, "docs", "references", "settings", "services", "blockassembly_settings.md")

	stalePhrases := []string{
		"Maximum blocks to catch up during reorg",
		"Maximum blocks to roll back during reorg",
		"Reorg catchup limit",
		"Reorg rollback limit",
		"Limits the maximum number of blocks Block Assembly will attempt to catch up",
		"Limits the maximum number of blocks to roll back",
		"capping the number of blocks to process",
		"If reorg is deeper than this value",
		"Increase for networks with deeper expected reorgs",
		"Works with `MaxBlockReorgCatchup`, `MaxBlockReorgRollback`",
	}

	for name, text := range map[string]string{
		"blockassembly_settings.go": blockassemblyGo,
		"blockassembly_settings.md": blockassemblyMd,
	} {
		for _, phrase := range stalePhrases {
			require.False(t, strings.Contains(text, phrase), "stale reorg-limit prose is back in %s: %q", name, phrase)
		}
	}

	for _, key := range []string{"blockassembly_maxBlockReorgCatchup", "blockassembly_maxBlockReorgRollback"} {
		line := lineContaining(t, blockassemblyGo, `key:"`+key+`"`)
		require.True(t, strings.Contains(line, "does not limit reorg depth"), "the %s longdesc must state that it does not limit reorg depth", key)
	}
}

func lineContaining(t *testing.T, text, needle string) string {
	t.Helper()

	for _, line := range strings.Split(text, "\n") {
		if strings.Contains(line, needle) {
			return line
		}
	}

	require.Failf(t, "line not found", "no line contains %q", needle)

	return ""
}
