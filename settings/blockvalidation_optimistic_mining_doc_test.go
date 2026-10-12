package settings

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestOptimisticMiningDocNoStalePhrases guards against the optimistic-mining prose
// regressing back to phrasing that named script validation, rather than the
// block-level checks (merkle root, coinbase/BIP34, duplicate transactions,
// transaction ordering and parent-spend checks, and the old-block-ID scan), as the
// work deferred to the background. CheckBlockSubtrees() runs synchronously, ahead
// of the optimistic AddBlock, so subtree (transaction and script) validation is
// already done by the time a block is optimistically added - only the block-level
// checks continue in the background. Both the settings reference doc and the
// struct tag longdesc in blockvalidation_settings.go described this incorrectly
// once; this test fails loudly if either regresses.
//
// This is intentionally a separate, minimal file rather than living in
// settings/settings_doc_test.go, which cross-checks doc/tag/runtime defaults for
// every settings key across all of docs/references/settings/.
func TestOptimisticMiningDocNoStalePhrases(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	require.True(t, ok, "unable to determine test file location")

	settingsDir := filepath.Dir(thisFile)
	repoRoot := filepath.Dir(settingsDir)

	docPath := filepath.Join(repoRoot, "docs", "references", "settings", "services", "blockvalidation_settings.md")
	docBytes, err := os.ReadFile(docPath)
	require.NoError(t, err, "reading settings reference doc")

	settingsGoPath := filepath.Join(settingsDir, "blockvalidation_settings.go")
	settingsGoBytes, err := os.ReadFile(settingsGoPath)
	require.NoError(t, err, "reading blockvalidation_settings.go")

	stalePhrases := []string{
		"before full script validation completes",
		"Full script validation continues in parallel",
		// Deliberately stops before "background": both "runs in background" and
		// "runs in the background" have been in the tree, so anchoring on either
		// one alone would let the other back in.
		"subtree validation runs in",
	}

	for _, phrase := range stalePhrases {
		require.NotContains(t, string(docBytes), phrase,
			"stale optimistic-mining prose is back in the settings doc: %q", phrase)
		require.NotContains(t, string(settingsGoBytes), phrase,
			"stale optimistic-mining prose is back in the longdesc struct tag: %q", phrase)
	}
}
