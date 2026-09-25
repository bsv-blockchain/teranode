package utxoset

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestOnlyTheSchemaAndTheStampNameTheStampedTable is the design's SC-2, a source scan. The
// tx_mined_stamped table records, per stamped window, the tip at which the deep stamp
// completed. It is NOT the per-window marker table that was rejected on 15 September, and the
// difference is exactly who touches it: only the stamp writes it and only the drop reads and
// deletes it. No reorg, un-mine, invalidation or unspend path may name it, because a table that
// every chain switch had to keep consistent would be the rejected design under a new name.
//
// The schema declares it, stamp.go writes it and reads it back for the completion check, and
// tx_mined.go's drop reads and deletes it. Nothing else may.
func TestOnlyTheSchemaAndTheStampNameTheStampedTable(t *testing.T) {
	allowed := map[string]bool{
		"schema.go":   true,
		"stamp.go":    true,
		"tx_mined.go": true,
	}

	entries, err := os.ReadDir(".")
	require.NoError(t, err)

	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}

		src, err := os.ReadFile(filepath.Clean(name))
		require.NoError(t, err)

		if !strings.Contains(string(src), "tx_mined_stamped") {
			continue
		}

		require.True(t, allowed[name],
			"%s names tx_mined_stamped: only the schema, the stamp and the drop may, or the completion record becomes the per-window marker table the design rejected", name)
	}
}
