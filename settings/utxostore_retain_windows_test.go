package settings

import (
	"testing"

	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

// TestUtxoStoreRetainWindowsIndefinitelyIsWired proves the loader reads the key. A struct tag
// alone is not wiring: only a getBool call in settings.go populates the field, and a
// default-value assertion cannot tell a wired false from a disconnected one.
func TestUtxoStoreRetainWindowsIndefinitelyIsWired(t *testing.T) {
	gocore.Config().Set("utxostore_retainWindowsIndefinitely", "")
	require.False(t, NewSettings().UtxoStore.RetainWindowsIndefinitely, "off by default: windows and undo partitions drop on their rules")

	gocore.Config().Set("utxostore_retainWindowsIndefinitely", "true")
	t.Cleanup(func() { gocore.Config().Set("utxostore_retainWindowsIndefinitely", "") })

	require.True(t, NewSettings().UtxoStore.RetainWindowsIndefinitely)
}
