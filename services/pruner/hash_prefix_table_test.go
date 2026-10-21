package pruner

import (
	"testing"

	"github.com/bsv-blockchain/teranode/stores/blob/storetypes"
	"github.com/stretchr/testify/require"
)

// TestDefaultHashPrefixDelegatesToSharedTable locks the pruner's view of the
// blob layout to the single source. The failure this prevents: the pruner's
// private table drifting from the daemon's, so deletion looks for files under
// one prefix while the writer put them under another.
func TestDefaultHashPrefixDelegatesToSharedTable(t *testing.T) {
	for _, st := range []storetypes.BlobStoreType{
		storetypes.TXSTORE, storetypes.SUBTREESTORE, storetypes.BLOCKSTORE,
		storetypes.TEMPSTORE, storetypes.BLOCKPERSISTERSTORE, storetypes.PEERREGISTRYSTORE,
	} {
		require.Equal(t, storetypes.DefaultHashPrefix(st), defaultHashPrefix(st), "pruner/daemon hash-prefix tables diverged for %s", st)
	}
}
