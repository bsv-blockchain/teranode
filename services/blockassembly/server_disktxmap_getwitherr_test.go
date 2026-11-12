package blockassembly

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
	"github.com/stretchr/testify/require"
)

// getWithErrTxMap wraps a real TxInpointsMap and forces GetWithErr to fail for
// a chosen hash. It stands in for DiskTxMap's GetWithErr in a test that does
// not need a real Badger store, isolating storeSubtreeData's type-assertion
// wiring from DiskTxMap's own internals (covered separately by
// subtreeprocessor.TestDiskTxMap_GetWithErr_DoesNotRecordOnMap).
type getWithErrTxMap struct {
	subtreeprocessor.TxInpointsMap
	failFor chainhash.Hash
	failErr error
}

func (g *getWithErrTxMap) GetWithErr(hash chainhash.Hash) (*subtreepkg.TxInpoints, bool, error) {
	if hash.Equal(g.failFor) {
		return nil, false, g.failErr
	}

	inpoints, found := g.TxInpointsMap.Get(hash)

	return inpoints, found, nil
}

// storeSubtreeData must handle a ParentTxMap read error (via GetWithErr)
// itself, rather than treating it as a plain "not found" without comment or
// letting it propagate as a panic. A read error causing this node's meta to
// be skipped is the existing "not found" behaviour (see
// "Test storeSubtreeData - meta missing"); what this test pins is that the
// GetWithErr path is actually taken and does not crash or hang storage.
func TestStoreSubtreeData_ParentTxMapGetWithErr(t *testing.T) {
	server, subtreeStore, subtree, txMap := setup(t)

	failingNode := subtree.Nodes[0].Hash

	wrapped := &getWithErrTxMap{
		TxInpointsMap: txMap,
		failFor:       failingNode,
		failErr:       errors.NewStorageError("disk tx map: reading %s on disk 0", failingNode, errors.NewProcessingError("boom")),
	}

	subtreeRetryChan := make(chan *subtreeRetrySend, 1_000)

	subtreeDone, allDone, err := server.storeSubtreeData(t.Context(), subtreeprocessor.NewSubtreeRequest{
		Subtree:           subtree,
		ParentTxMap:       wrapped,
		DeletedTxs:        nil,
		ErrChan:           nil,
		OnStorageComplete: nil,
	}, subtreeRetryChan)
	require.NoError(t, err)

	storedOK := <-subtreeDone
	require.True(t, storedOK, "the subtree itself still stores despite the meta read error")

	<-allDone

	// The meta build bails out on the first missing node (existing
	// behaviour), so no meta is stored - same observable outcome as the
	// "meta missing" case, just reached through the GetWithErr path.
	_, err = subtreeStore.Get(t.Context(), subtree.RootHash()[:], fileformat.FileTypeSubtreeMeta)
	require.Error(t, err)
}
