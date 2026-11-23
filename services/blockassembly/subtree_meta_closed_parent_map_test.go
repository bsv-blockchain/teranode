package blockassembly

import (
	"sync"
	"testing"

	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// A subtree storage request carries the tx map that was current when its subtree
// completed, and the storage worker reads it asynchronously. A fork reorg closes
// the pre-reorg disk tx map at its first forward step's commit, and a single block
// clears its previous map at its commit, so a request still in flight may read a
// map that has been closed under it. The read must be memory safe, and the outcome
// is a missing subtree meta file, logged, rather than a crash or wrong inpoints.
func TestBuildSubtreeMeta_ParentMapClosedUnderTheRequest(t *testing.T) {
	diskMap, err := subtreeprocessor.NewDiskTxMap(subtreeprocessor.DiskTxMapOptions{BasePaths: []string{t.TempDir()}})
	require.NoError(t, err)

	st, err := subtreepkg.NewTreeByLeafCount(1024)
	require.NoError(t, err)
	require.NoError(t, st.AddCoinbaseNode())

	for i := range 200 {
		tx := newTestTx(t, byte(i))

		parents, err := subtreepkg.NewTxInpointsFromTx(tx)
		require.NoError(t, err)

		diskMap.Set(*tx.TxIDChainHash(), &parents)
		require.NoError(t, st.AddNode(*tx.TxIDChainHash(), 1, 100))
	}

	require.NoError(t, diskMap.Flush())

	ba := &BlockAssembly{logger: ulogger.TestLogger{}}
	req := subtreeprocessor.NewSubtreeRequest{Subtree: st, ParentTxMap: diskMap}

	_, ok := ba.buildSubtreeMeta(req)
	require.True(t, ok, "precondition: the meta builds while the map is open")

	// Storage workers read the map while it is closed, as a request still in
	// flight at a reorg step's commit would.
	var wg sync.WaitGroup

	start := make(chan struct{})

	for range 4 {
		wg.Add(1)

		go func() {
			defer wg.Done()
			<-start

			for range 20 {
				_, _ = ba.buildSubtreeMeta(req)
			}
		}()
	}

	close(start)
	require.NoError(t, diskMap.Close())
	wg.Wait()

	_, ok = ba.buildSubtreeMeta(req)
	require.False(t, ok, "a closed parent map holds no entries, so the meta is not built")

	_, found := diskMap.Get(st.Nodes[1].Hash)
	require.False(t, found, "a read of the closed map finds nothing")
}
