package blockvalidation

import (
	"context"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestReloadSubtreesForInvalidBlock_IsRaceFreeAgainstConcurrentRelease pins the
// synchronisation on the invalid-block reload.
//
// The reload replaced the block's SubtreeSlices header and then filled its
// entries with no lock held, while the lastValidatedBlocks TTL cleaner runs
// ReleaseSubtreeNodes on the same block under the block's subtree mutex — the
// block stays reachable in that cache between the Get and the Delete around
// this call. Taking the mutex on one side only is no synchronisation at all.
//
// The consequence is not academic: entries the reload has just filled get nil-ed
// underneath it, UpdateTxMinedStatus then reports "missing subtree %d of %d",
// and the invalid block's transactions are never un-mined — leaving stale mined
// flags in the UTXO store.
//
// Run under -race, which is what fails this test if either side stops
// synchronising. Only the absence of a race is asserted: which of the two
// writers lands last is legitimately unspecified.
func TestReloadSubtreesForInvalidBlock_IsRaceFreeAgainstConcurrentRelease(t *testing.T) {
	subtreeStore := blobmemory.New()

	hashes := make([]*chainhash.Hash, 0, 2)

	for _, label := range []string{"race-a", "race-b"} {
		st, err := subtreepkg.NewTreeByLeafCount(2)
		require.NoError(t, err)

		if label == "race-a" {
			require.NoError(t, st.AddCoinbaseNode())
			require.NoError(t, st.AddNode(chainhash.HashH([]byte(label)), 1, 0))
		} else {
			require.NoError(t, st.AddNode(chainhash.HashH([]byte(label+"-0")), 1, 0))
			require.NoError(t, st.AddNode(chainhash.HashH([]byte(label+"-1")), 1, 0))
		}

		serialized, err := st.Serialize()
		require.NoError(t, err)
		require.NoError(t, subtreeStore.Set(context.Background(), st.RootHash()[:], fileformat.FileTypeSubtree, serialized))

		hashes = append(hashes, st.RootHash())
	}

	nBits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	coinbase, err := bt.NewTxFromString(model.CoinbaseHex)
	require.NoError(t, err)

	header := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      1,
		Bits:           *nBits,
	}

	block, err := model.NewBlock(header, coinbase, hashes, 4, 123, 0, 0)
	require.NoError(t, err)

	u := &BlockValidation{logger: ulogger.TestLogger{}, subtreeStore: subtreeStore}

	ctx := context.Background()

	var wg sync.WaitGroup

	wg.Add(2)

	go func() {
		defer wg.Done()

		for i := 0; i < 50; i++ {
			u.reloadSubtreesForInvalidBlock(ctx, block)
		}
	}()

	go func() {
		defer wg.Done()

		for i := 0; i < 50; i++ {
			// The TTL cleaner's eviction, which takes the block's subtree mutex.
			_ = block.ReleaseSubtreeNodes(nil)
		}
	}()

	wg.Wait()
}
