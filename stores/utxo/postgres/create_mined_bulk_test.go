package postgres

import (
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/stretchr/testify/require"
)

// TestSendCreateBatchMinedBulk verifies the consolidated UNNEST bulk path (which
// replaced the COPY+staging path) writes single-block mined metadata identically
// to createDirect across a multi-item batch — the legacy catch-up create path.
//
// Mined items: block_ids/block_heights/subtree_idxs populated, unmined_since
// cleared. Unmined items in the same batch: unmined_since = blockHeight, no
// block ids. This exercises the per-row CASE discrimination in the bulk INSERT.
func TestSendCreateBatchMinedBulk(t *testing.T) {
	store, ctx := setupTestStore(t)

	const blockHeight = uint32(100)
	const blockID = uint32(42)
	const subtreeIdx = 7

	mined := []bool{true, true, true, false, false}
	batch := make([]*batchCreateItem, len(mined))
	for i, isMined := range mined {
		opts := &utxo.CreateOptions{}
		if isMined {
			opts.MinedBlockInfos = []utxo.MinedBlockInfo{{
				BlockID: blockID, BlockHeight: blockHeight, SubtreeIdx: subtreeIdx, OnLongestChain: true,
			}}
		}
		batch[i] = &batchCreateItem{
			tx:          makeBenchCreateTx(),
			blockHeight: blockHeight,
			options:     opts,
			done:        make(chan batchCreateResult, 1),
		}
	}

	store.sendCreateBatch(batch) // len > 1 → bulk UNNEST path

	for i, it := range batch {
		res := <-it.done
		require.NoError(t, res.Err, "create item %d", i)
		require.NotNil(t, res.Data, "create item %d", i)
	}

	for i, isMined := range mined {
		txHash := batch[i].tx.TxIDChainHash()
		got, err := store.Get(ctx, txHash,
			fields.BlockIDs, fields.BlockHeights, fields.SubtreeIdxs, fields.UnminedSince)
		require.NoError(t, err, "get item %d", i)
		require.NotNil(t, got, "get item %d", i)

		if isMined {
			require.Equal(t, uint32(0), got.UnminedSince, "mined item %d: unmined_since must be cleared", i)
			require.Equal(t, []uint32{blockID}, got.BlockIDs, "mined item %d: block ids", i)
			require.Equal(t, []uint32{blockHeight}, got.BlockHeights, "mined item %d: block heights", i)
			require.Equal(t, []int{subtreeIdx}, got.SubtreeIdxs, "mined item %d: subtree idxs", i)
		} else {
			require.Equal(t, blockHeight, got.UnminedSince, "unmined item %d: unmined_since must be set", i)
			require.Empty(t, got.BlockIDs, "unmined item %d: no block ids", i)
		}
	}
}
