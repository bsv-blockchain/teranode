package teraslab_test

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/stretchr/testify/require"
)

// TestBlockIDsCatchupLifecycle pins down teraslab's BlockIDs behaviour in exactly
// the situation that wedges a TeraTestNet catchup at ~height 220: validation of a
// block reads each non-in-block parent's BlockIDs (model/Block.go
// getParentTxMetaBlockIDs) and aborts with "parent transaction ... has no block
// IDs" when the parent record exists but carries zero block entries.
//
// The catchup quick-validate path Creates a block's transactions (unmined) before
// the block is accepted and its txs are SetMined. So an in-block parent legitimately
// has no block IDs until mined — meaning the upstream validator MUST recognise it as
// in-block rather than relying on BlockIDs. These subtests confirm teraslab returns
// the CORRECT signal at each step, which is what tells us whether the wedge is a
// teraslab bug or an upstream in-block-detection bug.
func TestBlockIDsCatchupLifecycle(t *testing.T) {
	store, ctx, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()

	// A parent transaction and a child that spends parent.Outputs[0] — the same
	// parent→child shape a block carries across its subtrees during catchup.
	parent, err := bt.NewTxFromString("010000000000000000ef0152a9231baa4e4b05dc30c8fbb7787bab5f460d4d33b039c39dd8cc006f3363e4020000006b483045022100ce3605307dd1633d3c14de4a0cf0df1439f392994e561b648897c4e540baa9ad02207af74878a7575a95c9599e9cdc7e6d73308608ee59abcd90af3ea1a5c0cca41541210275f8390df62d1e951920b623b8ef9c2a67c4d2574d408e422fb334dd1f3ee5b6ffffffff706b9600000000001976a914a32f7eaae3afd5f73a2d6009b93f91aa11d16eef88ac05404b4c00000000001976a914aabb8c2f08567e2d29e3a64f1f833eee85aaf74d88ac80841e00000000001976a914a4aff400bef2fa074169453e703c611c6b9df51588ac204e0000000000001976a9144669d92d46393c38594b2f07587f01b3e5289f6088ac204e0000000000001976a914a461497034343a91683e86b568c8945fb73aca0288ac99fe2a00000000001976a914de7850e419719258077abd37d4fcccdb0a659b9388ac00000000")
	require.NoError(t, err)

	// Create the parent UNMINED, as catchup quick-validate does (no SetMinedMulti yet).
	_, err = store.Create(ctx, parent, 0)
	require.NoError(t, err)

	parentHash := parent.TxIDChainHash()

	t.Run("created-but-unmined parent exists with empty BlockIDs", func(t *testing.T) {
		// This is the exact trigger for the upstream "has no block IDs" abort.
		// teraslab is CORRECT to return (exists, empty) here — an unmined tx has no
		// block membership. If this passes, the wedge is upstream in-block detection,
		// NOT teraslab dropping mined state.
		data, err := store.Get(ctx, parentHash, fields.BlockIDs)
		require.NoError(t, err, "unmined parent must exist (not ErrTxNotFound)")
		require.Empty(t, data.BlockIDs, "an unmined tx legitimately has no block IDs")
	})

	t.Run("mined parent returns its block IDs", func(t *testing.T) {
		_, err := store.SetMinedMulti(ctx, []*chainhash.Hash{parentHash}, utxo.MinedBlockInfo{
			BlockID: 220, BlockHeight: 220, SubtreeIdx: 0, OnLongestChain: true,
		})
		require.NoError(t, err)

		data, err := store.Get(ctx, parentHash, fields.BlockIDs)
		require.NoError(t, err)
		require.Equal(t, []uint32{220}, data.BlockIDs, "mined parent must return its block ID")
		require.Equal(t, []uint32{220}, data.BlockHeights)
	})

	t.Run("re-mining the same block is idempotent (no duplicate accumulation)", func(t *testing.T) {
		// Catchup re-validates the same block hundreds of times (the log shows ~495
		// attempts on one block). Re-marking the same (tx, blockID) must NOT append a
		// duplicate block entry — otherwise the entries grow without bound and
		// eventually exceed teraslab's inline cap, surfacing "block entries: truncated".
		for i := 0; i < 5; i++ {
			_, err := store.SetMinedMulti(ctx, []*chainhash.Hash{parentHash}, utxo.MinedBlockInfo{
				BlockID: 220, BlockHeight: 220, SubtreeIdx: 0, OnLongestChain: true,
			})
			require.NoError(t, err)
		}

		data, err := store.Get(ctx, parentHash, fields.BlockIDs)
		require.NoError(t, err)
		require.Equal(t, []uint32{220}, data.BlockIDs, "re-mining the same block must not duplicate block entries")
	})
}
