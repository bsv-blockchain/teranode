package teraslab_test

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMarkTransactionsOnLongestChain(t *testing.T) {
	store, _, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()
	ctx := context.Background()

	err := store.SetBlockHeight(100)
	require.NoError(t, err)

	tx1, _ := bt.NewTxFromString("010000000000000000ef0152a9231baa4e4b05dc30c8fbb7787bab5f460d4d33b039c39dd8cc006f3363e4020000006b483045022100ce3605307dd1633d3c14de4a0cf0df1439f392994e561b648897c4e540baa9ad02207af74878a7575a95c9599e9cdc7e6d73308608ee59abcd90af3ea1a5c0cca41541210275f8390df62d1e951920b623b8ef9c2a67c4d2574d408e422fb334dd1f3ee5b6ffffffff706b9600000000001976a914a32f7eaae3afd5f73a2d6009b93f91aa11d16eef88ac05404b4c00000000001976a914aabb8c2f08567e2d29e3a64f1f833eee85aaf74d88ac80841e00000000001976a914a4aff400bef2fa074169453e703c611c6b9df51588ac204e0000000000001976a9144669d92d46393c38594b2f07587f01b3e5289f6088ac204e0000000000001976a914a461497034343a91683e86b568c8945fb73aca0288ac99fe2a00000000001976a914de7850e419719258077abd37d4fcccdb0a659b9388ac00000000")

	tx2, _ := bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff17032dff0c2f71646c6e6b2f5e931c7f7b6199adf35e1300ffffffff01d15fa012000000001976a91417db35d440a673a218e70a5b9d07f895facf50d288ac00000000")

	_, err = store.Create(ctx, tx1, 100)
	require.NoError(t, err)
	_, err = store.Create(ctx, tx2, 100)
	require.NoError(t, err)

	t.Run("initial UnminedSince is set to block height", func(t *testing.T) {
		resp, err := store.Get(ctx, tx1.TxIDChainHash(), fields.UnminedSince)
		require.NoError(t, err)
		assert.Equal(t, uint32(100), resp.UnminedSince)
	})

	t.Run("mark on longest chain clears UnminedSince", func(t *testing.T) {
		txHashes := []chainhash.Hash{*tx1.TxIDChainHash(), *tx2.TxIDChainHash()}
		err := store.MarkTransactionsOnLongestChain(ctx, txHashes, true)
		require.NoError(t, err)

		resp, err := store.Get(ctx, tx1.TxIDChainHash(), fields.UnminedSince)
		require.NoError(t, err)
		assert.Equal(t, uint32(0), resp.UnminedSince)

		resp, err = store.Get(ctx, tx2.TxIDChainHash(), fields.UnminedSince)
		require.NoError(t, err)
		assert.Equal(t, uint32(0), resp.UnminedSince)
	})

	t.Run("mark off longest chain sets UnminedSince to current height", func(t *testing.T) {
		err := store.SetBlockHeight(101)
		require.NoError(t, err)

		txHashes := []chainhash.Hash{*tx1.TxIDChainHash(), *tx2.TxIDChainHash()}
		err = store.MarkTransactionsOnLongestChain(ctx, txHashes, false)
		require.NoError(t, err)

		resp, err := store.Get(ctx, tx1.TxIDChainHash(), fields.UnminedSince)
		require.NoError(t, err)
		assert.Equal(t, uint32(101), resp.UnminedSince)
	})

	t.Run("mark back on longest chain clears UnminedSince again", func(t *testing.T) {
		txHashes := []chainhash.Hash{*tx1.TxIDChainHash()}
		err := store.MarkTransactionsOnLongestChain(ctx, txHashes, true)
		require.NoError(t, err)

		resp, err := store.Get(ctx, tx1.TxIDChainHash(), fields.UnminedSince)
		require.NoError(t, err)
		assert.Equal(t, uint32(0), resp.UnminedSince)
	})

	t.Run("empty hash list is no-op", func(t *testing.T) {
		err := store.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{}, true)
		require.NoError(t, err)
	})
}

func TestSetMinedMulti(t *testing.T) {
	store, _, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()
	ctx := context.Background()

	err := store.SetBlockHeight(100)
	require.NoError(t, err)

	tx, _ := bt.NewTxFromString("010000000000000000ef0152a9231baa4e4b05dc30c8fbb7787bab5f460d4d33b039c39dd8cc006f3363e4020000006b483045022100ce3605307dd1633d3c14de4a0cf0df1439f392994e561b648897c4e540baa9ad02207af74878a7575a95c9599e9cdc7e6d73308608ee59abcd90af3ea1a5c0cca41541210275f8390df62d1e951920b623b8ef9c2a67c4d2574d408e422fb334dd1f3ee5b6ffffffff706b9600000000001976a914a32f7eaae3afd5f73a2d6009b93f91aa11d16eef88ac05404b4c00000000001976a914aabb8c2f08567e2d29e3a64f1f833eee85aaf74d88ac80841e00000000001976a914a4aff400bef2fa074169453e703c611c6b9df51588ac204e0000000000001976a9144669d92d46393c38594b2f07587f01b3e5289f6088ac204e0000000000001976a914a461497034343a91683e86b568c8945fb73aca0288ac99fe2a00000000001976a914de7850e419719258077abd37d4fcccdb0a659b9388ac00000000")

	_, err = store.Create(ctx, tx, 0)
	require.NoError(t, err)

	txHash := tx.TxIDChainHash()

	t.Run("set mined once", func(t *testing.T) {
		blockIDsMap, err := store.SetMinedMulti(ctx, []*chainhash.Hash{txHash},
			utxo.MinedBlockInfo{BlockID: 123, BlockHeight: 100, SubtreeIdx: 2, OnLongestChain: true})
		require.NoError(t, err)
		require.Len(t, blockIDsMap, 1)
		assert.Equal(t, []uint32{123}, blockIDsMap[*txHash])

		resp, err := store.Get(ctx, txHash, fields.BlockIDs)
		require.NoError(t, err)
		assert.Equal(t, []uint32{123}, resp.BlockIDs)
		assert.Equal(t, []uint32{100}, resp.BlockHeights)
		assert.Equal(t, []int{2}, resp.SubtreeIdxs)
	})

	t.Run("set mined second block", func(t *testing.T) {
		blockIDsMap, err := store.SetMinedMulti(ctx, []*chainhash.Hash{txHash},
			utxo.MinedBlockInfo{BlockID: 124, BlockHeight: 101, SubtreeIdx: 1, OnLongestChain: true})
		require.NoError(t, err)
		require.Len(t, blockIDsMap, 1)
		assert.Equal(t, []uint32{123, 124}, blockIDsMap[*txHash])

		resp, err := store.Get(ctx, txHash, fields.BlockIDs)
		require.NoError(t, err)
		assert.Equal(t, []uint32{123, 124}, resp.BlockIDs)
		assert.Equal(t, []uint32{100, 101}, resp.BlockHeights)
		assert.Equal(t, []int{2, 1}, resp.SubtreeIdxs)
	})

	t.Run("unset mined for one block", func(t *testing.T) {
		blockIDsMap, err := store.SetMinedMulti(ctx, []*chainhash.Hash{txHash},
			utxo.MinedBlockInfo{BlockID: 123, BlockHeight: 100, SubtreeIdx: 2, UnsetMined: true})
		require.NoError(t, err)
		require.Len(t, blockIDsMap, 1)

		resp, err := store.Get(ctx, txHash, fields.BlockIDs)
		require.NoError(t, err)
		assert.Equal(t, []uint32{124}, resp.BlockIDs)
		assert.Equal(t, []uint32{101}, resp.BlockHeights)
		assert.Equal(t, []int{1}, resp.SubtreeIdxs)
	})
}
