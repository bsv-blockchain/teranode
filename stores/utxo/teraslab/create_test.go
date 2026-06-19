package teraslab_test

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateAndGet(t *testing.T) {
	store, ctx, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()

	tx, _ := bt.NewTxFromString("010000000000000000ef0152a9231baa4e4b05dc30c8fbb7787bab5f460d4d33b039c39dd8cc006f3363e4020000006b483045022100ce3605307dd1633d3c14de4a0cf0df1439f392994e561b648897c4e540baa9ad02207af74878a7575a95c9599e9cdc7e6d73308608ee59abcd90af3ea1a5c0cca41541210275f8390df62d1e951920b623b8ef9c2a67c4d2574d408e422fb334dd1f3ee5b6ffffffff706b9600000000001976a914a32f7eaae3afd5f73a2d6009b93f91aa11d16eef88ac05404b4c00000000001976a914aabb8c2f08567e2d29e3a64f1f833eee85aaf74d88ac80841e00000000001976a914a4aff400bef2fa074169453e703c611c6b9df51588ac204e0000000000001976a9144669d92d46393c38594b2f07587f01b3e5289f6088ac204e0000000000001976a914a461497034343a91683e86b568c8945fb73aca0288ac99fe2a00000000001976a914de7850e419719258077abd37d4fcccdb0a659b9388ac00000000")

	t.Run("create and get metadata", func(t *testing.T) {
		meta, err := store.Create(ctx, tx, 0)
		require.NoError(t, err)
		require.NotNil(t, meta)

		resp, err := store.Get(ctx, tx.TxIDChainHash())
		require.NoError(t, err)
		assert.Equal(t, tx.TxIDChainHash().String(), resp.Tx.TxID())
		assert.Equal(t, uint64(215), resp.Fee)
		assert.Equal(t, uint64(328), resp.SizeInBytes)
		assert.False(t, resp.IsCoinbase)
		assert.False(t, resp.Conflicting)
		assert.False(t, resp.Locked)
	})

	t.Run("create duplicate fails with ErrTxExists", func(t *testing.T) {
		_, err := store.Create(ctx, tx, 0)
		require.Error(t, err)
		assert.True(t, errors.Is(err, errors.ErrTxExists))
	})

	t.Run("get non-existent tx fails", func(t *testing.T) {
		fakeHash := tx.TxIDChainHash()
		fakeHash[0] ^= 0xFF
		_, err := store.Get(ctx, fakeHash)
		require.Error(t, err)
		assert.True(t, errors.Is(err, errors.ErrTxNotFound))
	})
}

func TestCreateCoinbase(t *testing.T) {
	store, ctx, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()

	coinbaseTx, _ := bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff19031404002f6d332d617369612fdf5128e62eda1a07e94dbdbdffffffff0500ca9a3b000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac00ca9a3b000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac00ca9a3b000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac00ca9a3b000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac00ca9a3b000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac00000000")

	_, err := store.Create(ctx, coinbaseTx, 0)
	require.NoError(t, err)

	resp, err := store.Get(ctx, coinbaseTx.TxIDChainHash())
	require.NoError(t, err)
	assert.True(t, resp.IsCoinbase)
}

func TestCreateConflicting(t *testing.T) {
	store, ctx, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()

	tx, _ := bt.NewTxFromString("010000000000000000ef0152a9231baa4e4b05dc30c8fbb7787bab5f460d4d33b039c39dd8cc006f3363e4020000006b483045022100ce3605307dd1633d3c14de4a0cf0df1439f392994e561b648897c4e540baa9ad02207af74878a7575a95c9599e9cdc7e6d73308608ee59abcd90af3ea1a5c0cca41541210275f8390df62d1e951920b623b8ef9c2a67c4d2574d408e422fb334dd1f3ee5b6ffffffff706b9600000000001976a914a32f7eaae3afd5f73a2d6009b93f91aa11d16eef88ac05404b4c00000000001976a914aabb8c2f08567e2d29e3a64f1f833eee85aaf74d88ac80841e00000000001976a914a4aff400bef2fa074169453e703c611c6b9df51588ac204e0000000000001976a9144669d92d46393c38594b2f07587f01b3e5289f6088ac204e0000000000001976a914a461497034343a91683e86b568c8945fb73aca0288ac99fe2a00000000001976a914de7850e419719258077abd37d4fcccdb0a659b9388ac00000000")

	_, err := store.Create(ctx, tx, 0, utxo.WithConflicting(true))
	require.NoError(t, err)

	resp, err := store.Get(ctx, tx.TxIDChainHash(), fields.Conflicting)
	require.NoError(t, err)
	assert.True(t, resp.Conflicting)
}

func TestCreateWithMinedBlockInfo(t *testing.T) {
	store, ctx, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()

	err := store.SetBlockHeight(100)
	require.NoError(t, err)

	tx, _ := bt.NewTxFromString("010000000000000000ef0152a9231baa4e4b05dc30c8fbb7787bab5f460d4d33b039c39dd8cc006f3363e4020000006b483045022100ce3605307dd1633d3c14de4a0cf0df1439f392994e561b648897c4e540baa9ad02207af74878a7575a95c9599e9cdc7e6d73308608ee59abcd90af3ea1a5c0cca41541210275f8390df62d1e951920b623b8ef9c2a67c4d2574d408e422fb334dd1f3ee5b6ffffffff706b9600000000001976a914a32f7eaae3afd5f73a2d6009b93f91aa11d16eef88ac05404b4c00000000001976a914aabb8c2f08567e2d29e3a64f1f833eee85aaf74d88ac80841e00000000001976a914a4aff400bef2fa074169453e703c611c6b9df51588ac204e0000000000001976a9144669d92d46393c38594b2f07587f01b3e5289f6088ac204e0000000000001976a914a461497034343a91683e86b568c8945fb73aca0288ac99fe2a00000000001976a914de7850e419719258077abd37d4fcccdb0a659b9388ac00000000")

	_, err = store.Create(ctx, tx, 100, utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{
		BlockID:        42,
		BlockHeight:    100,
		SubtreeIdx:     3,
		OnLongestChain: true,
	}))
	require.NoError(t, err)

	resp, err := store.Get(ctx, tx.TxIDChainHash())
	require.NoError(t, err)
	require.Len(t, resp.BlockIDs, 1)
	assert.Equal(t, uint32(42), resp.BlockIDs[0])
	assert.Equal(t, uint32(100), resp.BlockHeights[0])
	assert.Equal(t, 3, resp.SubtreeIdxs[0])
}

func TestDelete(t *testing.T) {
	store, ctx, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()

	tx, _ := bt.NewTxFromString("010000000000000000ef0152a9231baa4e4b05dc30c8fbb7787bab5f460d4d33b039c39dd8cc006f3363e4020000006b483045022100ce3605307dd1633d3c14de4a0cf0df1439f392994e561b648897c4e540baa9ad02207af74878a7575a95c9599e9cdc7e6d73308608ee59abcd90af3ea1a5c0cca41541210275f8390df62d1e951920b623b8ef9c2a67c4d2574d408e422fb334dd1f3ee5b6ffffffff706b9600000000001976a914a32f7eaae3afd5f73a2d6009b93f91aa11d16eef88ac05404b4c00000000001976a914aabb8c2f08567e2d29e3a64f1f833eee85aaf74d88ac80841e00000000001976a914a4aff400bef2fa074169453e703c611c6b9df51588ac204e0000000000001976a9144669d92d46393c38594b2f07587f01b3e5289f6088ac204e0000000000001976a914a461497034343a91683e86b568c8945fb73aca0288ac99fe2a00000000001976a914de7850e419719258077abd37d4fcccdb0a659b9388ac00000000")

	_, err := store.Create(ctx, tx, 0)
	require.NoError(t, err)

	err = store.Delete(ctx, tx.TxIDChainHash())
	require.NoError(t, err)

	_, err = store.Get(ctx, tx.TxIDChainHash())
	require.Error(t, err)
	assert.True(t, errors.Is(err, errors.ErrTxNotFound))
}

func TestCreateAfterSpendReturnsErrSpent(t *testing.T) {
	store, _, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()
	ctx := context.Background()

	tx, _ := bt.NewTxFromString("010000000000000000ef0152a9231baa4e4b05dc30c8fbb7787bab5f460d4d33b039c39dd8cc006f3363e4020000006b483045022100ce3605307dd1633d3c14de4a0cf0df1439f392994e561b648897c4e540baa9ad02207af74878a7575a95c9599e9cdc7e6d73308608ee59abcd90af3ea1a5c0cca41541210275f8390df62d1e951920b623b8ef9c2a67c4d2574d408e422fb334dd1f3ee5b6ffffffff706b9600000000001976a914a32f7eaae3afd5f73a2d6009b93f91aa11d16eef88ac05404b4c00000000001976a914aabb8c2f08567e2d29e3a64f1f833eee85aaf74d88ac80841e00000000001976a914a4aff400bef2fa074169453e703c611c6b9df51588ac204e0000000000001976a9144669d92d46393c38594b2f07587f01b3e5289f6088ac204e0000000000001976a914a461497034343a91683e86b568c8945fb73aca0288ac99fe2a00000000001976a914de7850e419719258077abd37d4fcccdb0a659b9388ac00000000")

	_, err := store.Create(ctx, tx, 0)
	require.NoError(t, err)

	// Create spending tx
	spendTx := &bt.Tx{Version: 1, Inputs: []*bt.Input{{
		PreviousTxOutIndex: 0,
		PreviousTxScript:   tx.Outputs[0].LockingScript,
		PreviousTxSatoshis: tx.Outputs[0].Satoshis,
	}}, Outputs: []*bt.Output{{Satoshis: tx.Outputs[0].Satoshis, LockingScript: tx.Outputs[0].LockingScript}}}
	_ = spendTx.Inputs[0].PreviousTxIDAdd(tx.TxIDChainHash())

	_, err = store.Spend(ctx, spendTx, store.GetBlockHeight()+1)
	require.NoError(t, err)

	// Creating again should return ErrSpent (not ErrTxExists)
	_, err = store.Create(ctx, tx, 0)
	require.Error(t, err)
	assert.True(t, errors.Is(err, errors.ErrSpent))
}
