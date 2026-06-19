package teraslab_test

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFreezeAndUnfreeze(t *testing.T) {
	store, _, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()
	ctx := context.Background()
	tSettings := test.CreateBaseTestSettings(t)

	tx, _ := bt.NewTxFromString("010000000000000000ef0152a9231baa4e4b05dc30c8fbb7787bab5f460d4d33b039c39dd8cc006f3363e4020000006b483045022100ce3605307dd1633d3c14de4a0cf0df1439f392994e561b648897c4e540baa9ad02207af74878a7575a95c9599e9cdc7e6d73308608ee59abcd90af3ea1a5c0cca41541210275f8390df62d1e951920b623b8ef9c2a67c4d2574d408e422fb334dd1f3ee5b6ffffffff706b9600000000001976a914a32f7eaae3afd5f73a2d6009b93f91aa11d16eef88ac05404b4c00000000001976a914aabb8c2f08567e2d29e3a64f1f833eee85aaf74d88ac80841e00000000001976a914a4aff400bef2fa074169453e703c611c6b9df51588ac204e0000000000001976a9144669d92d46393c38594b2f07587f01b3e5289f6088ac204e0000000000001976a914a461497034343a91683e86b568c8945fb73aca0288ac99fe2a00000000001976a914de7850e419719258077abd37d4fcccdb0a659b9388ac00000000")

	_, err := store.Create(ctx, tx, 0)
	require.NoError(t, err)

	utxoHash0, _ := util.UTXOHashFromOutput(tx.TxIDChainHash(), tx.Outputs[0], 0)
	spends := []*utxo.Spend{{
		TxID:         tx.TxIDChainHash(),
		Vout:         0,
		UTXOHash:     utxoHash0,
		SpendingData: spend.NewSpendingData(tx.TxIDChainHash(), 0),
	}}
	testSpend := spends[0]

	t.Run("freeze prevents spending", func(t *testing.T) {
		err := store.FreezeUTXOs(ctx, spends, tSettings)
		require.NoError(t, err)

		spendTx := &bt.Tx{Version: 1, Inputs: []*bt.Input{{
			PreviousTxOutIndex: 0,
			PreviousTxScript:   tx.Outputs[0].LockingScript,
			PreviousTxSatoshis: tx.Outputs[0].Satoshis,
		}}, Outputs: []*bt.Output{{Satoshis: tx.Outputs[0].Satoshis, LockingScript: tx.Outputs[0].LockingScript}}}
		_ = spendTx.Inputs[0].PreviousTxIDAdd(tx.TxIDChainHash())

		result, err := store.Spend(ctx, spendTx, store.GetBlockHeight()+1)
		require.ErrorIs(t, err, errors.ErrUtxoError)
		require.ErrorIs(t, result[0].Err, errors.ErrFrozen)
	})

	t.Run("GetSpend returns FROZEN status", func(t *testing.T) {
		resp, err := store.GetSpend(ctx, testSpend)
		require.NoError(t, err)
		assert.Equal(t, int(utxo.Status_FROZEN), resp.Status)
		require.NotNil(t, resp.SpendingData)
		assert.Equal(t, subtree.FrozenBytesTxHash, *resp.SpendingData.TxID)
	})

	t.Run("unfreeze allows spending", func(t *testing.T) {
		err := store.UnFreezeUTXOs(ctx, spends, tSettings)
		require.NoError(t, err)

		resp, err := store.GetSpend(ctx, testSpend)
		require.NoError(t, err)
		assert.Equal(t, int(utxo.Status_OK), resp.Status)

		spendTx := &bt.Tx{Version: 1, Inputs: []*bt.Input{{
			PreviousTxOutIndex: 0,
			PreviousTxScript:   tx.Outputs[0].LockingScript,
			PreviousTxSatoshis: tx.Outputs[0].Satoshis,
		}}, Outputs: []*bt.Output{{Satoshis: tx.Outputs[0].Satoshis, LockingScript: tx.Outputs[0].LockingScript}}}
		_ = spendTx.Inputs[0].PreviousTxIDAdd(tx.TxIDChainHash())

		_, err = store.Spend(ctx, spendTx, store.GetBlockHeight()+1)
		require.NoError(t, err)
	})
}

func TestSetLocked(t *testing.T) {
	store, _, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()
	ctx := context.Background()

	tx, _ := bt.NewTxFromString("010000000000000000ef0152a9231baa4e4b05dc30c8fbb7787bab5f460d4d33b039c39dd8cc006f3363e4020000006b483045022100ce3605307dd1633d3c14de4a0cf0df1439f392994e561b648897c4e540baa9ad02207af74878a7575a95c9599e9cdc7e6d73308608ee59abcd90af3ea1a5c0cca41541210275f8390df62d1e951920b623b8ef9c2a67c4d2574d408e422fb334dd1f3ee5b6ffffffff706b9600000000001976a914a32f7eaae3afd5f73a2d6009b93f91aa11d16eef88ac05404b4c00000000001976a914aabb8c2f08567e2d29e3a64f1f833eee85aaf74d88ac80841e00000000001976a914a4aff400bef2fa074169453e703c611c6b9df51588ac204e0000000000001976a9144669d92d46393c38594b2f07587f01b3e5289f6088ac204e0000000000001976a914a461497034343a91683e86b568c8945fb73aca0288ac99fe2a00000000001976a914de7850e419719258077abd37d4fcccdb0a659b9388ac00000000")

	_, err := store.Create(ctx, tx, 0)
	require.NoError(t, err)

	t.Run("SetLocked true prevents spending", func(t *testing.T) {
		err := store.SetLocked(ctx, []chainhash.Hash{*tx.TxIDChainHash()}, true)
		require.NoError(t, err)

		spendTx := &bt.Tx{Version: 1, Inputs: []*bt.Input{{
			PreviousTxOutIndex: 0,
			PreviousTxScript:   tx.Outputs[0].LockingScript,
			PreviousTxSatoshis: tx.Outputs[0].Satoshis,
		}}, Outputs: []*bt.Output{{Satoshis: tx.Outputs[0].Satoshis, LockingScript: tx.Outputs[0].LockingScript}}}
		_ = spendTx.Inputs[0].PreviousTxIDAdd(tx.TxIDChainHash())

		_, err = store.Spend(ctx, spendTx, store.GetBlockHeight()+1)
		require.Error(t, err)
	})

	t.Run("SetLocked false allows spending", func(t *testing.T) {
		err := store.SetLocked(ctx, []chainhash.Hash{*tx.TxIDChainHash()}, false)
		require.NoError(t, err)

		spendTx := &bt.Tx{Version: 1, Inputs: []*bt.Input{{
			PreviousTxOutIndex: 0,
			PreviousTxScript:   tx.Outputs[0].LockingScript,
			PreviousTxSatoshis: tx.Outputs[0].Satoshis,
		}}, Outputs: []*bt.Output{{Satoshis: tx.Outputs[0].Satoshis, LockingScript: tx.Outputs[0].LockingScript}}}
		_ = spendTx.Inputs[0].PreviousTxIDAdd(tx.TxIDChainHash())

		_, err = store.Spend(ctx, spendTx, store.GetBlockHeight()+1)
		require.NoError(t, err)
	})
}
