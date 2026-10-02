//go:build teraslab

package teraslab_test

import (
	"context"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/require"
)

func TestSpendAndGetSpend(t *testing.T) {
	store, _, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()
	ctx := context.Background()

	tx, _ := bt.NewTxFromString("010000000000000000ef0152a9231baa4e4b05dc30c8fbb7787bab5f460d4d33b039c39dd8cc006f3363e4020000006b483045022100ce3605307dd1633d3c14de4a0cf0df1439f392994e561b648897c4e540baa9ad02207af74878a7575a95c9599e9cdc7e6d73308608ee59abcd90af3ea1a5c0cca41541210275f8390df62d1e951920b623b8ef9c2a67c4d2574d408e422fb334dd1f3ee5b6ffffffff706b9600000000001976a914a32f7eaae3afd5f73a2d6009b93f91aa11d16eef88ac05404b4c00000000001976a914aabb8c2f08567e2d29e3a64f1f833eee85aaf74d88ac80841e00000000001976a914a4aff400bef2fa074169453e703c611c6b9df51588ac204e0000000000001976a9144669d92d46393c38594b2f07587f01b3e5289f6088ac204e0000000000001976a914a461497034343a91683e86b568c8945fb73aca0288ac99fe2a00000000001976a914de7850e419719258077abd37d4fcccdb0a659b9388ac00000000")

	_, err := store.Create(ctx, tx, 0)
	require.NoError(t, err)

	utxoHash0, _ := util.UTXOHashFromOutput(tx.TxIDChainHash(), tx.Outputs[0], 0)

	t.Run("GetSpend before spend returns OK", func(t *testing.T) {
		resp, err := store.GetSpend(ctx, &utxo.Spend{
			TxID:     tx.TxIDChainHash(),
			Vout:     0,
			UTXOHash: utxoHash0,
		})
		require.NoError(t, err)
		require.Equal(t, int(utxo.Status_OK), resp.Status)
		require.Nil(t, resp.SpendingData)
	})

	spendTx := &bt.Tx{Version: 1, Inputs: []*bt.Input{{
		PreviousTxOutIndex: 0,
		PreviousTxScript:   tx.Outputs[0].LockingScript,
		PreviousTxSatoshis: tx.Outputs[0].Satoshis,
	}}, Outputs: []*bt.Output{{Satoshis: tx.Outputs[0].Satoshis, LockingScript: tx.Outputs[0].LockingScript}}}
	_ = spendTx.Inputs[0].PreviousTxIDAdd(tx.TxIDChainHash())

	t.Run("Spend succeeds", func(t *testing.T) {
		spends, err := store.Spend(ctx, spendTx, store.GetBlockHeight()+1)
		require.NoError(t, err)
		require.Len(t, spends, 1)
		require.Nil(t, spends[0].Err)
	})

	t.Run("GetSpend after spend returns SPENT with correct spending data", func(t *testing.T) {
		resp, err := store.GetSpend(ctx, &utxo.Spend{
			TxID:     tx.TxIDChainHash(),
			Vout:     0,
			UTXOHash: utxoHash0,
		})
		require.NoError(t, err)
		require.Equal(t, int(utxo.Status_SPENT), resp.Status)
		require.NotNil(t, resp.SpendingData)
		require.Equal(t, spendTx.TxIDChainHash().String(), resp.SpendingData.TxID.String())
	})
}

func TestDoubleSpendReturnsConflictingTxID(t *testing.T) {
	store, _, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()
	ctx := context.Background()

	tx, _ := bt.NewTxFromString("010000000000000000ef0152a9231baa4e4b05dc30c8fbb7787bab5f460d4d33b039c39dd8cc006f3363e4020000006b483045022100ce3605307dd1633d3c14de4a0cf0df1439f392994e561b648897c4e540baa9ad02207af74878a7575a95c9599e9cdc7e6d73308608ee59abcd90af3ea1a5c0cca41541210275f8390df62d1e951920b623b8ef9c2a67c4d2574d408e422fb334dd1f3ee5b6ffffffff706b9600000000001976a914a32f7eaae3afd5f73a2d6009b93f91aa11d16eef88ac05404b4c00000000001976a914aabb8c2f08567e2d29e3a64f1f833eee85aaf74d88ac80841e00000000001976a914a4aff400bef2fa074169453e703c611c6b9df51588ac204e0000000000001976a9144669d92d46393c38594b2f07587f01b3e5289f6088ac204e0000000000001976a914a461497034343a91683e86b568c8945fb73aca0288ac99fe2a00000000001976a914de7850e419719258077abd37d4fcccdb0a659b9388ac00000000")

	_, err := store.Create(ctx, tx, 0)
	require.NoError(t, err)

	// First spend
	spendTx1 := &bt.Tx{Version: 1, Inputs: []*bt.Input{{
		PreviousTxOutIndex: 0,
		PreviousTxScript:   tx.Outputs[0].LockingScript,
		PreviousTxSatoshis: tx.Outputs[0].Satoshis,
	}}, Outputs: []*bt.Output{
		{Satoshis: tx.Outputs[0].Satoshis, LockingScript: tx.Outputs[0].LockingScript},
	}}
	_ = spendTx1.Inputs[0].PreviousTxIDAdd(tx.TxIDChainHash())
	_, err = store.Spend(ctx, spendTx1, store.GetBlockHeight()+1)
	require.NoError(t, err)

	// Second spend with different tx (double spend)
	spendTx2 := &bt.Tx{Version: 1, Inputs: []*bt.Input{{
		PreviousTxOutIndex: 0,
		PreviousTxScript:   tx.Outputs[0].LockingScript,
		PreviousTxSatoshis: tx.Outputs[0].Satoshis,
	}}, Outputs: []*bt.Output{
		{Satoshis: tx.Outputs[1].Satoshis, LockingScript: tx.Outputs[1].LockingScript},
	}}
	_ = spendTx2.Inputs[0].PreviousTxIDAdd(tx.TxIDChainHash())

	spends, err := store.Spend(ctx, spendTx2, store.GetBlockHeight()+1)
	require.ErrorIs(t, err, errors.ErrUtxoError)
	require.Len(t, spends, 1)
	require.ErrorIs(t, spends[0].Err, errors.ErrSpent)
	require.NotNil(t, spends[0].ConflictingTxID)
	require.Equal(t, spendTx1.TxIDChainHash().String(), spends[0].ConflictingTxID.String())
}

func TestUnspend(t *testing.T) {
	store, _, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()
	ctx := context.Background()

	tx, _ := bt.NewTxFromString("010000000000000000ef0152a9231baa4e4b05dc30c8fbb7787bab5f460d4d33b039c39dd8cc006f3363e4020000006b483045022100ce3605307dd1633d3c14de4a0cf0df1439f392994e561b648897c4e540baa9ad02207af74878a7575a95c9599e9cdc7e6d73308608ee59abcd90af3ea1a5c0cca41541210275f8390df62d1e951920b623b8ef9c2a67c4d2574d408e422fb334dd1f3ee5b6ffffffff706b9600000000001976a914a32f7eaae3afd5f73a2d6009b93f91aa11d16eef88ac05404b4c00000000001976a914aabb8c2f08567e2d29e3a64f1f833eee85aaf74d88ac80841e00000000001976a914a4aff400bef2fa074169453e703c611c6b9df51588ac204e0000000000001976a9144669d92d46393c38594b2f07587f01b3e5289f6088ac204e0000000000001976a914a461497034343a91683e86b568c8945fb73aca0288ac99fe2a00000000001976a914de7850e419719258077abd37d4fcccdb0a659b9388ac00000000")

	_, err := store.Create(ctx, tx, 0)
	require.NoError(t, err)

	utxoHash0, _ := util.UTXOHashFromOutput(tx.TxIDChainHash(), tx.Outputs[0], 0)

	spendTx := &bt.Tx{Version: 1, Inputs: []*bt.Input{{
		PreviousTxOutIndex: 0,
		PreviousTxScript:   tx.Outputs[0].LockingScript,
		PreviousTxSatoshis: tx.Outputs[0].Satoshis,
	}}, Outputs: []*bt.Output{{Satoshis: tx.Outputs[0].Satoshis, LockingScript: tx.Outputs[0].LockingScript}}}
	_ = spendTx.Inputs[0].PreviousTxIDAdd(tx.TxIDChainHash())

	_, err = store.Spend(ctx, spendTx, store.GetBlockHeight()+1)
	require.NoError(t, err)

	// Unspend
	spends := []*utxo.Spend{{
		TxID:         tx.TxIDChainHash(),
		Vout:         0,
		UTXOHash:     utxoHash0,
		SpendingData: spend.NewSpendingData(spendTx.TxIDChainHash(), 0),
	}}
	err = store.Unspend(ctx, spends, false)
	require.NoError(t, err)

	// Should be spendable again
	resp, err := store.GetSpend(ctx, &utxo.Spend{
		TxID:     tx.TxIDChainHash(),
		Vout:     0,
		UTXOHash: utxoHash0,
	})
	require.NoError(t, err)
	require.Equal(t, int(utxo.Status_OK), resp.Status)
}

func TestSpendConflictingTxFails(t *testing.T) {
	store, _, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()
	ctx := context.Background()

	tx, _ := bt.NewTxFromString("010000000000000000ef0152a9231baa4e4b05dc30c8fbb7787bab5f460d4d33b039c39dd8cc006f3363e4020000006b483045022100ce3605307dd1633d3c14de4a0cf0df1439f392994e561b648897c4e540baa9ad02207af74878a7575a95c9599e9cdc7e6d73308608ee59abcd90af3ea1a5c0cca41541210275f8390df62d1e951920b623b8ef9c2a67c4d2574d408e422fb334dd1f3ee5b6ffffffff706b9600000000001976a914a32f7eaae3afd5f73a2d6009b93f91aa11d16eef88ac05404b4c00000000001976a914aabb8c2f08567e2d29e3a64f1f833eee85aaf74d88ac80841e00000000001976a914a4aff400bef2fa074169453e703c611c6b9df51588ac204e0000000000001976a9144669d92d46393c38594b2f07587f01b3e5289f6088ac204e0000000000001976a914a461497034343a91683e86b568c8945fb73aca0288ac99fe2a00000000001976a914de7850e419719258077abd37d4fcccdb0a659b9388ac00000000")

	_, err := store.Create(ctx, tx, 0, utxo.WithConflicting(true))
	require.NoError(t, err)

	spendTx := &bt.Tx{Version: 1, Inputs: []*bt.Input{{
		PreviousTxOutIndex: 0,
		PreviousTxScript:   tx.Outputs[0].LockingScript,
		PreviousTxSatoshis: tx.Outputs[0].Satoshis,
	}}, Outputs: []*bt.Output{{Satoshis: tx.Outputs[0].Satoshis, LockingScript: tx.Outputs[0].LockingScript}}}
	_ = spendTx.Inputs[0].PreviousTxIDAdd(tx.TxIDChainHash())

	spends, err := store.Spend(ctx, spendTx, store.GetBlockHeight()+1)
	require.ErrorIs(t, err, errors.ErrUtxoError)
	require.Len(t, spends, 1)
	require.ErrorIs(t, spends[0].Err, errors.ErrTxConflicting)
}

// TestSpendConcurrentBatched exercises the spend batcher's new path: many
// transactions spending at the SAME block height fire concurrently and coalesce
// into one SpendBatch RPC. It verifies the global-indexed response is routed back
// per-transaction without cross-contamination — valid spends each get their own
// vout's result, and a concurrently-batched invalid spend (non-existent parent)
// fails in isolation without affecting its siblings.
func TestSpendConcurrentBatched(t *testing.T) {
	store, _, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()
	ctx := context.Background()

	parent := newTestTx(t)
	_, err := store.Create(ctx, parent, 0)
	require.NoError(t, err)
	height := store.GetBlockHeight() + 1

	n := len(parent.Outputs)

	// Build all spend txs up front (no testify calls inside goroutines).
	validTxs := make([]*bt.Tx, n)
	for i := 0; i < n; i++ {
		validTxs[i] = makeSpendTx(t, parent, uint32(i))
	}
	// One invalid spend referencing a non-existent parent, batched alongside.
	missing, _ := chainhash.NewHashFromStr("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	invalidTx := &bt.Tx{Version: 1, Inputs: []*bt.Input{{
		PreviousTxOutIndex: 0,
		PreviousTxScript:   parent.Outputs[0].LockingScript,
		PreviousTxSatoshis: parent.Outputs[0].Satoshis,
	}}, Outputs: []*bt.Output{{Satoshis: parent.Outputs[0].Satoshis, LockingScript: parent.Outputs[0].LockingScript}}}
	require.NoError(t, invalidTx.Inputs[0].PreviousTxIDAdd(missing))

	type res struct {
		spends []*utxo.Spend
		err    error
	}
	validRes := make([]res, n)
	var invalidRes res

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(vout int) {
			defer wg.Done()
			validRes[vout].spends, validRes[vout].err = store.Spend(ctx, validTxs[vout], height)
		}(i)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		invalidRes.spends, invalidRes.err = store.Spend(ctx, invalidTx, height)
	}()
	wg.Wait()

	// Every valid, distinct-output spend succeeds and maps to its own vout.
	for i := 0; i < n; i++ {
		require.NoError(t, validRes[i].err, "valid spend vout %d", i)
		require.Len(t, validRes[i].spends, 1)
		require.Nil(t, validRes[i].spends[0].Err, "valid spend vout %d result err", i)
		require.Equal(t, uint32(i), validRes[i].spends[0].Vout, "result must reference its own vout")
	}

	// The invalid spend fails in isolation (non-existent parent) — no sibling contamination.
	require.Error(t, invalidRes.err)
	require.ErrorIs(t, invalidRes.err, errors.ErrUtxoError)
	require.Len(t, invalidRes.spends, 1)
	require.ErrorIs(t, invalidRes.spends[0].Err, errors.ErrTxNotFound)

	// Confirm each parent output is now SPENT by ITS spending tx (no swap).
	for i := 0; i < n; i++ {
		utxoHash, err := util.UTXOHashFromOutput(parent.TxIDChainHash(), parent.Outputs[i], uint32(i))
		require.NoError(t, err)
		resp, err := store.GetSpend(ctx, &utxo.Spend{TxID: parent.TxIDChainHash(), Vout: uint32(i), UTXOHash: utxoHash})
		require.NoError(t, err)
		require.Equal(t, int(utxo.Status_SPENT), resp.Status, "vout %d should be spent", i)
		require.NotNil(t, resp.SpendingData)
		require.Equal(t, validTxs[i].TxIDChainHash().String(), resp.SpendingData.TxID.String(),
			"vout %d must be spent by its own tx", i)
	}
}
