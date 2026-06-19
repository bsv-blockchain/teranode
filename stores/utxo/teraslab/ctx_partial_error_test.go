package teraslab_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	teranodeerrors "github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreateRespectsCallerContext verifies gap #13: Create must observe caller
// context cancellation rather than blocking indefinitely on the batch result.
func TestCreateRespectsCallerContext(t *testing.T) {
	store, _, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()

	tx, _ := bt.NewTxFromString("010000000000000000ef0152a9231baa4e4b05dc30c8fbb7787bab5f460d4d33b039c39dd8cc006f3363e4020000006b483045022100ce3605307dd1633d3c14de4a0cf0df1439f392994e561b648897c4e540baa9ad02207af74878a7575a95c9599e9cdc7e6d73308608ee59abcd90af3ea1a5c0cca41541210275f8390df62d1e951920b623b8ef9c2a67c4d2574d408e422fb334dd1f3ee5b6ffffffff706b9600000000001976a914a32f7eaae3afd5f73a2d6009b93f91aa11d16eef88ac05404b4c00000000001976a914aabb8c2f08567e2d29e3a64f1f833eee85aaf74d88ac80841e00000000001976a914a4aff400bef2fa074169453e703c611c6b9df51588ac204e0000000000001976a9144669d92d46393c38594b2f07587f01b3e5289f6088ac204e0000000000001976a914a461497034343a91683e86b568c8945fb73aca0288ac99fe2a00000000001976a914de7850e419719258077abd37d4fcccdb0a659b9388ac00000000")

	t.Run("already-cancelled context returns ctx.Err quickly", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		start := time.Now()
		_, err := store.Create(ctx, tx, 0)
		elapsed := time.Since(start)

		require.Error(t, err)
		assert.True(t, errors.Is(err, context.Canceled), "expected context.Canceled, got %v", err)
		// Returning before the batcher's flush window proves we did not block on the channel.
		assert.Less(t, elapsed, 250*time.Millisecond, "Create should return promptly on a cancelled context")
	})

	t.Run("deadline-exceeded context returns ctx.Err", func(t *testing.T) {
		// Use a different tx so we exercise a fresh batch item.
		tx2, _ := bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff19031404002f6d332d617369612fdf5128e62eda1a07e94dbdbdffffffff0500ca9a3b000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac00ca9a3b000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac00ca9a3b000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac00ca9a3b000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac00ca9a3b000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac00000000")

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
		defer cancel()
		// Give the deadline a moment to actually fire so the select has something
		// to observe; with 1ns the deadline is effectively past immediately.
		time.Sleep(2 * time.Millisecond)

		_, err := store.Create(ctx, tx2, 0)
		require.Error(t, err)
		assert.True(t,
			errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled),
			"expected DeadlineExceeded or Canceled, got %v", err)
	})
}

// TestGetRespectsCallerContext mirrors TestCreateRespectsCallerContext for Get.
func TestGetRespectsCallerContext(t *testing.T) {
	store, _, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()

	hash := chainhash.Hash{0xde, 0xad, 0xbe, 0xef}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	_, err := store.Get(ctx, &hash)
	elapsed := time.Since(start)

	require.Error(t, err)
	assert.True(t, errors.Is(err, context.Canceled), "expected context.Canceled, got %v", err)
	assert.Less(t, elapsed, 250*time.Millisecond, "Get should return promptly on a cancelled context")
}

// TestUnspendSurfacesPartialError verifies gap #14a: a PartialError from
// UnspendBatch must propagate to the caller, not be swallowed.
func TestUnspendSurfacesPartialError(t *testing.T) {
	store, _, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()
	ctx := context.Background()

	// Build a spend referencing a transaction that does not exist in the store.
	// UnspendBatch should report this as a PartialError (per-item TxNotFound).
	missing := &chainhash.Hash{}
	for i := range missing {
		missing[i] = byte(0xA0 + i)
	}

	bogusUtxoHash := &chainhash.Hash{}
	for i := range bogusUtxoHash {
		bogusUtxoHash[i] = byte(0x10 + i)
	}

	spends := []*utxo.Spend{{
		TxID:         missing,
		Vout:         0,
		UTXOHash:     bogusUtxoHash,
		SpendingData: spend.NewSpendingData(missing, 0),
	}}

	err := store.Unspend(ctx, spends, false)
	require.Error(t, err, "Unspend must return the partial error instead of nil")
}

// TestPreserveTransactionsSurfacesPartialError verifies gap #14b: PartialError
// from PreserveTransactions must propagate to the caller.
func TestPreserveTransactionsSurfacesPartialError(t *testing.T) {
	store, _, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()
	ctx := context.Background()

	missing := chainhash.Hash{}
	for i := range missing {
		missing[i] = byte(0x55 + i)
	}

	err := store.PreserveTransactions(ctx, []chainhash.Hash{missing}, 1000)
	require.Error(t, err, "PreserveTransactions must return the partial error instead of nil")
}

// TestUnspendRoundTripStillWorks guards against a regression where surfacing
// the partial error on Unspend accidentally breaks the success path.
func TestUnspendRoundTripStillWorks(t *testing.T) {
	store, _, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()
	ctx := context.Background()

	tx, _ := bt.NewTxFromString("010000000000000000ef0152a9231baa4e4b05dc30c8fbb7787bab5f460d4d33b039c39dd8cc006f3363e4020000006b483045022100ce3605307dd1633d3c14de4a0cf0df1439f392994e561b648897c4e540baa9ad02207af74878a7575a95c9599e9cdc7e6d73308608ee59abcd90af3ea1a5c0cca41541210275f8390df62d1e951920b623b8ef9c2a67c4d2574d408e422fb334dd1f3ee5b6ffffffff706b9600000000001976a914a32f7eaae3afd5f73a2d6009b93f91aa11d16eef88ac05404b4c00000000001976a914aabb8c2f08567e2d29e3a64f1f833eee85aaf74d88ac80841e00000000001976a914a4aff400bef2fa074169453e703c611c6b9df51588ac204e0000000000001976a9144669d92d46393c38594b2f07587f01b3e5289f6088ac204e0000000000001976a914a461497034343a91683e86b568c8945fb73aca0288ac99fe2a00000000001976a914de7850e419719258077abd37d4fcccdb0a659b9388ac00000000")

	_, err := store.Create(ctx, tx, 0)
	require.NoError(t, err)

	utxoHash0, err := util.UTXOHashFromOutput(tx.TxIDChainHash(), tx.Outputs[0], 0)
	require.NoError(t, err)

	spendTx := &bt.Tx{Version: 1, Inputs: []*bt.Input{{
		PreviousTxOutIndex: 0,
		PreviousTxScript:   tx.Outputs[0].LockingScript,
		PreviousTxSatoshis: tx.Outputs[0].Satoshis,
	}}, Outputs: []*bt.Output{{Satoshis: tx.Outputs[0].Satoshis, LockingScript: tx.Outputs[0].LockingScript}}}
	_ = spendTx.Inputs[0].PreviousTxIDAdd(tx.TxIDChainHash())

	_, err = store.Spend(ctx, spendTx, store.GetBlockHeight()+1)
	require.NoError(t, err)

	spends := []*utxo.Spend{{
		TxID:         tx.TxIDChainHash(),
		Vout:         0,
		UTXOHash:     utxoHash0,
		SpendingData: spend.NewSpendingData(spendTx.TxIDChainHash(), 0),
	}}
	require.NoError(t, store.Unspend(ctx, spends, false))

	// Sanity: silence unused import if the partial-error helper drifts.
	_ = teranodeerrors.ErrUtxoError
}
