package aerospike_test

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// spendableChildTx builds a tx that spends output vout of parentTx.
func spendableChildTx(t *testing.T, parentTx *bt.Tx, vout uint32) *bt.Tx {
	t.Helper()

	child := bt.NewTx()
	require.NoError(t, child.From(
		parentTx.TxIDChainHash().String(), vout,
		parentTx.Outputs[vout].LockingScript.String(),
		parentTx.Outputs[vout].Satoshis,
	))
	require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa",
		parentTx.Outputs[vout].Satoshis/3))

	return child
}

// TestCreateWithCreating verifies that Create(..., WithCreating(true)) stores the
// tx in the tentative creating state on the batched (single-record) path: the
// record exists with Creating=true and its outputs cannot be spent until finalize.
func TestCreateWithCreating(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)
	cleanDB(t, client)

	// Grandparent (spendable) so tx's input parent exists, and to serve as the control.
	_, err := store.Create(ctx, tests.ParentTx, 100)
	require.NoError(t, err)

	// Create tx in the tentative creating state.
	md, err := store.Create(ctx, tx, 100, utxo.WithCreating(true))
	require.NoError(t, err)
	require.True(t, md.Creating, "returned meta should reflect the creating state")

	// GetMeta must surface Creating=true.
	got := &meta.Data{}
	require.NoError(t, store.GetMeta(ctx, tx.TxIDChainHash(), got))
	require.True(t, got.Creating, "stored record should be in the creating state")

	// Spending the tentative tx's output must fail with ErrTxCreating.
	child := spendableChildTx(t, tx, 0)
	_, err = store.Spend(ctx, child, 101)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrTxCreating), "expected ErrTxCreating, got: %v", err)

	// Control: an output of a normally-created tx (the grandparent) is spendable.
	controlChild := spendableChildTx(t, tests.ParentTx, 0)
	_, err = store.Spend(ctx, controlChild, 101)
	require.NoError(t, err)
}

// TestCreateWithCreating_MultiRecord verifies the tentative creating state on the
// paginated (multi-record) create path: with a small UtxoBatchSize the tx spans
// several records, and the creating gate must still reject spends until finalize.
func TestCreateWithCreating_MultiRecord(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.UtxoBatchSize = 2 // tx has 6 outputs → multiple records

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)
	cleanDB(t, client)

	_, err := store.Create(ctx, tests.ParentTx, 100)
	require.NoError(t, err)

	md, err := store.Create(ctx, tx, 100, utxo.WithCreating(true))
	require.NoError(t, err)
	require.True(t, md.Creating)

	got := &meta.Data{}
	require.NoError(t, store.GetMeta(ctx, tx.TxIDChainHash(), got))
	require.True(t, got.Creating, "paginated record should be in the creating state")

	child := spendableChildTx(t, tx, 0)
	_, err = store.Spend(ctx, child, 101)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrTxCreating), "expected ErrTxCreating, got: %v", err)
}

// TestCreateOnlyDuplicate_MultiRecord_CreatingFlag verifies the duplicate-create guard on the
// paginated path: with create-first on, a CreateOnly duplicate that does not carry WithCreating
// must leave another attempt's tentative flag alone; with it off, the legacy recovery clears it.
func TestCreateOnlyDuplicate_MultiRecord_CreatingFlag(t *testing.T) {
	for _, createFirst := range []bool{true, false} {
		name := "create-first-off"
		if createFirst {
			name = "create-first-on"
		}

		t.Run(name, func(t *testing.T) {
			logger := ulogger.NewErrorTestLogger(t)
			tSettings := test.CreateBaseTestSettings(t)
			tSettings.UtxoStore.UtxoBatchSize = 2
			tSettings.UtxoStore.UseCreateFirstOrder = createFirst

			client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
			t.Cleanup(deferFn)
			cleanDB(t, client)

			_, err := store.Create(ctx, tests.ParentTx, 100)
			require.NoError(t, err)

			_, err = store.Create(ctx, tx, 100, utxo.WithCreating(true))
			require.NoError(t, err)

			_, err = store.Create(ctx, tx, 100, utxo.WithCreateOnly())
			require.Error(t, err)
			require.True(t, errors.Is(err, errors.ErrTxExists), "expected ErrTxExists, got: %v", err)

			got := &meta.Data{}
			require.NoError(t, store.GetMeta(ctx, tx.TxIDChainHash(), got))
			require.Equal(t, createFirst, got.Creating, "a duplicate create must clear the creating flag only when create-first is off")
		})
	}
}
