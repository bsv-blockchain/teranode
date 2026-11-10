package aerospike_test

import (
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	teranode_aerospike "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
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

// writeRawBins writes the given bins to record idx of hash with the raw client, which is how a state
// the store cannot reach through its own API is reproduced.
func writeRawBins(t *testing.T, client *uaerospike.Client, store *teranode_aerospike.Store, hash *chainhash.Hash, idx uint32, bins ...*aerospike.Bin) {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)

	key, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), uaerospike.CalculateKeySourceInternal(hash, idx))
	require.NoError(t, err)

	policy := util.GetAerospikeWritePolicy(tSettings, 0)
	policy.RecordExistsAction = aerospike.UPDATE_ONLY

	require.Nil(t, client.PutBins(policy, key, bins...))
}

// deleteRawMaster deletes the master record of hash with the raw client and leaves its pagination
// children, the state an attempt that died after writing them leaves behind.
func deleteRawMaster(t *testing.T, client *uaerospike.Client, store *teranode_aerospike.Store, hash *chainhash.Hash) {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)

	key, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), hash[:])
	require.NoError(t, err)

	existed, aErr := client.Delete(util.GetAerospikeWritePolicy(tSettings, 0), key)
	require.Nil(t, aErr)
	require.True(t, existed)
}

// TestCreateWithCreating_SurvivingChildThatCannotBeAdoptedFailsTheCreate pins that a pagination child
// left by an earlier attempt, which keeps that attempt's token when the adoption fails, does not let
// the create succeed: the finalize of this attempt would never clear the child and its outputs would
// stay unspendable. The create is undone, so the store is as it was before the attempt, and a later
// create adopts the child and finalizes.
func TestCreateWithCreating_SurvivingChildThatCannotBeAdoptedFailsTheCreate(t *testing.T) {
	for name, batchLevel := range map[string]bool{"a record error": false, "a batch error": true} {
		t.Run(name, func(t *testing.T) {
			logger := ulogger.NewErrorTestLogger(t)
			tSettings := test.CreateBaseTestSettings(t)
			tSettings.UtxoStore.UtxoBatchSize = 2

			client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
			t.Cleanup(deferFn)
			cleanDB(t, client)

			_, err := store.Create(ctx, tests.ParentTx, 100)
			require.NoError(t, err)

			hash := tx.TxIDChainHash()

			first, err := store.Create(ctx, tx, 100, utxo.WithCreating(true))
			require.NoError(t, err)
			require.True(t, childRecordExists(t, client, store, hash, 1), "precondition: the tx paginates")

			deleteRawMaster(t, client, store, hash)

			store.FailChildAdoption(batchLevel)

			_, err = store.Create(ctx, tx, 101, utxo.WithCreating(true))
			require.ErrorIs(t, err, errors.ErrProcessing, "an adoption that fails is a transient failure of the create")
			require.ErrorIs(t, store.GetMeta(ctx, hash, &meta.Data{}), errors.ErrTxNotFound, "the master this attempt wrote is removed")
			require.True(t, childRecordExists(t, client, store, hash, 1), "the surviving child is left as it was")

			store.RestoreBatchOperate()

			second, err := store.Create(ctx, tx, 101, utxo.WithCreating(true))
			require.NoError(t, err, "with the failure gone the retry adopts the child")
			require.NotEqual(t, first.Attempt, second.Attempt)

			require.NoError(t, store.FinalizeTransaction(ctx, tx, second.Attempt))

			got := &meta.Data{}
			require.NoError(t, store.GetMeta(ctx, hash, got))
			require.False(t, got.Creating, "the master is clear")
			require.Nil(t, recordBins(t, client, store, hash, 1, fields.Creating)[fields.Creating.String()], "the adopted child is clear too")
		})
	}
}

// TestFinalizeTransaction_ChildCreatingUnderAnotherAttemptKeepsMasterCreating pins that a child whose
// clear was filtered out is not taken for final: a child that is still creating under another attempt's
// token means the master must stay creating, because clearing it would leave that child's outputs
// unspendable for good.
func TestFinalizeTransaction_ChildCreatingUnderAnotherAttemptKeepsMasterCreating(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.UtxoBatchSize = 2

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)
	cleanDB(t, client)

	_, err := store.Create(ctx, tests.ParentTx, 100)
	require.NoError(t, err)

	hash := tx.TxIDChainHash()

	md, err := store.Create(ctx, tx, 100, utxo.WithCreating(true))
	require.NoError(t, err)
	require.True(t, childRecordExists(t, client, store, hash, 1), "precondition: the tx paginates")

	writeRawBins(t, client, store, hash, 1, aerospike.NewBin(fields.Attempt.String(), []byte("another attempt!")))

	err = store.FinalizeTransaction(ctx, tx, md.Attempt)
	require.Error(t, err, "a child that is creating under another attempt is not cleared")
	require.Contains(t, err.Error(), "child")

	got := &meta.Data{}
	require.NoError(t, store.GetMeta(ctx, hash, got))
	require.True(t, got.Creating, "the master stays creating while a child is not clear")
	require.Equal(t, md.Attempt, got.Attempt)

	writeRawBins(t, client, store, hash, 1, aerospike.NewBin(fields.Attempt.String(), md.Attempt))
	require.NoError(t, store.FinalizeTransaction(ctx, tx, md.Attempt), "once the child carries the token the finalize completes")

	require.NoError(t, store.GetMeta(ctx, hash, got))
	require.False(t, got.Creating)
}

// TestFinalizeTransaction_ChildAlreadyFinalIsCleared pins the other half of the filtered-out rule: a
// child that is no longer creating is success, so the finalize still clears the master.
func TestFinalizeTransaction_ChildAlreadyFinalIsCleared(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.UtxoBatchSize = 2

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)
	cleanDB(t, client)

	_, err := store.Create(ctx, tests.ParentTx, 100)
	require.NoError(t, err)

	hash := tx.TxIDChainHash()

	md, err := store.Create(ctx, tx, 100, utxo.WithCreating(true))
	require.NoError(t, err)

	writeRawBins(t, client, store, hash, 1, aerospike.NewBin(fields.Creating.String(), nil))

	require.NoError(t, store.FinalizeTransaction(ctx, tx, md.Attempt))

	got := &meta.Data{}
	require.NoError(t, store.GetMeta(ctx, hash, got))
	require.False(t, got.Creating, "a child that is already final does not hold the master back")
}
