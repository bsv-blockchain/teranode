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

	tx := uniqueTx(tests.Tx.Clone())

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

	tx := uniqueTx(tests.Tx.Clone())

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
// paginated path: a record that carries an attempt token is a tentative record of some
// attempt, so a CreateOnly duplicate that spent nothing leaves its creating state alone
// whatever the setting says. Only FinalizeTransaction and the roll-forward healers finish it.
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

			tx := uniqueTx(tests.Tx.Clone())

			_, err := store.Create(ctx, tests.ParentTx, 100)
			require.NoError(t, err)

			md, err := store.Create(ctx, tx, 100, utxo.WithCreating(true))
			require.NoError(t, err)
			require.NotEmpty(t, md.Attempt, "precondition: a tentative record carries an attempt token")

			_, err = store.Create(ctx, tx, 100, utxo.WithCreateOnly())
			require.Error(t, err)
			require.True(t, errors.Is(err, errors.ErrTxExists), "expected ErrTxExists, got: %v", err)

			got := &meta.Data{}
			require.NoError(t, store.GetMeta(ctx, tx.TxIDChainHash(), got))
			require.True(t, got.Creating, "a duplicate create leaves a record that carries an attempt token creating, whatever the setting says")
			require.Equal(t, md.Attempt, got.Attempt, "the duplicate must not take ownership of another attempt's record")

			// The record is still tentative, so its outputs stay gated: this caller spent nothing.
			child := spendableChildTx(t, tx, 0)
			_, err = store.Spend(ctx, child, 101)
			require.ErrorIs(t, err, errors.ErrTxCreating, "an output of a tentative record must not be spendable")

			inputs, err := utxo.GetSpends(tx)
			require.NoError(t, err)
			resp, err := store.GetSpend(ctx, inputs[0])
			require.NoError(t, err)
			require.Nil(t, resp.SpendingData, "nothing spent this record's input, so it may not be final")
		})
	}
}

// TestCreateOnlyDuplicate_ClearsATokenlessCreatingRecord verifies the other half of the rule:
// a creating record with no attempt token came from a create that did not defer the spend of
// its inputs - a coinbase or a seeded transaction - and its own clear failed. No create-first
// healer reaches it, because none of them looks for a record without a token, so a duplicate
// clears it whatever the setting says.
func TestCreateOnlyDuplicate_ClearsATokenlessCreatingRecord(t *testing.T) {
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

			tx := uniqueTx(tests.Tx.Clone())

			_, err := store.Create(ctx, tests.ParentTx, 100)
			require.NoError(t, err)

			md, err := store.Create(ctx, tx, 100, utxo.WithCreating(true))
			require.NoError(t, err)
			require.NotEmpty(t, md.Attempt, "precondition: the create wrote a token to remove")

			// Reproduce the state the store cannot reach through its own API: creating, no token.
			writeRawBins(t, client, store, tx.TxIDChainHash(), 0, aerospike.NewBin(fields.Attempt.String(), nil))

			_, err = store.Create(ctx, tx, 100, utxo.WithCreateOnly())
			require.Error(t, err)
			require.True(t, errors.Is(err, errors.ErrTxExists), "expected ErrTxExists, got: %v", err)

			got := &meta.Data{}
			require.NoError(t, store.GetMeta(ctx, tx.TxIDChainHash(), got))
			require.False(t, got.Creating, "a record with no attempt token is cleared by a duplicate whatever the setting says")
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
// stay unspendable. The master the create published may already be in another actor's roll-forward,
// so the create leaves it creating and returns its token for the caller's rollback. The rollback
// leaves a hold, and once the hold is adopted a later create adopts the child and finalizes.
func TestCreateWithCreating_SurvivingChildThatCannotBeAdoptedFailsTheCreate(t *testing.T) {
	for name, batchLevel := range map[string]bool{"a record error": false, "a batch error": true} {
		t.Run(name, func(t *testing.T) {
			logger := ulogger.NewErrorTestLogger(t)
			tSettings := test.CreateBaseTestSettings(t)
			tSettings.UtxoStore.UtxoBatchSize = 2

			client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
			t.Cleanup(deferFn)
			cleanDB(t, client)

			tx := uniqueTx(tests.Tx.Clone())

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

			failed := utxo.RollBackAttempt(err)
			require.NotNil(t, failed, "the error names the attempt the caller must roll back")
			require.NotEqual(t, first.Attempt, failed)

			published := &meta.Data{}
			require.NoError(t, store.GetMeta(ctx, hash, published), "the published master is not removed under a possible roll-forward")
			require.True(t, published.Creating)
			require.False(t, published.RollingBack)
			require.Equal(t, failed, published.Attempt)
			require.Equal(t, first.Attempt, recordBins(t, client, store, hash, 1, fields.Attempt)[fields.Attempt.String()], "the surviving child keeps the earlier token")

			store.RestoreBatchOperate()

			require.NoError(t, rollBackAsCreator(ctx, logger, store, tx, failed), "the caller rolls the failed attempt back")
			requireHeld(t, ctx, store, hash, "the rollback leaves a hold")
			require.True(t, childRecordExists(t, client, store, hash, 1), "the surviving child of the earlier attempt is left as it was")

			cleared, err := store.ClearCreateFirstHold(ctx, hash, true)
			require.NoError(t, err)
			require.True(t, cleared, "a block-context create adopts the hold")

			second, err := store.Create(ctx, tx, 101, utxo.WithCreating(true))
			require.NoError(t, err, "with the failure gone the retry adopts the child")
			require.NotEqual(t, first.Attempt, second.Attempt)
			require.NotEqual(t, failed, second.Attempt)

			require.NoError(t, store.FinalizeTransaction(ctx, tx, second.Attempt, second.Attempt))

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

	tx := uniqueTx(tests.Tx.Clone())

	_, err := store.Create(ctx, tests.ParentTx, 100)
	require.NoError(t, err)

	hash := tx.TxIDChainHash()

	md, err := store.Create(ctx, tx, 100, utxo.WithCreating(true))
	require.NoError(t, err)
	require.True(t, childRecordExists(t, client, store, hash, 1), "precondition: the tx paginates")

	writeRawBins(t, client, store, hash, 1, aerospike.NewBin(fields.Attempt.String(), []byte("another attempt!")))

	err = store.FinalizeTransaction(ctx, tx, md.Attempt, md.Attempt)
	require.Error(t, err, "a child that is creating under another attempt is not cleared")
	require.Contains(t, err.Error(), "child")

	got := &meta.Data{}
	require.NoError(t, store.GetMeta(ctx, hash, got))
	require.True(t, got.Creating, "the master stays creating while a child is not clear")
	require.Equal(t, md.Attempt, got.Attempt)
	require.False(t, got.Finalizing, "a finalize that cannot end does not mark the master, so the attempt can still be rolled back")

	for idx := uint32(1); childRecordExists(t, client, store, hash, idx); idx++ {
		require.Contains(t, recordBins(t, client, store, hash, idx, fields.Creating), fields.Creating.String(), "child %d: a refused finalize clears no child, also not one of its own attempt", idx)
	}

	writeRawBins(t, client, store, hash, 1, aerospike.NewBin(fields.Attempt.String(), md.Attempt))
	require.NoError(t, store.FinalizeTransaction(ctx, tx, md.Attempt, md.Attempt), "once the child carries the token the finalize completes")

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

	tx := uniqueTx(tests.Tx.Clone())

	_, err := store.Create(ctx, tests.ParentTx, 100)
	require.NoError(t, err)

	hash := tx.TxIDChainHash()

	md, err := store.Create(ctx, tx, 100, utxo.WithCreating(true))
	require.NoError(t, err)

	writeRawBins(t, client, store, hash, 1, aerospike.NewBin(fields.Creating.String(), nil))

	require.NoError(t, store.FinalizeTransaction(ctx, tx, md.Attempt, md.Attempt))

	got := &meta.Data{}
	require.NoError(t, store.GetMeta(ctx, hash, got))
	require.False(t, got.Creating, "a child that is already final does not hold the master back")
}

// TestCreate_ChildWrittenByARetryTakesTheTokenOfItsMaster pins that a paginated transaction whose
// first create wrote the master and missed a pagination child can still be completed. The retry
// finds the master, so it is not the attempt that owns the record, and it writes the missing child.
// That child must carry the token of the master. Under the token of the retry no finalize could
// ever clear it: the master's attempt is the only one that can be finalized, and its finalize is
// filtered on its own token.
func TestCreate_ChildWrittenByARetryTakesTheTokenOfItsMaster(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.UtxoBatchSize = 2
	tSettings.UtxoStore.UseCreateFirstOrder = true

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)
	cleanDB(t, client)

	tx := uniqueTx(tests.Tx.Clone())

	_, err := store.Create(ctx, tests.ParentTx, 100)
	require.NoError(t, err)

	hash := tx.TxIDChainHash()

	first, err := store.Create(ctx, tx, 100, utxo.WithCreating(true))
	require.NoError(t, err)
	require.True(t, childRecordExists(t, client, store, hash, 1), "precondition: the tx paginates")

	deleteChildRecordsByHand(t, client, store, hash, 1, 1)
	require.False(t, childRecordExists(t, client, store, hash, 1), "precondition: the first create missed child 1")

	_, _, err = store.SpendAndCreate(ctx, tx, 101)
	require.ErrorIs(t, err, errors.ErrTxExists)
	require.True(t, utxo.IsRolledForward(err), "the retry completes the attempt that wrote the master, got: %v", err)

	got := &meta.Data{}
	require.NoError(t, store.GetMeta(ctx, hash, got))
	require.False(t, got.Creating, "the master is final")

	childBins := recordBins(t, client, store, hash, 1, fields.Creating, fields.Attempt)
	require.NotNil(t, childBins, "the retry wrote the missing child")
	require.NotContains(t, childBins, fields.Creating.String(), "the finalize of the master's attempt cleared the child the retry wrote")
	require.Equal(t, first.Attempt, childBins[fields.Attempt.String()], "the child carries the token of its master")
}
