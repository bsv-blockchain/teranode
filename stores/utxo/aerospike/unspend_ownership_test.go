package aerospike_test

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	utxo2 "github.com/bsv-blockchain/teranode/test/longtest/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestUnspendOwnership_HappyPath verifies that Unspend with matching SpendingData
// successfully clears the spend.
func TestUnspendOwnership_HappyPath(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	_, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)

	_, err := store.Create(ctx, tx, 101)
	require.NoError(t, err)

	localSpendTx := utxo2.GetSpendingTx(tx, 0)
	spendsRet, err := store.Spend(ctx, localSpendTx, store.GetBlockHeight()+1)
	require.NoError(t, err)
	require.Len(t, spendsRet, 1)

	// SpendingData here matches what GetSpends populated: NewSpendingData(localSpendTx.TxIDChainHash(), 0).
	err = store.Unspend(ctx, spendsRet)
	require.NoError(t, err, "happy path Unspend with matching SpendingData should succeed")

	// Verify the UTXO is now unspent — we should be able to Spend it again.
	spendTxAlt := utxo2.GetSpendingTx(tx, 0)
	_, err = store.Spend(ctx, spendTxAlt, store.GetBlockHeight()+1)
	require.NoError(t, err, "after successful Unspend the UTXO should be re-spendable")
}

// TestUnspendOwnership_Mismatch verifies that Unspend is idempotent when the
// caller's SpendingData doesn't match the stored value: the call succeeds and
// the legitimate spend stays intact. The safety guarantee is "never wipe a
// spend we don't own".
func TestUnspendOwnership_Mismatch(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	_, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)

	_, err := store.Create(ctx, tx, 101)
	require.NoError(t, err)

	// Spend with a real spending tx — store records SpendingData = NewSpendingData(localSpendTx.TxIDChainHash(), 0).
	localSpendTx := utxo2.GetSpendingTx(tx, 0)
	_, err = store.Spend(ctx, localSpendTx, store.GetBlockHeight()+1)
	require.NoError(t, err)

	utxoHash, err := util.UTXOHashFromOutput(tx.TxIDChainHash(), tx.Outputs[0], 0)
	require.NoError(t, err)

	wrongTxHash := chainhash.HashH([]byte("wrong-spender"))
	mismatched := []*utxo.Spend{{
		TxID:         tx.TxIDChainHash(),
		Vout:         0,
		UTXOHash:     utxoHash,
		SpendingData: spendpkg.NewSpendingData(&wrongTxHash, 0),
	}}

	err = store.Unspend(ctx, mismatched)
	require.NoError(t, err, "mismatched Unspend must be an idempotent no-op, not an error")

	// The UTXO must still be spent — attempting to Spend with a NEW spender should fail with ErrSpent
	// and report the ORIGINAL spender's TxID as the conflict source.
	spendTxRetry := utxo2.GetSpendingTx(tx, 0)
	spendsRet, err := store.Spend(ctx, spendTxRetry, store.GetBlockHeight()+1)
	require.Error(t, err, "UTXO should still be spent after the no-op mismatched Unspend")
	require.Len(t, spendsRet, 1)
	require.Equal(t, localSpendTx.TxIDChainHash().String(), spendsRet[0].ConflictingTxID.String(),
		"the original spender's TxID must still be the recorded conflicting TxID")
}

// TestUnspendOwnership_NotFound verifies that Unspend against a record the store
// has never seen (Lua's ERROR_CODE_TX_NOT_FOUND branch) is surfaced as
// NotFoundError — matches the SQL backend.
func TestUnspendOwnership_NotFound(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	_, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)

	// No store.Create — the record does not exist.
	unknownTxHash := chainhash.HashH([]byte("never-created"))
	utxoHash, err := util.UTXOHashFromOutput(tx.TxIDChainHash(), tx.Outputs[0], 0)
	require.NoError(t, err)

	spend := []*utxo.Spend{{
		TxID:         &unknownTxHash,
		Vout:         0,
		UTXOHash:     utxoHash,
		SpendingData: spendpkg.NewSpendingData(&unknownTxHash, 0),
	}}

	err = store.Unspend(ctx, spend)
	require.Error(t, err, "expected error when unspending an unknown transaction")
	require.True(t, errors.Is(err, errors.ErrNotFound), "expected NotFoundError, got: %v", err)
}

// TestUnspendOwnership_MismatchOnUnspent verifies that Unspend with a non-nil
// SpendingData against a never-spent UTXO is an idempotent no-op (the row is
// already in the desired state — nothing to clear). Mirrors the SQL backend's
// case-2 behaviour.
func TestUnspendOwnership_MismatchOnUnspent(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	_, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)

	_, err := store.Create(ctx, tx, 101)
	require.NoError(t, err)

	utxoHash, err := util.UTXOHashFromOutput(tx.TxIDChainHash(), tx.Outputs[0], 0)
	require.NoError(t, err)

	// Never call Spend — the row exists with spending_data nil.
	someTxHash := chainhash.HashH([]byte("some-other-tx"))
	spend := []*utxo.Spend{{
		TxID:         tx.TxIDChainHash(),
		Vout:         0,
		UTXOHash:     utxoHash,
		SpendingData: spendpkg.NewSpendingData(&someTxHash, 0),
	}}

	err = store.Unspend(ctx, spend)
	require.NoError(t, err, "Unspend against an already-unspent UTXO must be a no-op success")

	// The UTXO must still be spendable normally — confirm by spending it.
	freshSpend := utxo2.GetSpendingTx(tx, 0)
	_, err = store.Spend(ctx, freshSpend, store.GetBlockHeight()+1)
	require.NoError(t, err, "UTXO should remain freshly spendable after the no-op Unspend")
}

// TestUnspendOwnership_NilSpendingDataRejected verifies that Unspend rejects
// callers that pass a nil SpendingData. The escape hatch was removed because
// every production caller derives spends from Spend()/SetConflicting()/GetSpends(),
// all of which populate SpendingData.
func TestUnspendOwnership_NilSpendingDataRejected(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	_, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)

	_, err := store.Create(ctx, tx, 101)
	require.NoError(t, err)

	localSpendTx := utxo2.GetSpendingTx(tx, 0)
	_, err = store.Spend(ctx, localSpendTx, store.GetBlockHeight()+1)
	require.NoError(t, err)

	utxoHash, err := util.UTXOHashFromOutput(tx.TxIDChainHash(), tx.Outputs[0], 0)
	require.NoError(t, err)

	noSpendingData := []*utxo.Spend{{
		TxID:         tx.TxIDChainHash(),
		Vout:         0,
		UTXOHash:     utxoHash,
		SpendingData: nil,
	}}

	err = store.Unspend(ctx, noSpendingData)
	require.Error(t, err, "Unspend with nil SpendingData must error")
	require.True(t, errors.Is(err, errors.ErrProcessing), "expected processing error, got: %v", err)

	// The UTXO must still be spent — attempting to Spend with a NEW spender should still fail.
	spendTxRetry := utxo2.GetSpendingTx(tx, 0)
	spendsRet, err := store.Spend(ctx, spendTxRetry, store.GetBlockHeight()+1)
	require.Error(t, err, "UTXO should still be spent after rejected nil-SpendingData Unspend")
	require.Len(t, spendsRet, 1)
	require.Equal(t, localSpendTx.TxIDChainHash().String(), spendsRet[0].ConflictingTxID.String(),
		"the original spender's TxID must still be the recorded conflicting TxID")
}
