package validator

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/teranode/errors"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// checkExtendedFixture returns a validator over a real SQLite store holding a
// parent whose output 0 is locked by script, and a child spending it, extended
// by hand as the batch caller extends it.
func checkExtendedFixture(t *testing.T, name string, script []byte) (*Validator, *bt.Tx, *bt.Tx) {
	t.Helper()

	f := ambiguityFixture{name: name, realScript: script, realSats: 10_000}
	v, parent, _ := newAmbiguityValidator(t, "check_extended_"+name, f)
	v.settings.Policy.MinMiningTxFee = 0
	v.txValidator = NewTxValidator(v.logger, v.settings)

	child := spendOf(t, parent, script, 10_000, 9_000)
	child.SetExtended(true)

	return v, parent, child
}

func TestCheckExtendedTransaction_ValidAndWritesNothing(t *testing.T) {
	v, parent, child := checkExtendedFixture(t, "valid", []byte{0x51})
	ctx := t.Context()

	require.NoError(t, v.CheckExtendedTransaction(ctx, child, 500, []uint32{499}, &Options{}))

	_, err := v.utxoStore.Get(ctx, child.TxIDChainHash())
	require.ErrorIs(t, err, errors.ErrTxNotFound, "a check must not create the transaction")

	answers, err := v.utxoStore.ParentOutputsForValidation(ctx, []utxostore.Outpoint{{TxID: *parent.TxIDChainHash(), Vout: 0}})
	require.NoError(t, err)
	require.Equal(t, utxostore.ParentOutputNotMined, answers[0].Status, "the parent is still there, unchanged")
}

func TestCheckExtendedTransaction_Rejections(t *testing.T) {
	t.Run("a failing script is invalid", func(t *testing.T) {
		v, _, child := checkExtendedFixture(t, "false", []byte{0x00})
		err := v.CheckExtendedTransaction(t.Context(), child, 500, []uint32{499}, &Options{})
		require.ErrorIs(t, err, errors.ErrTxInvalid)
	})

	t.Run("a coinbase is refused", func(t *testing.T) {
		v, _, _ := checkExtendedFixture(t, "coinbase", []byte{0x51})
		coinbase := bt.NewTx()
		require.NoError(t, coinbase.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0))
		coinbase.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x01, 0x01})
		coinbase.AddOutput(&bt.Output{Satoshis: 1, LockingScript: bscript.NewFromBytes([]byte{0x51})})

		require.Error(t, v.CheckExtendedTransaction(t.Context(), coinbase, 500, []uint32{0}, &Options{}))
	})

	t.Run("a non-final transaction is refused", func(t *testing.T) {
		v, _, child := checkExtendedFixture(t, "nonfinal", []byte{0x51})
		child.LockTime = 10_000
		child.Inputs[0].SequenceNumber = 0

		err := v.CheckExtendedTransaction(t.Context(), child, 500, []uint32{499}, &Options{})
		require.ErrorIs(t, err, errors.ErrNonFinal)
	})

	t.Run("an unextended transaction is refused, never read from the store", func(t *testing.T) {
		v, _, child := checkExtendedFixture(t, "unextended", []byte{0x51})
		child.Inputs[0].PreviousTxScript = nil
		child.SetExtended(false)

		err := v.CheckExtendedTransaction(t.Context(), child, 500, []uint32{499}, &Options{})
		require.ErrorIs(t, err, errors.ErrProcessing)
		require.Nil(t, child.Inputs[0].PreviousTxScript, "the check must not extend the transaction itself")
	})

	t.Run("a heights slice of the wrong length is refused", func(t *testing.T) {
		v, _, child := checkExtendedFixture(t, "heights", []byte{0x51})
		require.ErrorIs(t, v.CheckExtendedTransaction(t.Context(), child, 500, nil, &Options{}), errors.ErrProcessing)
	})
}
