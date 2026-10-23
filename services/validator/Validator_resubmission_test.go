package validator

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestValidate_ResubmissionIsRecognisedByItsOwnTxid pins how a store that deletes a coin on
// spend recognises a transaction submitted again: by looking it up under its own txid when every
// coin it asks for is gone and no other spender is known. Before this, the only thing that could
// say "you took these yourself" was the undo journal, so a transaction resubmitted after the
// journal had dropped its spends was rejected as spending coins that are already spent, and the
// journal was kept for 1440 blocks to put that day off.
//
// A double spend is a different transaction with a different txid, so the lookup cannot mistake
// one for a resubmission; the cases below pin that the check stays out of the way whenever the
// store names another spender or the record is conflicting.
func TestValidate_ResubmissionIsRecognisedByItsOwnTxid(t *testing.T) {
	makeTx := func(t *testing.T) (*bt.Tx, *bt.Tx) {
		privateKey, publicKey := bec.PrivateKeyFromBytes([]byte("THIS_IS_A_DETERMINISTIC_PRIVATE_KEY"))
		parent := transactions.Create(t,
			transactions.WithCoinbaseData(100, "/Test miner/"),
			transactions.WithP2PKHOutputs(1, 50e8, publicKey),
		)
		tx := transactions.Create(t,
			transactions.WithPrivateKey(privateKey),
			transactions.WithInput(parent, 0),
			transactions.WithP2PKHOutputs(1, 1000),
			transactions.WithChangeOutput(),
		)

		return tx, parent
	}

	// setup wires a store whose spend of tx fails with the given per-input results, and whose
	// record for tx is existing (nil for "not held").
	setup := func(t *testing.T, tx, parent *bt.Tx, spends []*utxo.Spend, existing *meta.Data) (*Validator, *utxo.MockUtxostore) {
		mockStore := &utxo.MockUtxostore{}
		settings := test.CreateBaseTestSettings(t)

		validator, err := New(context.Background(), ulogger.TestLogger{}, settings, mockStore, nil, nil, nil, nil, nil)
		require.NoError(t, err)

		mockStore.On("ParentOutputsForValidation", mock.Anything, mock.Anything).Return([]utxo.ParentOutput{{
			Status: utxo.ParentOutputNotMined, Satoshis: parent.Outputs[0].Satoshis, LockingScript: parent.Outputs[0].LockingScript,
		}}, nil)
		mockStore.On("GetBlockState").Return(utxo.BlockState{Height: 100, MedianTime: 1000000000})
		mockStore.On("SpendAndCreate", mock.Anything, tx, mock.Anything, mock.Anything).
			Return(nil, spends, errors.NewUtxoError("spend failed"))

		if existing == nil {
			mockStore.On("GetMeta", mock.Anything, mock.Anything, mock.Anything).Return(errors.NewTxNotFoundError("not held"))
		} else {
			mockStore.On("GetMeta", mock.Anything, mock.Anything, mock.Anything).
				Run(func(args mock.Arguments) { *args.Get(2).(*meta.Data) = *existing }).
				Return(nil)
		}

		return validator.(*Validator), mockStore
	}

	spentByNobodyKnown := func(tx *bt.Tx) []*utxo.Spend {
		return []*utxo.Spend{{TxID: tx.Inputs[0].PreviousTxIDChainHash(), Vout: 0, Err: errors.ErrSpent}}
	}

	t.Run("a mined transaction submitted again is recognised", func(t *testing.T) {
		tx, parent := makeTx(t)
		v, store := setup(t, tx, parent, spentByNobodyKnown(tx), &meta.Data{Tx: tx, BlockIDs: []uint32{7}})

		got, err := v.validateInternal(context.Background(), tx, 100, &Options{})

		require.NoError(t, err)
		require.Equal(t, []uint32{7}, got.BlockIDs)
		store.AssertNotCalled(t, "Create", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("an unmined transaction submitted again is recognised", func(t *testing.T) {
		tx, parent := makeTx(t)
		v, _ := setup(t, tx, parent, spentByNobodyKnown(tx), &meta.Data{Tx: tx})

		got, err := v.validateInternal(context.Background(), tx, 100, &Options{})

		require.NoError(t, err)
		require.NotNil(t, got)
	})

	t.Run("it is recognised before the create-conflicting path can condemn it", func(t *testing.T) {
		tx, parent := makeTx(t)
		v, store := setup(t, tx, parent, spentByNobodyKnown(tx), &meta.Data{Tx: tx, BlockIDs: []uint32{7}})

		_, err := v.validateInternal(context.Background(), tx, 100, &Options{CreateConflicting: true})

		require.NoError(t, err)
		store.AssertNotCalled(t, "Create", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("a transaction the store does not hold is still rejected", func(t *testing.T) {
		tx, parent := makeTx(t)
		v, _ := setup(t, tx, parent, spentByNobodyKnown(tx), nil)

		got, err := v.validateInternal(context.Background(), tx, 100, &Options{})

		require.Error(t, err)
		require.Nil(t, got)
		require.Contains(t, err.Error(), "error spending utxos")
	})

	t.Run("a conflicting record is not taken as a resubmission", func(t *testing.T) {
		tx, parent := makeTx(t)
		v, _ := setup(t, tx, parent, spentByNobodyKnown(tx), &meta.Data{Tx: tx, Conflicting: true})

		got, err := v.validateInternal(context.Background(), tx, 100, &Options{})

		require.Error(t, err)
		require.Nil(t, got)
	})

	t.Run("a spend the store attributes to another transaction is left to conflict handling", func(t *testing.T) {
		tx, parent := makeTx(t)
		other := chainhash.Hash{0x0b}
		spends := []*utxo.Spend{{TxID: tx.Inputs[0].PreviousTxIDChainHash(), Vout: 0, Err: errors.ErrSpent, ConflictingTxID: &other}}
		v, store := setup(t, tx, parent, spends, &meta.Data{Tx: tx, BlockIDs: []uint32{7}})

		got, err := v.validateInternal(context.Background(), tx, 100, &Options{})

		require.Error(t, err)
		require.Nil(t, got)
		store.AssertNotCalled(t, "GetMeta", mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("a failure that is not a spent coin is left alone", func(t *testing.T) {
		tx, parent := makeTx(t)
		spends := []*utxo.Spend{{TxID: tx.Inputs[0].PreviousTxIDChainHash(), Vout: 0, Err: errors.NewUtxoFrozenError("frozen")}}
		v, store := setup(t, tx, parent, spends, &meta.Data{Tx: tx, BlockIDs: []uint32{7}})

		got, err := v.validateInternal(context.Background(), tx, 100, &Options{})

		require.Error(t, err)
		require.Nil(t, got)
		store.AssertNotCalled(t, "GetMeta", mock.Anything, mock.Anything, mock.Anything)
	})
}
