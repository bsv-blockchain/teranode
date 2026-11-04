package utxo

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func dwpHash(b byte) *chainhash.Hash {
	h := chainhash.Hash{}
	h[0] = b

	return &h
}

func dwpTx(t *testing.T, parents ...*chainhash.Hash) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	script := bscript.NewFromBytes([]byte{bscript.OpTRUE})

	for i, p := range parents {
		require.NoError(t, tx.From(p.String(), uint32(i), script.String(), 1000))
	}

	return tx
}

func TestDeleteWithParents(t *testing.T) {
	ctx := context.Background()
	txHash := dwpHash(0xaa)
	getFields := []fields.FieldName{fields.Tx, fields.Conflicting}

	t.Run("tx not found returns nil and deletes", func(t *testing.T) {
		s := &MockUtxostore{}
		s.On("Get", mock.Anything, txHash, getFields).Return(nil, errors.NewTxNotFoundError("gone"))
		s.On("Delete", mock.Anything, txHash).Return(nil)

		require.NoError(t, DeleteWithParents(ctx, s, txHash))
		s.AssertNotCalled(t, "Unspend", mock.Anything, mock.Anything, mock.Anything)
		s.AssertCalled(t, "Delete", mock.Anything, txHash)
	})

	t.Run("delete error is wrapped as storage error", func(t *testing.T) {
		s := &MockUtxostore{}
		s.On("Get", mock.Anything, txHash, getFields).Return(nil, errors.NewNotFoundError("gone"))
		s.On("Delete", mock.Anything, txHash).Return(errors.NewServiceError("boom"))

		err := DeleteWithParents(ctx, s, txHash)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrStorageError))
		require.False(t, errors.Is(err, errors.ErrNotFound))
	})

	t.Run("delete not found is tolerated", func(t *testing.T) {
		s := &MockUtxostore{}
		s.On("Get", mock.Anything, txHash, getFields).Return(nil, errors.NewNotFoundError("gone"))
		s.On("Delete", mock.Anything, txHash).Return(errors.NewTxNotFoundError("gone"))

		require.NoError(t, DeleteWithParents(ctx, s, txHash))
	})

	t.Run("clear refs error stops before delete", func(t *testing.T) {
		s := &MockUtxostore{}
		s.On("Get", mock.Anything, txHash, getFields).Return(nil, errors.NewServiceError("db down"))

		err := DeleteWithParents(ctx, s, txHash)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrStorageError))
		s.AssertNotCalled(t, "Delete", mock.Anything, mock.Anything)
	})

	t.Run("unspends parents then deletes", func(t *testing.T) {
		parent := dwpHash(0x01)
		tx := dwpTx(t, parent)

		s := &MockUtxostore{}
		s.On("Get", mock.Anything, txHash, getFields).Return(&meta.Data{Tx: tx}, nil)
		s.On("PreviousOutputsDecorate", mock.Anything, tx).Return(nil)
		s.On("Unspend", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		s.On("Delete", mock.Anything, txHash).Return(nil)

		require.NoError(t, DeleteWithParents(ctx, s, txHash))
		s.AssertNumberOfCalls(t, "Unspend", 1)
		s.AssertNotCalled(t, "RemoveFromConflictingChildren", mock.Anything, mock.Anything)
		s.AssertCalled(t, "Delete", mock.Anything, txHash)
	})
}

func TestClearParentRefs(t *testing.T) {
	ctx := context.Background()
	txHash := dwpHash(0xaa)
	getFields := []fields.FieldName{fields.Tx, fields.Conflicting}

	t.Run("tx not found returns nil", func(t *testing.T) {
		for _, notFound := range []error{errors.NewTxNotFoundError("gone"), errors.NewNotFoundError("gone")} {
			s := &MockUtxostore{}
			s.On("Get", mock.Anything, txHash, getFields).Return(nil, notFound)

			require.NoError(t, ClearParentRefs(ctx, s, txHash))
			s.AssertNotCalled(t, "Unspend", mock.Anything, mock.Anything, mock.Anything)
		}
	})

	t.Run("get error is a storage error", func(t *testing.T) {
		s := &MockUtxostore{}
		s.On("Get", mock.Anything, txHash, getFields).Return(nil, errors.NewServiceError("db down"))

		err := ClearParentRefs(ctx, s, txHash)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrStorageError))
	})

	t.Run("nil tx is a processing error", func(t *testing.T) {
		s := &MockUtxostore{}
		s.On("Get", mock.Anything, txHash, getFields).Return(&meta.Data{}, nil)

		err := ClearParentRefs(ctx, s, txHash)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrProcessing))
		s.AssertNotCalled(t, "PreviousOutputsDecorate", mock.Anything, mock.Anything)
	})

	t.Run("decorate error is a storage error", func(t *testing.T) {
		tx := dwpTx(t, dwpHash(0x01))

		s := &MockUtxostore{}
		s.On("Get", mock.Anything, txHash, getFields).Return(&meta.Data{Tx: tx}, nil)
		s.On("PreviousOutputsDecorate", mock.Anything, tx).Return(errors.NewServiceError("boom"))

		err := ClearParentRefs(ctx, s, txHash)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrStorageError))
		s.AssertNotCalled(t, "Unspend", mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("input without locking script is a processing error", func(t *testing.T) {
		parent := dwpHash(0x01)
		tx := bt.NewTx()
		tx.Inputs = []*bt.Input{{PreviousTxOutIndex: 0}}
		require.NoError(t, tx.Inputs[0].PreviousTxIDAdd(parent))

		s := &MockUtxostore{}
		s.On("Get", mock.Anything, txHash, getFields).Return(&meta.Data{Tx: tx}, nil)
		s.On("PreviousOutputsDecorate", mock.Anything, tx).Return(nil)

		err := ClearParentRefs(ctx, s, txHash)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrProcessing))
		s.AssertNotCalled(t, "Unspend", mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("tx without inputs skips unspend", func(t *testing.T) {
		tx := bt.NewTx()

		s := &MockUtxostore{}
		s.On("Get", mock.Anything, txHash, getFields).Return(&meta.Data{Tx: tx, Conflicting: true}, nil)
		s.On("PreviousOutputsDecorate", mock.Anything, tx).Return(nil)

		require.NoError(t, ClearParentRefs(ctx, s, txHash))
		s.AssertNotCalled(t, "Unspend", mock.Anything, mock.Anything, mock.Anything)
		s.AssertNotCalled(t, "RemoveFromConflictingChildren", mock.Anything, mock.Anything)
	})

	t.Run("unspend builds one spend per input", func(t *testing.T) {
		p1, p2 := dwpHash(0x01), dwpHash(0x02)
		tx := dwpTx(t, p1, p2)

		s := &MockUtxostore{}
		s.On("Get", mock.Anything, txHash, getFields).Return(&meta.Data{Tx: tx}, nil)
		s.On("PreviousOutputsDecorate", mock.Anything, tx).Return(nil)

		var got []*Spend

		s.On("Unspend", mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
			got = args.Get(1).([]*Spend)
		}).Return(nil)

		require.NoError(t, ClearParentRefs(ctx, s, txHash))
		s.AssertNumberOfCalls(t, "Unspend", 1)
		s.AssertNotCalled(t, "RemoveFromConflictingChildren", mock.Anything, mock.Anything)
		require.Len(t, got, 2)
		require.Equal(t, *p1, *got[0].TxID)
		require.Equal(t, uint32(0), got[0].Vout)
		require.NotNil(t, got[0].UTXOHash)
		require.Equal(t, *txHash, *got[0].SpendingData.TxID)
		require.Equal(t, 0, got[0].SpendingData.Vin)
		require.Equal(t, *p2, *got[1].TxID)
		require.Equal(t, uint32(1), got[1].Vout)
		require.Equal(t, *txHash, *got[1].SpendingData.TxID)
		require.Equal(t, 1, got[1].SpendingData.Vin)
	})

	t.Run("unspend error other than not found is a storage error", func(t *testing.T) {
		tx := dwpTx(t, dwpHash(0x01))

		s := &MockUtxostore{}
		s.On("Get", mock.Anything, txHash, getFields).Return(&meta.Data{Tx: tx, Conflicting: true}, nil)
		s.On("PreviousOutputsDecorate", mock.Anything, tx).Return(nil)
		s.On("Unspend", mock.Anything, mock.Anything, mock.Anything).Return(errors.NewServiceError("boom"))

		err := ClearParentRefs(ctx, s, txHash)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrStorageError))
		s.AssertNumberOfCalls(t, "Unspend", 1)
		s.AssertNotCalled(t, "RemoveFromConflictingChildren", mock.Anything, mock.Anything)
	})

	t.Run("missing parent is skipped and surviving parent is unspent", func(t *testing.T) {
		missing, alive := dwpHash(0x01), dwpHash(0x02)
		tx := dwpTx(t, missing, alive)

		s := &MockUtxostore{}
		s.On("Get", mock.Anything, txHash, getFields).Return(&meta.Data{Tx: tx}, nil)
		s.On("PreviousOutputsDecorate", mock.Anything, tx).Return(nil)

		s.On("Unspend", mock.Anything, mock.MatchedBy(func(sp []*Spend) bool { return len(sp) == 2 }), mock.Anything).
			Return(errors.NewTxNotFoundError("parent gone")).Once()
		s.On("Unspend", mock.Anything, mock.MatchedBy(func(sp []*Spend) bool { return len(sp) == 1 && *sp[0].TxID == *missing }), mock.Anything).
			Return(errors.NewNotFoundError("parent gone")).Once()
		s.On("Unspend", mock.Anything, mock.MatchedBy(func(sp []*Spend) bool { return len(sp) == 1 && *sp[0].TxID == *alive }), mock.Anything).
			Return(nil).Once()

		require.NoError(t, ClearParentRefs(ctx, s, txHash))
		s.AssertNumberOfCalls(t, "Unspend", 3)
		s.AssertExpectations(t)
	})

	t.Run("per-spend retry stops on a non-not-found error", func(t *testing.T) {
		p1, p2 := dwpHash(0x01), dwpHash(0x02)
		tx := dwpTx(t, p1, p2)

		s := &MockUtxostore{}
		s.On("Get", mock.Anything, txHash, getFields).Return(&meta.Data{Tx: tx}, nil)
		s.On("PreviousOutputsDecorate", mock.Anything, tx).Return(nil)

		s.On("Unspend", mock.Anything, mock.MatchedBy(func(sp []*Spend) bool { return len(sp) == 2 }), mock.Anything).
			Return(errors.NewTxNotFoundError("parent gone")).Once()
		s.On("Unspend", mock.Anything, mock.MatchedBy(func(sp []*Spend) bool { return len(sp) == 1 && *sp[0].TxID == *p1 }), mock.Anything).
			Return(errors.NewServiceError("boom")).Once()

		err := ClearParentRefs(ctx, s, txHash)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrStorageError))
		s.AssertNumberOfCalls(t, "Unspend", 2)
	})

	t.Run("conflicting tx removes each parent once", func(t *testing.T) {
		p1, p2 := dwpHash(0x01), dwpHash(0x02)
		tx := dwpTx(t, p1, p2, p1)

		s := &MockUtxostore{}
		s.On("Get", mock.Anything, txHash, getFields).Return(&meta.Data{Tx: tx, Conflicting: true}, nil)
		s.On("PreviousOutputsDecorate", mock.Anything, tx).Return(nil)
		s.On("Unspend", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		var got []ConflictingChildRemoval

		s.On("RemoveFromConflictingChildren", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
			got = args.Get(1).([]ConflictingChildRemoval)
		}).Return(nil)

		require.NoError(t, ClearParentRefs(ctx, s, txHash))
		require.Len(t, got, 2)
		require.Equal(t, *p1, *got[0].ParentHash)
		require.Equal(t, *txHash, *got[0].ChildHash)
		require.Equal(t, *p2, *got[1].ParentHash)
		require.Equal(t, *txHash, *got[1].ChildHash)
	})

	t.Run("remove from conflicting children error is a storage error", func(t *testing.T) {
		tx := dwpTx(t, dwpHash(0x01))

		s := &MockUtxostore{}
		s.On("Get", mock.Anything, txHash, getFields).Return(&meta.Data{Tx: tx, Conflicting: true}, nil)
		s.On("PreviousOutputsDecorate", mock.Anything, tx).Return(nil)
		s.On("Unspend", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		s.On("RemoveFromConflictingChildren", mock.Anything, mock.Anything).Return(errors.NewServiceError("boom"))

		err := ClearParentRefs(ctx, s, txHash)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrStorageError))
	})
}

func TestIsNotFoundErr(t *testing.T) {
	require.True(t, isNotFoundErr(errors.NewTxNotFoundError("x")))
	require.True(t, isNotFoundErr(errors.NewNotFoundError("x")))
	require.False(t, isNotFoundErr(errors.NewServiceError("x")))
	require.False(t, isNotFoundErr(nil))
}
