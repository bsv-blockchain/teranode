package utxo

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/stretchr/testify/require"
)

type recoveryReadStore struct {
	Store
	lookupErr error
	response  *SpendResponse
}

func (s recoveryReadStore) Get(context.Context, *chainhash.Hash, ...fields.FieldName) (*meta.Data, error) {
	return nil, s.lookupErr
}
func (s recoveryReadStore) GetSpend(context.Context, *Spend) (*SpendResponse, error) {
	return s.response, nil
}

func TestRecoveryConfirmedPrunedParent(t *testing.T) {
	parent, winner := chainhash.Hash{1}, chainhash.Hash{2}
	input := &bt.Input{}
	require.NoError(t, input.PreviousTxIDAdd(&parent))
	tx := bt.NewTx()
	tx.Inputs = []*bt.Input{input}
	for _, lookupErr := range []error{nil, errors.ErrTxNotFound, errors.ErrNotFound, errors.NewStorageError("offline")} {
		store := recoveryReadStore{lookupErr: lookupErr}
		owned, err := winnerInputsOwned(t.Context(), store, winner, tx, absentParentIsOwned)
		if lookupErr != nil && !errors.Is(lookupErr, errors.ErrTxNotFound) && !errors.Is(lookupErr, errors.ErrNotFound) {
			require.Error(t, err)
			require.False(t, owned)
		} else {
			require.NoError(t, err)
			require.True(t, owned)
		}
		owned, _ = winnerIsApplied(t.Context(), store, winner, tx)
		require.False(t, owned, "unconfirmed winners require input ownership")

		owned, err = winnerInputsOwned(t.Context(), store, winner, tx, absentParentIsNotOwned)
		require.False(t, owned, "a reversed winner does not own a pruned parent")
		if lookupErr != nil && !errors.Is(lookupErr, errors.ErrTxNotFound) && !errors.Is(lookupErr, errors.ErrNotFound) {
			require.Error(t, err, "a storage error is not a pruned parent")
		} else {
			require.NoError(t, err)
		}
	}
}

func TestRecoveryMissingDisplacedOwner(t *testing.T) {
	parent, original, outsider := chainhash.Hash{1}, chainhash.Hash{2}, chainhash.Hash{3}
	r := &ConflictRecovery{OriginalSpends: []*Spend{{TxID: &parent, SpendingData: spend.NewSpendingData(&original, 0)}}}
	for _, lookupErr := range []error{nil, errors.ErrTxNotFound, errors.ErrNotFound} {
		store := recoveryReadStore{lookupErr: lookupErr, response: &SpendResponse{SpendingData: spend.NewSpendingData(&outsider, 0)}}
		displaced, leave, release, err := displacedConflictOwners(t.Context(), store, r, nil)
		require.NoError(t, err, "missing owner must not abort compensation before restoration")
		require.Empty(t, displaced.order)
		require.Equal(t, []bool{false}, leave)
		require.Empty(t, release)
	}
}
