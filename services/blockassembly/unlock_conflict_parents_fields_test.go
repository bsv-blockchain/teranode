package blockassembly

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestUnlockConflictParentsAsksForOutpointsNotTheWholeTx pins the field set
// unlockConflictParents asks for, and that the parents it unlocks come from the
// inpoints alone. The walk reads parent hashes only, so a record carrying no
// Tx must still unlock its parents, and a future edit that widens the request
// back to fields.Tx fails here.
func TestUnlockConflictParentsAsksForOutpointsNotTheWholeTx(t *testing.T) {
	ctx := context.Background()
	mockStore := &utxo.MockUtxostore{}

	txHash := chainhash.HashH([]byte("winner"))
	parentA := chainhash.HashH([]byte("parent-a"))
	parentB := chainhash.HashH([]byte("parent-b"))

	// Only TxInpoints is set: no Tx, so reading txMeta.Tx would find nothing.
	inputA := &bt.Input{PreviousTxOutIndex: 0}
	require.NoError(t, inputA.PreviousTxIDAdd(&parentA))

	inputB := &bt.Input{PreviousTxOutIndex: 3}
	require.NoError(t, inputB.PreviousTxIDAdd(&parentB))

	inpoints, err := subtree.NewTxInpointsFromInputs([]*bt.Input{inputA, inputB})
	require.NoError(t, err)

	var askedFor []fields.FieldName

	mockStore.On("Get", mock.Anything, &txHash, mock.Anything).
		Run(func(args mock.Arguments) {
			askedFor = args.Get(2).([]fields.FieldName)
		}).
		Return(&meta.Data{TxInpoints: inpoints}, nil)

	var unlocked []chainhash.Hash

	mockStore.On("SetLocked", mock.Anything, mock.Anything, false).
		Run(func(args mock.Arguments) {
			unlocked = args.Get(1).([]chainhash.Hash)
		}).
		Return(nil)

	b := newReplayTestAssembler(mockStore, nil)

	require.NoError(t, b.unlockConflictParents(ctx, []chainhash.Hash{txHash}))

	require.Contains(t, askedFor, fields.TxInpoints)
	require.NotContains(t, askedFor, fields.Tx,
		"unlockConflictParents reads parent hashes only and must not pull the transaction body")
	require.NotContains(t, askedFor, fields.Inputs)

	require.ElementsMatch(t, []chainhash.Hash{parentA, parentB}, unlocked)
}
