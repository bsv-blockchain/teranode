// Package subtreevalidation provides functionality for validating subtrees in a blockchain context.
package subtreevalidation

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func hashMatcher(want chainhash.Hash) interface{} {
	return mock.MatchedBy(func(h *chainhash.Hash) bool { return h.Equal(want) })
}

func newGhostTestTx(t *testing.T, parentTxHash chainhash.Hash) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	input := &bt.Input{PreviousTxOutIndex: 0}
	require.NoError(t, input.PreviousTxIDAdd(&parentTxHash))
	tx.Inputs = append(tx.Inputs, input)
	tx.Outputs = append(tx.Outputs, &bt.Output{Satoshis: 1000, LockingScript: &bscript.Script{bscript.OpTRUE}})

	return tx
}

// A conflicting tx whose parent slot records a spender with no store record (a
// confirmed ghost) must pass the counter-conflicting check: an absent counter
// has no BlockIDs, so it is not mined on our chain. Before the ghost tolerance
// this scenario returned a ProcessingError and wedged the block forever.
func TestCheckCounterConflictingOnCurrentChain_ToleratesGhostSpender(t *testing.T) {
	ctx := context.Background()
	mockStore := &utxo.MockUtxostore{}

	parentTxHash := chainhash.HashH([]byte("ghost-parent"))
	ghostTxHash := chainhash.HashH([]byte("ghost-spender"))

	winningTx := newGhostTestTx(t, parentTxHash)
	winningTxHash := chainhash.HashH([]byte("ghost-winner"))

	parentTx := newGhostTestTx(t, chainhash.HashH([]byte("ghost-grandparent")))

	mockStore.On("Get", mock.Anything, hashMatcher(winningTxHash), []fields.FieldName{fields.Tx}).
		Return(&meta.Data{Tx: winningTx}, nil)
	mockStore.On("Get", mock.Anything, hashMatcher(parentTxHash), []fields.FieldName{fields.Utxos, fields.DeletedChildren}).
		Return(&meta.Data{SpendingDatas: []*spend.SpendingData{spend.NewSpendingData(&ghostTxHash, 0)}}, nil)
	mockStore.On("GetConflictingChildren", mock.Anything, ghostTxHash).
		Return(([]chainhash.Hash)(nil), errors.NewTxNotFoundError("%v not found", ghostTxHash))
	mockStore.On("Get", mock.Anything, hashMatcher(ghostTxHash), []fields.FieldName{fields.Conflicting}).
		Return(nil, errors.NewTxNotFoundError("%v not found", ghostTxHash))
	mockStore.On("Get", mock.Anything, hashMatcher(parentTxHash), []fields.FieldName{fields.Tx}).
		Return(&meta.Data{Tx: parentTx}, nil)

	// the surviving counter set is just the tx itself
	mockStore.On("GetMeta", mock.Anything, hashMatcher(winningTxHash), mock.Anything).
		Return(&meta.Data{}, nil)
	mockStore.On("Get", mock.Anything, hashMatcher(winningTxHash), []fields.FieldName{fields.Utxos, fields.ConflictingChildren, fields.DeletedChildren}).
		Return(&meta.Data{}, nil)

	u := &Server{logger: ulogger.NewErrorTestLogger(t), utxoStore: mockStore}

	err := u.checkCounterConflictingOnCurrentChain(ctx, winningTxHash, map[uint32]bool{7: true})

	require.NoError(t, err)
	mockStore.AssertExpectations(t)
}

// A live counter that IS mined on our chain must still be rejected — the ghost
// tolerance must not weaken the mined-on-our-chain check for present records.
func TestCheckCounterConflictingOnCurrentChain_StillRejectsMinedCounter(t *testing.T) {
	ctx := context.Background()
	mockStore := &utxo.MockUtxostore{}

	parentTxHash := chainhash.HashH([]byte("mined-parent"))
	counterTxHash := chainhash.HashH([]byte("mined-counter"))

	conflictingTx := newGhostTestTx(t, parentTxHash)
	conflictingTxHash := chainhash.HashH([]byte("mined-conflicting"))

	mockStore.On("Get", mock.Anything, hashMatcher(conflictingTxHash), []fields.FieldName{fields.Tx}).
		Return(&meta.Data{Tx: conflictingTx}, nil)
	mockStore.On("Get", mock.Anything, hashMatcher(parentTxHash), []fields.FieldName{fields.Utxos, fields.DeletedChildren}).
		Return(&meta.Data{SpendingDatas: []*spend.SpendingData{spend.NewSpendingData(&counterTxHash, 0)}}, nil)
	mockStore.On("GetConflictingChildren", mock.Anything, counterTxHash).
		Return([]chainhash.Hash{}, nil)

	mockStore.On("GetMeta", mock.Anything, hashMatcher(conflictingTxHash), mock.Anything).
		Return(&meta.Data{}, nil)
	// the live counter is mined in block 7, which is on our current chain
	mockStore.On("GetMeta", mock.Anything, hashMatcher(counterTxHash), mock.Anything).
		Return(&meta.Data{BlockIDs: []uint32{7}}, nil)

	// only the conflicting tx's own cone is walked here; the counter's cone was
	// already walked (and frozen-checked) inside GetCounterConflictingTxHashes, so
	// the per-member re-walk was dropped in issue 1391
	mockStore.On("Get", mock.Anything, hashMatcher(conflictingTxHash), []fields.FieldName{fields.Utxos, fields.ConflictingChildren, fields.DeletedChildren}).
		Return(&meta.Data{}, nil)

	u := &Server{logger: ulogger.NewErrorTestLogger(t), utxoStore: mockStore}

	err := u.checkCounterConflictingOnCurrentChain(ctx, conflictingTxHash, map[uint32]bool{7: true})

	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrTxInvalid))
	mockStore.AssertExpectations(t)
}
