package utxo

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestProcessConflicting_RollbackKeepsResolvedWinnerSpends pins that a failed
// resolution leaves an already-resolved winner as it found it. The winner's
// step-3 spend is an idempotent success in both stores, because it already holds
// the outpoint, so the rollback used to unspend it and then re-mark the winner
// conflicting. That left a mined winner with its inputs unspent, which the retry
// refuses as not conflicting, and the intent went with the refusal.
func TestProcessConflicting_RollbackKeepsResolvedWinnerSpends(t *testing.T) {
	ctx := context.Background()

	pending := createTestHash("rollback-pending-winner")
	resolved := createTestHash("rollback-resolved-winner")
	loser := createTestHash("rollback-pending-loser")
	contested := createTestHash("rollback-contested-parent")
	held := createTestHash("rollback-held-parent")

	pendingTx := bt.NewTx()
	resolvedTx := bt.NewTx()
	isTx := func(want *bt.Tx) interface{} {
		return mock.MatchedBy(func(tx *bt.Tx) bool { return tx == want })
	}

	contestedSpend := &Spend{TxID: &contested, Vout: 0}
	heldSpend := &Spend{TxID: &held, Vout: 0}

	spy := &walSpyStore{MockUtxostore: &MockUtxostore{}}
	spy.On("Get", mock.Anything, &pending, mock.Anything).Return(&meta.Data{Tx: pendingTx, Conflicting: true}, nil)
	spy.On("Get", mock.Anything, &resolved, mock.Anything).Return(&meta.Data{Tx: resolvedTx, Conflicting: false}, nil)
	spy.On("Get", mock.Anything, &loser, mock.Anything).Return(&meta.Data{Tx: bt.NewTx()}, nil)
	spy.On("GetCounterConflicting", mock.Anything, pending).Return([]chainhash.Hash{loser}, nil)
	// Step 1 demotes the pending winner's loser, whose spend of the contested
	// parent step 2 then undoes.
	spy.On("SetConflicting", mock.Anything, []chainhash.Hash{loser}, true).Return([]*Spend{contestedSpend}, []chainhash.Hash{}, nil)
	spy.On("Unspend", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	spy.On("SpendAndCreate", mock.Anything, isTx(pendingTx), mock.Anything, mock.Anything).
		Return(nil, []*Spend{{TxID: &contested, Vout: 0}}, nil)
	// The resolved winner already holds its parent, so its spend succeeds.
	spy.On("SpendAndCreate", mock.Anything, isTx(resolvedTx), mock.Anything, mock.Anything).
		Return(nil, []*Spend{heldSpend}, nil)
	spy.On("SpendAndCreate", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, []*Spend{}, nil)
	// Step 4 fails, so the rollback runs.
	spy.On("SetConflicting", mock.Anything, []chainhash.Hash{pending, resolved}, false).
		Return([]*Spend(nil), []chainhash.Hash(nil), errors.NewStorageError("step 4 failed"))
	spy.On("SetConflicting", mock.Anything, mock.Anything, mock.Anything).Return([]*Spend{}, []chainhash.Hash{}, nil)
	spy.On("SetLocked", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	_, _, err := ProcessConflicting(ctx, spy, 100, chainhash.Hash{}, []chainhash.Hash{pending, resolved}, nil, NoAncestryGuard,
		WithResolvedWinners([]chainhash.Hash{resolved}))
	require.Error(t, err)

	var rolledBack []*Spend

	for _, call := range spy.Calls {
		if call.Method == "Unspend" && !call.Arguments.Get(2).([]bool)[0] {
			rolledBack = append(rolledBack, call.Arguments.Get(1).([]*Spend)...)
		}

		if call.Method == "SetConflicting" && call.Arguments.Bool(2) {
			require.NotContains(t, call.Arguments.Get(1).([]chainhash.Hash), resolved, "the rollback must not re-mark a winner that was resolved before the call")
		}
	}

	require.Len(t, rolledBack, 1, "only the spend this call made is undone")
	require.Equal(t, contested, *rolledBack[0].TxID)
}

// TestResolvedForwardLosers_PrunedCandidateIsALeaf pins that a pruned entry in a
// parent's ConflictingChildren does not fail loser recovery. The list is
// append-only and conflicting transactions are pruned after the retention
// window, so an entry can name a record the store no longer holds. A store may
// report that as ErrTxNotFound rather than a nil record, and treating it as an
// error failed every re-application of the block. A pruned parent is the same.
func TestResolvedForwardLosers_PrunedCandidateIsALeaf(t *testing.T) {
	ctx := context.Background()

	parent := createTestHash("pruned-candidate-parent")
	gone := createTestHash("pruned-candidate-gone")
	loser := createTestHash("pruned-candidate-loser")
	gonePrimary := createTestHash("pruned-candidate-gone-parent")

	winnerTx := createTestTransactionWithInputs(parent, 0)
	winner := *winnerTx.TxIDChainHash()

	otherWinnerTx := createTestTransactionWithInputs(gonePrimary, 0)
	otherWinner := *otherWinnerTx.TxIDChainHash()

	loserInpoints := inpointsFromTx(t, createTestTransactionWithInputs(parent, 0))

	store := &MockUtxostore{}
	store.On("Get", mock.Anything, &winner, mock.Anything).Return(&meta.Data{TxInpoints: inpointsFromTx(t, winnerTx)}, nil)
	store.On("Get", mock.Anything, &otherWinner, mock.Anything).Return(&meta.Data{TxInpoints: inpointsFromTx(t, otherWinnerTx)}, nil)
	store.On("Get", mock.Anything, &parent, mock.Anything).Return(&meta.Data{ConflictingChildren: []chainhash.Hash{gone, loser}}, nil)
	store.On("Get", mock.Anything, &gonePrimary, mock.Anything).Return(nil, errors.NewTxNotFoundError("pruned parent"))
	store.On("Get", mock.Anything, &gone, mock.Anything).Return(nil, errors.NewTxNotFoundError("pruned"))
	store.On("Get", mock.Anything, &loser, mock.Anything).Return(&meta.Data{TxInpoints: loserInpoints, Conflicting: true}, nil)

	losers, err := ResolvedForwardLosers(ctx, store, []chainhash.Hash{winner, otherWinner},
		map[chainhash.Hash]struct{}{winner: {}, otherWinner: {}})

	require.NoError(t, err)
	require.Equal(t, []chainhash.Hash{loser}, losers)
}

// TestForwardResolutionApplied_PrunedParentIsNotApplied pins the documented
// answer for a parent the store no longer holds: not applied, with no error,
// whichever way the store reports the missing record.
func TestForwardResolutionApplied_PrunedParentIsNotApplied(t *testing.T) {
	ctx := context.Background()

	parent := createTestHash("applied-pruned-parent")
	winnerTx := createTestTransactionWithInputs(parent, 0)
	winner := *winnerTx.TxIDChainHash()

	store := &MockUtxostore{}
	store.On("Get", mock.Anything, &winner, mock.Anything).Return(&meta.Data{TxInpoints: inpointsFromTx(t, winnerTx)}, nil)
	store.On("Get", mock.Anything, &parent, mock.Anything).Return(nil, errors.NewTxNotFoundError("pruned parent"))

	applied, err := ForwardResolutionApplied(ctx, store, winner)

	require.NoError(t, err)
	require.False(t, applied)
}
