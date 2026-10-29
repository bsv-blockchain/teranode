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

// TestProcessConflicting_GuardFailureLeavesNoIntent proves that a resolution
// refused before it mutates anything does not leave its intent in the WAL.
//
// The intent is recorded before the step-0 checks on purpose, so a kill between
// the record and the first mutation is covered. But a refusal at step 0 leaves
// nothing to recover, and an intent that outlives it is replayed on the next
// start with the guard bypassed. On mainnet on 2026-10-19 that replay ran the
// resolution of block 971704 against winners that were already resolved, and
// demoted their mined descendants.
func TestProcessConflicting_GuardFailureLeavesNoIntent(t *testing.T) {
	ctx := context.Background()

	winner := createTestHash("already-resolved-winner")

	spy := &walSpyStore{MockUtxostore: &MockUtxostore{}}
	spy.On("Get", mock.Anything, &winner, mock.Anything).Return(&meta.Data{Tx: bt.NewTx(), Conflicting: false}, nil)

	_, _, err := ProcessConflicting(ctx, spy, 100, chainhash.Hash{}, []chainhash.Hash{winner}, nil, NoAncestryGuard)

	require.Error(t, err)
	require.Contains(t, err.Error(), "tx is not conflicting")
	require.Len(t, spy.begun, 1, "the intent is recorded before the checks")
	require.Equal(t, []chainhash.Hash{spy.begun[0].IntentID()}, spy.completed, "a refusal that mutated nothing must take its intent back out of the WAL")
}

// TestProcessConflicting_RefusalKeepsAnEarlierRunsIntent pins that the refusal
// above removes only an intent this call created. A failed earlier run of the
// same list can leave a pending winner not conflicting: its rollback clears the
// conflicting flag on all of step 1's cascade, and a winner is in its own
// counter-conflicting set. The retry refuses that winner, and the intent the
// failed run left is the only thing whose seeded replay repairs it.
func TestProcessConflicting_RefusalKeepsAnEarlierRunsIntent(t *testing.T) {
	ctx := context.Background()

	winner := createTestHash("earlier-run-winner")

	spy := &walSpyStore{MockUtxostore: &MockUtxostore{}, intentExists: true}
	spy.On("Get", mock.Anything, &winner, mock.Anything).Return(&meta.Data{Tx: bt.NewTx(), Conflicting: false}, nil)

	_, _, err := ProcessConflicting(ctx, spy, 100, chainhash.Hash{}, []chainhash.Hash{winner}, nil, NoAncestryGuard)

	require.Error(t, err)
	require.Contains(t, err.Error(), "tx is not conflicting")
	require.Len(t, spy.begun, 1)
	require.Empty(t, spy.completed, "a refusal must keep an intent an earlier run left behind")
}

// TestProcessConflicting_PartialStepOneKeepsIntent pins the other side of that
// boundary. Step 1 marks the losers one cascade level per SetConflicting call,
// each its own transaction, so a failure on a later level leaves the earlier
// levels committed while step1Committed is still false, and the deferred
// rollback skips them. The intent is then the only record that the operation
// ever started, and it must stay for replay.
func TestProcessConflicting_PartialStepOneKeepsIntent(t *testing.T) {
	ctx := context.Background()

	winner := createTestHash("partial-step-one-winner")
	loser := createTestHash("partial-step-one-loser")
	child := createTestHash("partial-step-one-child")

	spy := &walSpyStore{MockUtxostore: &MockUtxostore{}}
	spy.On("Get", mock.Anything, &winner, mock.Anything).Return(&meta.Data{Tx: bt.NewTx(), Conflicting: true}, nil)
	spy.On("GetCounterConflicting", mock.Anything, winner).Return([]chainhash.Hash{loser}, nil)
	// The first cascade level commits and names a child; the second level fails.
	spy.On("SetConflicting", mock.Anything, []chainhash.Hash{loser}, true).Return([]*Spend{}, []chainhash.Hash{child}, nil).Once()
	spy.On("SetConflicting", mock.Anything, []chainhash.Hash{child}, true).Return([]*Spend(nil), []chainhash.Hash(nil), errors.NewStorageError("second level failed")).Once()

	_, _, err := ProcessConflicting(ctx, spy, 100, chainhash.Hash{}, []chainhash.Hash{winner}, nil, NoAncestryGuard)

	require.Error(t, err)
	require.Len(t, spy.begun, 1)
	require.Empty(t, spy.completed, "a resolution that committed part of step 1 must keep its intent for replay")
}

// TestProcessConflicting_TransientStepZeroFailureKeepsIntent pins why only a
// refusal of the winner list may take the intent back out. The intent id is
// deterministic and Begin is an upsert, so this call may have adopted the record
// of an earlier run of the same list that was killed after mutating. A transient
// failure at step 0, here the counter-conflicting lookup, says nothing about that
// torn state, and deleting the intent would leave the next restart nothing to
// recover.
func TestProcessConflicting_TransientStepZeroFailureKeepsIntent(t *testing.T) {
	ctx := context.Background()

	winner := createTestHash("transient-step-zero-winner")

	spy := &walSpyStore{MockUtxostore: &MockUtxostore{}}
	spy.On("Get", mock.Anything, &winner, mock.Anything).Return(&meta.Data{Tx: bt.NewTx(), Conflicting: true}, nil)
	spy.On("GetCounterConflicting", mock.Anything, winner).Return([]chainhash.Hash(nil), errors.NewStorageError("store unavailable"))

	_, _, err := ProcessConflicting(ctx, spy, 100, chainhash.Hash{}, []chainhash.Hash{winner}, nil, NoAncestryGuard)

	require.Error(t, err)
	require.Contains(t, err.Error(), "store unavailable")
	require.Len(t, spy.begun, 1)
	require.Empty(t, spy.completed, "a transient step-0 failure must keep the intent, which may guard an earlier run's torn state")
}

// TestProcessConflicting_ResolvedWinnerIsNotWalkedOrRefused pins
// WithResolvedWinners: a winner the store already shows resolved is neither
// refused for being not conflicting nor walked for losers, and it stays a winner,
// so it is spent at step 3 and cleared at step 4 with the rest.
func TestProcessConflicting_ResolvedWinnerIsNotWalkedOrRefused(t *testing.T) {
	ctx := context.Background()

	pending := createTestHash("resolved-option-pending")
	resolved := createTestHash("resolved-option-resolved")

	spy := &walSpyStore{MockUtxostore: &MockUtxostore{}}
	spy.On("Get", mock.Anything, &pending, mock.Anything).Return(&meta.Data{Tx: bt.NewTx(), Conflicting: true}, nil)
	spy.On("Get", mock.Anything, &resolved, mock.Anything).Return(&meta.Data{Tx: bt.NewTx(), Conflicting: false}, nil)
	spy.On("GetCounterConflicting", mock.Anything, pending).Return([]chainhash.Hash{}, nil)
	spy.On("SetConflicting", mock.Anything, mock.Anything, mock.Anything).Return([]*Spend{}, []chainhash.Hash{}, nil)
	spy.On("Unspend", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	spy.On("SpendAndCreate", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, []*Spend{}, nil)
	spy.On("SetLocked", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	winners := []chainhash.Hash{pending, resolved}

	_, _, err := ProcessConflicting(ctx, spy, 100, chainhash.Hash{}, winners, nil, NoAncestryGuard, WithResolvedWinners([]chainhash.Hash{resolved}))

	require.NoError(t, err)
	spy.AssertNotCalled(t, "GetCounterConflicting", mock.Anything, resolved)
	spy.AssertNumberOfCalls(t, "SpendAndCreate", 2)
	spy.AssertCalled(t, "SetConflicting", mock.Anything, winners, false)
	require.Equal(t, []chainhash.Hash{spy.begun[0].IntentID()}, spy.completed)
}
