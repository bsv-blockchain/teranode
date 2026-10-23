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
