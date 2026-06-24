package utxo

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// walSpyStore wraps MockUtxostore and records conflict-WAL lifecycle calls so
// the ProcessConflicting / ReverseProcessConflicting wrappers can be asserted.
// The embedded MockUtxostore satisfies the rest of the Store interface; only
// the three WAL methods are overridden here.
type walSpyStore struct {
	*MockUtxostore

	beginErr  error
	begun     []ConflictIntent
	completed []chainhash.Hash
}

func (w *walSpyStore) BeginConflictIntent(_ context.Context, intent ConflictIntent) error {
	if w.beginErr != nil {
		return w.beginErr
	}

	w.begun = append(w.begun, intent)

	return nil
}

func (w *walSpyStore) CompleteConflictIntent(_ context.Context, intentID chainhash.Hash) error {
	w.completed = append(w.completed, intentID)
	return nil
}

func (w *walSpyStore) PendingConflictIntents(_ context.Context) ([]ConflictIntent, error) {
	return nil, nil
}

// TestProcessConflicting_WALBeginFailureAborts proves that if the WAL intent
// cannot be recorded durably, ProcessConflicting aborts BEFORE mutating any
// state — no Get, no SetConflicting, no Spend. Without a durable intent there is
// no crash-recovery guarantee, so proceeding would be unsafe.
func TestProcessConflicting_WALBeginFailureAborts(t *testing.T) {
	ctx := context.Background()

	// Bare mock with NO expectations: any state-mutating call would panic,
	// proving the function aborted before touching the store.
	spy := &walSpyStore{MockUtxostore: &MockUtxostore{}, beginErr: errors.NewStorageError("wal unavailable")}

	_, _, err := ProcessConflicting(ctx, spy, 100, chainhash.Hash{}, []chainhash.Hash{createTestHash("winner")}, nil)

	require.Error(t, err)
	require.ErrorIs(t, err, errors.ErrStorageError)
	require.Empty(t, spy.begun, "no intent should be recorded when Begin fails")
	require.Empty(t, spy.completed, "operation must not complete an intent it never began")
}

// TestReverseProcessConflicting_WALBeginFailureAborts is the reverse-path twin.
func TestReverseProcessConflicting_WALBeginFailureAborts(t *testing.T) {
	ctx := context.Background()

	spy := &walSpyStore{MockUtxostore: &MockUtxostore{}, beginErr: errors.NewStorageError("wal unavailable")}

	_, _, err := ReverseProcessConflicting(ctx, spy, 100, chainhash.Hash{}, []chainhash.Hash{createTestHash("demoted")})

	require.Error(t, err)
	require.ErrorIs(t, err, errors.ErrStorageError)
	require.Empty(t, spy.begun)
	require.Empty(t, spy.completed)
}

// TestReverseProcessConflicting_WALLifecycleOnSuccess proves a successful reverse
// records exactly one intent and then completes it (so nothing is left for
// replay). Mirrors the RestoresOriginalSpender scenario but through the spy.
func TestReverseProcessConflicting_WALLifecycleOnSuccess(t *testing.T) {
	ctx := context.Background()
	mockStore := &MockUtxostore{}
	spy := &walSpyStore{MockUtxostore: mockStore}

	parentHash := createTestHash("wal-reverse-parent")
	demotedHash := createTestHash("wal-reverse-demoted")
	counterHash := createTestHash("wal-reverse-counter")

	const vout = uint32(1)

	demotedTx := createSpendableTestTransaction(parentHash, vout)
	counterTx := createSpendableTestTransaction(parentHash, vout)

	mockStore.On("Get", mock.Anything, &demotedHash, mock.Anything).
		Return(&meta.Data{Tx: demotedTx, Conflicting: false}, nil).Once()
	mockStore.On("Get", mock.Anything, &parentHash, mock.Anything).
		Return(&meta.Data{ConflictingChildren: []chainhash.Hash{demotedHash, counterHash}}, nil).Once()
	mockStore.On("Get", mock.Anything, &counterHash, mock.Anything).
		Return(&meta.Data{Tx: counterTx, Conflicting: true}, nil).Once()
	mockStore.On("SetConflicting", mock.Anything, []chainhash.Hash{demotedHash}, true).
		Return([]*Spend{{TxID: &demotedHash, Vout: 0}}, []chainhash.Hash{}, nil).Once()
	mockStore.On("Unspend", mock.Anything, mock.AnythingOfType("[]*utxo.Spend"), mock.Anything).
		Return(nil).Once()
	mockStore.On("Get", mock.Anything, &counterHash, mock.Anything).
		Return(&meta.Data{Tx: counterTx}, nil).Once()
	mockStore.On("Spend", mock.Anything, counterTx, mock.Anything, mock.Anything).
		Return([]*Spend{}, nil).Once()
	mockStore.On("SetConflicting", mock.Anything, []chainhash.Hash{counterHash}, false).
		Return([]*Spend{}, []chainhash.Hash{}, nil).Once()

	_, _, err := ReverseProcessConflicting(ctx, spy, 100, chainhash.Hash{}, []chainhash.Hash{demotedHash})
	require.NoError(t, err)

	require.Len(t, spy.begun, 1, "exactly one intent recorded")
	require.Equal(t, ConflictIntentReverse, spy.begun[0].Kind)
	require.Len(t, spy.completed, 1, "successful op must complete its intent")
	require.Equal(t, spy.begun[0].IntentID(), spy.completed[0], "completed id must match the begun intent")
	mockStore.AssertExpectations(t)
}
