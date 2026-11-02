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
	"github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/stretchr/testify/require"
)

// errStore answers the store calls that the recovery helpers make with programmable results. A
// method without a function panics, which marks a call the test did not expect.
type errStore struct {
	Store
	get            func(*chainhash.Hash, ...fields.FieldName) (*meta.Data, error)
	getSpend       func(*Spend) (*SpendResponse, error)
	batchDecorate  func([]*UnresolvedMetaData) error
	load           func(chainhash.Hash) (*ConflictRecovery, error)
	begin          func(ConflictIntent) error
	complete       func(chainhash.Hash) error
	setConflicting func([]chainhash.Hash, bool) ([]*Spend, []chainhash.Hash, error)
	counter        func(chainhash.Hash) ([]chainhash.Hash, error)
	release        func(chainhash.Hash, []chainhash.Hash) error
}

func (s errStore) Get(_ context.Context, h *chainhash.Hash, f ...fields.FieldName) (*meta.Data, error) {
	return s.get(h, f...)
}

func (s errStore) GetSpend(_ context.Context, sp *Spend) (*SpendResponse, error) {
	return s.getSpend(sp)
}

func (s errStore) BatchDecorate(_ context.Context, u []*UnresolvedMetaData, _ ...fields.FieldName) error {
	return s.batchDecorate(u)
}

func (s errStore) LoadConflictRecovery(_ context.Context, id chainhash.Hash) (*ConflictRecovery, error) {
	return s.load(id)
}

func (s errStore) BeginConflictIntent(_ context.Context, i ConflictIntent) error {
	return s.begin(i)
}

func (s errStore) CompleteConflictIntent(_ context.Context, id chainhash.Hash) error {
	return s.complete(id)
}

func (s errStore) SetConflicting(_ context.Context, h []chainhash.Hash, v bool) ([]*Spend, []chainhash.Hash, error) {
	return s.setConflicting(h, v)
}

func (s errStore) GetCounterConflicting(_ context.Context, h chainhash.Hash) ([]chainhash.Hash, error) {
	return s.counter(h)
}

func (s errStore) ReleaseConflictLocks(_ context.Context, id chainhash.Hash, p []chainhash.Hash) error {
	return s.release(id, p)
}

var errInjected = errors.NewStorageError("injected store failure")

func oneInputTx(t *testing.T, parent chainhash.Hash) *bt.Tx {
	t.Helper()

	input := &bt.Input{PreviousTxSatoshis: 1000, PreviousTxScript: bscript.NewFromBytes([]byte{0x51})}
	require.NoError(t, input.PreviousTxIDAdd(&parent))

	tx := bt.NewTx()
	tx.Inputs = []*bt.Input{input}

	return tx
}

func TestConflictRecovery_EncodeDecodeValidate(t *testing.T) {
	_, err := EncodeConflictRecovery(&ConflictRecovery{Version: 2, Phase: ConflictRecoveryPlanning})
	require.ErrorContains(t, err, "unsupported conflict recovery checklist version")

	_, err = DecodeConflictRecovery([]byte("{not json"))
	require.ErrorContains(t, err, "invalid conflict recovery checklist")

	_, err = DecodeConflictRecovery([]byte(`{"Version":1,"Phase":"bogus"}`))
	require.ErrorContains(t, err, "unknown conflict recovery phase")

	_, err = DecodeConflictRecovery([]byte(`{"Version":1,"Phase":"planning","OriginalSpends":[{"txId":null}]}`))
	require.ErrorContains(t, err, "invalid spend in conflict recovery checklist")

	require.ErrorContains(t, (*ConflictRecovery)(nil).Validate(), "unsupported")
}

func TestPrepareConflictRecovery_ReadFailures(t *testing.T) {
	ctx := t.Context()
	parent, member, winner := chainhash.Hash{1}, chainhash.Hash{2}, chainhash.Hash{3}
	memberTx := oneInputTx(t, parent)
	winningSpends := []*Spend{{TxID: &parent, Vout: 0, UTXOHash: &chainhash.Hash{9}, SpendingData: spend.NewSpendingData(&winner, 0)}}
	okMeta := func(h *chainhash.Hash, f ...fields.FieldName) (*meta.Data, error) {
		if h.IsEqual(&parent) {
			return &meta.Data{Locked: false}, nil
		}

		return &meta.Data{Tx: memberTx, Conflicting: true}, nil
	}
	okSpend := func(sp *Spend) (*SpendResponse, error) {
		return &SpendResponse{Status: int(Status_SPENT), SpendingData: spend.NewSpendingData(&member, 0)}, nil
	}

	cases := []struct {
		name  string
		store errStore
		want  string
	}{
		{"tx read fails", errStore{get: func(*chainhash.Hash, ...fields.FieldName) (*meta.Data, error) { return nil, errInjected }}, "injected"},
		{"tx missing", errStore{get: func(*chainhash.Hash, ...fields.FieldName) (*meta.Data, error) { return nil, nil }}, "cannot snapshot conflict transaction"},
		{"tx body missing", errStore{get: func(*chainhash.Hash, ...fields.FieldName) (*meta.Data, error) { return &meta.Data{}, nil }}, "cannot snapshot conflict transaction"},
		{"spend read fails", errStore{get: okMeta, getSpend: func(*Spend) (*SpendResponse, error) { return nil, errInjected }}, "injected"},
		{"outpoint missing", errStore{get: okMeta, getSpend: func(*Spend) (*SpendResponse, error) { return &SpendResponse{Status: int(Status_NOT_FOUND)}, nil }}, "cannot snapshot conflict outpoint"},
		{"parent read fails", errStore{get: func(h *chainhash.Hash, f ...fields.FieldName) (*meta.Data, error) {
			if h.IsEqual(&parent) {
				return nil, errInjected
			}
			return okMeta(h, f...)
		}, getSpend: okSpend}, "injected"},
		{"parent missing", errStore{get: func(h *chainhash.Hash, f ...fields.FieldName) (*meta.Data, error) {
			if h.IsEqual(&parent) {
				return nil, nil
			}
			return okMeta(h, f...)
		}, getSpend: okSpend}, "conflict parent"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := prepareConflictRecovery(ctx, tc.store, []chainhash.Hash{member}, winningSpends)
			require.ErrorContains(t, err, tc.want)
		})
	}

	// The happy path of the same inputs keeps the member's spend and takes the free parent lock.
	r, err := prepareConflictRecovery(ctx, errStore{get: okMeta, getSpend: okSpend}, []chainhash.Hash{member}, winningSpends)
	require.NoError(t, err)
	require.Len(t, r.OriginalSpends, 1)
	require.Equal(t, []chainhash.Hash{parent}, r.Parents)
}

func TestVerifyConflictSpends_Failures(t *testing.T) {
	ctx := t.Context()
	parent, owner := chainhash.Hash{1}, chainhash.Hash{2}
	spends := []*Spend{{TxID: &parent, Vout: 0, UTXOHash: &chainhash.Hash{9}, SpendingData: spend.NewSpendingData(&owner, 0)}}

	err := verifyConflictSpends(ctx, errStore{getSpend: func(*Spend) (*SpendResponse, error) { return nil, errInjected }}, spends)
	require.ErrorIs(t, err, errInjected)

	err = verifyConflictSpends(ctx, errStore{getSpend: func(*Spend) (*SpendResponse, error) { return &SpendResponse{Status: int(Status_OK)}, nil }}, spends)
	require.ErrorContains(t, err, "conflict ownership changed")
}

func TestCurrentChainGuard_NoCheck(t *testing.T) {
	ctx := t.Context()
	var guard CurrentChainGuard
	s := errStore{}

	require.Equal(t, Store(s), guard.protect(s), "a guard without a check wraps nothing")
	require.NoError(t, guard.checkTransactions(ctx, s, []chainhash.Hash{{1}}))
	require.Nil(t, guard.protectForward(ctx, s, nil, chainhash.Hash{}))

	_, err := guard.BlockID(ctx, chainhash.Hash{})
	require.ErrorContains(t, err, "no block ID resolver")

	confirmed, unconfirmed, err := splitConfirmedConflictWinners(ctx, s, []chainhash.Hash{{1}}, guard)
	require.NoError(t, err)
	require.False(t, confirmed)
	require.Nil(t, unconfirmed)
}

func TestChainProtectedStore_CheckUnconfirmedFailures(t *testing.T) {
	ctx := t.Context()
	guard := NewCurrentChainGuard(func(context.Context, []uint32) (bool, error) { return false, nil })
	h := chainhash.Hash{1}

	err := guard.checkTransactions(ctx, errStore{batchDecorate: func([]*UnresolvedMetaData) error { return errInjected }}, []chainhash.Hash{h})
	require.ErrorIs(t, err, errInjected)

	err = guard.checkTransactions(ctx, errStore{batchDecorate: func(u []*UnresolvedMetaData) error {
		u[0].Err = errInjected
		return nil
	}}, []chainhash.Hash{h})
	require.ErrorContains(t, err, "cannot authorize conflict undo for")

	err = guard.checkTransactions(ctx, errStore{batchDecorate: func([]*UnresolvedMetaData) error { return nil }}, []chainhash.Hash{h})
	require.ErrorContains(t, err, "is missing")

	require.NoError(t, guard.checkTransactions(ctx, errStore{}, nil), "nothing to check")

	failing := NewCurrentChainGuard(func(context.Context, []uint32) (bool, error) { return false, errInjected })
	err = failing.checkTransactions(ctx, errStore{batchDecorate: func(u []*UnresolvedMetaData) error {
		u[0].Data = &meta.Data{BlockIDs: []uint32{5}}
		return nil
	}}, []chainhash.Hash{h})
	require.ErrorContains(t, err, "current-chain lookup failed")
}

func TestCompleteConflictIntent_CountsFailure(t *testing.T) {
	err := CompleteConflictIntent(t.Context(), errStore{complete: func(chainhash.Hash) error { return errInjected }}, chainhash.Hash{1})
	require.ErrorContains(t, err, "failed to complete conflict WAL intent")
}

func TestRecoveryHelpers_StoreFailures(t *testing.T) {
	ctx := t.Context()
	h := chainhash.Hash{1}
	winners := []chainhash.Hash{h}
	failGet := errStore{get: func(*chainhash.Hash, ...fields.FieldName) (*meta.Data, error) { return nil, errInjected }}
	nilGet := errStore{get: func(*chainhash.Hash, ...fields.FieldName) (*meta.Data, error) { return nil, nil }}
	guard := NewCurrentChainGuard(func(context.Context, []uint32) (bool, error) { return true, nil })

	_, err := conflictWinnersReversed(ctx, failGet, winners)
	require.ErrorContains(t, err, "error getting tx")
	reversed, err := conflictWinnersReversed(ctx, nilGet, winners)
	require.NoError(t, err)
	require.False(t, reversed)
	reversed, err = conflictWinnersReversed(ctx, nilGet, nil)
	require.NoError(t, err)
	require.False(t, reversed)

	_, _, err = splitConfirmedConflictWinners(ctx, failGet, winners, guard)
	require.ErrorContains(t, err, "error getting tx")
	_, _, err = splitConfirmedConflictWinners(ctx, nilGet, winners, guard)
	require.ErrorContains(t, err, "conflict winner is missing")

	_, err = conflictWinnersSettled(ctx, failGet, winners, guard)
	require.ErrorContains(t, err, "error getting tx")
	_, err = conflictWinnersSettled(ctx, nilGet, winners, guard)
	require.ErrorContains(t, err, "changed before acknowledgement")

	_, err = UnmarkConflictingRecursively(ctx, errStore{setConflicting: func([]chainhash.Hash, bool) ([]*Spend, []chainhash.Hash, error) {
		return nil, nil, errInjected
	}}, winners)
	require.ErrorIs(t, err, errInjected)

	owned, err := winnerInputsOwned(ctx, nilGet, h, bt.NewTx(), absentParentIsError)
	require.NoError(t, err)
	require.False(t, owned, "a transaction without inputs owns nothing")

	intent := ConflictIntent{Kind: ConflictIntentForward, BlockHash: chainhash.Hash{7}}
	failLoad := errStore{load: func(chainhash.Hash) (*ConflictRecovery, error) { return nil, errInjected }}

	_, err = HealStaleForwardIntent(ctx, failLoad, intent, guard, nil)
	require.ErrorIs(t, err, errInjected)
	require.ErrorIs(t, RetireConflictIntentsForBlocks(ctx, failLoad, []chainhash.Hash{{7}}), errInjected)
	_, err = RollbackConflictIntent(ctx, failLoad, intent)
	require.ErrorIs(t, err, errInjected)

	_, err = rollbackConflictIntent(ctx, errStore{}, intent, nil)
	require.ErrorContains(t, err, "legacy conflict intent")

	failRelease := errStore{release: func(chainhash.Hash, []chainhash.Hash) error { return errInjected }}
	require.ErrorIs(t, retireConflictIntent(ctx, failRelease, intent.IntentID(), &ConflictRecovery{Version: 1, Phase: ConflictRecoveryFinished}), errInjected)
	_, err = rollbackConflictIntent(ctx, failRelease, intent, &ConflictRecovery{Version: 1, Phase: ConflictRecoveryPrepared})
	require.ErrorIs(t, err, errInjected)
}

func TestProcessConflicting_EntryFailures(t *testing.T) {
	ctx := t.Context()
	h := chainhash.Hash{1}
	winners := []chainhash.Hash{h}
	planning := &ConflictRecovery{Version: 1, Phase: ConflictRecoveryPlanning}
	loadPlanning := func(chainhash.Hash) (*ConflictRecovery, error) { return planning, nil }
	beginOK := func(ConflictIntent) error { return nil }

	_, _, err := ProcessConflicting(ctx, errStore{load: func(chainhash.Hash) (*ConflictRecovery, error) { return nil, errInjected }}, 1, chainhash.Hash{}, winners, nil, NoAncestryGuard)
	require.ErrorIs(t, err, errInjected)

	_, _, err = ProcessConflicting(ctx, errStore{load: func(chainhash.Hash) (*ConflictRecovery, error) { return nil, nil }, begin: func(ConflictIntent) error { return errInjected }}, 1, chainhash.Hash{}, winners, nil, NoAncestryGuard)
	require.ErrorContains(t, err, "failed to record conflict WAL intent")

	_, _, err = ProcessConflicting(ctx, errStore{load: loadPlanning, begin: beginOK, get: func(*chainhash.Hash, ...fields.FieldName) (*meta.Data, error) { return nil, nil }}, 1, chainhash.Hash{}, winners, nil, NoAncestryGuard)
	require.ErrorContains(t, err, "winning tx not found")

	_, _, err = ProcessConflicting(ctx, errStore{load: loadPlanning, begin: beginOK, get: func(*chainhash.Hash, ...fields.FieldName) (*meta.Data, error) {
		return &meta.Data{Conflicting: true}, nil
	}}, 1, chainhash.Hash{}, winners, nil, NoAncestryGuard)
	require.ErrorContains(t, err, "winning transaction body is missing")

	_, _, err = ProcessConflicting(ctx, errStore{load: loadPlanning, begin: beginOK, get: func(*chainhash.Hash, ...fields.FieldName) (*meta.Data, error) { return nil, errInjected }}, 1, chainhash.Hash{}, winners, nil, NoAncestryGuard)
	require.ErrorContains(t, err, "error getting tx")

	tx := oneInputTx(t, chainhash.Hash{2})
	_, _, err = ProcessConflicting(ctx, errStore{load: loadPlanning, begin: beginOK, get: func(*chainhash.Hash, ...fields.FieldName) (*meta.Data, error) {
		return &meta.Data{Tx: tx, Conflicting: true}, nil
	},
		counter: func(chainhash.Hash) ([]chainhash.Hash, error) { return nil, errInjected }}, 1, chainhash.Hash{}, winners, nil, NoAncestryGuard)
	require.ErrorContains(t, err, "error getting counter conflicting txs")

	// A finished checklist refuses an unknown winner that is still Conflicting, before it reads
	// the recorded winners.
	finished := &ConflictRecovery{Version: 1, Phase: ConflictRecoveryFinished, Winners: []chainhash.Hash{{5}}}
	loadFinished := func(chainhash.Hash) (*ConflictRecovery, error) { return finished, nil }
	confirmed := WithRecoveryChainGuard(NewCurrentChainGuard(func(context.Context, []uint32) (bool, error) { return true, nil }))
	calls := 0
	_, _, err = ProcessConflicting(ctx, errStore{load: loadFinished, begin: beginOK,
		get: func(hh *chainhash.Hash, _ ...fields.FieldName) (*meta.Data, error) {
			calls++
			if hh.IsEqual(&h) {
				return &meta.Data{Tx: tx, Conflicting: true}, nil
			}
			return nil, errInjected
		}}, 1, chainhash.Hash{}, winners, nil, NoAncestryGuard, confirmed)
	require.ErrorContains(t, err, "acknowledge the existing conflict intent before adding winners")
	require.Equal(t, 1, calls)

	// An unknown winner that is not Conflicting and does not own its inputs is an error.
	parent := chainhash.Hash{2}
	_, _, err = ProcessConflicting(ctx, errStore{load: loadFinished, begin: beginOK,
		get: func(hh *chainhash.Hash, _ ...fields.FieldName) (*meta.Data, error) {
			if hh.IsEqual(&parent) {
				return &meta.Data{SpendingDatas: []*spend.SpendingData{spend.NewSpendingData(&chainhash.Hash{5}, 0)}}, nil
			}
			return &meta.Data{Tx: tx, Conflicting: false}, nil
		}}, 1, chainhash.Hash{}, winners, nil, NoAncestryGuard)
	require.ErrorContains(t, err, "tx is not conflicting and does not own its inputs")
}

// TestProcessConflicting_FinishedRecordAcceptsAppliedExtraWinner pins the reset and catch-up
// paths: reorgBlocks filters the winners a moveBack already promoted, so the finished record holds a
// subset. A later retry passes the full list. An extra winner that is already applied needs no
// rollback, so the retry returns the saved result instead of refusing on every attempt.
func TestProcessConflicting_FinishedRecordAcceptsAppliedExtraWinner(t *testing.T) {
	ctx := t.Context()
	y, z, parentY, parentZ := chainhash.Hash{1}, chainhash.Hash{5}, chainhash.Hash{2}, chainhash.Hash{3}
	txY, txZ := oneInputTx(t, parentY), oneInputTx(t, parentZ)
	loser := chainhash.Hash{9}
	finished := &ConflictRecovery{Version: 1, Phase: ConflictRecoveryFinished, Winners: []chainhash.Hash{z}, Losers: []chainhash.Hash{loser}}
	completed := 0
	store := errStore{
		load:  func(chainhash.Hash) (*ConflictRecovery, error) { return finished, nil },
		begin: func(ConflictIntent) error { return nil },
		get: func(hh *chainhash.Hash, _ ...fields.FieldName) (*meta.Data, error) {
			switch *hh {
			case y:
				return &meta.Data{Tx: txY, Conflicting: false}, nil
			case z:
				return &meta.Data{Tx: txZ, Conflicting: false}, nil
			case parentY:
				return &meta.Data{SpendingDatas: []*spend.SpendingData{spend.NewSpendingData(&y, 0)}}, nil
			case parentZ:
				return &meta.Data{SpendingDatas: []*spend.SpendingData{spend.NewSpendingData(&z, 0)}}, nil
			}
			return nil, errInjected
		},
		complete: func(chainhash.Hash) error { completed++; return nil },
	}

	losers, marked, err := ProcessConflicting(ctx, store, 1, chainhash.Hash{}, []chainhash.Hash{y, z}, nil, NoAncestryGuard)
	require.NoError(t, err)
	require.Equal(t, []chainhash.Hash{loser}, marked)
	require.True(t, losers.Exists(loser))
	require.Equal(t, 1, completed)
}
