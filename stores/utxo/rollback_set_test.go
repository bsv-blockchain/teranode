package utxo

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

// TestRollbackSet pins which of a failed spend's successful inputs are reversed.
func TestRollbackSet(t *testing.T) {
	fresh := []*Spend{{Vout: 1}}
	idem := []*Spend{{Vout: 2}}

	require.Equal(t, append(append([]*Spend{}, fresh...), idem...), RollbackSet(append([]*Spend{}, fresh...), idem, false),
		"an idempotent match is reversed when no input can be a marker hit: it may be an orphan of an earlier failed attempt")
	require.Equal(t, fresh, RollbackSet(fresh, idem, true),
		"an idempotent match is held back when an input may be a marker hit: it is then the confirmed spend")
	require.Equal(t, idem, RollbackSet(nil, idem, false),
		"a call whose only successful inputs were idempotent matches is still healed")
	require.Empty(t, RollbackSet(nil, idem, true))
}

// TestReplayRejectionsFirst: errors.JoinCapped keeps only the first few links,
// so the links that identify a pruned replay must lead.
func TestReplayRejectionsFirst(t *testing.T) {
	spent := errors.NewStorageError("device overload")
	missing := errors.NewTxNotFoundError("parent missing")
	pruned := errors.NewUtxoSpendingTxPrunedError("pruned")

	errs := make([]error, 0, 23)
	for i := 0; i < 20; i++ {
		errs = append(errs, spent)
	}

	errs = append(errs, missing, nil, pruned)

	ordered := ReplayRejectionsFirst(errs)
	require.Len(t, ordered, 22, "nil entries are dropped, nothing else is")
	require.ErrorIs(t, ordered[0], errors.ErrUtxoSpendingTxPruned)
	require.ErrorIs(t, ordered[1], errors.ErrTxNotFound)
	require.ErrorIs(t, errors.JoinCapped(10, ordered...), errors.ErrUtxoSpendingTxPruned,
		"the marker rejection survives the cap on a wide transaction")
	require.NotErrorIs(t, errors.JoinCapped(10, errs...), errors.ErrUtxoSpendingTxPruned,
		"control: in input order the cap drops it")
}

// TestRollbackSetLeavesCallerSliceUntouched pins that RollbackSet never writes
// into the caller's backing array. With spare capacity, append(written, ...)
// would put the idempotent matches into memory the caller still owns.
func TestRollbackSetLeavesCallerSliceUntouched(t *testing.T) {
	fresh := &Spend{Vout: 1}
	idem := &Spend{Vout: 2}

	backing := make([]*Spend, 1, 4)
	backing[0] = fresh

	rollback := RollbackSet(backing, []*Spend{idem}, false)

	require.Equal(t, []*Spend{fresh, idem}, rollback)
	require.Nil(t, backing[:2][1], "the caller's spare capacity must not receive the idempotent match")
}

// TestIsReplayAnswerIsTheMarkerOnly: a missing parent is what an ordinary retry
// sees too, so only the marker rejection marks an idempotent match historical.
func TestIsReplayAnswerIsTheMarkerOnly(t *testing.T) {
	require.True(t, IsReplayAnswer(errors.NewUtxoSpendingTxPrunedError("pruned")))
	require.False(t, IsReplayAnswer(errors.NewTxNotFoundError("parent missing")))
	require.False(t, IsReplayAnswer(nil))
	require.False(t, AnyPrunedReplay([]*Spend{{Err: errors.NewTxNotFoundError("parent missing")}}))
}

// TestIsPrunedReplayRejectionIsTheMarkerOnly: the SQL bulk spend gives a duplicate input its
// representative's refusal only for the marker rejection. A missing parent, a spent output or any
// other error is not one, and the answer is read through a wrapping error as well.
func TestIsPrunedReplayRejectionIsTheMarkerOnly(t *testing.T) {
	pruned := errors.NewUtxoSpendingTxPrunedError("pruned")

	require.True(t, IsPrunedReplayRejection(pruned))
	require.True(t, IsPrunedReplayRejection(errors.NewProcessingError("wrapped", pruned)))
	require.False(t, IsPrunedReplayRejection(errors.NewTxNotFoundError("parent missing")), "a missing parent is not a replay answer")
	require.False(t, IsPrunedReplayRejection(errors.NewUtxoSpentError(chainhash.Hash{}, 0, chainhash.Hash{}, nil)))
	require.False(t, IsPrunedReplayRejection(nil))
}

// TestAnyPrunedReplayAndNeedsSpendRollbackSkipNilSpends: a spend list can hold nil entries, which
// neither helper may dereference, and an entry with no error says nothing.
func TestAnyPrunedReplayAndNeedsSpendRollbackSkipNilSpends(t *testing.T) {
	pruned := &Spend{Err: errors.NewUtxoSpendingTxPrunedError("pruned")}
	spent := &Spend{Err: errors.NewUtxoSpentError(chainhash.Hash{}, 0, chainhash.Hash{}, nil)}
	missing := &Spend{Err: errors.NewTxNotFoundError("parent missing")}
	clean := &Spend{}

	require.False(t, AnyPrunedReplay([]*Spend{nil}))
	require.False(t, AnyPrunedReplay([]*Spend{nil, clean}))
	require.True(t, AnyPrunedReplay([]*Spend{nil, clean, pruned}))

	require.False(t, NeedsSpendRollback([]*Spend{nil}, false))
	require.False(t, NeedsSpendRollback([]*Spend{nil, clean}, false), "an entry with no error needs no rollback")
	require.True(t, NeedsSpendRollback([]*Spend{nil, clean, spent}, false))
	require.True(t, NeedsSpendRollback([]*Spend{nil, pruned}, false), "the marker rejection reverses what the call wrote")
	require.False(t, NeedsSpendRollback([]*Spend{nil, missing}, false), "a missing parent is a failure only for a RequireParentOutputs caller")
	require.True(t, NeedsSpendRollback([]*Spend{nil, missing}, true))
}

func TestIsHistoricalSpend(t *testing.T) {
	for _, c := range []struct {
		name           string
		recordedHeight uint32
		recorded       bool
		incoming       uint32
		retention      uint32
		historical     bool
	}{
		{"a pruned child's spend 801 blocks back", 1200, true, 2001, 288, true},
		{"an honest retry 2 blocks later", 2001, true, 2003, 288, false},
		{"one block under half the retention", 1000, true, 1143, 288, false},
		{"exactly half the retention", 1000, true, 1144, 288, true},
		{"a spend written before heights were stored", 0, false, 5, 288, true},
		{"a re-applied block below the recorded height", 2005, true, 2001, 288, false},
		{"retention 0 prunes nothing, so nothing is history", 10, true, 5000, 0, false},
		{"retention 0 with no recorded height", 0, false, 5000, 0, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.historical, IsHistoricalSpend(c.recordedHeight, c.recorded, c.incoming, c.retention))
		})
	}

	require.Equal(t, uint32(144), HistoricalSpendAge(288))
}
