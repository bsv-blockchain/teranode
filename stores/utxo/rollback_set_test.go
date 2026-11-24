package utxo

import (
	"context"
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

// TestIsReplayAnswerIsThePrunedReplayOnly: a missing parent is what an ordinary
// retry sees too, so only the pruned-replay refusal holds idempotent matches back.
func TestIsReplayAnswerIsThePrunedReplayOnly(t *testing.T) {
	require.True(t, IsReplayAnswer(errors.NewUtxoSpendingTxPrunedError("pruned")))
	require.False(t, IsReplayAnswer(errors.NewTxNotFoundError("parent missing")))
	require.False(t, IsReplayAnswer(nil))
	require.False(t, AnyPrunedReplay([]*Spend{{Err: errors.NewTxNotFoundError("parent missing")}}))
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
		{"a spend written before heights were stored", 0, false, 5000, 288, false},
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

// TestKeepBelowHeight pins the height below which a create-first reversal leaves a spend in place.
// Retention 288 makes HistoricalSpendAge 144, so a spend is history from 144 blocks before the
// current height, and the bound is the lower of the record's height and the first height that is
// not history.
func TestKeepBelowHeight(t *testing.T) {
	const retention = 288

	for _, tc := range []struct {
		name         string
		recordHeight uint32
		current      uint32
		retention    uint32
		want         uint32
	}{
		{name: "record older than the age rule: the record height bounds", recordHeight: 500, current: 1000, retention: retention, want: 500},
		{name: "record newer than the age rule: the age rule bounds", recordHeight: 900, current: 1000, retention: retention, want: 857},
		{name: "record exactly at the first height that is not history", recordHeight: 857, current: 1000, retention: retention, want: 857},
		{name: "record one below it", recordHeight: 856, current: 1000, retention: retention, want: 856},
		{name: "record one above it", recordHeight: 858, current: 1000, retention: retention, want: 857},
		{name: "record height unknown: the age rule alone", recordHeight: 0, current: 1000, retention: retention, want: 857},
		{name: "chain as old as the age: no height can be history", recordHeight: 900, current: 144, retention: retention, want: 1},
		{name: "chain younger than the age: nothing is kept", recordHeight: 900, current: 143, retention: retention, want: 0},
		{name: "retention 0: nothing is ever history", recordHeight: 900, current: 100000, retention: 0, want: 0},
		{name: "retention 1: the age clamps to 1 block", recordHeight: 5000, current: 1000, retention: 1, want: 1000},
		{name: "height at the top of the range does not wrap", recordHeight: 0, current: 4294967295, retention: retention, want: 4294967295 - 144 + 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, KeepBelowHeight(tc.recordHeight, tc.current, tc.retention))
		})
	}
}

// TestKeepBelowHeightKeepsExactlyTheHistoricalSpendsThatPredateTheRecord checks the bound against
// IsHistoricalSpend for every spend height around each boundary: a spend is kept when it is both
// history at the current height and older than the record, and cleared otherwise.
func TestKeepBelowHeightKeepsExactlyTheHistoricalSpendsThatPredateTheRecord(t *testing.T) {
	for _, retention := range []uint32{0, 1, 2, 3, 288} {
		for _, current := range []uint32{0, 1, 2, 143, 144, 145, 1000} {
			for _, recordHeight := range []uint32{0, 1, 2, 500, 856, 857, 858, 1000, 1001} {
				keepBelow := KeepBelowHeight(recordHeight, current, retention)

				for spendHeight := uint32(1); spendHeight <= 1100; spendHeight++ {
					kept := spendHeight < keepBelow
					want := IsHistoricalSpend(spendHeight, true, current, retention) && (recordHeight == 0 || spendHeight < recordHeight)

					require.Equal(t, want, kept, "retention %d, current %d, record %d, spend %d", retention, current, recordHeight, spendHeight)
				}
			}
		}
	}
}

// TestReversalGuardContext pins what the guard helpers carry, and that each one keeps what the other set.
func TestReversalGuardContext(t *testing.T) {
	hash := chainhash.HashH([]byte("record"))

	_, ok := ReversalGuardFrom(context.Background())
	require.False(t, ok, "a context that no reversal set carries no guard")

	ctx := WithReversalCallHeight(context.Background(), 700)

	guard, ok := ReversalGuardFrom(ctx)
	require.True(t, ok)
	require.Nil(t, guard.Record, "a call height alone guards nothing")
	require.Equal(t, uint32(700), guard.CallHeight)

	guard, ok = ReversalGuardFrom(WithReversedRecord(ctx, &hash))
	require.True(t, ok)
	require.Equal(t, hash, *guard.Record)
	require.Equal(t, uint32(700), guard.CallHeight, "naming the record keeps the call height")

	guard, _ = ReversalGuardFrom(WithReversalCallHeight(WithReversedRecord(context.Background(), &hash), 9))
	require.Equal(t, hash, *guard.Record, "setting the call height keeps the record")
	require.Equal(t, uint32(9), guard.CallHeight)
}
