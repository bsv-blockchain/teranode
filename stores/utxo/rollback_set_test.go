package utxo

import (
	"testing"

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
