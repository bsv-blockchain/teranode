package utxo

import (
	"context"
	"fmt"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// returningSource is a LanePositionSource that hands out one position and records what comes back.
type returningSource struct {
	position uint64
	refuse   bool
	returned []uint64
}

func (r *returningSource) Next(context.Context) (uint64, error) { return r.position, nil }

func (r *returningSource) Return(position uint64) bool {
	if r.refuse {
		return false
	}

	r.returned = append(r.returned, position)

	return true
}

// fixedPosition is a LanePositionSource that takes nothing back.
type fixedPosition struct{ position uint64 }

func (f fixedPosition) Next(context.Context) (uint64, error) { return f.position, nil }

// The reasons a burned position can have, the three that may go back and the ones that never do.
var allLaneReasons = []string{
	LaneReturnExists, LaneReturnSpendRejected, LaneReturnRolledBack,
	"outcome_unknown", "error", "parent_failed", "list_failed", "take_failed",
}

// Only the three reasons a store can prove go back: the insert refused because the transaction
// exists, a spend refused before any create, a database transaction rolled back whole. A write whose
// outcome is unknown may carry its position, so outcome_unknown must never be among them.
func TestLaneReturnableAllowsOnlyProvableReasons(t *testing.T) {
	want := map[string]bool{LaneReturnExists: true, LaneReturnSpendRejected: true, LaneReturnRolledBack: true}

	for _, reason := range allLaneReasons {
		require.Equal(t, want[reason], laneReturnable(reason), reason)
	}

	require.False(t, laneReturnable(""))
}

// A position goes back only when the store proved it unwritten AND the reason is allowed.
func TestReturnLanePositionReturnsOnlyAProvenAllowedReason(t *testing.T) {
	for _, reason := range allLaneReasons {
		for _, unwritten := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/unwritten=%t", reason, unwritten), func(t *testing.T) {
				src := &returningSource{}

				var positions LanePositions

				positions.NameLaneReturns("gatestore")
				positions.SetLanePositionSource(src)

				returned := prometheusLanePositionsReturned.WithLabelValues("gatestore", reason)
				before := testutil.ToFloat64(returned)

				positions.ReturnLanePosition(lane.Pack(3, 77), reason, unwritten)

				if unwritten && laneReturnable(reason) {
					require.Equal(t, []uint64{lane.Pack(3, 77)}, src.returned)
					require.InDelta(t, 1, testutil.ToFloat64(returned)-before, 0)
				} else {
					require.Empty(t, src.returned)
					require.InDelta(t, 0, testutil.ToFloat64(returned)-before, 0)
				}
			})
		}
	}
}

// A source that refuses the position, or one that takes nothing back, leaves it burned and
// uncounted as returned.
func TestReturnLanePositionCountsOnlyWhatTheSourceTookBack(t *testing.T) {
	returned := prometheusLanePositionsReturned.WithLabelValues("refusestore", LaneReturnExists)
	before := testutil.ToFloat64(returned)

	var refusing LanePositions

	refusing.NameLaneReturns("refusestore")
	refusing.SetLanePositionSource(&returningSource{refuse: true})
	refusing.ReturnLanePosition(lane.Pack(1, 1), LaneReturnExists, true)

	var plain LanePositions

	plain.NameLaneReturns("refusestore")
	plain.SetLanePositionSource(fixedPosition{lane.Pack(1, 2)})
	plain.ReturnLanePosition(lane.Pack(1, 2), LaneReturnExists, true)

	require.InDelta(t, before, testutil.ToFloat64(returned), 0)
}

// A store switched to a source that takes nothing back must not keep handing positions to the
// source it had before: those belong to the old allocator's grant.
func TestSetLanePositionSourceDropsTheOldReturner(t *testing.T) {
	old := &returningSource{}

	var positions LanePositions

	positions.SetLanePositionSource(old)
	positions.SetLanePositionSource(fixedPosition{lane.Pack(1, 9)})
	positions.ReturnLanePosition(lane.Pack(1, 9), LaneReturnExists, true)

	positions.SetLanePositionSource(nil)
	positions.ReturnLanePosition(lane.Pack(1, 10), LaneReturnExists, true)

	require.Empty(t, old.returned)
}

func TestMarkLanePositionUnwritten(t *testing.T) {
	err := errors.NewTxExistsError("exists")
	require.False(t, LanePositionUnwritten(err))

	require.Same(t, err, MarkLanePositionUnwritten(err))
	require.True(t, LanePositionUnwritten(err))
	require.ErrorIs(t, err, errors.ErrTxExists, "marking keeps the error what it was")

	require.False(t, LanePositionUnwritten(errors.NewTxExistsError("another")), "each error is marked on its own")
	require.False(t, LanePositionUnwritten(nil))
	require.False(t, LanePositionUnwritten(context.Canceled))
	require.NoError(t, MarkLanePositionUnwritten(nil))
	require.Equal(t, context.Canceled, MarkLanePositionUnwritten(context.Canceled), "a foreign error is left as it is")

	spent := errors.NewUtxoSpentError(chainhash.Hash{}, 0, chainhash.Hash{}, nil)
	require.False(t, LanePositionUnwritten(MarkLanePositionUnwritten(spent)), "an error with its own data type stays unmarked")
}

func TestRefusedSpend(t *testing.T) {
	refused := []*Spend{nil, {Err: errors.NewUtxoSpentError([32]byte{}, 0, [32]byte{}, nil)}}

	cases := []struct {
		name   string
		err    error
		spends []*Spend
		want   bool
	}{
		{"a spend refused on its record", errors.NewProcessingError("failed"), refused, true},
		{"a utxo error", errors.NewUtxoError("2 of 2 inputs could not be spent"), nil, true},
		{"a spent utxo", errors.NewUtxoSpentError(chainhash.Hash{}, 0, chainhash.Hash{}, nil), nil, true},
		{"the transaction exists", errors.NewTxExistsError("exists"), refused, false},
		{"the caller's context ended", context.Canceled, nil, false},
		{"a spend cut short by the context ending", errors.NewContextCanceledError("cancelled"),
			[]*Spend{{Err: errors.NewContextCanceledError("cancelled")}}, false},
		{"a write marked unknown", MarkCreateOutcomeUnknown(errors.NewServiceUnavailableError("timed out")), nil, false},
		{"anything else", errors.NewStorageError("disk"), nil, false},
		{"no error", nil, refused, false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.want, RefusedSpend(c.err, c.spends))
		})
	}
}

// returningSequentialStore is the sequential fake with lane positions, so SequentialSpendAndCreate
// takes one and decides whether it goes back.
type returningSequentialStore struct {
	fakeSequentialStore
	LanePositions
}

// SequentialSpendAndCreate returns a position when its spend was refused, since the create never
// ran, and when the store's Create marked its refusal. An unmarked ErrTxExists, which on Aerospike
// can be the create's own earlier attempt, stays burned, and so does a spend that failed for another
// reason.
func TestSequentialSpendAndCreateReturnsOnlyProvenPositions(t *testing.T) {
	tx := bt.NewTx()
	tx.LockTime = 12

	cases := []struct {
		name       string
		store      fakeSequentialStore
		opts       []CreateOption
		wantReturn bool
	}{
		{"a refused spend", fakeSequentialStore{
			spends:   []*Spend{{Err: errors.NewUtxoSpentError(chainhash.Hash{}, 0, chainhash.Hash{}, nil)}},
			spendErr: errors.NewUtxoError("1 of 1 inputs could not be spent"),
		}, nil, true},
		{"a spend that failed with a storage error", fakeSequentialStore{
			spendErr: errors.NewStorageError("disk"),
		}, nil, false},
		{"a refusal the store marked", fakeSequentialStore{
			createErr: MarkLanePositionUnwritten(errors.NewTxExistsError("exists")),
		}, []CreateOption{WithCreateOnly()}, true},
		{"a refusal the store did not mark", fakeSequentialStore{
			createErr: errors.NewTxExistsError("exists"),
		}, []CreateOption{WithCreateOnly()}, false},
		{"a marked error that is not a refusal", fakeSequentialStore{
			createErr: MarkLanePositionUnwritten(MarkCreateOutcomeUnknown(errors.NewStorageError("lost"))),
		}, []CreateOption{WithCreateOnly()}, false},
		{"a named position is the caller's", fakeSequentialStore{
			createErr: MarkLanePositionUnwritten(errors.NewTxExistsError("exists")),
		}, []CreateOption{WithCreateOnly(), WithLanePosition(lane.Pack(9, 41))}, false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			src := &returningSource{position: lane.Pack(9, 40)}
			s := &returningSequentialStore{fakeSequentialStore: c.store}
			s.SetLanePositionSource(src)

			_, _, err := SequentialSpendAndCreate(context.Background(), ulogger.TestLogger{}, s, tx, 100, c.opts...)
			require.Error(t, err)

			if c.wantReturn {
				require.Equal(t, []uint64{lane.Pack(9, 40)}, src.returned)
			} else {
				require.Empty(t, src.returned)
			}
		})
	}
}
