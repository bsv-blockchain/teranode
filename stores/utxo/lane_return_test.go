package utxo

import (
	"context"
	"fmt"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
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

			_, _, err := SequentialSpendAndCreate(context.Background(), ulogger.TestLogger{}, s, tx, 100, false, c.opts...)
			require.Error(t, err)

			if c.wantReturn {
				require.Equal(t, []uint64{lane.Pack(9, 40)}, src.returned)
			} else {
				require.Empty(t, src.returned)
			}
		})
	}
}

// The spend-first loop calls Create more than once with the position it took, when it goes around a
// hold that it cleared. A position goes back only on a terminal exit: given back before a round
// that goes around, it would be handed to another create while this call still uses it. It goes
// back after a refused spend in a later round only when every earlier Create was proven unwritten.
func TestSequentialSpendAndCreateReturnsAPositionOnlyOnATerminalExit(t *testing.T) {
	shortenUnspendBackoff(t)

	tx := bt.NewTx()
	tx.LockTime = 13

	hold := meta.Data{Creating: true, RollingBack: true, Hold: true}
	refused := errors.NewUtxoError("1 of 1 inputs could not be spent")

	cases := []struct {
		name       string
		store      fakeSequentialStore
		wantErr    bool
		wantReturn bool
	}{
		{"a create that goes around and then succeeds keeps its position", fakeSequentialStore{
			spends:      []*Spend{{}},
			createErrs:  []error{MarkLanePositionUnwritten(errors.NewTxExistsError("held"))},
			createMeta:  &meta.Data{},
			existing:    hold,
			clearResult: true,
		}, false, false},
		{"a refused spend after a marked create failure returns the position once", fakeSequentialStore{
			spendsSeq:   [][]*Spend{{{}}, {{Err: errors.NewUtxoSpentError(chainhash.Hash{}, 0, chainhash.Hash{}, nil)}}},
			spendErrs:   []error{nil, refused},
			createErrs:  []error{MarkLanePositionUnwritten(errors.NewTxExistsError("held"))},
			existing:    hold,
			clearResult: true,
			existingSeq: []meta.Data{hold, hold, {}},
		}, true, true},
		{"a refused spend after the claim wrote a record for the txid leaves the position burned", fakeSequentialStore{
			spendsSeq:   [][]*Spend{{{}}, {{Err: errors.NewUtxoSpentError(chainhash.Hash{}, 0, chainhash.Hash{}, nil)}}},
			spendErrs:   []error{nil, refused},
			createErrs:  []error{MarkLanePositionUnwritten(errors.NewTxExistsError("held"))},
			createMeta:  &meta.Data{Creating: true, Attempt: []byte{5}},
			existing:    hold,
			clearResult: true,
			getMetaErrs: []error{nil, nil, errors.NewTxNotFoundError("no record")},
		}, true, false},
		{"a refused spend after a create failure the store did not mark leaves the position burned", fakeSequentialStore{
			spendsSeq:   [][]*Spend{{{}}, {{Err: errors.NewUtxoSpentError(chainhash.Hash{}, 0, chainhash.Hash{}, nil)}}},
			spendErrs:   []error{nil, refused},
			createErrs:  []error{errors.NewTxExistsError("held")},
			existing:    hold,
			clearResult: true,
			existingSeq: []meta.Data{hold, hold, {}},
		}, true, false},
		{"a marked refusal that ends the call returns the position once", fakeSequentialStore{
			spends:    []*Spend{{}},
			createErr: MarkLanePositionUnwritten(errors.NewTxExistsError("exists")),
			existing:  meta.Data{},
		}, true, true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			src := &returningSource{position: lane.Pack(9, 50)}
			s := &returningSequentialStore{fakeSequentialStore: c.store}
			s.SetLanePositionSource(src)

			_, _, err := SequentialSpendAndCreate(context.Background(), ulogger.TestLogger{}, s, tx, 100, false)
			require.Equal(t, c.wantErr, err != nil, "%v", err)

			if c.wantReturn {
				require.Equal(t, []uint64{lane.Pack(9, 50)}, src.returned)
			} else {
				require.Empty(t, src.returned)
			}
		})
	}
}

// A store that never named itself counts its returns under store "unknown".
func TestReturnLanePositionCountsAnUnnamedStoreAsUnknown(t *testing.T) {
	returned := prometheusLanePositionsReturned.WithLabelValues("unknown", LaneReturnExists)
	before := testutil.ToFloat64(returned)

	var positions LanePositions

	positions.SetLanePositionSource(&returningSource{})
	positions.ReturnLanePosition(lane.Pack(2, 5), LaneReturnExists, true)

	require.InDelta(t, 1, testutil.ToFloat64(returned)-before, 0)
}

// returningLaneSpender is the sequential fake with lane positions whose spend records the spender's
// position in the parents' slots, as a LaneSpender does.
type returningLaneSpender struct {
	returningSequentialStore
}

func (l *returningLaneSpender) SpendWithLanePosition(ctx context.Context, tx *bt.Tx, blockHeight uint32, _ uint64, flags IgnoreFlags) ([]*Spend, error) {
	return l.Spend(ctx, tx, blockHeight, flags)
}

var allLaneBurnReasons = []string{
	LaneBurnExists, LaneBurnSpendRejected, LaneBurnOutcomeUnknown, LaneBurnRolledBack,
	LaneBurnParentFailed, LaneBurnListFailed, LaneBurnTakeFailed, LaneBurnError,
}

// burnedFor reads the burn counter of one store for every reason, for the unknown caller.
func burnedFor(store string) map[string]float64 {
	counts := make(map[string]float64, len(allLaneBurnReasons))

	for _, reason := range allLaneBurnReasons {
		counts[reason] = testutil.ToFloat64(prometheusLanePositionsBurned.WithLabelValues(store, reason, CallerUnknown.String()))
	}

	return counts
}

// A create-first call takes its position before it picks the order, so it owns the position on
// every exit: still carried by a record the call left (no burn, no return), burned once, or burned
// once and returned once when the store proved no record carries it. The record of a rolled back
// attempt becomes a hold that carries no position.
func TestSequentialSpendAndCreateCreateFirstBurnsOrReturnsItsPositionOnce(t *testing.T) {
	shortenUnspendBackoff(t)

	tx := scriptedTwoInputTx(t)
	two := twoInputTx()

	hold := meta.Data{Creating: true, RollingBack: true, Hold: true}
	refused := errors.NewUtxoError("1 of 2 inputs could not be spent")
	spentErr := errors.NewUtxoSpentError(chainhash.Hash{}, 0, chainhash.Hash{}, nil)
	marked := func() error { return MarkLanePositionUnwritten(errors.NewTxExistsError("exists")) }
	creating := &meta.Data{Creating: true, Attempt: []byte{9}}

	cases := []struct {
		name       string
		store      fakeSequentialStore
		laneSpend  bool
		tx         *bt.Tx
		wantErr    bool
		wantBurn   string
		wantReturn bool
	}{
		{name: "a create-first success leaves the position on the record", store: fakeSequentialStore{
			spends: []*Spend{{}, {}}, createMeta: creating,
		}},
		{name: "a refused spend that rolls back burns the position and returns it", store: fakeSequentialStore{
			spends: []*Spend{{Err: spentErr}, {}}, spendErr: refused, createMeta: creating,
		}, wantErr: true, wantBurn: LaneBurnSpendRejected, wantReturn: true},
		{name: "a lane spend refused on every input before its write returns the position", store: fakeSequentialStore{
			spends: []*Spend{{Err: spentErr}, {Err: spentErr}}, spendErr: refused, createMeta: creating,
		}, laneSpend: true, wantErr: true, wantBurn: LaneBurnSpendRejected, wantReturn: true},
		{name: "a lane spend that may have written a parent's slot keeps the position burned", store: fakeSequentialStore{
			spends: []*Spend{{Err: spentErr}, {}}, spendErr: refused, createMeta: creating,
		}, laneSpend: true, wantErr: true, wantBurn: LaneBurnSpendRejected},
		{name: "a refused spend whose rollback fails leaves the position on the creating record", store: fakeSequentialStore{
			spends: []*Spend{{Err: spentErr}, {}}, spendErr: refused, createMeta: creating,
			beginErr: errors.NewStorageError("begin boom"),
		}, wantErr: true},
		{name: "a refused spend whose rollback does not begin, the record being final, leaves the position on that record", store: fakeSequentialStore{
			spends: []*Spend{{Err: spentErr}, {}}, spendErr: refused, createMeta: creating, beginDeclined: true,
		}, wantErr: true},
		{name: "a failed create whose rollback does not begin, the record being final, leaves the position on that record", store: fakeSequentialStore{
			createErr:     MarkRollBackAttempt(errors.NewProcessingError("adoption failed"), []byte{9}),
			beginDeclined: true,
		}, wantErr: true},
		{name: "a spend of unknown outcome leaves the position on the creating record", store: fakeSequentialStore{
			spends: []*Spend{{}, {}}, spendErr: context.DeadlineExceeded, createMeta: creating,
		}, wantErr: true},
		{name: "a spend that waits on a committing parent leaves the position on the creating record", store: fakeSequentialStore{
			spends: []*Spend{{Err: errors.NewTxLockedError("locked")}, {Err: errors.NewTxCreatingError("creating")}}, spendErr: refused, createMeta: creating,
		}, wantErr: true},
		{name: "a finalize that fails leaves the position on the creating record", store: fakeSequentialStore{
			spends: []*Spend{{}, {}}, createMeta: creating, finalizeErr: errors.NewStorageError("finalize boom"),
		}, wantErr: true},
		{name: "a finalize that finds the record final leaves the position on that record", store: fakeSequentialStore{
			spends: []*Spend{{}, {}}, createMeta: creating, finalizeErr: MarkAlreadyFinal(errors.NewTxExistsError("final")),
		}, wantErr: true},
		{name: "a plain duplicate the store proved unwritten returns the position", store: fakeSequentialStore{
			createErr: marked(),
		}, wantErr: true, wantBurn: LaneBurnExists, wantReturn: true},
		{name: "a duplicate the store did not prove unwritten stays burned", store: fakeSequentialStore{
			createErr: errors.NewTxExistsError("exists"),
		}, wantErr: true, wantBurn: LaneBurnExists},
		{name: "a duplicate whose record cannot be read still burns the position", store: fakeSequentialStore{
			createErr: errors.NewTxExistsError("exists"), getMetaErr: errors.NewStorageError("read boom"),
		}, wantErr: true, wantBurn: LaneBurnExists},
		{name: "a rolling back record the store proved unwritten returns the position", store: fakeSequentialStore{
			createErr: marked(), existing: hold,
		}, wantErr: true, wantBurn: LaneBurnExists, wantReturn: true},
		{name: "a rolling back record that is not proven keeps the position burned", store: fakeSequentialStore{
			createErr: errors.NewTxExistsError("exists"), existing: hold,
		}, wantErr: true, wantBurn: LaneBurnExists},
		{name: "a create error that is not a duplicate burns the position without returning it", store: fakeSequentialStore{
			createErr: errors.NewStorageError("disk"),
		}, wantErr: true, wantBurn: LaneBurnError},
		{name: "a create of unknown outcome burns the position as outcome_unknown", store: fakeSequentialStore{
			createErr: MarkCreateOutcomeUnknown(errors.NewServiceUnavailableError("timed out")),
		}, wantErr: true, wantBurn: LaneBurnOutcomeUnknown},
		{name: "a failed create whose attempt is rolled back burns the position without returning it", store: fakeSequentialStore{
			createErr: MarkRollBackAttempt(errors.NewProcessingError("adoption failed"), []byte{9}),
		}, wantErr: true, wantBurn: LaneBurnError},
		{name: "a failed create whose rollback fails leaves the position on the creating record", store: fakeSequentialStore{
			createErr: MarkRollBackAttempt(errors.NewProcessingError("adoption failed"), []byte{9}),
			beginErr:  errors.NewStorageError("begin boom"),
		}, wantErr: true},
		{name: "a roll-forward that completes returns the proven position", store: fakeSequentialStore{
			createErr: marked(), existing: meta.Data{Creating: true}, spends: []*Spend{{}, {}},
		}, tx: two, wantErr: true, wantBurn: LaneBurnExists, wantReturn: true},
		{name: "a roll-forward that fails returns the proven position", store: fakeSequentialStore{
			createErr: marked(), existing: meta.Data{Creating: true},
			spends: []*Spend{{}, {Err: spentErr}}, spendErr: refused,
		}, tx: two, wantErr: true, wantBurn: LaneBurnExists, wantReturn: true},
		{name: "a roll-forward after an unproven refusal keeps the position burned", store: fakeSequentialStore{
			createErr: errors.NewTxExistsError("exists"), existing: meta.Data{Creating: true},
			spends: []*Spend{{}, {Err: spentErr}}, spendErr: refused,
		}, tx: two, wantErr: true, wantBurn: LaneBurnExists},
		{name: "a hold that clears, then a create that succeeds, leaves the position on the record", store: fakeSequentialStore{
			spends: []*Spend{{}, {}}, createErrs: []error{marked()}, createMeta: creating,
			existing: hold, clearResult: true,
		}},
		{name: "a hold that clears, then a refused spend, returns the position once", store: fakeSequentialStore{
			spends: []*Spend{{Err: spentErr}, {}}, spendErr: refused, createErrs: []error{marked()}, createMeta: creating,
			existing: hold, clearResult: true,
		}, wantErr: true, wantBurn: LaneBurnSpendRejected, wantReturn: true},
		{name: "a hold the store did not prove unwritten, then a refused spend, keeps the position burned", store: fakeSequentialStore{
			spends: []*Spend{{Err: spentErr}, {}}, spendErr: refused, createErrs: []error{errors.NewTxExistsError("held")}, createMeta: creating,
			existing: hold, clearResult: true,
		}, wantErr: true, wantBurn: LaneBurnSpendRejected},
	}

	for i, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			position := lane.Pack(9, uint64(60+i))
			src := &returningSource{position: position}
			name := fmt.Sprintf("createfirst-%d", i)

			var s SequentialStore

			if c.laneSpend {
				ls := &returningLaneSpender{returningSequentialStore{fakeSequentialStore: c.store}}
				ls.NameLaneBurns(name)
				ls.SetLanePositionSource(src)
				s = ls
			} else {
				rs := &returningSequentialStore{fakeSequentialStore: c.store}
				rs.NameLaneBurns(name)
				rs.SetLanePositionSource(src)
				s = rs
			}

			callTx := tx
			if c.tx != nil {
				callTx = c.tx
			}

			_, _, err := SequentialSpendAndCreate(context.Background(), ulogger.TestLogger{}, s, callTx, 100, true)
			require.Equal(t, c.wantErr, err != nil, "%v", err)

			for reason, n := range burnedFor(name) {
				if reason == c.wantBurn {
					require.InDelta(t, 1, n, 0, "the position burns once, as %s", reason)
				} else {
					require.InDelta(t, 0, n, 0, "no burn as %s", reason)
				}
			}

			if c.wantReturn {
				require.Equal(t, []uint64{position}, src.returned)
			} else {
				require.Empty(t, src.returned)
			}
		})
	}
}
