package utxo

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestLaneBurnReason(t *testing.T) {
	refused := []*Spend{nil, {Err: errors.NewUtxoSpentError([32]byte{}, 0, [32]byte{}, nil)}}

	cases := []struct {
		name   string
		err    error
		spends []*Spend
		want   string
	}{
		{"the transaction exists", errors.NewTxExistsError("exists"), nil, LaneBurnExists},
		{"exists wins over a refused spend", errors.NewTxExistsError("exists"), refused, LaneBurnExists},
		{"a spend refused on its record", errors.NewProcessingError("failed"), refused, LaneBurnSpendRejected},
		{"a spend timeout on its record", errors.NewUtxoError("spend failed", errors.NewServiceUnavailableError("batch timed out")),
			[]*Spend{{Err: errors.NewServiceUnavailableError("batch timed out")}}, LaneBurnError},
		{"a utxo error", errors.NewUtxoError("2 of 2 inputs could not be spent"), nil, LaneBurnSpendRejected},
		{"a spent utxo", errors.NewUtxoSpentError(chainhash.Hash{}, 0, chainhash.Hash{}, nil), nil, LaneBurnSpendRejected},
		{"a timed-out input does not hide a refused one", errors.NewUtxoError("spend failed", errors.NewServiceUnavailableError("timed out")),
			[]*Spend{{Err: errors.NewServiceUnavailableError("timed out")}, refused[1]}, LaneBurnSpendRejected},
		{"a storage failure on its record", errors.NewUtxoError("spend failed", errors.NewStorageError("disk")),
			[]*Spend{{Err: errors.NewStorageError("disk")}}, LaneBurnError},
		{"a utxo error wrapping a storage failure", errors.NewUtxoError("spend failed", errors.NewStorageError("disk")), nil, LaneBurnError},
		{"the caller's context ended", context.Canceled, nil, LaneBurnOutcomeUnknown},
		{"a spend cut short by the context ending", errors.NewContextCanceledError("cancelled"),
			[]*Spend{{Err: errors.NewContextCanceledError("cancelled")}}, LaneBurnOutcomeUnknown},
		{"a write marked unknown", MarkCreateOutcomeUnknown(errors.NewServiceUnavailableError("timed out")), nil, LaneBurnOutcomeUnknown},
		{"anything else", errors.NewStorageError("disk"), nil, LaneBurnError},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.want, LaneBurnReason(c.err, c.spends))

			// A position is returned as spend_rejected only when it also burns as spend_rejected,
			// so the returned counter never runs ahead of the burned one for that reason.
			require.Equal(t, c.want == LaneBurnSpendRejected, RefusedSpend(c.err, c.spends))
		})
	}
}

// A failed lane spend returns its position only when every input was refused before its write.
func TestLaneSpendWroteNothing(t *testing.T) {
	spent := &Spend{Err: errors.NewUtxoSpentError(chainhash.Hash{}, 0, chainhash.Hash{}, nil)}
	locked := &Spend{Err: errors.NewTxLockedError("locked")}

	cases := []struct {
		name   string
		spends []*Spend
		want   bool
	}{
		{"a double spend of one output", []*Spend{spent}, true},
		{"every input refused, for different reasons", []*Spend{spent, locked, {Err: errors.NewUtxoFrozenError("frozen")},
			{Err: errors.NewTxConflictingError("conflicting")}, {Err: errors.NewUtxoHashMismatchError("mismatch")},
			{Err: errors.NewTxCreatingError("creating")}, {Err: errors.NewTxNotFoundError("missing")},
			{Err: errors.NewTxCoinbaseImmatureError("immature")}}, true},
		{"one input spent before the refusal", []*Spend{{}, spent}, false},
		{"an input with no result", []*Spend{nil, spent}, false},
		{"no spends", nil, false},
		{"a storage error may have written", []*Spend{spent, {Err: errors.NewStorageError("disk")}}, false},
		{"a response that did not parse may have written", []*Spend{{Err: errors.NewProcessingError("could not parse response")}}, false},
		{"a timeout may have written", []*Spend{{Err: errors.NewServiceUnavailableError("batch timed out")}}, false},
		{"a refusal wrapped in a storage error may have written", []*Spend{{Err: errors.NewStorageError("disk", errors.NewTxNotFoundError("missing"))}}, false},
		{"the context ending may have written", []*Spend{{Err: errors.NewContextCanceledError("cancelled")}}, false},
		{"an invalid spend is not on the list", []*Spend{{Err: errors.NewUtxoError("invalid spend")}}, false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.want, LaneSpendWroteNothing(c.spends))
		})
	}
}

func TestWithCaller(t *testing.T) {
	for c := CallerUnknown; c < callerCount; c++ {
		var o CreateOptions

		WithCaller(c)(&o)
		require.Equal(t, c, o.Caller)
		require.NotEmpty(t, c.String())
	}

	var o CreateOptions

	WithCaller(callerCount + 3)(&o)
	require.Equal(t, CallerUnknown, o.Caller, "a caller out of range is unknown")
	require.Equal(t, "unknown", Caller(200).String())

	require.Equal(t, "validator", CallerValidator.String())
	require.Equal(t, "subtree_validation", CallerSubtreeValidation.String())
	require.Equal(t, "block_validation", CallerBlockValidation.String())
	require.Equal(t, "block_assembly", CallerBlockAssembly.String())
}

func TestTookLanePosition(t *testing.T) {
	require.True(t, TookLanePosition(0, 5), "no position named, one taken from the source")
	require.False(t, TookLanePosition(5, 5), "the caller named it")
	require.False(t, TookLanePosition(0, 0), "no source")
}

// burnCount reads the burned lane position counter for one store, by reason and caller.
func burnCount(store, reason string, caller Caller) float64 {
	return testutil.ToFloat64(prometheusLanePositionsBurned.WithLabelValues(store, reason, caller.String()))
}

// A create that wrote its record counts nothing; one that failed counts once, labelled with the
// store, the reason and the caller.
func TestBurnCreateLanePositionCountsOnce(t *testing.T) {
	var positions LanePositions

	positions.NameLaneBurns("teststore")

	before := burnCount("teststore", LaneBurnExists, CallerBlockValidation)

	positions.BurnCreateLanePosition(&CreateOptions{Caller: CallerBlockValidation}, lane.Pack(9, 1234), nil, nil)
	require.Equal(t, before, burnCount("teststore", LaneBurnExists, CallerBlockValidation), "a written record burns nothing")

	positions.BurnCreateLanePosition(&CreateOptions{Caller: CallerBlockValidation}, lane.Pack(9, 1234), errors.NewTxExistsError("exists"), nil)
	require.Equal(t, before+1, burnCount("teststore", LaneBurnExists, CallerBlockValidation))

	positions.BurnCreateLanePosition(&CreateOptions{Caller: CallerBlockValidation}, lane.None, errors.NewTxExistsError("exists"), nil)
	require.Equal(t, before+1, burnCount("teststore", LaneBurnExists, CallerBlockValidation), "no position taken, nothing burned")
}

// A store that never named itself counts under store "unknown".
func TestBurnLanePositionWithoutAStoreName(t *testing.T) {
	var positions LanePositions

	before := burnCount("unknown", LaneBurnError, CallerUnknown)

	positions.BurnLanePosition(CallerUnknown, LaneBurnError)

	require.Equal(t, before+1, burnCount("unknown", LaneBurnError, CallerUnknown))
}

// A caller that stopped waiting for its queued create gets its context error, and the record may
// still be written: the position counts as outcome_unknown.
func TestBurnCreateLanePositionCountsAGivenUpWaiterAsUnknown(t *testing.T) {
	var positions LanePositions

	positions.NameLaneBurns("waiterstore")

	before := burnCount("waiterstore", LaneBurnOutcomeUnknown, CallerValidator)

	positions.BurnCreateLanePosition(&CreateOptions{Caller: CallerValidator}, lane.Pack(9, 2), context.Canceled, nil)

	require.Equal(t, before+1, burnCount("waiterstore", LaneBurnOutcomeUnknown, CallerValidator))
}

// A refused input reported on its Spend record counts as spend_rejected even when the call's error
// has another code: the classification reads the records the call returned.
func TestBurnCreateLanePositionReadsTheSpendRecords(t *testing.T) {
	var positions LanePositions

	positions.NameLaneBurns("spendstore")

	before := burnCount("spendstore", LaneBurnSpendRejected, CallerUnknown)

	refused := []*Spend{{Err: errors.NewUtxoSpentError(chainhash.Hash{}, 0, chainhash.Hash{}, nil)}}

	positions.BurnCreateLanePosition(&CreateOptions{}, lane.Pack(9, 3), errors.NewProcessingError("the spend failed"), refused)

	require.Equal(t, before+1, burnCount("spendstore", LaneBurnSpendRejected, CallerUnknown))
}

// lanedSequentialStore is the sequential fake with lane positions, so SequentialSpendAndCreate takes
// one and records its burn.
type lanedSequentialStore struct {
	fakeSequentialStore
	LanePositions
}

type onePosition struct{ position uint64 }

func (o onePosition) Next(context.Context) (uint64, error) { return o.position, nil }

// SequentialSpendAndCreate records the burn of a position it took. A failed spend is classified from
// the Spend records it returned.
func TestSequentialSpendAndCreateRecordsItsBurns(t *testing.T) {
	tx := bt.NewTx()
	tx.LockTime = 11

	t.Run("a spend refused on its record", func(t *testing.T) {
		s := &lanedSequentialStore{fakeSequentialStore: fakeSequentialStore{
			spends:   []*Spend{{Err: errors.NewUtxoSpentError(chainhash.Hash{}, 0, chainhash.Hash{}, nil)}},
			spendErr: errors.NewProcessingError("the spend failed"),
		}}
		s.NameLaneBurns("seqstore")
		s.SetLanePositionSource(onePosition{lane.Pack(9, 4)})

		before := burnCount("seqstore", LaneBurnSpendRejected, CallerValidator)

		_, _, err := SequentialSpendAndCreate(context.Background(), ulogger.TestLogger{}, s, tx, 100,
			WithCaller(CallerValidator))
		require.Error(t, err)

		require.Equal(t, before+1, burnCount("seqstore", LaneBurnSpendRejected, CallerValidator))
		require.Zero(t, s.createCalls, "the create never ran")
	})

	t.Run("a spend batch timeout", func(t *testing.T) {
		timeoutErr := errors.NewServiceUnavailableError("batch timed out")
		s := &lanedSequentialStore{fakeSequentialStore: fakeSequentialStore{
			spends:   []*Spend{{Err: timeoutErr}},
			spendErr: errors.NewUtxoError("spend failed", timeoutErr),
		}}
		s.NameLaneBurns("seqstore")
		s.SetLanePositionSource(onePosition{lane.Pack(9, 7)})

		before := burnCount("seqstore", LaneBurnError, CallerValidator)

		_, _, err := SequentialSpendAndCreate(context.Background(), ulogger.TestLogger{}, s, tx, 100,
			WithCaller(CallerValidator))
		require.ErrorIs(t, err, errors.ErrUtxoError)

		require.Equal(t, before+1, burnCount("seqstore", LaneBurnError, CallerValidator))
		require.Zero(t, s.createCalls, "the create never ran")
	})

	t.Run("a create that fails", func(t *testing.T) {
		s := &lanedSequentialStore{fakeSequentialStore: fakeSequentialStore{
			createErr: errors.NewTxExistsError("exists"),
		}}
		s.NameLaneBurns("seqstore")
		s.SetLanePositionSource(onePosition{lane.Pack(9, 5)})

		before := burnCount("seqstore", LaneBurnExists, CallerBlockAssembly)

		_, _, err := SequentialSpendAndCreate(context.Background(), ulogger.TestLogger{}, s, tx, 100,
			WithCreateOnly(), WithCaller(CallerBlockAssembly))
		require.ErrorIs(t, err, errors.ErrTxExists)

		require.Equal(t, before+1, burnCount("seqstore", LaneBurnExists, CallerBlockAssembly))
	})

	t.Run("a create whose outcome the store could not confirm", func(t *testing.T) {
		s := &lanedSequentialStore{fakeSequentialStore: fakeSequentialStore{
			createErr: MarkCreateOutcomeUnknown(errors.NewServiceUnavailableError("batch did not complete")),
		}}
		s.NameLaneBurns("seqstore")
		s.SetLanePositionSource(onePosition{lane.Pack(9, 6)})

		before := burnCount("seqstore", LaneBurnOutcomeUnknown, CallerValidator)

		_, _, err := SequentialSpendAndCreate(context.Background(), ulogger.TestLogger{}, s, tx, 100,
			WithCreateOnly(), WithCaller(CallerValidator))
		require.True(t, IsCreateOutcomeUnknown(err))

		require.Equal(t, before+1, burnCount("seqstore", LaneBurnOutcomeUnknown, CallerValidator),
			"a create that may still land counts as outcome_unknown")
	})
}

// A create that names its transaction's ID, as a rebuilt transaction's does, is looked up by that ID,
// not by the hash of the bytes it was given.
func TestCreatedTxID(t *testing.T) {
	tx := bt.NewTx()
	tx.LockTime = 13

	named := chainhash.HashH([]byte("the id the create names"))
	require.NotEqual(t, named, *tx.TxIDChainHash())

	require.Equal(t, named, CreatedTxID(tx, &CreateOptions{TxID: &named}))
	require.Equal(t, *tx.TxIDChainHash(), CreatedTxID(tx, &CreateOptions{}))
	require.Equal(t, *tx.TxIDChainHash(), CreatedTxID(tx, nil))
	require.Equal(t, chainhash.Hash{}, CreatedTxID(nil, nil))
}
