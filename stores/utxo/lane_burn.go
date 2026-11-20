package utxo

import (
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// A burned lane position is one a store took from its source for a create and then wrote no record
// carrying. Burning is safe, since no record has the position, but every burned position inside a
// block's range is a hole in that block's lane map, so the stores count each one with why it burned
// and which service asked for the create. A position whose write cannot be confirmed either
// way is counted too, under outcome_unknown, so the counter can include positions that were written.
//
// The code that took the position from the source records its burn, from the result it saw. A store
// whose Create is handed a position the caller already took records nothing for it, so one burned
// position is counted once.

// The reasons a position burns.
const (
	// LaneBurnExists: the create answered ErrTxExists. Either the store already held the
	// transaction, and the record keeps the position it has, or, on Aerospike, another create of
	// the same transaction held its multi-record creation lock, and that create may still fail.
	// One exception: a utxoset create that carries a block, for a transaction held only as UTXO
	// rows with no identity or containment row, writes the new position on the containment row it
	// adds and still answers ErrTxExists, so it is counted here though the position was written.
	// Another: a conflicting create on SQL with the create batcher off commits its record, then
	// writes the conflict links in a second transaction. If that write fails with a lock error,
	// the retry meets the create's own record and answers ErrTxExists, so it is counted here
	// though the record carries the position, and the position is not returned.
	LaneBurnExists = "exists"

	// LaneBurnSpendRejected: an input could not be spent, so the transaction was not created.
	LaneBurnSpendRejected = "spend_rejected"

	// LaneBurnOutcomeUnknown: the caller stopped waiting, or the store could not confirm the
	// write, so the record may still have been written with the position. A count under this
	// reason can include positions that were in fact written.
	LaneBurnOutcomeUnknown = "outcome_unknown"

	// LaneBurnRolledBack: a utxoset create batch failed with its database transaction open, or a
	// chunk of a list failed before its COMMIT (in planning, at BEGIN or with its transaction open),
	// and the positions were not written again. A create batch that fails before its transaction
	// opens, in partition setup or at BEGIN, counts as an ordinary error.
	LaneBurnRolledBack = "rolled_back"

	// LaneBurnParentFailed: a parent of the transaction earlier in its list failed, so the
	// transaction was not written.
	LaneBurnParentFailed = "parent_failed"

	// LaneBurnListFailed: the list failed before the transaction's chunk was written, either in a
	// check that runs before any chunk or because another chunk failed first.
	LaneBurnListFailed = "list_failed"

	// LaneBurnTakeFailed: a list took positions one at a time and a later take failed, so the
	// positions already taken were never used.
	LaneBurnTakeFailed = "take_failed"

	// LaneBurnError: any other failed create. One exception: a conflicting create on SQL with the
	// create batcher off whose conflict-link write fails with an error that is not a lock error is
	// counted here, though its record committed carrying the position (see LaneBurnExists).
	LaneBurnError = "error"
)

// Caller names the service a create was made for. It is a byte, so it fits in CreateOptions' padding.
type Caller uint8

// The callers. The zero value is CallerUnknown, which is what every create that does not name itself
// gets.
const (
	CallerUnknown Caller = iota
	CallerValidator
	CallerSubtreeValidation
	CallerBlockValidation
	CallerBlockAssembly

	callerCount
)

var callerNames = [callerCount]string{
	CallerUnknown:           "unknown",
	CallerValidator:         "validator",
	CallerSubtreeValidation: "subtree_validation",
	CallerBlockValidation:   "block_validation",
	CallerBlockAssembly:     "block_assembly",
}

// String returns the caller's label value.
func (c Caller) String() string {
	if c >= callerCount {
		return callerNames[CallerUnknown]
	}

	return callerNames[c]
}

// withCaller holds one fixed option per caller, so WithCaller allocates nothing on the create path.
var withCaller = func() (opts [callerCount]CreateOption) {
	for c := range opts {
		caller := Caller(c) //nolint:gosec // c is below callerCount
		opts[c] = func(o *CreateOptions) { o.Caller = caller }
	}

	return opts
}()

// WithCaller names the service the create is made for. It changes nothing about the write: it labels
// the burned lane position counter when the create's position burns.
func WithCaller(c Caller) CreateOption {
	if c >= callerCount {
		c = CallerUnknown
	}

	return withCaller[c]
}

var prometheusLanePositionsBurned = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "teranode",
		Subsystem: "lane",
		Name:      "positions_burned_total",
		Help:      "Lane positions a store took for a create and wrote no record with, by store, reason and the service that asked for the create. Reason outcome_unknown counts positions whose write could not be confirmed and may have been written; reason exists can include a utxoset block create that wrote the position on a containment row; reasons exists and error can include a SQL conflicting create whose record committed before its conflict-link write failed",
	},
	[]string{"store", "reason", "caller"},
)

// LaneBurnReason returns why a create that took a position and failed with err burned it. spends are
// the spends the call returned, if any: a refused input is reported on its Spend.
func LaneBurnReason(err error, spends []*Spend) string {
	// A spend cut short by the context ending, or failed by a timeout or an unavailable service or
	// storage, is reported on the Spend records and as a utxo error, like a refused one. So both
	// are ruled out before the utxo error is read as a refusal.
	switch {
	case errors.Is(err, errors.ErrTxExists):
		return LaneBurnExists
	case spendRefused(spends):
		return LaneBurnSpendRejected
	case IsCreateOutcomeUnknown(err):
		return LaneBurnOutcomeUnknown
	case errors.IsTransientLocalError(err):
		return LaneBurnError
	case errors.Is(err, errors.ErrUtxoError), errors.Is(err, errors.ErrSpent):
		return LaneBurnSpendRejected
	default:
		return LaneBurnError
	}
}

// spendRefused reports whether a Spend record says its input was refused. A context or local
// service failure did not refuse the input.
func spendRefused(spends []*Spend) bool {
	for _, s := range spends {
		if s != nil && s.Err != nil && !errors.IsContextError(s.Err) && !errors.IsTransientLocalError(s.Err) {
			return true
		}
	}

	return false
}

// TookLanePosition reports whether a create that named the position named, and holds position after
// taking, took it from the source. Only then is a burn its own to record. A position the caller named
// was taken, and is recorded, by that caller.
func TookLanePosition(named, position uint64) bool {
	return named == lane.None && position != lane.None
}

// CreatedTxID is the id of the transaction a create writes: the one the options name, or the
// transaction's own.
func CreatedTxID(tx *bt.Tx, o *CreateOptions) chainhash.Hash {
	if o != nil && o.TxID != nil {
		return *o.TxID
	}

	if tx == nil {
		return chainhash.Hash{}
	}

	return *tx.TxIDChainHash()
}

// NameLaneBurns gives the store's name to the burned lane position counter. A store calls it when it
// is built. One that does not records its burns under store "unknown".
func (l *LanePositions) NameLaneBurns(store string) {
	l.burnStore = store
}

// BurnLanePosition counts one burned position.
func (l *LanePositions) BurnLanePosition(caller Caller, reason string) {
	l.BurnLanePositions(caller, reason, 1)
}

// BurnLanePositions counts n positions burned for the same reason and caller, with one counter
// lookup. A list counts its burns this way, once per reason.
func (l *LanePositions) BurnLanePositions(caller Caller, reason string, n int) {
	if n <= 0 {
		return
	}

	store := l.burnStore
	if store == "" {
		store = "unknown"
	}

	prometheusLanePositionsBurned.WithLabelValues(store, reason, caller.String()).Add(float64(n))
}

// BurnCreateLanePosition records the burn of a create that took position from the source and
// returned err, if it failed. A create that succeeded wrote the position, so it records nothing.
func (l *LanePositions) BurnCreateLanePosition(o *CreateOptions, position uint64, err error, spends []*Spend) {
	if err == nil || position == lane.None {
		return
	}

	l.BurnLanePosition(o.Caller, LaneBurnReason(err, spends))
}
