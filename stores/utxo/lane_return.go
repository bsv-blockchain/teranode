package utxo

import (
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// A lane position a store took for a create and then wrote no record with is burned: safe, since no
// record carries it, but a hole in its block's lane map. When the store can prove that no record
// carries it, the store hands it back to its source (ReturnLanePosition), and the next take fills
// the hole. Two things must both hold: the code that saw the create's result proves nothing carries
// the position, and the reason is one of those laneReturnable allows. The store returns a position
// once the create or its batch has its outcome, never while a write that may carry it can still
// commit, and at most once per position it took.

// The reasons a burned position can go back.
const (
	// LaneReturnExists: the store refused the create because it already held the transaction, and
	// the refusal wrote no record carrying the position.
	LaneReturnExists = "exists"

	// LaneReturnSpendRejected: an input's spend was refused before any create ran.
	LaneReturnSpendRejected = "spend_rejected"

	// LaneReturnRolledBack: the database transaction holding the create failed before its COMMIT
	// was sent, so it rolled back whole.
	LaneReturnRolledBack = "rolled_back"
)

// LanePositionReturner is a LanePositionSource that takes back a position no record carries, so a
// later Next gives it out again instead of leaving a hole in the lane. Return reports whether it
// took the position back; one it does not stays burned. *lane.Allocator implements it.
type LanePositionReturner interface {
	Return(position uint64) bool
}

var prometheusLanePositionsReturned = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "teranode",
		Subsystem: "lane",
		Name:      "positions_returned_total",
		Help:      "Lane positions a store took for a create, proved no record carries, and handed back to its source, which took them back for reuse; by store and reason. teranode_lane_positions_reused_total counts the ones given out again",
	},
	[]string{"store", "reason"},
)

// laneReturnable reports whether a position that burned for reason may go back to the source when
// the store proves nothing carries it. Any other reason, an error or a write whose outcome is
// unknown above all, burns for good: a write whose outcome is unknown may carry the position.
func laneReturnable(reason string) bool {
	switch reason {
	case LaneReturnExists, LaneReturnSpendRejected, LaneReturnRolledBack:
		return true
	default:
		return false
	}
}

// NameLaneReturns gives the store's name to the returned lane position counter. A store calls it
// when it is built. One that does not counts its returns under store "unknown".
func (l *LanePositions) NameLaneReturns(store string) {
	l.returnStore = store
}

// ReturnLanePosition hands position, which this store took from its source and wrote no record
// with, back to the source for reuse when unwritten is true and laneReturnable allows the reason.
// unwritten is the caller's proof, from its own code, that no record carries the position.
func (l *LanePositions) ReturnLanePosition(position uint64, reason string, unwritten bool) {
	if !unwritten || l.returner == nil || !laneReturnable(reason) || !l.returner.Return(position) {
		return
	}

	store := l.returnStore
	if store == "" {
		store = "unknown"
	}

	prometheusLanePositionsReturned.WithLabelValues(store, reason).Inc()
}

// laneUnwrittenKey marks an error with which a store proves its create wrote no record carrying the
// create's lane position.
const laneUnwrittenKey = "lane_position_unwritten"

// MarkLanePositionUnwritten marks err, the error of a create, as proof that the create wrote no
// record carrying its lane position, such as an insert the store refused because the transaction
// exists. It tries to set the mark on the first *errors.Error in err's chain. An error with none,
// such as a bare context error, stays unmarked, and so does one that carries its own data type,
// such as a spent-UTXO error, since SetData writes only into the generic data. Every error the
// stores mark is ErrTxExists with generic data. A store marks only an error its own code proves that
// for, and an unmarked error proves nothing, so a mark that does not take only loses a reuse. It
// returns err.
func MarkLanePositionUnwritten(err error) error {
	var te *errors.Error
	if errors.As(err, &te) && te != nil {
		te.SetData(laneUnwrittenKey, true)
	}

	return err
}

// LanePositionUnwritten reports whether a store marked err with MarkLanePositionUnwritten.
func LanePositionUnwritten(err error) bool {
	var te *errors.Error

	return errors.As(err, &te) && te != nil && te.GetData(laneUnwrittenKey) == true
}

// RefusedSpend reports whether err and spends, the failure of a spend, say an input's spend was
// refused: a Spend record carries an error that is not the context ending, or the call failed with a
// UTXO or spent error. A spend cut short by the context ending is reported the same way, so the
// context is ruled out first. A refused spend is LaneReturnSpendRejected when no create ran.
func RefusedSpend(err error, spends []*Spend) bool {
	if err == nil || errors.Is(err, errors.ErrTxExists) {
		return false
	}

	for _, s := range spends {
		if s != nil && s.Err != nil && !errors.IsContextError(s.Err) {
			return true
		}
	}

	if IsCreateOutcomeUnknown(err) {
		return false
	}

	return errors.Is(err, errors.ErrUtxoError) || errors.Is(err, errors.ErrSpent)
}
