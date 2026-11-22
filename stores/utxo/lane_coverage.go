package utxo

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Lane position coverage, for the Phase 1 soak at utxostore_laneMigration shadow: how much of what a
// store writes carries the lane positions the later readers will need. A record or spend without one
// is counted, never refused. Both counters split on carried, "yes" or "no", so the share carried is
// yes over the sum.
const (
	// LaneCoverageOwn counts created records by whether they carry their own lane position.
	LaneCoverageOwn = "own"
	// LaneCoverageParents counts created records that spend at least one parent by whether they
	// carry a lane position for every distinct parent.
	LaneCoverageParents = "parents"
)

var (
	prometheusLaneRecordPositions = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "lane",
			Name:      "record_positions_total",
			Help:      "Records a UTXO store created at utxostore_laneMigration shadow, by store, kind (own: the record's own lane position; parents: a lane position for every distinct parent, records with parents only) and carried (yes or no)",
		},
		[]string{"store", "kind", "carried"},
	)

	prometheusLaneSpendPositions = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "lane",
			Name:      "spend_positions_total",
			Help:      "Spent outputs a UTXO store recorded at utxostore_laneMigration shadow, by store and carried: yes on utxoset when the spend's journal row has a lane_position; yes on Aerospike when the spend sent the spender's lane position on the Lua or native route of a server the start-up probe found recording it (a same-spender repeat writes nothing and still counts yes). It covers spends that leave a spent record, a spend-journal row on utxoset or a parent slot on Aerospike; a below-checkpoint netted list on utxoset deletes its spent coins outright, writes neither, and is not counted",
		},
		[]string{"store", "carried"},
	)
)

func carriedLabel(carried bool) string {
	if carried {
		return "yes"
	}

	return "no"
}

// CountLaneRecord counts one created record's lane position coverage for store. hasParents is
// whether the record spends any parent; a record with none is left out of the parents kind.
func CountLaneRecord(store string, own bool, hasParents, parents bool) {
	prometheusLaneRecordPositions.WithLabelValues(store, LaneCoverageOwn, carriedLabel(own)).Inc()

	if hasParents {
		prometheusLaneRecordPositions.WithLabelValues(store, LaneCoverageParents, carriedLabel(parents)).Inc()
	}
}

// CountLaneSpends counts spent outputs that carried the position (carried) and those that did not
// (missing), with each store's meaning of carried as the spend_positions_total help states it.
func CountLaneSpends(store string, carried, missing int) {
	if carried > 0 {
		prometheusLaneSpendPositions.WithLabelValues(store, "yes").Add(float64(carried))
	}

	if missing > 0 {
		prometheusLaneSpendPositions.WithLabelValues(store, "no").Add(float64(missing))
	}
}
