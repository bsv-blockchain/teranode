// Package sql provides a SQL-based implementation of the UTXO store interface.
// It supports both PostgreSQL and SQLite backends with automatic schema creation
// and migration.
//
// # Features
//
//   - Full UTXO lifecycle management (create, spend, unspend)
//   - Transaction metadata storage
//   - Input/output tracking
//   - Block height and median time tracking
//   - Optional UTXO expiration with automatic cleanup
//   - Prometheus metrics integration
//   - Support for the alert system (freeze/unfreeze/reassign UTXOs)
//
// # Usage
//
//	store, err := sql.New(ctx, logger, settings, &url.URL{
//	    Scheme: "postgres",
//	    Host:   "localhost:5432",
//	    User:   "user",
//	    Path:   "dbname",
//	    RawQuery: "expiration=1h",
//	})
//
// # Database Schema
//
// The store uses the following tables:
//   - transactions: Stores base transaction data
//   - inputs: Stores transaction inputs with previous output references
//   - outputs: Stores transaction outputs and UTXO state
//   - block_ids: Stores which blocks a transaction appears in
//
// # Metrics
//
// The following Prometheus metrics are exposed:
//   - teranode_sql_utxo_get: Number of UTXO retrieval operations
//   - teranode_sql_utxo_spend: Number of UTXO spend operations
//   - teranode_sql_utxo_reset: Number of UTXO reset operations
//   - teranode_sql_utxo_delete: Number of UTXO delete operations
//   - teranode_sql_utxo_errors: Number of errors by function and type
package sql

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	prometheusUtxoGet    prometheus.Counter
	prometheusUtxoSpend  prometheus.Counter
	prometheusUtxoReset  prometheus.Counter
	prometheusUtxoDelete prometheus.Counter
	prometheusUtxoErrors *prometheus.CounterVec

	prometheusUtxoPartialSpendRollbacks *prometheus.CounterVec
	prometheusUtxoSpendRollbackFailed   prometheus.Counter
	prometheusUtxoSpendAbortInFlight    prometheus.Counter

	prometheusSQLUtxoGetCounterConflicting prometheus.Histogram
	prometheusSQLUtxoGetConflicting        prometheus.Histogram

	// only init the metrics once
	prometheusMetricsInitOnce sync.Once
)

func initPrometheusMetrics() {
	prometheusMetricsInitOnce.Do(_initPrometheusMetrics)
}

func _initPrometheusMetrics() {
	prometheusUtxoGet = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "sql",
			Name:      "utxo_get",
			Help:      "Number of utxo get calls done to sql",
		},
	)
	prometheusUtxoSpend = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "sql",
			Name:      "utxo_spend",
			Help:      "Number of utxo spend calls done to sql",
		},
	)
	prometheusUtxoReset = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "sql",
			Name:      "utxo_reset",
			Help:      "Number of utxo reset calls done to sql",
		},
	)
	prometheusUtxoDelete = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "sql",
			Name:      "utxo_delete",
			Help:      "Number of utxo delete calls done to sql",
		},
	)
	// Mirrors the aerospike counters: rolling back partial spends is a
	// corruption-prevention invariant (#1214), so its outcome must be observable.
	// "fired" = spends reverted, "spender_exists" = ref was not dangling and the
	// slots were left alone, "indeterminate" = the existence probe failed and a
	// ref may have been left behind.
	prometheusUtxoPartialSpendRollbacks = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "sql",
			Name:      "utxo_partial_spend_rollbacks",
			Help:      "Outcome of rolling back the successful spends of a failed spend batch (fired|spender_exists|indeterminate|transient_lock|transient_creating)",
		},
		[]string{
			"outcome", // utxo.RollbackOutcomes: fired | spender_exists | indeterminate | transient_lock | transient_creating
		},
	)

	prometheusUtxoSpendRollbackFailed = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "sql",
			Name:      "utxo_spend_rollback_failed",
			Help:      "Spends that could not be reverted by a partial-spend rollback, i.e. potential dangling spender refs left behind (a count of spends, not of calls)",
		},
	)

	// Mirrors the aerospike counter of the same name: a give-up on a per-item
	// wait (ctx.Done() or the per-item timer) inside the batched Spend leaves
	// that item still enqueued in the batcher, so a straggler UPDATE can land
	// after the rollback above has already run — the same #1214 dangling
	// reference this rollback exists to prevent, just uncounted until now.
	prometheusUtxoSpendAbortInFlight = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "sql",
			Name:      "utxo_spend_abort_in_flight",
			Help:      "Spend items still in flight when the batch aborted, excluded from the rollback set and applied by the batcher afterwards (residual dangling-ref window, see #1291)",
		},
	)

	prometheusUtxoErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "sql",
			Name:      "utxo_errors",
			Help:      "Number of utxo errors",
		},
		[]string{
			"function", // function raising the error
			"error",    // error returned
		},
	)

	prometheusSQLUtxoGetCounterConflicting = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "sql",
			Name:      "utxo_get_counter_conflicting",
			Help:      "Histogram of utxo get counter conflicting calls done to sql",
		},
	)

	prometheusSQLUtxoGetConflicting = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "sql",
			Name:      "utxo_get_conflicting",
			Help:      "Histogram of utxo get conflicting calls done to sql",
		},
	)
}
