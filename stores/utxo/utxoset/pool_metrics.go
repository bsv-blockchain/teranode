package utxoset

import (
	"sync/atomic"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// metricsPool is the pool of the store most recently opened, which the pool metrics below read
// at scrape time. A node opens one store, so this is its pool. Close clears it only if it is still
// this store's pool, so a test that opens a second store before closing the first keeps reading
// the second.
var metricsPool atomic.Pointer[pgxpool.Pool]

// poolStat reads one figure of the metrics pool's statistics, or zero when no store is open.
func poolStat(read func(*pgxpool.Stat) float64) func() float64 {
	return func() float64 {
		p := metricsPool.Load()
		if p == nil {
			return 0
		}

		return read(p.Stat())
	}
}

// The connection pool's own statistics. Every statement of the store, reads and writes alike,
// takes a connection from this one pool, so a caller that finds the pool empty waits behind the
// others. utxoset_pool_empty_acquire_total and utxoset_pool_empty_acquire_wait_seconds_total
// rising together means callers queue for connections; flat means the pool always had one free.
var (
	_ = promauto.NewCounterFunc(prometheus.CounterOpts{
		Name: "utxoset_pool_acquire_total",
		Help: "Connections taken from the utxoset pool",
	}, poolStat(func(s *pgxpool.Stat) float64 { return float64(s.AcquireCount()) }))

	poolEmptyAcquires = promauto.NewCounterFunc(prometheus.CounterOpts{
		Name: "utxoset_pool_empty_acquire_total",
		Help: "Connections taken from the utxoset pool that had to wait, because none was idle",
	}, poolStat(func(s *pgxpool.Stat) float64 { return float64(s.EmptyAcquireCount()) }))

	_ = promauto.NewCounterFunc(prometheus.CounterOpts{
		Name: "utxoset_pool_empty_acquire_wait_seconds_total",
		Help: "Time spent waiting for a connection from the utxoset pool when none was idle",
	}, poolStat(func(s *pgxpool.Stat) float64 { return s.EmptyAcquireWaitTime().Seconds() }))

	_ = promauto.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "utxoset_pool_acquired_conns",
		Help: "Connections of the utxoset pool in use now",
	}, poolStat(func(s *pgxpool.Stat) float64 { return float64(s.AcquiredConns()) }))

	poolMaxConns = promauto.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "utxoset_pool_max_conns",
		Help: "The most connections the utxoset pool may hold (pool_max_conns)",
	}, poolStat(func(s *pgxpool.Stat) float64 { return float64(s.MaxConns()) }))

	// getBatchHalves counts the mixed get batches by how their two lookups ran: "together" when
	// the pool had a spare connection for the second, "in_turn" when it did not.
	getBatchHalves = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "utxoset_get_batch_mixed_total",
		Help: "Get batches holding both body and no-body reads, by whether their two lookups ran together or in turn",
	}, []string{"mode"})
)

// poolHasSpareConn reports whether the pool can give out one more connection without the caller
// waiting: one is idle, or the pool is below its limit and will open a new one.
func poolHasSpareConn(p *pgxpool.Pool) bool {
	st := p.Stat()

	return st.IdleConns() > 0 || st.TotalConns() < st.MaxConns()
}

// poolSpare is the check sendGetBatch makes before it runs a mixed batch's second lookup on a
// connection of its own. A test seam: a test swaps it to force either answer.
var poolSpare = poolHasSpareConn
