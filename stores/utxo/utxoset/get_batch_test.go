package utxoset

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// overlapTracer holds the first statement it sees, once armed, until a second statement starts
// or the wait runs out, and records which happened. A batch whose two lookups run one after the
// other cannot start the second while the first is held, so it records no overlap.
type overlapTracer struct {
	armed      atomic.Bool
	started    atomic.Int32
	release    chan struct{}
	closeOnce  sync.Once
	overlapped atomic.Bool
}

func (o *overlapTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	if !o.armed.Load() {
		return ctx
	}

	switch o.started.Add(1) {
	case 1:
		select {
		case <-o.release:
			o.overlapped.Store(true)
		case <-time.After(3 * time.Second):
		}
	case 2:
		o.closeOnce.Do(func() { close(o.release) })
	}

	return ctx
}

func (o *overlapTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

// TestGetBatchRunsTheBodyAndNoBodyLookupsAtTheSameTime sends one batch with a read that wants
// the body and one that does not. The two halves are independent lookups, and run one after the
// other the body reader waits for the whole no-body lookup first, so they must be in flight
// together.
func TestGetBatchRunsTheBodyAndNoBodyLookupsAtTheSameTime(t *testing.T) {
	tracer := &overlapTracer{release: make(chan struct{})}
	poolTracer = tracer
	poolSpare = func(*pgxpool.Pool) bool { return true }

	t.Cleanup(func() { poolTracer, poolSpare = nil, poolHasSpareConn })

	s, ctx := newTestStore(t)

	a := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, a, 100)
	require.NoError(t, err)

	b := mkTx(t, 1, 6_000)
	_, err = s.Create(ctx, b, 100)
	require.NoError(t, err)

	noBody := &getItem{hash: *a.TxIDChainHash(), done: make(chan getResult, 1)}
	withBody := &getItem{hash: *b.TxIDChainHash(), wantBody: true, done: make(chan getResult, 1)}

	tracer.armed.Store(true)
	s.sendGetBatch([]*getItem{noBody, withBody})
	tracer.armed.Store(false)

	r := <-noBody.done
	require.NoError(t, r.err)
	require.NotNil(t, r.data)

	r = <-withBody.done
	require.NoError(t, r.err)
	require.NotNil(t, r.data)

	require.True(t, tracer.overlapped.Load(), "the body lookup started only after the no-body lookup finished")
}

// TestGetBatchRunsTheLookupsInTurnWithoutASpareConnection sends the same mixed batch when the pool
// has no spare connection. The second lookup would then wait for a connection in front of the
// spend and create writers, so the two lookups run one after the other on one connection, and
// each caller still gets its own answer.
func TestGetBatchRunsTheLookupsInTurnWithoutASpareConnection(t *testing.T) {
	tracer := &overlapTracer{release: make(chan struct{})}
	poolTracer = tracer
	poolSpare = func(*pgxpool.Pool) bool { return false }

	t.Cleanup(func() { poolTracer, poolSpare = nil, poolHasSpareConn })

	s, ctx := newTestStore(t)

	a := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, a, 100)
	require.NoError(t, err)

	b := mkTx(t, 1, 6_000)
	_, err = s.Create(ctx, b, 100)
	require.NoError(t, err)

	noBody := &getItem{hash: *a.TxIDChainHash(), done: make(chan getResult, 1)}
	withBody := &getItem{hash: *b.TxIDChainHash(), wantBody: true, done: make(chan getResult, 1)}

	inTurn := testutil.ToFloat64(getBatchHalves.WithLabelValues("in_turn"))

	tracer.armed.Store(true)
	s.sendGetBatch([]*getItem{noBody, withBody})
	tracer.armed.Store(false)

	r := <-noBody.done
	require.NoError(t, r.err)
	require.NotNil(t, r.data)
	require.Nil(t, r.data.Tx, "the no-body read gets no body")

	r = <-withBody.done
	require.NoError(t, r.err)
	require.NotNil(t, r.data)
	require.NotNil(t, r.data.Tx, "the body read gets its body")
	require.Equal(t, b.TxIDChainHash().String(), r.data.Tx.TxIDChainHash().String())

	require.False(t, tracer.overlapped.Load(), "with no spare connection the two lookups must not be in flight together")
	require.InDelta(t, inTurn+1, testutil.ToFloat64(getBatchHalves.WithLabelValues("in_turn")), 0)
}

// TestPoolHasSpareConnAndTheEmptyAcquireMetric holds every connection of a real pool. The spare
// check then says no, and a caller that asks for one more waits until a held connection is
// released, which utxoset_pool_empty_acquire_total counts. With that caller done, one connection
// is idle again and the check says yes.
func TestPoolHasSpareConnAndTheEmptyAcquireMetric(t *testing.T) {
	s, ctx := newTestStore(t)

	require.InDelta(t, float64(s.pool.Config().MaxConns), testutil.ToFloat64(poolMaxConns), 0, "the metrics read the open store's pool")
	require.True(t, poolHasSpareConn(s.pool))

	held := make([]*pgxpool.Conn, 0, s.pool.Config().MaxConns)
	t.Cleanup(func() {
		for _, c := range held {
			c.Release()
		}
	})

	for range s.pool.Config().MaxConns {
		c, err := s.pool.Acquire(ctx)
		require.NoError(t, err)

		held = append(held, c)
	}

	require.False(t, poolHasSpareConn(s.pool), "every connection is held")

	empty := testutil.ToFloat64(poolEmptyAcquires)

	got := make(chan *pgxpool.Conn, 1)

	go func() {
		c, err := s.pool.Acquire(ctx)
		if err != nil {
			got <- nil
			return
		}

		got <- c
	}()

	select {
	case <-got:
		t.Fatal("a caller got a connection while every connection was held")
	case <-time.After(100 * time.Millisecond):
	}

	held[len(held)-1].Release()
	held = held[:len(held)-1]

	// The waiting caller's connection joins held, so the cleanup releases it even when an
	// assertion below fails. Closing the store waits for every connection to come back.
	c := <-got
	require.NotNil(t, c, "the waiting caller gets the released connection")
	held = append(held, c)

	require.Greater(t, testutil.ToFloat64(poolEmptyAcquires), empty, "the wait for a connection is counted")
	require.False(t, poolHasSpareConn(s.pool), "the waiting caller took the released connection")

	held[len(held)-1].Release()
	held = held[:len(held)-1]

	require.True(t, poolHasSpareConn(s.pool), "one connection is idle again")
}
