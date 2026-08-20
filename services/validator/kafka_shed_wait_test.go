package validator

// Tests for the Kafka consumer's bounded shed-wait.
//
// A Kafka-ingested transaction has ALREADY been acknowledged to its submitter
// (propagation returns success as soon as the message is published), so the
// handler must never shed it by returning an error: this consumer runs with
// WithLogErrorAndMoveOn, which logs the error and commits the offset, silently
// destroying the transaction. It waits instead — pushing overload into consumer
// lag — but the wait is bounded, because the topic is retention-bounded and a
// blocked handler pins its records in memory. Past the deadline the transaction
// is force-processed (bounded queue overshoot) rather than dropped.

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// countingGateValidator is a validator whose back-pressure verdict is scripted:
// it reports overloaded for the first overloadedFor observations and clears
// afterwards, recording how many times it was asked.
type countingGateValidator struct {
	*MockValidator

	overloadedFor int32
	observations  atomic.Int32
}

func (v *countingGateValidator) IngestOverloaded() bool {
	return v.observations.Add(1) <= v.overloadedFor
}

func newShedWaitServer(v Interface) *Server {
	return &Server{
		logger:    ulogger.TestLogger{},
		validator: v,
	}
}

func TestAwaitIngestCapacity(t *testing.T) {
	initPrometheusMetrics()

	t.Run("no wait when capacity is available", func(t *testing.T) {
		gv := &countingGateValidator{MockValidator: &MockValidator{}, overloadedFor: 0}
		s := newShedWaitServer(gv)
		options := &Options{AddTXToBlockAssembly: true}

		require.NoError(t, s.awaitIngestCapacity(context.Background(), "txid", options, time.Now().Add(kafkaShedMaxWait)))
		require.False(t, options.SkipBackpressure, "an unshed transaction must not be force-processed")
		require.EqualValues(t, 1, gv.observations.Load(), "the verdict is consulted once when there is capacity")
	})

	t.Run("waits out a shed and then proceeds normally", func(t *testing.T) {
		// Overloaded on the first two observations, clear afterwards: the
		// handler must wait rather than force-process.
		gv := &countingGateValidator{MockValidator: &MockValidator{}, overloadedFor: 2}
		s := newShedWaitServer(gv)
		options := &Options{AddTXToBlockAssembly: true}

		start := time.Now()
		require.NoError(t, s.awaitIngestCapacity(context.Background(), "txid", options, time.Now().Add(kafkaShedMaxWait)))

		require.False(t, options.SkipBackpressure, "capacity returned before the deadline: no force-processing")
		require.GreaterOrEqual(t, time.Since(start), kafkaShedPollInterval, "the handler must actually hold the transaction")
	})

	t.Run("force-processes rather than dropping once the wait is exhausted", func(t *testing.T) {
		gv := &countingGateValidator{MockValidator: &MockValidator{}, overloadedFor: 1 << 30}
		s := newShedWaitServer(gv)
		options := &Options{AddTXToBlockAssembly: true}

		// Deadline already passed: a sustained shed must not turn into a drop.
		require.NoError(t, s.awaitIngestCapacity(context.Background(), "txid", options, time.Now().Add(-time.Second)))
		require.True(t, options.SkipBackpressure, "an acknowledged transaction must be force-processed, never shed")
	})

	t.Run("returns on shutdown instead of holding the record", func(t *testing.T) {
		gv := &countingGateValidator{MockValidator: &MockValidator{}, overloadedFor: 1 << 30}
		s := newShedWaitServer(gv)
		options := &Options{AddTXToBlockAssembly: true}

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := s.awaitIngestCapacity(ctx, "txid", options, time.Now().Add(time.Hour))
		require.Error(t, err)
		require.True(t, errors.Is(err, context.Canceled), "expected the context error, got: %v", err)
		require.False(t, options.SkipBackpressure)
	})

	t.Run("a validator that cannot report the verdict never waits", func(t *testing.T) {
		s := newShedWaitServer(&MockValidator{})
		options := &Options{AddTXToBlockAssembly: true}

		require.NoError(t, s.awaitIngestCapacity(context.Background(), "txid", options, time.Now().Add(-time.Hour)))
		require.False(t, options.SkipBackpressure)
	})
}
