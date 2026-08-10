package validator

import (
	"context"
	"net/url"
	"runtime"
	"sync"
	"testing"
	"time"

	batcher "github.com/bsv-blockchain/go-batcher/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// Test_ValidatorServer_Stop_DrainsBatcherBeforeStoppingProducers exercises the
// non-nil branches of validator.Server.Stop() that the existing nil-literal Stop
// tests never reach: the three async producer Stop() calls AND the ordering
// invariant that the tx-meta batcher is drained BEFORE the txmeta producer is
// stopped (so queued tx-meta flushes into the producer rather than being lost).
//
// Ordering is captured as a recorded event sequence: the batcher's flush appends
// "drain"; each producer's Stop appends "stop:<name>". The test asserts "drain"
// is the first event (i.e. it precedes every producer Stop) and that all three
// producers were stopped. It FAILS if the order is reversed or any Stop is
// skipped.
func Test_ValidatorServer_Stop_DrainsBatcherBeforeStoppingProducers(t *testing.T) {
	logger := ulogger.TestLogger{}

	var (
		mu      sync.Mutex
		events  []string
		drained int
	)

	record := func(name string) func() {
		return func() {
			mu.Lock()
			events = append(events, name)
			mu.Unlock()
		}
	}

	sendBatch := func(batch []*txmetaBatchItem) {
		mu.Lock()
		events = append(events, "drain")
		drained += len(batch)
		mu.Unlock()
	}

	// Large size + long timeout + no tick: queued items stay until the Stop-driven
	// drain flushes them, so "drain" is attributable to Stop(), not a timeout.
	b := batcher.NewWithPool(10_000, time.Hour, sendBatch, true, batcher.WithName("test_server_txmeta"))

	const n = 4
	for i := 0; i < n; i++ {
		h := chainhash.Hash{byte(i)}
		b.Put(&txmetaBatchItem{hash: &h, metaBytes: []byte{byte(i)}})
	}

	txmeta := &recordingProducer{onStop: record("stop:txmeta")}
	rejected := &recordingProducer{onStop: record("stop:rejected")}
	policy := &recordingProducer{onStop: record("stop:policy")}

	server := &Server{
		logger:                              logger,
		validator:                           &Validator{txmetaKafkaBatcher: b},
		txMetaKafkaProducerClient:           txmeta,
		rejectedTxKafkaProducerClient:       rejected,
		policyRejectedTxKafkaProducerClient: policy,
		// kafkaSignal and consumerClient left nil — those branches are skipped.
	}

	require.NoError(t, server.Stop(context.Background()))

	mu.Lock()
	defer mu.Unlock()

	require.NotEmpty(t, events)
	require.Equal(t, "drain", events[0], "the tx-meta batcher must be drained before any producer is stopped")
	require.ElementsMatch(t, []string{"stop:txmeta", "stop:rejected", "stop:policy"}, events[1:],
		"all three producers must be stopped after the drain")
	require.Equal(t, n, drained, "all queued tx-meta items must be flushed on Stop — no lost work")

	require.GreaterOrEqual(t, txmeta.stopCount(), 1)
	require.GreaterOrEqual(t, rejected.stopCount(), 1)
	require.GreaterOrEqual(t, policy.stopCount(), 1)
}

// Test_ValidatorServer_Stop_NoBatcher verifies Stop() still stops all three
// producers when the validator carries no tx-meta batcher (e.g. batching
// disabled) — the producer-stop branches must run regardless of the drain step.
func Test_ValidatorServer_Stop_NoBatcher(t *testing.T) {
	txmeta := &recordingProducer{}
	rejected := &recordingProducer{}
	policy := &recordingProducer{}

	server := &Server{
		logger:                              ulogger.TestLogger{},
		validator:                           &Validator{}, // no txmetaKafkaBatcher
		txMetaKafkaProducerClient:           txmeta,
		rejectedTxKafkaProducerClient:       rejected,
		policyRejectedTxKafkaProducerClient: policy,
	}

	require.NoError(t, server.Stop(context.Background()))

	require.Equal(t, 1, txmeta.stopCount())
	require.Equal(t, 1, rejected.stopCount())
	require.Equal(t, 1, policy.stopCount())
}

// spyPausableConsumer wraps a real KafkaConsumerGroup so pause/resume calls made by
// the controller are observable, while the calls still land on the real
// implementation (including its closed-consumer guard).
type spyPausableConsumer struct {
	*kafka.KafkaConsumerGroup

	mu      sync.Mutex
	pauses  int
	resumes int
}

func (s *spyPausableConsumer) PauseAll() {
	s.mu.Lock()
	s.pauses++
	s.mu.Unlock()

	s.KafkaConsumerGroup.PauseAll()
}

func (s *spyPausableConsumer) ResumeAll() {
	s.mu.Lock()
	s.resumes++
	s.mu.Unlock()

	s.KafkaConsumerGroup.ResumeAll()
}

func (s *spyPausableConsumer) counts() (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.pauses, s.resumes
}

// backpressureLifecycleServer builds a validator Server wired for backpressure with
// a real in-memory Kafka consumer and a queue-stats reader under the test's control.
// consumerCtx/consumerCancel are established the way Start does, so
// startKafkaBackpressure sees the same lifecycle it does in production.
func backpressureLifecycleServer(t *testing.T, topic string, headAge time.Duration) (*Server, *spyPausableConsumer, func()) {
	t.Helper()

	initPrometheusMetrics()

	kafkaURL, err := url.Parse("memory://localhost/" + topic)
	require.NoError(t, err)

	group, err := kafka.NewKafkaConsumerGroupFromURL(ulogger.TestLogger{}, kafkaURL, topic+"-group", true, nil)
	require.NoError(t, err)

	consumer := &spyPausableConsumer{KafkaConsumerGroup: group}

	baMock := blockassembly.NewMock()
	baMock.On("GetBlockAssemblyQueueStats", mock.Anything).
		Return(blockassembly.QueueStats{HeadAge: headAge}, nil).Maybe()

	tSettings := settings.NewSettings()
	tSettings.Validator.KafkaBackpressure = testBackpressureConfig()

	server := &Server{
		logger:              ulogger.TestLogger{},
		settings:            tSettings,
		consumerClient:      consumer,
		blockAssemblyClient: baMock,
	}

	return server, consumer, func() { _ = group.Close() }
}

// TestBackpressure_StopCancelsControllerGoroutine pins the A8 / bot-review defect:
// the controller goroutine used to be bound to the Start context while the consumer
// was bound to consumerCtx, so Stop's consumerCancel never reached it. It kept
// ticking, kept driving pause/resume against a closed consumer, never ran its
// resume-on-exit, and leaked one live goroutine per Start/Stop cycle.
//
// Binding it to consumerCtx makes the lifetimes identical by construction.
//
// The server, consumer and reader are built ONCE and only the consumerCtx +
// startKafkaBackpressure + consumerCancel triple is repeated, so the controller
// goroutine is the only thing each cycle allocates. Goroutine counting is a weak
// oracle in general; scoping it this way is what makes it a real assertion here
// rather than a measurement of unrelated test infrastructure.
func TestBackpressure_StopCancelsControllerGoroutine(t *testing.T) {
	const cycles = 8

	srv, _, cleanup := backpressureLifecycleServer(t, "mvp-controller-lifecycle", 50*time.Millisecond)
	defer cleanup()

	// One warm-up cycle so any lazily-created infrastructure goroutine (broker,
	// metrics registry, gRPC plumbing in the mock) exists before the baseline.
	srv.consumerCtx, srv.consumerCancel = context.WithCancel(context.Background())
	srv.startKafkaBackpressure(context.Background())
	srv.consumerCancel()

	// Give the warm-up cycle's goroutine time to observe the cancel and exit before
	// the baseline is taken. The post-loop assertion is the bounded-retry one; this
	// only has to be long enough that the baseline is not itself inflated.
	time.Sleep(100 * time.Millisecond)

	before := runtime.NumGoroutine()

	for i := 0; i < cycles; i++ {
		// Exactly what Start does...
		srv.consumerCtx, srv.consumerCancel = context.WithCancel(context.Background())
		srv.startKafkaBackpressure(context.Background())

		// ...and exactly what Stop does first, before closing the consumer.
		srv.consumerCancel()
	}

	// Every controller goroutine must be gone. Bounded settle loop rather than a
	// sleep: a goroutine observing ctx.Done() returns promptly but not
	// synchronously. Before the fix this grew by one per cycle.
	require.Eventually(t, func() bool {
		return runtime.NumGoroutine() <= before+1
	}, 5*time.Second, 20*time.Millisecond,
		"each Start/Stop cycle must end its controller goroutine; the leak grew with the cycle count (%d cycles)", cycles)
}

// TestBackpressure_ResumeOnExitRunsOnStop pins the consequence that mattered most:
// with the controller bound to the wrong context its deferred resume-on-exit never
// ran, so a shutdown while paused left the consumer paused. Bound to consumerCtx,
// the cancel that Stop issues before closing the consumer triggers the final resume
// while the client is still live.
func TestBackpressure_ResumeOnExitRunsOnStop(t *testing.T) {
	// Permanently hot signal, so the controller pauses and stays paused.
	server, consumer, cleanup := backpressureLifecycleServer(t, "mvp-controller-resume-on-exit", 5*time.Second)
	defer cleanup()

	server.consumerCtx, server.consumerCancel = context.WithCancel(context.Background())
	server.startKafkaBackpressure(context.Background())

	require.Eventually(t, func() bool {
		pauses, _ := consumer.counts()
		return pauses >= 1
	}, 5*time.Second, 10*time.Millisecond, "the controller should pause while the signal is hot")

	// Stop's first action.
	server.consumerCancel()

	require.Eventually(t, func() bool {
		_, resumes := consumer.counts()
		return resumes >= 1
	}, 5*time.Second, 10*time.Millisecond,
		"resume-on-exit must run on consumerCancel, so shutdown never leaves the consumer paused")
}
