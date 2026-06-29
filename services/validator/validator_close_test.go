package validator

import (
	"context"
	"sync"
	"testing"
	"time"

	batcher "github.com/bsv-blockchain/go-batcher/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/stretchr/testify/require"
)

// recordingProducer is a KafkaAsyncProducerI test double that records each Stop()
// call (count + an optional callback used to capture the global teardown order).
// Stop is safe to call more than once.
type recordingProducer struct {
	mu     sync.Mutex
	stops  int
	onStop func()
}

func (r *recordingProducer) Start(_ context.Context, _ chan *kafka.Message) {}

func (r *recordingProducer) Stop() error {
	r.mu.Lock()
	r.stops++
	r.mu.Unlock()

	if r.onStop != nil {
		r.onStop()
	}

	return nil
}

func (r *recordingProducer) BrokersURL() []string             { return nil }
func (r *recordingProducer) Publish(_ *kafka.Message)         {}
func (r *recordingProducer) TryPublish(_ *kafka.Message) bool { return false }

func (r *recordingProducer) stopCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.stops
}

// Test_Validator_Close_DrainsBatcherBeforeStoppingProducers verifies the local
// validator teardown invariant: Close() drains the tx-meta batcher FIRST (so
// queued tx-meta is flushed, not lost), and only THEN stops the three async
// producers. This mirrors validator.Server.Stop()'s ordering for the
// UseLocalValidator path where the daemon owns the *Validator directly.
func Test_Validator_Close_DrainsBatcherBeforeStoppingProducers(t *testing.T) {
	logger := ulogger.TestLogger{}

	var (
		mu      sync.Mutex
		events  []string
		drained [][]*txmetaBatchItem
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
		drained = append(drained, batch)
		mu.Unlock()
	}

	// Large size + long timeout + no tick: items stay queued until Close() drains
	// them, so the flush is attributable to Close (not an async timeout flush).
	b := batcher.NewWithPool(10_000, time.Hour, sendBatch, true, batcher.WithName("test_txmeta"))

	txmeta := &recordingProducer{onStop: record("stop:txmeta")}
	rejected := &recordingProducer{onStop: record("stop:rejected")}
	policy := &recordingProducer{onStop: record("stop:policy")}

	v := &Validator{
		logger:                              logger,
		txmetaKafkaBatcher:                  b,
		txmetaKafkaProducerClient:           txmeta,
		rejectedTxKafkaProducerClient:       rejected,
		policyRejectedTxKafkaProducerClient: policy,
	}

	const n = 5
	for i := 0; i < n; i++ {
		h := chainhash.Hash{byte(i)}
		b.Put(&txmetaBatchItem{hash: &h, metaBytes: []byte{byte(i)}})
	}

	require.NoError(t, v.Close())

	mu.Lock()
	defer mu.Unlock()

	require.NotEmpty(t, events)
	require.Equal(t, "drain", events[0], "the batcher must be drained before any producer is stopped")
	require.ElementsMatch(t, []string{"stop:txmeta", "stop:rejected", "stop:policy"}, events[1:],
		"all three producers must be stopped after the drain")

	total := 0
	for _, batch := range drained {
		total += len(batch)
	}

	require.Equal(t, n, total, "all queued tx-meta items must be flushed on Close — no lost work")
}

// Test_Validator_Close_Idempotent verifies Close() is safe to call more than
// once: the batcher Close and producer Stop are idempotent, so a repeated Close
// (or an overlap with Server.Stop()) must neither panic nor hang.
func Test_Validator_Close_Idempotent(t *testing.T) {
	logger := ulogger.TestLogger{}

	b := batcher.NewWithPool(10_000, time.Hour, func(_ []*txmetaBatchItem) {}, true, batcher.WithName("test_txmeta_idem"))

	txmeta := &recordingProducer{}
	rejected := &recordingProducer{}
	policy := &recordingProducer{}

	v := &Validator{
		logger:                              logger,
		txmetaKafkaBatcher:                  b,
		txmetaKafkaProducerClient:           txmeta,
		rejectedTxKafkaProducerClient:       rejected,
		policyRejectedTxKafkaProducerClient: policy,
	}

	require.NoError(t, v.Close())

	// Second Close must return promptly (a deadlock would manifest as a hang).
	done := make(chan error, 1)
	go func() { done <- v.Close() }()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("second Close() did not return — not idempotent (likely a deadlock)")
	}

	require.GreaterOrEqual(t, txmeta.stopCount(), 1)
	require.GreaterOrEqual(t, rejected.stopCount(), 1)
	require.GreaterOrEqual(t, policy.stopCount(), 1)
}

// Test_Validator_Close_NilFields verifies Close() is nil-safe — a Validator with
// no batcher and no producers (e.g. a minimally-constructed test instance) must
// Close without panicking.
func Test_Validator_Close_NilFields(t *testing.T) {
	v := &Validator{logger: ulogger.TestLogger{}}
	require.NoError(t, v.Close())
}
