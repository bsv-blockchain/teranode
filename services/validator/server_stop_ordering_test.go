package validator

import (
	"context"
	"sync"
	"testing"
	"time"

	batcher "github.com/bsv-blockchain/go-batcher/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/ulogger"
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
