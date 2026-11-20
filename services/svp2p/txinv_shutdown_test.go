package svp2p

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/svp2p/svp2ptest"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	inmemorykafka "github.com/bsv-blockchain/teranode/util/kafka/in_memory_kafka"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// erroringConsumerGroup satisfies kafka.KafkaConsumerGroupI (the same
// narrow interface s.blocksFinalConsumer is typed as) and always fails on
// Close — a stand-in for a real Kafka client that cannot cleanly close
// (an in-flight fetch, a wedged broker), forcing Server.Stop down its
// early-return path.
type erroringConsumerGroup struct{}

func (erroringConsumerGroup) Start(context.Context, func(message *kafka.KafkaMessage) error, ...kafka.ConsumerOption) {
}
func (erroringConsumerGroup) BrokersURL() []string { return nil }
func (erroringConsumerGroup) Close() error {
	return errors.NewProcessingError("erroringConsumerGroup: Close always fails, by design")
}
func (erroringConsumerGroup) PauseAll()  {}
func (erroringConsumerGroup) ResumeAll() {}

// debugRecordingLogger wraps ulogger.TestLogger, recording every Debugf
// call — this test's way of observing that a Kafka-delivered inv actually
// reached PeerManager.InvFromKafka (which logs at Debug on its
// departed-peer drop path; there is no connected peer in this test, so
// that log line is the one observable side effect delivery leaves behind).
type debugRecordingLogger struct {
	ulogger.TestLogger

	mu    sync.Mutex
	lines []string
}

func (l *debugRecordingLogger) Debugf(format string, args ...interface{}) {
	l.mu.Lock()
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
	l.mu.Unlock()
}

func (l *debugRecordingLogger) contains(substr string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, line := range l.lines {
		if strings.Contains(line, substr) {
			return true
		}
	}

	return false
}

// newShutdownFlushTestServer is newTestServer's own body, with one
// difference: a debugRecordingLogger instead of a bare ulogger.TestLogger,
// so this file's test can observe a Kafka-delivered message reaching
// PeerManager.InvFromKafka without adding a recording seam to production
// code.
func newShutdownFlushTestServer(t *testing.T) (*Server, *debugRecordingLogger) {
	t.Helper()

	logger := &debugRecordingLogger{}

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.ListenAddresses = []string{"127.0.0.1:0"}
	tSettings.Legacy.GRPCListenAddress = svp2ptest.FreePort(t)
	tSettings.Legacy.WorkingDir = t.TempDir()
	tSettings.GRPCAdminAPIKey = "test-admin-key"

	store, err := blockchain_store.NewStore(logger, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	blockchainClient, err := blockchain.NewLocalClient(logger, tSettings, store, nil, nil)
	require.NoError(t, err)

	return New(logger, tSettings, blockchainClient), logger
}

// TestServerStopFlushesLegacyInvProducerDespiteAnEarlyReturn is fix round
// 2's Important 1: an earlier version of Server.Stop ran
// legacyInvProducer.Stop() (the DC11 flush) and legacyInvConsumer.Close()
// inline near the end, behind blocksFinalConsumer.Close's own early
// return — so a real Kafka client that fails to close cleanly (forced here
// by swapping in erroringConsumerGroup) meant the DC11 flush never ran at
// all, silently dropping whatever was still queued. Both are now released
// from Server.Stop's defer instead, producer BEFORE consumer (see that
// defer's own doc comment for why the order is deliberate).
//
// Proved positively, on the REAL wiring, not a fake standing in for it: a
// tx inv produced right before Stop is called is picked up by the
// still-running legacy-inv consumer — which the defer keeps alive until
// AFTER the producer has been flushed — and reaches
// PeerManager.InvFromKafka for real, observed via its own departed-peer
// debug log line (there is no connected peer in this test, so that is the
// one side effect delivery leaves behind). Also checked directly:
// Stop() surfaces the forced error, and both fields are nil'd regardless.
func TestServerStopFlushesLegacyInvProducerDespiteAnEarlyReturn(t *testing.T) {
	srv, logger := newShutdownFlushTestServer(t)

	const invTopic = "legacy-inv-shutdown-flush-test"
	const finalTopic = "blocks-final-shutdown-flush-test"

	invURL, err := url.Parse("memory://localhost:9092/" + invTopic)
	require.NoError(t, err)
	srv.settings.Kafka.LegacyInvConfig = invURL

	finalURL, err := url.Parse("memory://localhost:9092/" + finalTopic)
	require.NoError(t, err)
	srv.settings.Kafka.BlocksFinalConfig = finalURL

	t.Cleanup(func() {
		inmemorykafka.GetSharedBroker().DropTopic(invTopic)
		inmemorykafka.GetSharedBroker().DropTopic(finalTopic)
	})

	cancel := startServer(t, srv)
	defer cancel()

	require.Eventually(t, func() bool {
		return inmemorykafka.GetSharedBroker().HasConsumer(invTopic)
	}, 5*time.Second, 10*time.Millisecond, "the legacy inv consumer never subscribed")

	// A tx inv "announced right before shutdown": srv.legacyInvProducer is
	// reached directly (same package as Server, private field) rather than
	// through a real peer/wire round trip, because newTestServer builds no
	// block-ingestion deps — PeerManager.Inv would never reach OnInv/Produce
	// without a BlockDownloader, which this test does not need: what is
	// under test is Server.Stop's flush ordering, not the produce path
	// itself (already covered by protocol/tx_inv_test.go and
	// bridge/legacy_inv_test.go).
	require.NotNil(t, srv.legacyInvProducer, "the producer must be built once LegacyInvConfig is set")

	const departedPeerAddr = "9.9.9.9:8333"
	srv.legacyInvProducer.Produce(departedPeerAddr, []chainhash.Hash{{0xAB}})

	// Force Server.Stop's blocksFinalConsumer.Close() branch to fail, so
	// the rest of Stop's body never runs the ordinary way — only the defer
	// does.
	srv.blocksFinalConsumer = erroringConsumerGroup{}

	require.Error(t, srv.Stop(context.Background()), "Stop must surface the forced Close error")

	require.Nil(t, srv.legacyInvProducer, "the producer field must still be released")
	require.Nil(t, srv.legacyInvConsumer, "the consumer field must still be released")

	// The departed-peer line is written by the consumer goroutine after the
	// flush; Stop returning does not order it, so poll rather than assert once.
	require.Eventually(t, func() bool { return logger.contains(departedPeerAddr) }, 5*time.Second, 10*time.Millisecond,
		"the flushed message must still reach PeerManager.InvFromKafka (observed via its departed-peer log line), despite the forced early return")
}
