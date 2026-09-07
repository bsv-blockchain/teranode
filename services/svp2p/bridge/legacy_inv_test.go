package bridge

import (
	"context"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	inmemorykafka "github.com/bsv-blockchain/teranode/util/kafka/in_memory_kafka"
	kafkamessage "github.com/bsv-blockchain/teranode/util/kafka/kafka_message"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// recordingInvHandler collects every InvHandler call so a test can assert
// positively on what arrived, mirroring recordingFinalHandler/
// recordingTxHandler's own shape in this package.
type recordingInvHandler struct {
	mu    sync.Mutex
	calls []invCall
}

type invCall struct {
	peerAddr string
	hashes   []chainhash.Hash
}

func (r *recordingInvHandler) handle(peerAddr string, hashes []chainhash.Hash) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.calls = append(r.calls, invCall{peerAddr: peerAddr, hashes: hashes})
}

func (r *recordingInvHandler) len() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.calls)
}

func (r *recordingInvHandler) at(i int) invCall {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.calls[i]
}

func (r *recordingInvHandler) hashes() []chainhash.Hash {
	r.mu.Lock()
	defer r.mu.Unlock()

	var out []chainhash.Hash
	for _, c := range r.calls {
		out = append(out, c.hashes...)
	}

	return out
}

func mustMarshalLegacyInv(t *testing.T, peerAddr string, hashes ...chainhash.Hash) []byte {
	t.Helper()

	msg := &kafkamessage.KafkaInvTopicMessage{PeerAddress: peerAddr}
	for _, h := range hashes {
		msg.Inv = append(msg.Inv, &kafkamessage.Inv{Type: kafkamessage.InvType_Tx, Hash: h.String()})
	}

	value, err := proto.Marshal(msg)
	require.NoError(t, err)

	return value
}

// alwaysFSM is a blockchain.ClientI fake reporting a fixed RUNNING answer
// (or a fixed error) for every IsFSMCurrentState call — enough for
// handleLegacyInvMessage's own RUNNING gate, without a real blockchain
// service. mu-guarded so a test can flip the answer between calls.
type alwaysFSM struct {
	mu      sync.Mutex
	running bool
	err     error

	// checked fires (non-blocking) every time IsFSMCurrentState is called,
	// carrying the value that call is about to return — a test's
	// synchronization point for "the gate has now been evaluated for this
	// message", used instead of a fixed sleep to avoid the exact race a
	// fixed wait would risk: whether a produced message is consumed before
	// or after a later state flip is otherwise unordered.
	checked chan bool
}

func (f *alwaysFSM) set(running bool, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.running, f.err = running, err
}

func (f *alwaysFSM) IsFSMCurrentState(context.Context, blockchain.FSMStateType) (bool, error) {
	f.mu.Lock()
	running, err := f.running, f.err
	f.mu.Unlock()

	if f.checked != nil {
		select {
		case f.checked <- running:
		default:
		}
	}

	return running, err
}

var _ blockchain.ClientI = (*fsmOnlyClient)(nil)

// fsmOnlyClient embeds blockchain.ClientI (every other method nil) and
// overrides only IsFSMCurrentState via alwaysFSM — the one method
// handleLegacyInvMessage's RUNNING gate calls.
type fsmOnlyClient struct {
	blockchain.ClientI
	fsm *alwaysFSM
}

func (c *fsmOnlyClient) IsFSMCurrentState(ctx context.Context, state blockchain.FSMStateType) (bool, error) {
	return c.fsm.IsFSMCurrentState(ctx, state)
}

func newFSMOnlyClient(running bool) *fsmOnlyClient {
	return &fsmOnlyClient{fsm: &alwaysFSM{running: running}}
}

// ---------------------------------------------------------------------------
// handleLegacyInvMessage: decode correctness AND the RUNNING gate, unit
// level — legacy applies this gate per-message, inside the handler, not by
// pausing the Kafka listener (see StartLegacyInvConsumer's own doc comment
// for the source citations behind that design, corrected after an initial
// wrong reading of the plan's H3).
// ---------------------------------------------------------------------------

func TestHandleLegacyInvMessage(t *testing.T) {
	logger := ulogger.TestLogger{}

	t.Run("a well-formed RUNNING message decodes and calls onInv with the peer address and every tx hash", func(t *testing.T) {
		handler := &recordingInvHandler{}
		client := newFSMOnlyClient(true)

		h1 := chainhash.Hash{0x01}
		h2 := chainhash.Hash{0x02}
		msg := &kafka.KafkaMessage{Value: mustMarshalLegacyInv(t, "1.2.3.4:8333", h1, h2)}

		err := handleLegacyInvMessage(context.Background(), logger, client, msg, handler.handle)
		require.NoError(t, err)
		require.Equal(t, 1, handler.len())

		call := handler.at(0)
		require.Equal(t, "1.2.3.4:8333", call.peerAddr)
		require.ElementsMatch(t, []chainhash.Hash{h1, h2}, call.hashes)
	})

	t.Run("non-tx inv entries are filtered out before onInv is called", func(t *testing.T) {
		handler := &recordingInvHandler{}
		client := newFSMOnlyClient(true)

		txHash := chainhash.Hash{0x01}
		blockMsg := &kafkamessage.KafkaInvTopicMessage{
			PeerAddress: "1.2.3.4:8333",
			Inv: []*kafkamessage.Inv{
				{Type: kafkamessage.InvType_Block, Hash: chainhash.Hash{0xFF}.String()},
				{Type: kafkamessage.InvType_Tx, Hash: txHash.String()},
			},
		}
		value, err := proto.Marshal(blockMsg)
		require.NoError(t, err)

		err = handleLegacyInvMessage(context.Background(), logger, client, &kafka.KafkaMessage{Value: value}, handler.handle)
		require.NoError(t, err)
		require.Equal(t, 1, handler.len())
		require.Equal(t, []chainhash.Hash{txHash}, handler.at(0).hashes, "the block entry must never reach onInv")
	})

	// TestHandleLegacyInvMessage_RunningGate is its own top-level test below
	// (E5-style, both directions) — kept out of this table so the
	// transition sequence reads clearly rather than as one more subtest.

	t.Run("an unparseable value is skipped, not retried", func(t *testing.T) {
		handler := &recordingInvHandler{}
		client := newFSMOnlyClient(true)

		err := handleLegacyInvMessage(context.Background(), logger, client, &kafka.KafkaMessage{Value: []byte("not a protobuf message")}, handler.handle)
		require.NoError(t, err)
		require.Zero(t, handler.len())
	})

	t.Run("a hash that does not parse is dropped but the rest of the message still reaches onInv", func(t *testing.T) {
		handler := &recordingInvHandler{}
		client := newFSMOnlyClient(true)

		good := chainhash.Hash{0x01}
		badMsg := &kafkamessage.KafkaInvTopicMessage{
			PeerAddress: "1.2.3.4:8333",
			Inv: []*kafkamessage.Inv{
				{Type: kafkamessage.InvType_Tx, Hash: "not-a-hash"},
				{Type: kafkamessage.InvType_Tx, Hash: good.String()},
			},
		}
		value, err := proto.Marshal(badMsg)
		require.NoError(t, err)

		err = handleLegacyInvMessage(context.Background(), logger, client, &kafka.KafkaMessage{Value: value}, handler.handle)
		require.NoError(t, err)
		require.Equal(t, 1, handler.len())
		require.Equal(t, []chainhash.Hash{good}, handler.at(0).hashes)
	})

	t.Run("no tx entries at all calls onInv for nothing, and never even reads FSM state", func(t *testing.T) {
		handler := &recordingInvHandler{}
		client := &blockchain.Mock{}
		// Deliberately NO .On("IsFSMCurrentState", ...) expectation set: if
		// handleLegacyInvMessage called it anyway, testify's Mock would
		// panic on the unexpected call — decode-and-filter must run BEFORE
		// the RUNNING gate, so a message with nothing answerable is never
		// worth an FSM round trip.

		emptyMsg := &kafkamessage.KafkaInvTopicMessage{PeerAddress: "1.2.3.4:8333"}
		value, err := proto.Marshal(emptyMsg)
		require.NoError(t, err)

		err = handleLegacyInvMessage(context.Background(), logger, client, &kafka.KafkaMessage{Value: value}, handler.handle)
		require.NoError(t, err)
		require.Zero(t, handler.len())
	})

	t.Run("a nil blockchain client fails closed: onInv is never called", func(t *testing.T) {
		handler := &recordingInvHandler{}

		hash := chainhash.Hash{0x01}
		msg := &kafka.KafkaMessage{Value: mustMarshalLegacyInv(t, "1.2.3.4:8333", hash)}

		err := handleLegacyInvMessage(context.Background(), logger, nil, msg, handler.handle)
		require.NoError(t, err)
		require.Zero(t, handler.len())
	})
}

// TestHandleLegacyInvMessage_RunningGateFlipsBothDirections is H6/H3
// together, at the handler level (this gate is applied per-message here,
// not by pausing a Kafka listener — see StartLegacyInvConsumer's own doc
// comment): not RUNNING drops the message without calling onInv, RUNNING
// lets an otherwise-identical message through, and flipping back to not
// RUNNING suppresses again — so this is proven to be a live gate reacting
// to each call, not a blanket refusal or a one-shot state.
func TestHandleLegacyInvMessage_RunningGateFlipsBothDirections(t *testing.T) {
	logger := ulogger.TestLogger{}
	handler := &recordingInvHandler{}
	client := newFSMOnlyClient(false)

	hash1 := chainhash.Hash{0x01}
	msg1 := &kafka.KafkaMessage{Value: mustMarshalLegacyInv(t, "1.2.3.4:8333", hash1)}

	err := handleLegacyInvMessage(context.Background(), logger, client, msg1, handler.handle)
	require.NoError(t, err, "not RUNNING must not be retried")
	require.Zero(t, handler.len(), "onInv must never be called while not RUNNING")

	client.fsm.set(true, nil)

	hash2 := chainhash.Hash{0x02}
	msg2 := &kafka.KafkaMessage{Value: mustMarshalLegacyInv(t, "1.2.3.4:8333", hash2)}

	err = handleLegacyInvMessage(context.Background(), logger, client, msg2, handler.handle)
	require.NoError(t, err)
	require.Equal(t, 1, handler.len(), "the RUNNING transition must let the next message reach onInv")
	require.Equal(t, []chainhash.Hash{hash2}, handler.at(0).hashes)

	client.fsm.set(false, nil)

	hash3 := chainhash.Hash{0x03}
	msg3 := &kafka.KafkaMessage{Value: mustMarshalLegacyInv(t, "1.2.3.4:8333", hash3)}

	err = handleLegacyInvMessage(context.Background(), logger, client, msg3, handler.handle)
	require.NoError(t, err)
	require.Equal(t, 1, handler.len(), "flipping back to not RUNNING must suppress again, not just once")
}

// TestHandleLegacyInvMessage_FSMErrorFailsClosed proves the fail-closed
// discipline: an error reading FSM state must not be treated as "keep the
// previous (RUNNING) answer".
func TestHandleLegacyInvMessage_FSMErrorFailsClosed(t *testing.T) {
	logger := ulogger.TestLogger{}
	handler := &recordingInvHandler{}
	client := newFSMOnlyClient(true)

	hash := chainhash.Hash{0x01}
	msg := &kafka.KafkaMessage{Value: mustMarshalLegacyInv(t, "1.2.3.4:8333", hash)}

	// First prove RUNNING really does let it through against this fixture.
	err := handleLegacyInvMessage(context.Background(), logger, client, msg, handler.handle)
	require.NoError(t, err)
	require.Equal(t, 1, handler.len())

	client.fsm.set(true, context.DeadlineExceeded)

	err = handleLegacyInvMessage(context.Background(), logger, client, msg, handler.handle)
	require.NoError(t, err)
	require.Equal(t, 1, handler.len(), "an FSM read error must fail closed, not fall back to the last-known RUNNING answer")
}

// ---------------------------------------------------------------------------
// StartLegacyInvConsumer: wiring, over a real in-memory Kafka broker. A
// PLAIN listener (see that function's own doc comment) — this is the
// blocks-final-shaped half of the test suite, not a controlled-listener one.
// ---------------------------------------------------------------------------

func TestStartLegacyInvConsumer_NilConfigIsANoOp(t *testing.T) {
	tSettings := settings.NewSettings()
	tSettings.Kafka.LegacyInvConfig = nil

	handler := &recordingInvHandler{}

	consumer, err := StartLegacyInvConsumer(t.Context(), ulogger.TestLogger{}, tSettings, nil, handler.handle)
	require.NoError(t, err)
	require.Nil(t, consumer, "an unconfigured topic must not build a consumer at all")
}

func TestStartLegacyInvConsumer_DecodesAndRelays(t *testing.T) {
	const topic = "legacy-inv-wiring-test"

	tSettings := settings.NewSettings()
	tSettings.ClientName = "svp2p-test"

	kafkaURL, err := url.Parse("memory://localhost:9092/" + topic)
	require.NoError(t, err)
	tSettings.Kafka.LegacyInvConfig = kafkaURL

	t.Cleanup(func() { inmemorykafka.GetSharedBroker().DropTopic(topic) })

	blockchainClient := &blockchain.Mock{}
	blockchainClient.Mock.On("IsFSMCurrentState", mock.Anything, mock.Anything).Return(true, nil)

	handler := &recordingInvHandler{}

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	consumer, err := StartLegacyInvConsumer(ctx, ulogger.TestLogger{}, tSettings, blockchainClient, handler.handle)
	require.NoError(t, err)
	require.NotNil(t, consumer)

	t.Cleanup(func() { require.NoError(t, consumer.Close()) })

	require.Eventually(t, func() bool {
		return inmemorykafka.GetSharedBroker().HasConsumer(topic)
	}, 5*time.Second, 10*time.Millisecond, "the legacy inv consumer never subscribed")

	hash := chainhash.Hash{0xAB}
	require.NoError(t, inmemorykafka.GetSharedBroker().Produce(ctx, topic, nil, mustMarshalLegacyInv(t, "9.9.9.9:8333", hash)))

	require.Eventually(t, func() bool {
		return handler.len() >= 1
	}, 5*time.Second, 10*time.Millisecond, "the produced message never reached onInv")

	call := handler.at(0)
	require.Equal(t, "9.9.9.9:8333", call.peerAddr)
	require.Equal(t, []chainhash.Hash{hash}, call.hashes)
}

// TestStartLegacyInvConsumer_AlwaysOnRegardlessOfFSM is the listener-level
// half of the corrected design: the consumer itself subscribes and stays
// subscribed whether or not the FSM is RUNNING — proving it is NOT a
// controlled listener — while the RUNNING gate (proven at the handler
// level above) still suppresses delivery for the not-RUNNING message and
// lets the RUNNING one through, in the same test.
func TestStartLegacyInvConsumer_AlwaysOnRegardlessOfFSM(t *testing.T) {
	const topic = "legacy-inv-always-on-test"

	tSettings := settings.NewSettings()
	tSettings.ClientName = "svp2p-test-alwayson"

	kafkaURL, err := url.Parse("memory://localhost:9092/" + topic)
	require.NoError(t, err)
	tSettings.Kafka.LegacyInvConfig = kafkaURL

	t.Cleanup(func() { inmemorykafka.GetSharedBroker().DropTopic(topic) })

	client := newFSMOnlyClient(false)
	// Buffered(1): the test reads exactly one notification per message, in
	// order, so it always knows precisely which message's gate check it is
	// synchronizing on.
	client.fsm.checked = make(chan bool, 1)

	handler := &recordingInvHandler{}

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	consumer, err := StartLegacyInvConsumer(ctx, ulogger.TestLogger{}, tSettings, client, handler.handle)
	require.NoError(t, err)
	require.NotNil(t, consumer)
	t.Cleanup(func() { require.NoError(t, consumer.Close()) })

	require.Eventually(t, func() bool {
		return inmemorykafka.GetSharedBroker().HasConsumer(topic)
	}, 5*time.Second, 10*time.Millisecond, "the legacy inv consumer must subscribe even while the FSM is not RUNNING")

	suppressedHash := chainhash.Hash{0x01}
	require.NoError(t, inmemorykafka.GetSharedBroker().Produce(ctx, topic, nil, mustMarshalLegacyInv(t, "1.1.1.1:8333", suppressedHash)))

	// Block until the gate has actually been evaluated for THIS message —
	// removing the race an unsynchronized "produce, then flip, then check"
	// sequence would have (whether msg1 is consumed before or after the
	// flip below is otherwise unordered).
	select {
	case wasRunning := <-client.fsm.checked:
		require.False(t, wasRunning, "the gate must have seen not-RUNNING for the first message")
	case <-time.After(5 * time.Second):
		t.Fatal("the RUNNING gate was never evaluated for the first message")
	}

	require.Zero(t, handler.len(), "the not-RUNNING message must not have reached onInv")

	client.fsm.set(true, nil)

	deliveredHash := chainhash.Hash{0x02}
	require.NoError(t, inmemorykafka.GetSharedBroker().Produce(ctx, topic, nil, mustMarshalLegacyInv(t, "2.2.2.2:8333", deliveredHash)))

	require.Eventually(t, func() bool {
		for _, h := range handler.hashes() {
			if h == deliveredHash {
				return true
			}
		}

		return false
	}, 5*time.Second, 20*time.Millisecond, "the RUNNING message must reach onInv")

	require.Equal(t, 1, handler.len(), "only the RUNNING message reached onInv — the suppressed one still never did")
	require.True(t, inmemorykafka.GetSharedBroker().HasConsumer(topic), "the consumer must still be subscribed — it was never paused")
}

// ---------------------------------------------------------------------------
// StartLegacyInvProducer: Produce + Stop (DC11 flush), over a real
// in-memory Kafka broker.
// ---------------------------------------------------------------------------

func TestStartLegacyInvProducer_NilConfigIsANoOp(t *testing.T) {
	tSettings := settings.NewSettings()
	tSettings.Kafka.LegacyInvConfig = nil

	producer, err := StartLegacyInvProducer(t.Context(), ulogger.TestLogger{}, tSettings)
	require.NoError(t, err)
	require.Nil(t, producer)

	// Produce and Stop on a nil producer must not panic: PeerManager.Inv and
	// Server.Stop call these unconditionally when the topic is unconfigured.
	require.NotPanics(t, func() { producer.Produce("1.2.3.4:8333", []chainhash.Hash{{0x01}}) })
	require.NoError(t, producer.Stop())
}

// TestStartLegacyInvProducer_ProducesAndStopFlushes proves the whole
// produce-then-flush contract in one place: a Produce call reaches the
// topic (a real consumer, forced RUNNING, picks it up), and Stop (the DC11
// flush) returns promptly rather than hanging with nothing in flight.
func TestStartLegacyInvProducer_ProducesAndStopFlushes(t *testing.T) {
	const topic = "legacy-inv-producer-test"

	tSettings := settings.NewSettings()
	tSettings.ClientName = "svp2p-test-producer"

	kafkaURL, err := url.Parse("memory://localhost:9092/" + topic)
	require.NoError(t, err)
	tSettings.Kafka.LegacyInvConfig = kafkaURL

	t.Cleanup(func() { inmemorykafka.GetSharedBroker().DropTopic(topic) })

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	producer, err := StartLegacyInvProducer(ctx, ulogger.TestLogger{}, tSettings)
	require.NoError(t, err)
	require.NotNil(t, producer)

	handler := &recordingInvHandler{}

	blockchainClient := &blockchain.Mock{}
	blockchainClient.Mock.On("IsFSMCurrentState", mock.Anything, mock.Anything).Return(true, nil)

	consumer, err := StartLegacyInvConsumer(ctx, ulogger.TestLogger{}, tSettings, blockchainClient, handler.handle)
	require.NoError(t, err)
	require.NotNil(t, consumer)
	t.Cleanup(func() { require.NoError(t, consumer.Close()) })

	require.Eventually(t, func() bool {
		return inmemorykafka.GetSharedBroker().HasConsumer(topic)
	}, 5*time.Second, 10*time.Millisecond, "the legacy inv consumer never subscribed")

	hash := chainhash.Hash{0xEF}
	producer.Produce("5.5.5.5:8333", []chainhash.Hash{hash})

	require.Eventually(t, func() bool {
		for _, h := range handler.hashes() {
			if h == hash {
				return true
			}
		}

		return false
	}, 5*time.Second, 10*time.Millisecond, "the produced message never reached the consumer")

	call := handler.at(0)
	require.Equal(t, "5.5.5.5:8333", call.peerAddr)

	stopped := make(chan struct{})

	go func() {
		defer close(stopped)

		require.NoError(t, producer.Stop())
	}()

	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("producer.Stop() (the DC11 flush) never returned")
	}
}
