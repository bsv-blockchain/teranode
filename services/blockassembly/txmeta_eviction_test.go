package blockassembly

import (
	"encoding/binary"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/txmetacache"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	inmemkafka "github.com/bsv-blockchain/teranode/util/kafka/in_memory_kafka"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// txMetaDeleteCollector consumes a txmeta topic on the in-memory Kafka broker and
// decodes the v1 wire format the way services/subtreevalidation's txmetaHandler
// does: [count LE32] then per entry [32 hash][1 action][LE32 contentLen][content].
type txMetaDeleteCollector struct {
	mu      sync.Mutex
	deletes map[chainhash.Hash]struct{}
}

func (c *txMetaDeleteCollector) handle(msg *kafka.KafkaMessage) error {
	data := msg.Value
	entries := binary.LittleEndian.Uint32(data[:4])
	offset := 4

	c.mu.Lock()
	defer c.mu.Unlock()

	for i := uint32(0); i < entries; i++ {
		var h chainhash.Hash

		copy(h[:], data[offset:offset+32])
		offset += 32

		action := data[offset]
		offset++

		contentLen := int(binary.LittleEndian.Uint32(data[offset:]))
		offset += 4 + contentLen

		if action == txmetacache.WireActionDELETE {
			c.deletes[h] = struct{}{}
		}
	}

	return nil
}

func (c *txMetaDeleteCollector) has(hashes ...chainhash.Hash) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, h := range hashes {
		if _, ok := c.deletes[h]; !ok {
			return false
		}
	}

	return true
}

func (c *txMetaDeleteCollector) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return len(c.deletes)
}

func newInMemoryTxMetaTopic(t *testing.T) (kafka.KafkaAsyncProducerI, *txMetaDeleteCollector) {
	t.Helper()

	topicURL, err := url.Parse(fmt.Sprintf("memory://localhost/ba-txmeta-evict-%d", time.Now().UnixNano()))
	require.NoError(t, err)

	logger := ulogger.TestLogger{}

	consumer, err := kafka.NewKafkaConsumerGroupFromURL(logger, topicURL, "ba-txmeta-evict-test", true, nil)
	require.NoError(t, err)

	collector := &txMetaDeleteCollector{deletes: map[chainhash.Hash]struct{}{}}

	consumer.Start(t.Context(), collector.handle)
	t.Cleanup(func() { _ = consumer.Close() })

	// The in-memory broker drops messages published before the consumer has
	// registered, and Start returns before it has.
	topic := strings.TrimPrefix(topicURL.Path, "/")
	require.Eventually(t, func() bool { return inmemkafka.GetSharedBroker().HasConsumer(topic) }, 2*time.Second, time.Millisecond,
		"in-memory consumer never registered for topic %s", topic)
	t.Cleanup(func() { inmemkafka.GetSharedBroker().DropTopic(topic) })

	producer, err := kafka.NewKafkaAsyncProducerFromURL(t.Context(), logger, topicURL, nil)
	require.NoError(t, err)

	producer.Start(t.Context(), make(chan *kafka.Message, 100))
	t.Cleanup(func() { _ = producer.Stop() })

	return producer, collector
}

// TestReplayPendingConflictIntents_ForwardEvictsDemotedFromTxMetaCache pins that
// replaying a forward intent publishes txmeta DELETEs for every tx the demotion
// marked conflicting (loser and cascaded child), not the winner. The store is a
// mock because ProcessConflicting deadlocks inside SetConflicting on sqlitememory.
func TestReplayPendingConflictIntents_ForwardEvictsDemotedFromTxMetaCache(t *testing.T) {
	winner := chainhash.HashH([]byte("replay-winner"))
	loser := chainhash.HashH([]byte("replay-loser"))
	child := chainhash.HashH([]byte("replay-child"))

	intent := utxo.ConflictIntent{
		Kind:        utxo.ConflictIntentForward,
		BlockHeight: 400,
		BlockHash:   chainhash.HashH([]byte("on-chain-block")),
		TxHashes:    []chainhash.Hash{winner},
		StartedAt:   1,
	}

	mockStore := &utxo.MockUtxostore{}
	mockStore.On("Get", mock.Anything, mock.Anything, mock.Anything).Return(&meta.Data{Conflicting: true}, nil)
	mockStore.On("GetCounterConflicting", mock.Anything, winner).Return([]chainhash.Hash{loser}, nil)
	mockStore.On("SetConflicting", mock.Anything, []chainhash.Hash{loser}, true).Return([]*utxo.Spend{}, []chainhash.Hash{child}, nil)
	mockStore.On("SetConflicting", mock.Anything, mock.Anything, mock.Anything).Return([]*utxo.Spend{}, []chainhash.Hash{}, nil)
	mockStore.On("Unspend", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mockStore.On("SpendAndCreate", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, []*utxo.Spend{}, nil)
	mockStore.On("SetLocked", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mockStore.On("BatchDecorate", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	bc := blockchainMembershipMock(true, false)
	bc.On("CheckBlockIsAncestorOfBlock", mock.Anything, mock.Anything, mock.Anything).Return(false, nil)

	producer, collector := newInMemoryTxMetaTopic(t)

	spy := &replaySpyStore{MockUtxostore: mockStore, pending: []utxo.ConflictIntent{intent}}
	b := newReplayTestAssembler(spy, bc)
	b.txMetaEvictor = subtreeprocessor.NewTxMetaEvictor(ulogger.TestLogger{}, producer)

	b.replayPendingConflictIntents(t.Context())

	require.Contains(t, spy.completed, intent.IntentID())
	require.Eventually(t, func() bool { return collector.has(loser, child) },
		5*time.Second, 10*time.Millisecond, "DELETEs for the loser and its cascaded child must reach the txmeta topic")
	require.False(t, collector.has(winner), "the promoted winner must not be evicted")
	require.Equal(t, 2, collector.count())
}

// TestReplayPendingConflictIntents_ReverseEvictsDemotedFromTxMetaCache pins the
// reverse direction: a replayed reverse flips the demoted winner (and its
// descendants) back to conflicting, and those must be evicted too.
func TestReplayPendingConflictIntents_ReverseEvictsDemotedFromTxMetaCache(t *testing.T) {
	demoted := chainhash.HashH([]byte("reverse-demoted"))
	child := chainhash.HashH([]byte("reverse-demoted-child"))

	demotedTx := bt.NewTx()
	require.NoError(t, demotedTx.From(chainhash.HashH([]byte("p")).String(), 0, "76a914000000000000000000000000000000000000000088ac", 1000))

	intent := utxo.ConflictIntent{
		Kind:        utxo.ConflictIntentReverse,
		BlockHeight: 401,
		TxHashes:    []chainhash.Hash{demoted},
		StartedAt:   1,
	}

	mockStore := &utxo.MockUtxostore{}
	mockStore.On("Get", mock.Anything, &demoted, mock.Anything).Return(&meta.Data{Tx: demotedTx}, nil)
	// parent has no conflicting children: nothing to promote
	mockStore.On("Get", mock.Anything, mock.Anything, mock.Anything).Return(&meta.Data{}, nil)
	mockStore.On("SetConflicting", mock.Anything, []chainhash.Hash{demoted}, true).Return([]*utxo.Spend{}, []chainhash.Hash{child}, nil)
	mockStore.On("SetConflicting", mock.Anything, mock.Anything, mock.Anything).Return([]*utxo.Spend{}, []chainhash.Hash{}, nil)
	mockStore.On("Unspend", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	producer, collector := newInMemoryTxMetaTopic(t)

	spy := &replaySpyStore{MockUtxostore: mockStore, pending: []utxo.ConflictIntent{intent}}
	// unknown block => off chain => a reverse intent is not stale and replays as is
	b := newReplayTestAssembler(spy, blockchainMembershipMock(false, true))
	b.txMetaEvictor = subtreeprocessor.NewTxMetaEvictor(ulogger.TestLogger{}, producer)

	b.replayPendingConflictIntents(t.Context())

	require.Contains(t, spy.completed, intent.IntentID())
	require.Eventually(t, func() bool { return collector.has(demoted, child) },
		5*time.Second, 10*time.Millisecond, "DELETEs for the demoted tx and its cascaded child must reach the txmeta topic")
	require.Equal(t, 2, collector.count())
}

// TestWithTxMetaProducer pins that a typed-nil producer (what the daemon passes
// when no txmeta topic is configured and the getter returns a nil pointer)
// is treated as "no producer", not as a live one that would panic on Publish.
func TestWithTxMetaProducer(t *testing.T) {
	t.Run("typed nil is no producer", func(t *testing.T) {
		var typedNil *kafka.KafkaAsyncProducer

		ba := &BlockAssembly{}
		WithTxMetaProducer(typedNil)(ba)

		require.Nil(t, ba.txMetaProducer)
	})

	t.Run("real producer is kept", func(t *testing.T) {
		producer, _ := newInMemoryTxMetaTopic(t)

		ba := &BlockAssembly{}
		WithTxMetaProducer(producer)(ba)

		require.Same(t, producer, ba.txMetaProducer)
	})
}

// TestMarkAsConflicting_EvictsMarkedFromTxMetaCache pins that the reload-time
// conflict marking publishes txmeta DELETEs for every tx it flips, the input and
// its whole cascade, like the demotion paths do. Otherwise subtree validation
// keeps serving them as non-conflicting from its cache.
func TestMarkAsConflicting_EvictsMarkedFromTxMetaCache(t *testing.T) {
	parentHash := chainhash.HashH([]byte("mark-parent"))
	childHash := chainhash.HashH([]byte("mark-child"))

	mockStore := new(utxo.MockUtxostore)
	mockStore.On("SetConflicting", mock.Anything, []chainhash.Hash{parentHash}, true).
		Return([]*utxo.Spend{}, []chainhash.Hash{childHash}, nil)
	mockStore.On("SetConflicting", mock.Anything, []chainhash.Hash{childHash}, true).
		Return([]*utxo.Spend{}, []chainhash.Hash{}, nil)

	mockStp := &subtreeprocessor.MockSubtreeProcessor{}
	mockStp.On("Remove", mock.Anything, mock.Anything).Return(nil)

	producer, collector := newInMemoryTxMetaTopic(t)

	ba := &BlockAssembler{
		utxoStore:        mockStore,
		subtreeProcessor: mockStp,
		settings:         &settings.Settings{},
		logger:           ulogger.TestLogger{},
		txMetaEvictor:    subtreeprocessor.NewTxMetaEvictor(ulogger.TestLogger{}, producer),
	}

	ba.markAsConflicting(t.Context(), parentHash)

	require.Eventually(t, func() bool { return collector.has(parentHash, childHash) }, 5*time.Second, 10*time.Millisecond,
		"every tx markAsConflicting flips must be evicted from subtree validation's txmeta cache")
	require.Equal(t, 2, collector.count())
}
