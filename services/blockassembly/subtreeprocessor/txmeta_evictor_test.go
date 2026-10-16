package subtreeprocessor

import (
	"encoding/binary"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/txmetacache"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	inmemkafka "github.com/bsv-blockchain/teranode/util/kafka/in_memory_kafka"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// txMetaTopicCollector reads a txmeta topic on the in-memory Kafka broker and
// decodes it the way services/subtreevalidation's txmetaHandler does: v1 and v2
// are told apart by the full header signature, and every entry is
// [(8 xxhash, v2 only)][32 hash][1 action][LE32 contentLen][content].
type txMetaTopicCollector struct {
	mu      sync.Mutex
	deletes []chainhash.Hash
	adds    int
}

func (c *txMetaTopicCollector) handle(msg *kafka.KafkaMessage) error {
	data := msg.Value

	var (
		offset  int
		entries uint32
		isV2    bool
	)

	if len(data) >= txmetacache.WireV2HeaderLen && data[0] == txmetacache.WireV2Magic &&
		data[1] == txmetacache.WireV2Version && data[2] == 0 && data[3] == 0 {
		candidate := binary.LittleEndian.Uint32(data[4:])
		if uint64(candidate)*uint64(txmetacache.WireV2MinEntrySize) <= uint64(len(data)-txmetacache.WireV2HeaderLen) {
			entries, offset, isV2 = candidate, txmetacache.WireV2HeaderLen, true
		}
	}

	if !isV2 {
		entries, offset = binary.LittleEndian.Uint32(data[:4]), 4
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for i := uint32(0); i < entries; i++ {
		if isV2 {
			offset += 8
		}

		var h chainhash.Hash

		copy(h[:], data[offset:offset+32])
		offset += 32

		action := data[offset]
		offset++

		contentLen := int(binary.LittleEndian.Uint32(data[offset:]))
		offset += 4 + contentLen

		if action == txmetacache.WireActionDELETE {
			c.deletes = append(c.deletes, h)
		} else {
			c.adds++
		}
	}

	return nil
}

func (c *txMetaTopicCollector) deleted() map[chainhash.Hash]struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()

	out := make(map[chainhash.Hash]struct{}, len(c.deletes))
	for _, h := range c.deletes {
		out[h] = struct{}{}
	}

	return out
}

// newInMemoryTxMetaTopic returns a started in-memory Kafka producer on a unique
// txmeta topic and a collector consuming that topic.
func newInMemoryTxMetaTopic(t *testing.T) (kafka.KafkaAsyncProducerI, *txMetaTopicCollector) {
	t.Helper()

	topicURL, err := url.Parse(fmt.Sprintf("memory://localhost/txmeta-evict-%d", time.Now().UnixNano()))
	require.NoError(t, err)

	logger := ulogger.TestLogger{}

	consumer, err := kafka.NewKafkaConsumerGroupFromURL(logger, topicURL, "txmeta-evict-test", true, nil)
	require.NoError(t, err)

	collector := &txMetaTopicCollector{}

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

// conflictScenario names the txs of a demotion: winner W (mined in the block, a
// double spend of loser L), loser L (the mempool spender W displaces) and child C
// (spends L's output, so the cascade reaches it).
type conflictScenario struct {
	loser, child, winner chainhash.Hash
}

// newConflictTestProcessor builds a processor over a mocked UTXO store whose
// SetConflicting reports C as L's spending child.
//
// The mock is deliberate: the sqlitememory store deadlocks inside SetConflicting
// (it reads through a second pooled connection on shared-cache SQLite while its
// own write transaction is open), so ProcessConflicting cannot run end to end on
// it. The existing TestProcessConflictingTransactions makes the same trade.
// failWinnerLookup makes the winner's Get fail so ProcessConflicting aborts
// before mutating anything.
func newConflictTestProcessor(t *testing.T, evictor *TxMetaEvictor, failWinnerLookup bool) (*SubtreeProcessor, conflictScenario) {
	t.Helper()

	scenario := conflictScenario{
		loser:  chainhash.HashH([]byte("loser")),
		child:  chainhash.HashH([]byte("child-of-loser")),
		winner: chainhash.HashH([]byte("winner")),
	}

	store := new(utxo.MockUtxostore)

	if failWinnerLookup {
		store.On("Get", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.NewStorageError("store down"))
	} else {
		store.On("Get", mock.Anything, mock.Anything, mock.Anything).Return(&meta.Data{Conflicting: true}, nil)
	}

	store.On("GetCounterConflicting", mock.Anything, scenario.winner).Return([]chainhash.Hash{scenario.loser}, nil)
	store.On("SetConflicting", mock.Anything, []chainhash.Hash{scenario.loser}, true).
		Return([]*utxo.Spend{}, []chainhash.Hash{scenario.child}, nil)
	store.On("SetConflicting", mock.Anything, []chainhash.Hash{scenario.child}, true).
		Return([]*utxo.Spend{}, []chainhash.Hash{}, nil)
	store.On("SetConflicting", mock.Anything, []chainhash.Hash{scenario.winner}, false).
		Return([]*utxo.Spend{}, []chainhash.Hash{}, nil)
	store.On("Unspend", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	store.On("SpendAndCreate", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, []*utxo.Spend{}, nil)
	store.On("SetLocked", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	store.On("BatchDecorate", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	bc := new(blockchain.Mock)
	bc.On("GetBlockIsMined", mock.Anything, mock.Anything).Return(true, nil)
	bc.On("CheckBlockIsAncestorOfBlock", mock.Anything, mock.Anything, mock.Anything).Return(false, nil)

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockAssembly.InitialMerkleItemsPerSubtree = 32

	var opts []Options
	if evictor != nil {
		opts = append(opts, WithTxMetaEvictor(evictor))
	}

	stp, err := NewSubtreeProcessor(t.Context(), ulogger.TestLogger{}, tSettings, blob_memory.New(), bc, store,
		make(chan NewSubtreeRequest, 10), opts...)
	require.NoError(t, err)

	return stp, scenario
}

func testBlock() *model.Block {
	return &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      1234567890,
			Nonce:          1,
		},
		Height: 11,
	}
}

// TestProcessConflictingTransactions_PublishesTxMetaDeletes pins that a
// successful demotion evicts every tx the store marked conflicting (the loser
// AND the cascaded child, not the winner) from subtree validation's txmeta
// cache, by publishing DELETEs on the txmeta topic.
func TestProcessConflictingTransactions_PublishesTxMetaDeletes(t *testing.T) {
	producer, collector := newInMemoryTxMetaTopic(t)

	stp, scenario := newConflictTestProcessor(t, NewTxMetaEvictor(ulogger.TestLogger{}, producer), false)

	losers, marked, err := stp.processConflictingTransactions(t.Context(), testBlock(),
		[]chainhash.Hash{scenario.winner}, map[chainhash.Hash]struct{}{})
	require.NoError(t, err)
	require.True(t, losers.Exists(scenario.loser))
	require.Contains(t, marked, scenario.child, "cascade must reach the child, otherwise this test proves nothing")

	require.Eventually(t, func() bool {
		got := collector.deleted()
		_, hasLoser := got[scenario.loser]
		_, hasChild := got[scenario.child]

		return hasLoser && hasChild
	}, 5*time.Second, 10*time.Millisecond, "DELETE for loser and cascaded child must reach the txmeta topic")

	got := collector.deleted()
	require.NotContains(t, got, scenario.winner, "the promoted winner is not conflicting and must not be evicted")
	require.Len(t, got, len(marked), "exactly the marked-conflicting set is evicted")
	require.Zero(t, collector.adds)
}

// TestProcessConflictingTransactions_NoEvictorIsNoop pins that block assembly
// without a txmeta producer (Kafka txmeta not configured) still demotes.
func TestProcessConflictingTransactions_NoEvictorIsNoop(t *testing.T) {
	stp, scenario := newConflictTestProcessor(t, nil, false)

	losers, _, err := stp.processConflictingTransactions(t.Context(), testBlock(),
		[]chainhash.Hash{scenario.winner}, map[chainhash.Hash]struct{}{})
	require.NoError(t, err)
	require.True(t, losers.Exists(scenario.loser))
}

// TestProcessConflictingTransactions_FailedDemotionPublishesNothing pins that
// DELETEs are only sent once the store demotion succeeded.
func TestProcessConflictingTransactions_FailedDemotionPublishesNothing(t *testing.T) {
	producer := kafka.NewKafkaAsyncProducerMock()

	stp, scenario := newConflictTestProcessor(t, NewTxMetaEvictor(ulogger.TestLogger{}, producer), true)

	_, _, err := stp.processConflictingTransactions(t.Context(), testBlock(),
		[]chainhash.Hash{scenario.winner}, map[chainhash.Hash]struct{}{})
	require.Error(t, err)
	require.Empty(t, producer.PublishChannel(), "nothing may be published when the demotion failed")
}

func TestTxMetaEvictor(t *testing.T) {
	hashes := func(n int) []chainhash.Hash {
		out := make([]chainhash.Hash, n)
		for i := range out {
			out[i] = chainhash.HashH([]byte(fmt.Sprintf("tx-%d", i)))
		}

		return out
	}

	t.Run("nil evictor and nil producer are no-ops", func(t *testing.T) {
		var nilEvictor *TxMetaEvictor

		require.NotPanics(t, func() { nilEvictor.Evict(hashes(3)) })
		require.Nil(t, NewTxMetaEvictor(ulogger.TestLogger{}, nil), "no producer, no evictor")

		var typedNil *kafka.KafkaAsyncProducer

		require.Nil(t, NewTxMetaEvictor(ulogger.TestLogger{}, typedNil), "a typed-nil producer must not become a live evictor")
	})

	t.Run("empty input publishes nothing", func(t *testing.T) {
		producer := kafka.NewKafkaAsyncProducerMock()
		NewTxMetaEvictor(ulogger.TestLogger{}, producer).Evict(nil)
		require.Empty(t, producer.PublishChannel())
	})

	t.Run("large cascades are split and every message is keyed by its first hash", func(t *testing.T) {
		producer := kafka.NewKafkaAsyncProducerMock()
		all := hashes(txMetaEvictBatchSize*2 + 5)

		NewTxMetaEvictor(ulogger.TestLogger{}, producer).Evict(all)

		ch := producer.PublishChannel()
		require.Len(t, ch, 3)

		var collector txMetaTopicCollector

		for i := 0; i < 3; i++ {
			msg := <-ch
			require.Equal(t, all[i*txMetaEvictBatchSize][:], msg.Key)
			require.NoError(t, collector.handle(&kafka.KafkaMessage{Value: msg.Value}))
		}

		require.Len(t, collector.deleted(), len(all))
	})

	t.Run("a full producer buffer is logged, never blocks or panics", func(t *testing.T) {
		producer := kafka.NewKafkaAsyncProducerMockWithBuffer(1)
		evictor := NewTxMetaEvictor(ulogger.TestLogger{}, producer)

		done := make(chan struct{})

		go func() {
			defer close(done)

			evictor.Evict(hashes(txMetaEvictBatchSize * 3))
		}()

		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("Evict blocked on a full producer")
		}
	})
}

// TestTxMetaEvictor_NeverEmitsAV2LookalikeCount pins that no DELETE message
// carries the one v1 entry count, 767 (0x02FF), whose little-endian bytes
// FF 02 00 00 are the v2 header signature. A receiver would then parse the
// message as v2 when the first hash happens to read as a small count, and drop
// every DELETE in it.
func TestTxMetaEvictor_NeverEmitsAV2LookalikeCount(t *testing.T) {
	producer := kafka.NewKafkaAsyncProducerMockWithBuffer(10)
	evictor := NewTxMetaEvictor(ulogger.TestLogger{}, producer)

	hashes := make([]chainhash.Hash, 767)
	for i := range hashes {
		hashes[i] = chainhash.HashH([]byte{byte(i), byte(i >> 8)})
	}

	evictor.Evict(hashes)

	total := 0

	for len(producer.PublishChannel()) > 0 {
		value := (<-producer.PublishChannel()).Value

		require.False(t, value[0] == txmetacache.WireV2Magic && value[1] == txmetacache.WireV2Version && value[2] == 0 && value[3] == 0,
			"a DELETE message must never start with the v2 header signature")

		total += int(binary.LittleEndian.Uint32(value[:4]))
	}

	require.Equal(t, len(hashes), total, "every hash must still be sent")
}
