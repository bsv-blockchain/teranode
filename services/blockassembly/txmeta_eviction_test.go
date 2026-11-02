package blockassembly

import (
	"context"
	"encoding/binary"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
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

// evictionFaultStore wraps a real UTXO store and injects the faults the replay eviction tests need.
type evictionFaultStore struct {
	utxo.Store

	// failUnspend applies the first Unspend and then fails it.
	failUnspend bool
	// failRollingBackSave fails saving the rolling_back phase, which models a process lost before
	// its compensation ran.
	failRollingBackSave bool
	// failRestore fails every RestoreConflictSpends, so a rollback cannot finish.
	failRestore bool
	// failMarkOf fails the SetConflicting(true) write that includes this hash, without applying it.
	failMarkOf *chainhash.Hash
}

func (s *evictionFaultStore) Unspend(ctx context.Context, spends []*utxo.Spend, flagAsLocked ...bool) error {
	if err := s.Store.Unspend(ctx, spends, flagAsLocked...); err != nil {
		return err
	}

	if s.failUnspend {
		s.failUnspend = false
		return errors.NewStorageError("injected unspend failure")
	}

	return nil
}

func (s *evictionFaultStore) SaveConflictRecovery(ctx context.Context, id chainhash.Hash, r *utxo.ConflictRecovery) error {
	if s.failRollingBackSave && r.Phase == utxo.ConflictRecoveryRollingBack {
		return errors.NewStorageError("process stopped before compensation")
	}

	return s.Store.SaveConflictRecovery(ctx, id, r)
}

func (s *evictionFaultStore) RestoreConflictSpends(ctx context.Context, spends []*utxo.Spend, blockHeight uint32) error {
	if s.failRestore {
		return errors.NewStorageError("injected restore failure")
	}

	return s.Store.RestoreConflictSpends(ctx, spends, blockHeight)
}

func (s *evictionFaultStore) SetConflicting(ctx context.Context, hashes []chainhash.Hash, value bool) ([]*utxo.Spend, []chainhash.Hash, error) {
	if value && s.failMarkOf != nil && slices.Contains(hashes, *s.failMarkOf) {
		return nil, nil, errors.NewStorageError("child write failed")
	}

	return s.Store.SetConflicting(ctx, hashes, value)
}

// evictionReplayFixture is a real sqlitememory store that holds a parent, a loser L that spends it,
// L's child C and a winner W that double spends L's input. W is conflicting and L owns the input.
type evictionReplayFixture struct {
	items                 *baTestItems
	store                 utxo.Store
	winner, loser, child  *bt.Tx
	intent                utxo.ConflictIntent
	collector             *txMetaDeleteCollector
	winnerHash, loserHash chainhash.Hash
	childHash             chainhash.Hash
}

func newEvictionReplayFixture(t *testing.T, kind utxo.ConflictIntentKind) *evictionReplayFixture {
	t.Helper()

	ctx := t.Context()
	items := setupBlockAssemblyTest(t)
	store := items.utxoStore
	require.NoError(t, store.SetBlockHeight(10))

	parent := createStaleReverseParent(ctx, t, store, 1)
	f := &evictionReplayFixture{items: items, store: store}

	if kind == utxo.ConflictIntentForward {
		f.loser = createStaleReverseSpender(ctx, t, store, parent, 0, 90000, true)
		f.winner = createStaleReverseSpender(ctx, t, store, parent, 0, 80000, false)
	} else {
		// A reverse demotes the applied winner W and promotes the conflicting counter L again.
		f.loser = createStaleReverseSpender(ctx, t, store, parent, 0, 90000, false)
		f.winner = createStaleReverseSpender(ctx, t, store, parent, 0, 80000, true)
	}

	// C spends the output of the transaction that owns the input, so a demotion cascades to it.
	owner := f.loser
	if kind == utxo.ConflictIntentReverse {
		owner = f.winner
	}

	f.child = createStaleReverseSpender(ctx, t, store, owner, 0, 70000, true)

	f.winnerHash, f.loserHash, f.childHash = *f.winner.TxIDChainHash(), *f.loser.TxIDChainHash(), *f.child.TxIDChainHash()
	f.intent = utxo.ConflictIntent{Kind: kind, BlockHeight: 10, BlockHash: chainhash.HashH([]byte("eviction-replay-block")), StartedAt: 1}

	if kind == utxo.ConflictIntentForward {
		f.intent.TxHashes = []chainhash.Hash{f.winnerHash}

		// The intent's block is on the longest chain, so the forward intent replays.
		bc := blockchainMembershipMock(true, false)
		bc.On("CheckBlockIsAncestorOfBlock", mock.Anything, mock.Anything, mock.Anything).Return(false, nil)
		items.blockAssembler.blockchainClient = bc
	} else {
		f.intent.TxHashes = []chainhash.Hash{f.winnerHash}

		// An unknown block is off the longest chain, so the reverse intent replays.
		items.blockAssembler.blockchainClient = blockchainMembershipMock(false, true)
	}

	producer, collector := newInMemoryTxMetaTopic(t)
	f.collector = collector
	items.blockAssembler.txMetaEvictor = subtreeprocessor.NewTxMetaEvictor(ulogger.TestLogger{}, producer)

	return f
}

func (f *evictionReplayFixture) pending(t *testing.T) []utxo.ConflictIntent {
	t.Helper()

	pending, err := f.store.PendingConflictIntents(t.Context())
	require.NoError(t, err)

	return pending
}

// TestReplayPendingConflictIntents_ForwardEvictsDemotedFromTxMetaCache pins
// that replaying a forward intent publishes txmeta DELETEs for every tx the
// demotion marked conflicting (loser and cascaded child), not the winner. The
// process stopped after the operation finished and before block assembly
// acknowledged it, so replay returns the saved result.
func TestReplayPendingConflictIntents_ForwardEvictsDemotedFromTxMetaCache(t *testing.T) {
	ctx := t.Context()
	f := newEvictionReplayFixture(t, utxo.ConflictIntentForward)

	_, _, err := utxo.ProcessConflicting(ctx, f.store, f.intent.BlockHeight, f.intent.BlockHash, f.intent.TxHashes, nil, utxo.NoAncestryGuard, utxo.WithDeferredConflictCompletion())
	require.NoError(t, err)

	f.items.blockAssembler.replayPendingConflictIntents(ctx)

	require.Len(t, f.pending(t), 1, "a finished forward intent stays pending until block assembly acknowledges it")
	require.Eventually(t, func() bool { return f.collector.has(f.loserHash, f.childHash) },
		5*time.Second, 10*time.Millisecond, "DELETEs for the loser and its cascaded child must reach the txmeta topic")
	require.False(t, f.collector.has(f.winnerHash), "the promoted winner must not be evicted")
	require.Equal(t, 2, f.collector.count())
}

// TestReplayPendingConflictIntents_ReverseEvictsDemotedFromTxMetaCache pins the
// reverse direction: a replayed reverse flips the demoted winner (and its
// descendants) back to conflicting, and those must be evicted too.
func TestReplayPendingConflictIntents_ReverseEvictsDemotedFromTxMetaCache(t *testing.T) {
	ctx := t.Context()
	f := newEvictionReplayFixture(t, utxo.ConflictIntentReverse)
	require.NoError(t, f.store.BeginConflictIntent(ctx, f.intent))

	f.items.blockAssembler.replayPendingConflictIntents(ctx)

	require.Empty(t, f.pending(t), "a successful reverse replay completes its intent")
	require.Eventually(t, func() bool { return f.collector.has(f.winnerHash, f.childHash) },
		5*time.Second, 10*time.Millisecond, "DELETEs for the demoted tx and its cascaded child must reach the txmeta topic")
	require.Equal(t, 2, f.collector.count())
}

// A stale forward heal that rolls back evicts what the rollback changed: every checklist tx.
func TestHealStaleConflictIntent_ForwardRollbackEvicts(t *testing.T) {
	ctx := t.Context()
	f := newEvictionReplayFixture(t, utxo.ConflictIntentForward)

	_, _, err := utxo.ProcessConflicting(ctx, f.store, f.intent.BlockHeight, f.intent.BlockHash, f.intent.TxHashes, nil, utxo.NoAncestryGuard, utxo.WithDeferredConflictCompletion())
	require.NoError(t, err)

	// Nothing is confirmed on the current chain, so the heal rolls the operation back.
	f.items.blockAssembler.blockchainClient = blockchainMembershipMock(false, false)

	require.NoError(t, f.items.blockAssembler.healStaleConflictIntent(ctx, f.intent))

	require.Eventually(t, func() bool { return f.collector.has(f.winnerHash, f.loserHash, f.childHash) },
		5*time.Second, 10*time.Millisecond, "the rollback flips the winner and restores the cascade, so all three must be evicted")
	require.Equal(t, 3, f.collector.count())
}

// A stale forward heal that changes nothing must evict nothing. A refused heal is retried on every
// startup, and each eviction leaves txmeta tombstones for the whole checklist.
func TestHealStaleConflictIntent_ForwardNoChangeEvictsNothing(t *testing.T) {
	t.Run("rollback refused", func(t *testing.T) {
		ctx := t.Context()
		f := newEvictionReplayFixture(t, utxo.ConflictIntentForward)

		_, _, err := utxo.ProcessConflicting(ctx, f.store, f.intent.BlockHeight, f.intent.BlockHash, f.intent.TxHashes, nil, utxo.NoAncestryGuard, utxo.WithDeferredConflictCompletion())
		require.NoError(t, err)

		// The loser is confirmed on the current chain, so the guard refuses the rollback.
		_, err = f.store.SetMinedMulti(ctx, []*chainhash.Hash{&f.loserHash}, utxo.MinedBlockInfo{BlockID: 5, BlockHeight: 9})
		require.NoError(t, err)
		f.items.blockAssembler.blockchainClient = blockchainMembershipMock(true, false)

		require.Error(t, f.items.blockAssembler.healStaleConflictIntent(ctx, f.intent))
		require.Len(t, f.pending(t), 1, "a refused heal keeps the intent")
		require.Never(t, func() bool { return f.collector.count() > 0 }, 300*time.Millisecond, 10*time.Millisecond,
			"a refused rollback changed nothing, so nothing may be evicted")
	})

	t.Run("reversed checklist retired", func(t *testing.T) {
		ctx := t.Context()
		f := newEvictionReplayFixture(t, utxo.ConflictIntentForward)

		_, _, err := utxo.ProcessConflicting(ctx, f.store, f.intent.BlockHeight, f.intent.BlockHash, f.intent.TxHashes, nil, utxo.NoAncestryGuard, utxo.WithDeferredConflictCompletion())
		require.NoError(t, err)

		// A reverse of the block ran and stopped before it retired the forward checklist.
		_, _, err = utxo.ReverseProcessConflicting(ctx, f.store, f.intent.BlockHeight, f.intent.BlockHash, f.intent.TxHashes)
		require.NoError(t, err)
		f.items.blockAssembler.blockchainClient = blockchainMembershipMock(false, false)

		require.NoError(t, f.items.blockAssembler.healStaleConflictIntent(ctx, f.intent))
		require.Empty(t, f.pending(t), "the heal retires the reversed checklist")
		require.Never(t, func() bool { return f.collector.count() > 0 }, 300*time.Millisecond, 10*time.Millisecond,
			"retirement changes no flag, so nothing may be evicted")
	})
}

// A stale reverse heal re-applies the winner and evicts what that demotion marked conflicting.
func TestHealStaleConflictIntent_ReverseEvictsDemoted(t *testing.T) {
	ctx := t.Context()
	f := newEvictionReplayFixture(t, utxo.ConflictIntentForward)

	// The reverse completed: W is conflicting and L owns the input. The block is back on the chain.
	reverse := f.intent
	reverse.Kind = utxo.ConflictIntentReverse

	require.NoError(t, f.items.blockAssembler.healStaleConflictIntent(ctx, reverse))

	require.Eventually(t, func() bool { return f.collector.has(f.loserHash, f.childHash) },
		5*time.Second, 10*time.Millisecond, "the loser and its cascaded child must be evicted")
	require.False(t, f.collector.has(f.winnerHash), "the re-applied winner must not be evicted")
	require.Equal(t, 2, f.collector.count())

	forward := reverse
	forward.Kind = utxo.ConflictIntentForward
	require.NoError(t, f.store.CompleteConflictIntent(ctx, forward.IntentID()))
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

// The tests below pin that a demotion which fails partway still evicts what it
// may have flipped. MarkConflictingRecursively writes one BFS level at a time,
// so a failure below the first level leaves the levels above conflicting in the
// store, and nothing retries them into an eviction: a revalidation reads them as
// conflicting and skips the demotion branch entirely.

// failingCascadeStore marks parent conflicting, reports child as its spender,
// then fails the write for child.
func failingCascadeStore(parent, child chainhash.Hash) *utxo.MockUtxostore {
	mockStore := new(utxo.MockUtxostore)
	mockStore.On("SetConflicting", mock.Anything, []chainhash.Hash{parent}, true).
		Return([]*utxo.Spend{}, []chainhash.Hash{child}, nil)
	mockStore.On("SetConflicting", mock.Anything, []chainhash.Hash{child}, true).
		Return([]*utxo.Spend(nil), []chainhash.Hash(nil), errors.NewStorageError("child write failed"))

	return mockStore
}

func TestMarkAsConflicting_CascadeErrorEvictsFlippedFromTxMetaCache(t *testing.T) {
	parentHash := chainhash.HashH([]byte("mark-fail-parent"))
	childHash := chainhash.HashH([]byte("mark-fail-child"))

	mockStp := &subtreeprocessor.MockSubtreeProcessor{}

	producer, collector := newInMemoryTxMetaTopic(t)

	ba := &BlockAssembler{
		utxoStore:        failingCascadeStore(parentHash, childHash),
		subtreeProcessor: mockStp,
		settings:         &settings.Settings{},
		logger:           ulogger.TestLogger{},
		txMetaEvictor:    subtreeprocessor.NewTxMetaEvictor(ulogger.TestLogger{}, producer),
	}

	ba.markAsConflicting(t.Context(), parentHash)

	require.Eventually(t, func() bool { return collector.has(parentHash, childHash) }, 5*time.Second, 10*time.Millisecond,
		"a cascade that failed partway must still evict what it may have flipped")
	mockStp.AssertNotCalled(t, "Remove", mock.Anything, mock.Anything)
}

func TestValidateParentChain_CascadeErrorEvictsFlippedFromTxMetaCache(t *testing.T) {
	parentHash := chainhash.HashH([]byte("vpc-conflicting-parent"))
	childHash := chainhash.HashH([]byte("vpc-child"))
	grandchildHash := chainhash.HashH([]byte("vpc-grandchild"))

	mockStore := failingCascadeStore(childHash, grandchildHash)
	mockStore.On("BatchDecorate", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			for _, unresolved := range args.Get(1).([]*utxo.UnresolvedMetaData) {
				if unresolved.Hash.IsEqual(&parentHash) {
					unresolved.Data = &meta.Data{UnminedSince: 1, Conflicting: true}
				}
			}
		}).
		Return(nil)

	testSettings := &settings.Settings{}
	testSettings.BlockAssembly.ParentValidationBatchSize = 50
	testSettings.BlockAssembly.OnRestartRemoveInvalidParentChainTxs = true

	producer, collector := newInMemoryTxMetaTopic(t)

	ba := &BlockAssembler{
		utxoStore:     mockStore,
		settings:      testSettings,
		logger:        ulogger.TestLogger{},
		txMetaEvictor: subtreeprocessor.NewTxMetaEvictor(ulogger.TestLogger{}, producer),
	}

	childTx := &utxo.UnminedTransaction{
		Node:       &subtree.Node{Hash: childHash, Fee: 1000, SizeInBytes: 250},
		TxInpoints: singleParentInpointsPtr(parentHash, 0),
		CreatedAt:  1,
	}

	_, err := ba.validateParentChain(t.Context(), []*utxo.UnminedTransaction{childTx}, map[uint32]bool{1: true})
	require.NoError(t, err)

	require.Eventually(t, func() bool { return collector.has(childHash, grandchildHash) }, 5*time.Second, 10*time.Millisecond,
		"a cascade that failed partway must still evict what it may have flipped")
}

// The process stopped while the forward operation was mutating, so the loser and child are
// conflicting. The replay's rollback then fails. It must still evict what it may have flipped.
func TestReplayPendingConflictIntents_FailedForwardEvictsFlipped(t *testing.T) {
	ctx := t.Context()
	f := newEvictionReplayFixture(t, utxo.ConflictIntentForward)

	crashed := &evictionFaultStore{Store: f.store, failUnspend: true, failRollingBackSave: true}
	_, _, err := utxo.ProcessConflicting(ctx, crashed, f.intent.BlockHeight, f.intent.BlockHash, f.intent.TxHashes, nil, utxo.NoAncestryGuard, utxo.WithDeferredConflictCompletion())
	require.Error(t, err)

	f.items.blockAssembler.utxoStore = &evictionFaultStore{Store: f.store, failRestore: true}
	f.items.blockAssembler.replayPendingConflictIntents(ctx)

	require.Len(t, f.pending(t), 1, "a failed replay must leave its intent for the next start")
	require.Eventually(t, func() bool { return f.collector.has(f.loserHash, f.childHash) },
		5*time.Second, 10*time.Millisecond, "a replay that failed partway must still evict what it may have flipped")
}

// The reverse cascade fails at the child's write after it flipped the demoted winner.
func TestReplayPendingConflictIntents_FailedReverseEvictsFlipped(t *testing.T) {
	ctx := t.Context()
	f := newEvictionReplayFixture(t, utxo.ConflictIntentReverse)
	require.NoError(t, f.store.BeginConflictIntent(ctx, f.intent))

	f.items.blockAssembler.utxoStore = &evictionFaultStore{Store: f.store, failMarkOf: &f.childHash}
	f.items.blockAssembler.replayPendingConflictIntents(ctx)

	require.Len(t, f.pending(t), 1, "a failed replay must leave its intent for the next start")
	require.Eventually(t, func() bool { return f.collector.has(f.winnerHash, f.childHash) },
		5*time.Second, 10*time.Millisecond, "a reverse that failed partway must still evict what it may have flipped")
}
