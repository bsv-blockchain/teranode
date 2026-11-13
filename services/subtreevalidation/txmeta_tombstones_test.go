package subtreevalidation

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/txmetacache"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/require"
)

// newDemotionTestServer returns a server whose store is a real txmeta cache, so
// the handler's DELETEs reach TxMetaCache.DeleteDemoted.
func newDemotionTestServer(t *testing.T) (*Server, *txmetacache.TxMetaCache) {
	t.Helper()

	InitPrometheusMetrics()

	tSettings := test.CreateBaseTestSettings(t)

	store, err := txmetacache.NewTxMetaCache(t.Context(), tSettings, ulogger.TestLogger{}, &utxo.MockUtxostore{}, txmetacache.Unallocated, 64)
	require.NoError(t, err)

	cache := store.(*txmetacache.TxMetaCache)

	return &Server{logger: ulogger.TestLogger{}, settings: tSettings, utxoStore: cache}, cache
}

// demotionTestEntries returns n tx hashes and a valid serialised meta for each.
func demotionTestEntries(t *testing.T, n int) ([]chainhash.Hash, [][]byte) {
	t.Helper()

	hashes := make([]chainhash.Hash, n)
	payloads := make([][]byte, n)

	for i := range hashes {
		hashes[i] = chainhash.HashH([]byte{byte(i), 'd'})

		data := &meta.Data{
			Fee:         1,
			SizeInBytes: 100,
			TxInpoints:  subtree.NewTxInpointsFromPacked([]chainhash.Hash{chainhash.HashH([]byte{byte(i), 'p'})}, []uint32{1, 0}),
		}

		b, err := data.MetaBytes()
		require.NoError(t, err)

		payloads[i] = b
	}

	return hashes, payloads
}

func isCachedIn(t *testing.T, cache *txmetacache.TxMetaCache, h chainhash.Hash) bool {
	_, ok := cache.GetMetaCached(t.Context(), h)

	return ok
}

// serializeV2ForE2E encodes all ADDs as one v2 message, in the layout publishV2 uses.
func serializeV2ForE2E(hashes []chainhash.Hash, payloads [][]byte) []byte {
	size := txmetacache.WireV2HeaderLen
	for i := range hashes {
		size += txmetacache.WireV2MinEntrySize + len(payloads[i])
	}

	buf := make([]byte, size)
	buf[0] = txmetacache.WireV2Magic
	buf[1] = txmetacache.WireV2Version
	putUint32LE(buf[4:], uint32(len(hashes)))
	off := txmetacache.WireV2HeaderLen

	for i := range hashes {
		binary8LE(buf[off:], xxhash.Sum64(hashes[i][:]))
		off += 8
		copy(buf[off:], hashes[i][:])
		off += 32
		buf[off] = txmetacache.WireActionADD
		off++
		putUint32LE(buf[off:], uint32(len(payloads[i])))
		off += 4
		copy(buf[off:], payloads[i])
		off += len(payloads[i])
	}

	return buf
}

var txmetaWireFormats = []struct {
	format    string
	serialize func([]chainhash.Hash, [][]byte) []byte
}{
	{"v1", serializeV1ForE2E},
	{"v2", serializeV2ForE2E},
}

// TestTxmetaHandler_LateAddAfterDeleteIsNotCached pins the reordering case: a
// DELETE from block assembly overtakes the validator's ADD for the same tx (they
// travel on different partitions). The late ADD must not re-cache the demoted tx
// as non-conflicting; other ADDs in the same message are still applied.
func TestTxmetaHandler_LateAddAfterDeleteIsNotCached(t *testing.T) {
	for _, tc := range txmetaWireFormats {
		t.Run(tc.format, func(t *testing.T) {
			server, cache := newDemotionTestServer(t)

			hashes, payloads := demotionTestEntries(t, 2)
			demoted, unrelated := hashes[0], hashes[1]

			require.NoError(t, server.txmetaHandler(t.Context(), &kafka.KafkaMessage{Value: txmetacache.EncodeDeleteBatch([]chainhash.Hash{demoted})}))
			require.NoError(t, server.txmetaHandler(t.Context(), &kafka.KafkaMessage{Value: tc.serialize(hashes, payloads)}))

			require.False(t, isCachedIn(t, cache, demoted), "an ADD arriving after the DELETE for the same tx must not re-cache it")
			require.True(t, isCachedIn(t, cache, unrelated), "other ADDs in the message are still applied")
		})
	}
}

// deleteDuringSetStore runs beforeSet once, inside the next batch write and just
// before it, to place a DELETE consumed from another partition in the middle of
// an ADD's write.
type deleteDuringSetStore struct {
	*txmetacache.TxMetaCache
	beforeSet func()
}

func (s *deleteDuringSetStore) runBeforeSet() {
	if f := s.beforeSet; f != nil {
		s.beforeSet = nil
		f()
	}
}

func (s *deleteDuringSetStore) SetCacheMultiSequential(keys, values [][]byte) error {
	s.runBeforeSet()

	return s.TxMetaCache.SetCacheMultiSequential(keys, values)
}

func (s *deleteDuringSetStore) SetCacheMultiSequentialWithHashes(keys, values [][]byte, hashes []uint64) error {
	s.runBeforeSet()

	return s.TxMetaCache.SetCacheMultiSequentialWithHashes(keys, values, hashes)
}

// TestTxmetaHandler_DeleteDuringAddWriteIsNotCached pins the interleaving where
// the DELETE for a tx is handled while an ADD for it is being written. The tx
// must still end up evicted; other ADDs stay cached. Both wire formats are
// covered, since they reach the cache through different Set calls.
func TestTxmetaHandler_DeleteDuringAddWriteIsNotCached(t *testing.T) {
	for _, tc := range txmetaWireFormats {
		t.Run(tc.format, func(t *testing.T) {
			server, cache := newDemotionTestServer(t)

			hashes, payloads := demotionTestEntries(t, 2)
			demoted, unrelated := hashes[0], hashes[1]

			store := &deleteDuringSetStore{TxMetaCache: cache}
			store.beforeSet = func() {
				require.NoError(t, server.txmetaHandler(t.Context(), &kafka.KafkaMessage{Value: txmetacache.EncodeDeleteBatch([]chainhash.Hash{demoted})}))
			}
			server.utxoStore = store

			require.NoError(t, server.txmetaHandler(t.Context(), &kafka.KafkaMessage{Value: tc.serialize(hashes, payloads)}))
			require.Nil(t, store.beforeSet, "the DELETE must have run inside the ADD's write")

			require.False(t, isCachedIn(t, cache, demoted), "a DELETE handled during the ADD's write must still evict the tx")
			require.True(t, isCachedIn(t, cache, unrelated), "other ADDs in the message are still applied")
		})
	}
}

// TestNew_TxMetaCacheFailureRefusesToStart pins that a txmeta cache New cannot
// build, here because the tombstone window is not positive, fails New instead
// of starting the service with a nil store.
func TestNew_TxMetaCacheFailureRefusesToStart(t *testing.T) {
	utxoStore, validatorClient, txStore, subtreeStore, blockchainClient, deferFunc := setup(t)
	defer deferFunc()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.SubtreeValidation.TxMetaCacheEnabled = true
	tSettings.SubtreeValidation.TxMetaTombstoneWindow = 0

	nilConsumer := &kafka.KafkaConsumerGroup{}

	server, err := New(t.Context(), ulogger.TestLogger{}, tSettings, subtreeStore, txStore, utxoStore, validatorClient, blockchainClient, nilConsumer, nilConsumer, nil, nil)
	require.ErrorIs(t, err, errors.ErrConfiguration)
	require.Nil(t, server)
}

// TestTxmetaHandler_LargeDeleteCascadeRoundTrip pins that a demotion cascade
// split by txmetacache.SplitDeleteBatches survives the real handler: every
// batch is read as v1 (none is 767 long) and every tx is evicted.
func TestTxmetaHandler_LargeDeleteCascadeRoundTrip(t *testing.T) {
	for _, n := range []int{767, txmetacache.MaxDeleteBatch + 767} {
		server, cache := newDemotionTestServer(t)
		hashes, payloads := demotionTestEntries(t, 2)

		cascade := make([]chainhash.Hash, n)
		for i := range cascade {
			cascade[i] = chainhash.HashH([]byte{byte(i), byte(i >> 8), byte(i >> 16), 'c'})
		}

		// A first hash whose leading bytes read as a small count: a 767-entry
		// v1 message starting with it passes the handler's v2 detection, and
		// every DELETE in it is lost. The two cacheable txs follow it and end
		// the cascade.
		cascade[0] = chainhash.Hash{1}
		cascade[1], cascade[n-1] = hashes[0], hashes[1]

		require.NoError(t, server.txmetaHandler(t.Context(), &kafka.KafkaMessage{Value: serializeV1ForE2E(hashes, payloads)}))
		require.True(t, isCachedIn(t, cache, hashes[0]))
		require.True(t, isCachedIn(t, cache, hashes[1]))

		for _, batch := range txmetacache.SplitDeleteBatches(cascade) {
			require.NoError(t, server.txmetaHandler(t.Context(), &kafka.KafkaMessage{Value: txmetacache.EncodeDeleteBatch(batch)}))
		}

		require.False(t, isCachedIn(t, cache, hashes[0]), "n=%d: a tx in the first batch must be evicted", n)
		require.False(t, isCachedIn(t, cache, hashes[1]), "n=%d: the last tx of the cascade must be evicted", n)
	}
}
