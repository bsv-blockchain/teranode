package subtreevalidation

import (
	"context"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// fieldRecordingStore captures the field set each BatchDecorate asks for, so a
// test can assert what block validation actually pulls out of the UTXO store.
type fieldRecordingStore struct {
	utxo.Store

	mu     sync.Mutex
	fields [][]fields.FieldName
}

func (s *fieldRecordingStore) BatchDecorate(_ context.Context, items []*utxo.UnresolvedMetaData, f ...fields.FieldName) error {
	s.mu.Lock()
	s.fields = append(s.fields, append([]fields.FieldName(nil), f...))
	s.mu.Unlock()

	for _, item := range items {
		item.Data = &meta.Data{Fee: 1, SizeInBytes: 191}
	}

	return nil
}

func (s *fieldRecordingStore) requested(t *testing.T) []fields.FieldName {
	t.Helper()

	s.mu.Lock()
	defer s.mu.Unlock()

	require.Len(t, s.fields, 1, "expected exactly one BatchDecorate call")

	return s.fields[0]
}

func contains(set []fields.FieldName, f fields.FieldName) bool {
	for _, v := range set {
		if v == f {
			return true
		}
	}

	return false
}

func newFieldTestServer(store utxo.Store) *Server {
	s := settings.NewSettings()
	s.BlockValidation.ProcessTxMetaUsingStoreBatchSize = 1024
	s.BlockValidation.ProcessTxMetaUsingStoreConcurrency = 1
	s.BlockValidation.ProcessTxMetaUsingStoreMissingTxThreshold = 0
	s.BlockValidation.ProcessTxMetaUsingStoreRetries = 0

	return &Server{logger: ulogger.TestLogger{}, utxoStore: store, settings: s}
}

// Block validation never reads TxInpoints: on that path txMetaSlice is consulted
// only for isSet (check_block_subtrees.go), and the sole reader of .txInpoints in
// the service is the peer-announced path building subtree meta. TxInpoints is the
// largest field in the record — every parent hash, 32 bytes each — and
// deserializing it costs ~7.5% of subtree-validator CPU (processInputsToTxInpoints)
// before anything else it drags into the response and the allocator.
func TestProcessTxMetaUsingStore_BlockValidationOmitsTxInpoints(t *testing.T) {
	store := &fieldRecordingStore{}
	server := newFieldTestServer(store)

	txHashes := []chainhash.Hash{{1}}
	txMetaSlice := make([]metaSliceItem, 1)

	_, err := server.processTxMetaUsingStore(context.Background(), txHashes, txMetaSlice, map[uint32]bool{}, true, false, true)
	require.NoError(t, err)

	requested := store.requested(t)
	require.False(t, contains(requested, fields.TxInpoints),
		"block validation must not pull TxInpoints, it never reads them: %v", requested)

	// The two fields processTxMetaUsingStore itself branches on must survive the
	// trim, or transactions get silently misclassified.
	require.True(t, contains(requested, fields.Conflicting),
		"Conflicting gates the counter-conflicting check: %v", requested)
	require.True(t, contains(requested, fields.Creating),
		"Creating gates the incomplete-creation auto-recovery: %v", requested)
}

// The peer-announced path builds subtree meta from .txInpoints, so it must keep
// asking for the full set. This is also what keeps the reduced set safe: a
// partial meta must never reach the txmeta cache.
func TestProcessTxMetaUsingStore_PeerAnnouncedPathKeepsTxInpoints(t *testing.T) {
	store := &fieldRecordingStore{}
	server := newFieldTestServer(store)

	txHashes := []chainhash.Hash{{1}}
	txMetaSlice := make([]metaSliceItem, 1)

	_, err := server.processTxMetaUsingStore(context.Background(), txHashes, txMetaSlice, map[uint32]bool{}, true, false, false)
	require.NoError(t, err)

	requested := store.requested(t)
	require.True(t, contains(requested, fields.TxInpoints),
		"the subtree-meta path must keep TxInpoints: %v", requested)
	require.Equal(t, TxMetaFieldsForDecorate, requested,
		"the cache-populating path must request the full field set")
}

// The invariant that makes this safe: a reduced field set is only ever used on a
// read that also skips the cache. A partial meta reaching the cache wedges block
// validation later (see TxMetaCache.BatchDecorate). Tying the two together means
// no caller can ask for the cheap read and still poison the cache.
func TestProcessTxMetaUsingStore_ReducedFieldsImplyCacheSkipped(t *testing.T) {
	require.NotEqual(t, TxMetaFieldsForDecorate, txMetaFieldsForBlockValidation,
		"the block-validation set must actually be a reduction")

	require.True(t, contains(TxMetaFieldsForDecorate, fields.TxInpoints),
		"the full set is what the cache-populating path relies on")
	require.False(t, contains(txMetaFieldsForBlockValidation, fields.TxInpoints),
		"the reduced set is the one that must never be cached")
}

// unknownCachingStore caches tx meta (it implements txMetaCacheOps) but is not
// the concrete cache processTxMetaUsingStore knows how to unwrap — a second
// cache layer, or a future tracing/metrics decorator.
type unknownCachingStore struct {
	fieldRecordingStore
}

func (s *unknownCachingStore) Delete(_ context.Context, _ *chainhash.Hash) error { return nil }
func (s *unknownCachingStore) SetCacheFromBytes(_, _ []byte) error               { return nil }
func (s *unknownCachingStore) SetCacheMulti(_, _ [][]byte) error                 { return nil }
func (s *unknownCachingStore) SetCacheMultiSequential(_, _ [][]byte) error       { return nil }
func (s *unknownCachingStore) SetCacheMultiSequentialWithHashes(_, _ [][]byte, _ []uint64) error {
	return nil
}

// The reduced field set must be chosen only where the cache is provably out of
// the way. If a caching store cannot be unwrapped, the read still goes through
// it, so it has to keep asking for the full set — otherwise an inpoints-less
// meta reaches a cache and a later subtree-meta serialize wedges the block.
func TestProcessTxMetaUsingStore_UnwrappableCacheKeepsFullFields(t *testing.T) {
	store := &unknownCachingStore{}
	server := newFieldTestServer(store)

	txHashes := []chainhash.Hash{{1}}
	txMetaSlice := make([]metaSliceItem, 1)

	_, err := server.processTxMetaUsingStore(context.Background(), txHashes, txMetaSlice, map[uint32]bool{}, true, false, true)
	require.NoError(t, err)

	requested := store.requested(t)
	require.Equal(t, TxMetaFieldsForDecorate, requested,
		"a cache that cannot be bypassed must still get the full field set: %v", requested)
}
