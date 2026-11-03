package subtreevalidation

import (
	"context"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	bt "github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation/subtreevalidation_api"
	"github.com/bsv-blockchain/teranode/stores/txmetacache"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	utxometa "github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// storeCachedSubtreeOf stores a blessed subtree holding the coinbase placeholder
// followed by txHashes, and returns its root hash.
func storeCachedSubtreeOf(t *testing.T, server *Server, txHashes []chainhash.Hash) chainhash.Hash {
	t.Helper()

	st, err := subtreepkg.NewIncompleteTreeByLeafCount(len(txHashes) + 1)
	require.NoError(t, err)
	require.NoError(t, st.AddCoinbaseNode())

	for _, h := range txHashes {
		require.NoError(t, st.AddNode(h, 1, 1))
	}

	serialized, err := st.Serialize()
	require.NoError(t, err)

	rootHash := st.RootHash()
	require.NoError(t, server.subtreeStore.Set(context.Background(), rootHash[:], fileformat.FileTypeSubtree, serialized))

	return *rootHash
}

// TestCachedSubtreeConflictingNodes_RetriesTransientStoreError pins that the
// conflicting probe goes through the same transient-failure retry as every other
// decorate read in this service. The probe now runs on every cached subtree of
// every block, so without the retry one store timeout fails CheckBlockSubtrees.
func TestCachedSubtreeConflictingNodes_RetriesTransientStoreError(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	server.settings.BlockValidation.ProcessTxMetaUsingStoreRetries = 3

	tx := chainhash.HashH([]byte("probe-retry-tx"))
	rootHash := storeCachedSubtreeOf(t, server, []chainhash.Hash{tx})

	store := &flakyBatchDecorateStore{
		failures: 1,
		err:      errors.NewStorageError("error in aerospike map store batch records", errors.NewError("ResultCode: TIMEOUT")),
	}
	server.utxoStore = store

	conflicting, err := server.cachedSubtreeConflictingNodes(context.Background(), []chainhash.Hash{rootHash})
	require.NoError(t, err, "a transient store failure must be retried, not fail the block")
	require.Empty(t, conflicting)
	require.Equal(t, 2, store.callCount(), "expected one failed attempt followed by a successful one")
}

// barrierDecorateStore holds every BatchDecorate call until `want` calls are in
// flight at once, and fails the call if that never happens. It also flags the
// transactions in `conflicting`.
type barrierDecorateStore struct {
	utxo.Store

	want        int
	conflicting map[chainhash.Hash]bool

	mu       sync.Mutex
	inFlight int
	reached  chan struct{}
}

func (s *barrierDecorateStore) BatchDecorate(ctx context.Context, items []*utxo.UnresolvedMetaData, _ ...fields.FieldName) error {
	s.mu.Lock()
	s.inFlight++
	if s.inFlight == s.want {
		close(s.reached)
	}
	s.mu.Unlock()

	select {
	case <-s.reached:
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(5 * time.Second):
		return errors.NewProcessingError("batches of one subtree were never in flight together")
	}

	for _, item := range items {
		item.Data = &utxometa.Data{Conflicting: s.conflicting[item.Hash]}
	}

	return nil
}

// TestCachedSubtreeConflictingNodes_BatchesOfOneSubtreeRunConcurrently pins that
// a large cached subtree's batches go to the store in parallel. Parallelism only
// across subtrees leaves a block of a few 1M-leaf subtrees doing ~1000 serial
// round trips each.
func TestCachedSubtreeConflictingNodes_BatchesOfOneSubtreeRunConcurrently(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	server.settings.BlockValidation.ProcessTxMetaUsingStoreBatchSize = 2
	server.settings.BlockValidation.ProcessTxMetaUsingStoreConcurrency = 4

	// Coinbase + 7 txs = 4 batches of 2 leaves.
	txs := make([]chainhash.Hash, 7)
	for i := range txs {
		txs[i] = chainhash.HashH([]byte{byte(i)})
	}

	rootHash := storeCachedSubtreeOf(t, server, txs)

	server.utxoStore = &barrierDecorateStore{
		want:        2,
		conflicting: map[chainhash.Hash]bool{txs[0]: true, txs[6]: true},
		reached:     make(chan struct{}),
	}

	conflicting, err := server.cachedSubtreeConflictingNodes(context.Background(), []chainhash.Hash{rootHash})
	require.NoError(t, err)
	require.Equal(t, []chainhash.Hash{txs[0], txs[6]}, conflicting, "flagged txs must come back in subtree order")
}

// TestCachedSubtreeConflictingNodes_EmptySubtreeWithUnsetBatchSize guards the
// batch-count arithmetic: an unset batch size falls back to the leaf count, which
// is zero for an empty subtree.
func TestCachedSubtreeConflictingNodes_EmptySubtreeWithUnsetBatchSize(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	server.settings.BlockValidation.ProcessTxMetaUsingStoreBatchSize = 0

	// Hand-built, since an empty Subtree cannot Serialize (it has no root hash):
	// root hash, fees, size, leaf count 0, conflicting count 0.
	serialized := make([]byte, chainhash.HashSize+8+8+8+8)

	rootHash := chainhash.HashH([]byte("empty-subtree"))
	require.NoError(t, server.subtreeStore.Set(context.Background(), rootHash[:], fileformat.FileTypeSubtree, serialized))

	conflicting, err := server.cachedSubtreeConflictingNodes(context.Background(), []chainhash.Hash{rootHash})
	require.NoError(t, err)
	require.Empty(t, conflicting)
}

// TestCachedSubtreeConflictingNodes_CancelledContextIsNotAStorageFault pins that a
// store failure seen while the context is cancelled — shutdown, or a sibling
// subtree failing the errgroup — is reported as cancellation. blockvalidation
// treats a storage fault as recoverable and redelivers the block for it.
func TestCachedSubtreeConflictingNodes_CancelledContextIsNotAStorageFault(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	server.settings.BlockValidation.ProcessTxMetaUsingStoreRetries = 3

	tx := chainhash.HashH([]byte("probe-cancel-tx"))
	rootHash := storeCachedSubtreeOf(t, server, []chainhash.Hash{tx})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server.utxoStore = &flakyBatchDecorateStore{
		failures: 100,
		err:      errors.NewStorageError("error in aerospike map store batch records", errors.NewError("ResultCode: TIMEOUT")),
		onCall:   func(int) { cancel() },
	}

	_, err := server.cachedSubtreeConflictingNodes(ctx, []chainhash.Hash{rootHash})
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrContextCanceled), "expected a context-cancelled error, got: %v", err)
	require.False(t, errors.Is(err, errors.ErrStorageError), "a cancellation must not surface as a storage fault, got: %v", err)
}

// countingDecorateStore counts BatchDecorate calls and reports every tx as
// present and not conflicting.
type countingDecorateStore struct {
	utxo.Store

	calls atomic.Int64
}

func (s *countingDecorateStore) BatchDecorate(_ context.Context, items []*utxo.UnresolvedMetaData, _ ...fields.FieldName) error {
	s.calls.Add(1)

	for _, item := range items {
		item.Data = &utxometa.Data{}
	}

	return nil
}

// TestCheckBlockSubtrees_UnboundBodyIsNotProbed pins that the cached-subtree
// probe only runs on a body the header commits to. The subtree list is a peer
// claim until then, and each listed cached subtree costs a store read per leaf.
func TestCheckBlockSubtrees_UnboundBodyIsNotProbed(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	block := buildCachedSubtree(t, server, chainhash.HashH([]byte("unbound-tx")), false)
	wrongRoot := chainhash.HashH([]byte("not-the-merkle-root"))
	block.Header.HashMerkleRoot = &wrongRoot

	store := &countingDecorateStore{Store: server.utxoStore}
	server.utxoStore = store

	blockBytes, err := block.Bytes()
	require.NoError(t, err)

	_, err = server.CheckBlockSubtrees(context.Background(), &subtreevalidation_api.CheckBlockSubtreesRequest{Block: blockBytes})
	require.Error(t, err)
	require.True(t, errors.Is(errors.UnwrapGRPC(err), errors.ErrBlockCorrupt), "an unbound body must be rejected as corrupt, got: %v", err)
	require.Zero(t, store.calls.Load(), "an unbound body must not reach the store")
}

// TestCheckBlockSubtrees_RepeatedCachedSubtreeIsNotProbed pins the repeat case:
// listing one cached subtree many times would multiply store reads per 32 bytes
// of body.
func TestCheckBlockSubtrees_RepeatedCachedSubtreeIsNotProbed(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	block := buildCachedSubtree(t, server, chainhash.HashH([]byte("repeated-tx")), false)
	block.Subtrees = append(block.Subtrees, block.Subtrees[0], block.Subtrees[0])

	store := &countingDecorateStore{Store: server.utxoStore}
	server.utxoStore = store

	blockBytes, err := block.Bytes()
	require.NoError(t, err)

	_, err = server.CheckBlockSubtrees(context.Background(), &subtreevalidation_api.CheckBlockSubtreesRequest{Block: blockBytes})
	require.Error(t, err)
	require.True(t, errors.Is(errors.UnwrapGRPC(err), errors.ErrBlockCorrupt), "a repeated subtree list must be rejected as corrupt, got: %v", err)
	require.Zero(t, store.calls.Load(), "a repeated subtree list must not reach the store")
}

// itemErrStore reports `itemErr` on every item for the first `failures` calls,
// the way Aerospike reports a per-record timeout: BatchDecorate itself succeeds.
type itemErrStore struct {
	utxo.Store

	failures int
	itemErr  error
	calls    atomic.Int64
}

func (s *itemErrStore) BatchDecorate(_ context.Context, items []*utxo.UnresolvedMetaData, _ ...fields.FieldName) error {
	attempt := int(s.calls.Add(1))

	for _, item := range items {
		if attempt <= s.failures {
			item.Data = nil
			item.Err = s.itemErr

			continue
		}

		item.Data = &utxometa.Data{}
		item.Err = nil
	}

	return nil
}

// TestCachedSubtreeConflictingNodes_RetriesTransientItemError pins the per-record
// half of the retry: a transient failure reported on an item, not by the call,
// is retried like a failed call.
func TestCachedSubtreeConflictingNodes_RetriesTransientItemError(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	server.settings.BlockValidation.ProcessTxMetaUsingStoreRetries = 3

	rootHash := storeCachedSubtreeOf(t, server, []chainhash.Hash{chainhash.HashH([]byte("item-retry-tx"))})

	// Aerospike stores the client's own error on the item, not a teranode error.
	store := &itemErrStore{failures: 1, itemErr: aerospike.ErrTimeout}
	server.utxoStore = store

	conflicting, err := server.cachedSubtreeConflictingNodes(context.Background(), []chainhash.Hash{rootHash})
	require.NoError(t, err, "a transient per-record failure must be retried, not fail the block")
	require.Empty(t, conflicting)
	require.Equal(t, int64(2), store.calls.Load())
}

// TestCachedSubtreeConflictingNodes_PersistentItemErrorFailsAfterBoundedRetries
// pins that a per-record failure that never clears fails the check once the
// retry budget is spent, rather than retrying forever or counting as "not
// conflicting".
func TestCachedSubtreeConflictingNodes_PersistentItemErrorFailsAfterBoundedRetries(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	server.settings.BlockValidation.ProcessTxMetaUsingStoreRetries = 3

	rootHash := storeCachedSubtreeOf(t, server, []chainhash.Hash{chainhash.HashH([]byte("item-fatal-tx"))})

	store := &itemErrStore{failures: 100, itemErr: errors.NewProcessingError("malformed record")}
	server.utxoStore = store

	_, err := server.cachedSubtreeConflictingNodes(context.Background(), []chainhash.Hash{rootHash})
	require.Error(t, err)
	require.Equal(t, int64(4), store.calls.Load(), "one attempt plus the configured 3 retries")
}

// fullMetaStore answers BatchDecorate with a complete, non-conflicting meta —
// one the txmeta cache would accept.
type fullMetaStore struct {
	utxo.Store
}

func (s *fullMetaStore) BatchDecorate(_ context.Context, items []*utxo.UnresolvedMetaData, _ ...fields.FieldName) error {
	parent := chainhash.HashH([]byte("parent"))

	for _, item := range items {
		item.Data = &utxometa.Data{
			Fee:         1,
			SizeInBytes: 1,
			TxInpoints:  subtreepkg.NewTxInpointsFromPacked([]chainhash.Hash{parent}, []uint32{1, 0}),
		}
	}

	return nil
}

// TestCachedSubtreeConflictingNodes_DoesNotPopulateTxMetaCache pins that the
// probe reads the origin store directly. It asks for one field; a read through
// the cache wrapper would populate the cache, which decorateReadPath reserves
// for reads that ask for everything the cache needs.
func TestCachedSubtreeConflictingNodes_DoesNotPopulateTxMetaCache(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	tx := chainhash.HashH([]byte("cache-bypass-tx"))
	rootHash := storeCachedSubtreeOf(t, server, []chainhash.Hash{tx})

	cache, err := txmetacache.NewTxMetaCache(context.Background(), server.settings, ulogger.TestLogger{}, &fullMetaStore{}, txmetacache.Unallocated)
	require.NoError(t, err)

	server.utxoStore = cache

	_, err = server.cachedSubtreeConflictingNodes(context.Background(), []chainhash.Hash{rootHash})
	require.NoError(t, err)

	_, cached := cache.(*txmetacache.TxMetaCache).GetMetaCached(context.Background(), tx)
	require.False(t, cached, "the conflicting probe must not populate the txmeta cache")
}

// TestCachedSubtreeConflictingNodes_MissingSubtreeBlobFails pins that a cached
// subtree that cannot be read fails the check rather than counting as "no
// conflicts" — that would turn a storage fault into a bypass.
func TestCachedSubtreeConflictingNodes_MissingSubtreeBlobFails(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	_, err := server.cachedSubtreeConflictingNodes(context.Background(), []chainhash.Hash{chainhash.HashH([]byte("no-such-subtree"))})
	require.Error(t, err)
}

// TestCachedSubtreeConflictingNodes_TruncatedSubtreeBlobFails is the same for a
// blob that is present but cut short.
func TestCachedSubtreeConflictingNodes_TruncatedSubtreeBlobFails(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	// Header claims two leaves; neither follows.
	blob := make([]byte, chainhash.HashSize+8+8+8)
	blob[chainhash.HashSize+16] = 2

	rootHash := chainhash.HashH([]byte("truncated-subtree"))
	require.NoError(t, server.subtreeStore.Set(context.Background(), rootHash[:], fileformat.FileTypeSubtree, blob))

	_, err := server.cachedSubtreeConflictingNodes(context.Background(), []chainhash.Hash{rootHash})
	require.Error(t, err)
}

// TestProcessTxMetaUsingStore_UnbatchedRunsCounterConflictingCheck pins that the
// GetMeta branch (SubtreeValidation.BatchMissingTransactions off) runs the same
// counter-spend check as the batched branch. The two used to disagree.
func TestProcessTxMetaUsingStore_UnbatchedRunsCounterConflictingCheck(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	const ancestorBlockID = uint32(77)

	parentTx := bt.NewTx()
	require.NoError(t, parentTx.AddOpReturnOutput([]byte("parent")))

	conflictingTx := bt.NewTx()
	require.NoError(t, conflictingTx.FromUTXOs(&bt.UTXO{
		TxIDHash:      parentTx.TxIDChainHash(),
		Vout:          0,
		LockingScript: parentTx.Outputs[0].LockingScript,
		Satoshis:      0,
	}))

	counterSpender := chainhash.HashH([]byte("counter-spender-tx"))

	mockUtxo := server.utxoStore.(*utxo.MockUtxostore)

	mockUtxo.On("GetMeta", mock.Anything, conflictingTx.TxIDChainHash(), mock.Anything).
		Return(&utxometa.Data{Conflicting: true}, nil)
	mockUtxo.On("Get", mock.Anything, conflictingTx.TxIDChainHash(), mock.Anything).
		Return(&utxometa.Data{Tx: conflictingTx}, nil)
	mockUtxo.On("Get", mock.Anything, parentTx.TxIDChainHash(), mock.Anything).
		Return(&utxometa.Data{
			Tx:            parentTx,
			SpendingDatas: []*spend.SpendingData{spend.NewSpendingData(&counterSpender, 0)},
		}, nil)
	mockUtxo.On("Get", mock.Anything, &counterSpender, mock.Anything).
		Return(&utxometa.Data{Tx: bt.NewTx()}, nil)
	mockUtxo.On("GetMeta", mock.Anything, &counterSpender, mock.Anything).
		Return(&utxometa.Data{BlockIDs: []uint32{ancestorBlockID}}, nil)

	txHashes := []chainhash.Hash{*conflictingTx.TxIDChainHash()}
	metaSlice := make([]metaSliceItem, len(txHashes))

	_, err := server.processTxMetaUsingStore(context.Background(), txHashes, metaSlice, map[uint32]bool{ancestorBlockID: true}, false, false, false)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrTxInvalid), "a counter-spend on the ancestry must surface as tx-invalid, got: %v", err)
}

// TestCheckBlockSubtrees_RealStoreConflictingFlagRejectsCachedSubtree drives the
// cached path through a real store: the conflicting flag the probe reads is the
// one the store holds, not one a test double fabricates.
func TestCheckBlockSubtrees_RealStoreConflictingFlagRejectsCachedSubtree(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	const ancestorBlockID = uint32(77)

	utxoStoreURL, err := url.Parse("sqlitememory:///cached_conflict_probe")
	require.NoError(t, err)

	store, err := sql.New(ctx, ulogger.TestLogger{}, server.settings, utxoStoreURL)
	require.NoError(t, err)

	_, _, err = store.SpendAndCreate(ctx, parentTx1, 122, utxo.WithCreateOnly())
	require.NoError(t, err)

	// The counter-spender: spends the same parent output as tx1, and is mined on
	// the candidate's ancestry.
	counterSpender := tx1.Clone()
	counterSpender.Version = 2

	_, _, err = store.SpendAndCreate(ctx, counterSpender, 122, utxo.WithSpendOnly())
	require.NoError(t, err)

	_, _, err = store.SpendAndCreate(ctx, counterSpender, 122, utxo.WithCreateOnly())
	require.NoError(t, err)

	_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{counterSpender.TxIDChainHash()}, utxo.MinedBlockInfo{
		BlockID:        ancestorBlockID,
		BlockHeight:    122,
		OnLongestChain: true,
	})
	require.NoError(t, err)

	// tx1 is flagged conflicting in the store. SetConflicting hangs on
	// sqlitememory, so the flag is set at creation.
	_, _, err = store.SpendAndCreate(ctx, tx1, 123, utxo.WithConflicting(true), utxo.WithCreateOnly())
	require.NoError(t, err)

	server.utxoStore = store

	// Empty trailer: only the store says tx1 is conflicting.
	block := buildCachedSubtree(t, server, *tx1.TxIDChainHash(), false)

	mockBlockchain := server.blockchainClient.(*blockchain.Mock)
	mockBlockchain.On("GetBlockHeaderIDs", mock.Anything, mock.Anything, mock.Anything).
		Return([]uint32{ancestorBlockID}, nil)

	blockBytes, err := block.Bytes()
	require.NoError(t, err)

	_, err = server.CheckBlockSubtrees(ctx, &subtreevalidation_api.CheckBlockSubtreesRequest{Block: blockBytes})
	require.Error(t, err, "a cached subtree must not be blessed for a chain that already confirmed the counter-spender")
	require.True(t, errors.Is(errors.UnwrapGRPC(err), errors.ErrTxInvalid), "got: %v", err)
}
