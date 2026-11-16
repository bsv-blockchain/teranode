package blockvalidation

import (
	"context"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// lostReplySubtreesSetClient commits SetBlockSubtreesSet on the real store and then
// reports a failure, the way a gRPC deadline or a connection reset during a
// blockchain restart does. afterCommit runs between the two, so a test can let the
// setMined worker react to the notification the server has already sent.
type lostReplySubtreesSetClient struct {
	blockchain.ClientI
	afterCommit func(hash *chainhash.Hash)
}

func (c *lostReplySubtreesSetClient) SetBlockSubtreesSet(ctx context.Context, hash *chainhash.Hash) error {
	if err := c.ClientI.SetBlockSubtreesSet(ctx, hash); err != nil {
		return err
	}

	if c.afterCommit != nil {
		c.afterCommit(hash)
	}

	return errors.NewServiceError("connection reset by peer after SetBlockSubtreesSet committed")
}

// parkedSetMinedStore holds the first SetMinedMulti call made after arm until
// release is closed, and records every hash it was given. That freezes the setMined
// worker at the point where it holds the block and has already read its subtree
// nodes, which is where a concurrent release does its damage.
type parkedSetMinedStore struct {
	utxostore.Store

	armed       atomic.Bool
	once        sync.Once
	releaseOnce sync.Once
	entered     chan struct{}
	release     chan struct{}

	mu     sync.Mutex
	hashes []chainhash.Hash
}

func newParkedSetMinedStore(store utxostore.Store) *parkedSetMinedStore {
	return &parkedSetMinedStore{
		Store:   store,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (s *parkedSetMinedStore) SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, minedBlockInfo utxostore.MinedBlockInfo) (map[chainhash.Hash][]uint32, error) {
	if s.armed.Load() {
		s.once.Do(func() {
			close(s.entered)
			<-s.release
		})

		// Read the hashes only after the park: they point into the block's node
		// arrays, so a release that ran while the worker was parked shows up here.
		s.mu.Lock()
		for _, hash := range hashes {
			s.hashes = append(s.hashes, *hash)
		}
		s.mu.Unlock()
	}

	return s.Store.SetMinedMulti(ctx, hashes, minedBlockInfo)
}

// unpark lets the parked SetMinedMulti call go. Safe to call more than once, so a
// test can both release it on purpose and from a cleanup after a failed assertion.
func (s *parkedSetMinedStore) unpark() {
	s.releaseOnce.Do(func() { close(s.release) })
}

func (s *parkedSetMinedStore) recordedHashes() []chainhash.Hash {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]chainhash.Hash(nil), s.hashes...)
}

// TestSubtreesSetFailure_SetMinedWorkerOwnsTheBlock covers a SetBlockSubtreesSet
// that commits but reports failure. The server has already sent BlockSubtreesSet,
// so the setMined worker takes the cached block and starts tagging its
// transactions. ValidateBlock's failure path used to delete the cache entry (a
// no-op by then) and release the same block anyway, returning its node arrays to
// the pool while the worker still read hashes from them. Whoever takes the block
// out of the cache now owns it, so ValidateBlock must leave it alone.
func TestSubtreesSetFailure_SetMinedWorkerOwnsTheBlock(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		testSubtreesSetFailureSetMinedWorkerOwnsTheBlock(t, false)
	})

	t.Run("optimistic mining", func(t *testing.T) {
		testSubtreesSetFailureSetMinedWorkerOwnsTheBlock(t, true)
	})
}

func testSubtreesSetFailureSetMinedWorkerOwnsTheBlock(t *testing.T, optimisticMining bool) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	txMetaStore, subtreeValidationClient, _, txStore, subtreeStore, deferFunc := setup(t)
	defer deferFunc()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.OptimisticMining = optimisticMining
	tSettings.BlockValidation.OptimisticMiningPeerBlocks = optimisticMining

	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)
	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	block, _, childTx := buildBlockWithReservedID(ctx, t, tSettings, blockchainClient, txMetaStore, subtreeStore)

	// The subscription channel stays silent: this test runs the setMined worker's
	// call itself, at the moment the lost reply leaves the race open.
	tracker := newTrackingBlockchainClient(blockchainClient).withNotificationChannel()
	client := &lostReplySubtreesSetClient{ClientI: tracker}
	store := newParkedSetMinedStore(txMetaStore)
	t.Cleanup(store.unpark)

	// The service's own workers run on workerCtx. Stopping them before the test
	// starts leaves the revalidation channel undrained, so the test can read what
	// the failure path queued.
	workerCtx, stopWorkers := context.WithCancel(ctx)
	bv := NewBlockValidation(workerCtx, ulogger.TestLogger{}, tSettings, client, subtreeStore, txStore, store, nil, subtreeValidationClient)
	stopWorkers()

	select {
	case <-bv.revalidateWorkerStopped:
	case <-time.After(5 * time.Second):
		t.Fatal("revalidation worker did not stop")
	}

	setMinedDone := make(chan error, 1)
	setMinedStarted := make(chan struct{})

	client.afterCommit = func(hash *chainhash.Hash) {
		store.armed.Store(true)

		go func() {
			close(setMinedStarted)
			setMinedDone <- bv.setTxMinedStatus(ctx, hash)
		}()

		select {
		case <-store.entered:
		case <-time.After(10 * time.Second):
			t.Error("setMined worker never reached SetMinedMulti")
		}
	}

	err = bv.ValidateBlock(ctx, block, "test", !optimisticMining)

	var queued revalidateBlockData

	if optimisticMining {
		// ValidateBlock returned after the optimistic AddBlock; updateSubtreesDAH runs
		// in the background and its failure path ends by queueing a revalidation.
		require.NoError(t, err)

		select {
		case queued = <-bv.revalidateBlockChan:
		case <-time.After(10 * time.Second):
			t.Fatal("the failed subtrees_set did not queue a revalidation")
		}
	} else {
		require.Error(t, err, "a failed SetBlockSubtreesSet must fail ValidateBlock")
	}

	<-setMinedStarted

	// The failure path has finished and the worker is still parked holding the
	// block. Its subtrees must still be loaded: releasing them is the worker's job.
	require.True(t, block.SubtreesLoaded(), "ValidateBlock released a block the setMined worker had already taken from the cache")

	if optimisticMining {
		require.NotSame(t, block, queued.block, "the revalidation must not reload subtrees into the object the setMined worker is using")
		require.Equal(t, block.Hash(), queued.block.Hash())
		require.Equal(t, block.Subtrees, queued.block.Subtrees)
		require.False(t, queued.block.SubtreesLoaded(), "the queued copy must carry no subtrees of its own")
	}

	store.unpark()

	select {
	case err = <-setMinedDone:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("setMined worker did not finish")
	}

	require.Contains(t, store.recordedHashes(), *childTx.TxIDChainHash(), "the worker must tag this block's own transactions")

	meta, err := txMetaStore.Get(ctx, childTx.TxIDChainHash())
	require.NoError(t, err)
	require.Contains(t, meta.BlockIDs, block.ID)

	// The worker owned the block, so the worker released it.
	require.False(t, block.SubtreesLoaded(), "the setMined worker must release the block it took")

	_, stillCached := bv.lastValidatedBlocks.Get(*block.Hash())
	require.False(t, stillCached)
}

// TestSubtreesSetFailure_ValidateBlockReleasesAnUntakenBlock is the other side of the
// handover: when SetBlockSubtreesSet fails and nothing has taken the block, the
// failure path removes it from the cache and releases its subtrees itself.
func TestSubtreesSetFailure_ValidateBlockReleasesAnUntakenBlock(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	txMetaStore, subtreeValidationClient, _, txStore, subtreeStore, deferFunc := setup(t)
	defer deferFunc()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.OptimisticMining = false

	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)
	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	block, _, _ := buildBlockWithReservedID(ctx, t, tSettings, blockchainClient, txMetaStore, subtreeStore)

	tracker := newTrackingBlockchainClient(blockchainClient).withNotificationChannel()
	client := &lostReplySubtreesSetClient{ClientI: tracker}

	bv := NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, client, subtreeStore, txStore, txMetaStore, nil, subtreeValidationClient)

	var cachedAtFailure bool

	client.afterCommit = func(hash *chainhash.Hash) {
		_, cachedAtFailure = bv.lastValidatedBlocks.Get(*hash)
	}

	require.Error(t, bv.ValidateBlock(ctx, block, "test", true))

	require.True(t, cachedAtFailure, "the block must be cached before SetBlockSubtreesSet, for the setMined worker")
	require.False(t, block.SubtreesLoaded(), "a block nobody took must be released by the failure path")

	_, stillCached := bv.lastValidatedBlocks.Get(*block.Hash())
	require.False(t, stillCached)
}

// TestReclaimBlockAfterFailedSubtreesSet pins the ownership rule on its own: the
// side that removes the block from the cache releases it, and a block that never
// entered the cache stays with ValidateBlock.
func TestReclaimBlockAfterFailedSubtreesSet(t *testing.T) {
	newBV := func(t *testing.T) *BlockValidation {
		bv := &BlockValidation{
			logger:              ulogger.TestLogger{},
			lastValidatedBlocks: newTestLastValidatedBlocks(),
		}
		t.Cleanup(bv.lastValidatedBlocks.Stop)

		return bv
	}

	t.Run("cached and untaken: released here", func(t *testing.T) {
		bv := newBV(t)
		block := blockWithLoadedSubtree(t)
		bv.lastValidatedBlocks.Set(*block.Hash(), block)

		require.True(t, bv.reclaimBlockAfterFailedSubtreesSet(block, true))
		require.False(t, block.SubtreesLoaded())
		require.Zero(t, bv.lastValidatedBlocks.Len())
	})

	t.Run("cached and already taken: left alone", func(t *testing.T) {
		bv := newBV(t)
		block := blockWithLoadedSubtree(t)
		bv.lastValidatedBlocks.Set(*block.Hash(), block)

		_, took := bv.lastValidatedBlocks.GetAndDelete(*block.Hash())
		require.True(t, took)

		require.False(t, bv.reclaimBlockAfterFailedSubtreesSet(block, true))
		require.True(t, block.SubtreesLoaded())
	})

	t.Run("never cached: released here", func(t *testing.T) {
		bv := newBV(t)
		block := blockWithLoadedSubtree(t)

		require.True(t, bv.reclaimBlockAfterFailedSubtreesSet(block, false))
		require.False(t, block.SubtreesLoaded())
	})

	t.Run("entry replaced by another object: both released", func(t *testing.T) {
		bv := newBV(t)
		block := blockWithLoadedSubtree(t)
		other := detachedBlockCopy(block)
		other.SubtreeSlices = loadedSubtrees(t)
		bv.lastValidatedBlocks.Set(*block.Hash(), other)

		require.True(t, bv.reclaimBlockAfterFailedSubtreesSet(block, true))
		require.False(t, block.SubtreesLoaded())
		require.False(t, other.SubtreesLoaded())
	})
}

func TestDetachedBlockCopy(t *testing.T) {
	block := blockWithLoadedSubtree(t)
	block.Height = 7
	block.ID = 9
	block.CoinbaseBUMP = model.HexBytes{1, 2, 3}
	block.SetNodeAllocator(NodeAllocFromPool)

	copied := detachedBlockCopy(block)

	require.NotSame(t, block, copied)
	require.Equal(t, block.Hash(), copied.Hash())
	require.Equal(t, block.Subtrees, copied.Subtrees)
	require.Equal(t, block.TransactionCount, copied.TransactionCount)
	require.Equal(t, block.SizeInBytes, copied.SizeInBytes)
	require.Equal(t, block.Height, copied.Height)
	require.Equal(t, block.ID, copied.ID)
	require.Equal(t, block.CoinbaseBUMP, copied.CoinbaseBUMP)
	require.NotNil(t, copied.NodeAllocator())
	require.False(t, copied.SubtreesLoaded())

	// The subtree list is the copy's own: editing it must not reach the original.
	copied.Subtrees[0] = &chainhash.Hash{0xff}
	require.NotEqual(t, block.Subtrees[0], copied.Subtrees[0])
}

func newTestLastValidatedBlocks() *expiringmap.ExpiringMap[chainhash.Hash, *model.Block] {
	return expiringmap.New[chainhash.Hash, *model.Block](time.Hour).WithEvictionFunction(evictLastValidatedBlock)
}

// blockWithLoadedSubtree returns a block with a header, one subtree hash and that
// subtree loaded in SubtreeSlices.
func blockWithLoadedSubtree(t *testing.T) *model.Block {
	t.Helper()

	return &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
		},
		Subtrees:      []*chainhash.Hash{{0x01}},
		SubtreeSlices: loadedSubtrees(t),
	}
}

func loadedSubtrees(t *testing.T) []*subtreepkg.Subtree {
	t.Helper()

	st, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, st.AddCoinbaseNode())
	require.NoError(t, st.AddNode(chainhash.HashH([]byte("tx")), 1, 0))

	return []*subtreepkg.Subtree{st}
}
