package subtreeprocessor

import (
	"fmt"
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
	"github.com/bsv-blockchain/teranode/stores/blob/null"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

const (
	blockMaxSizeTestTxSize = 100
	blockMaxSizeTestBudget = 2000
)

// blockMaxSizeHarness acknowledges every NewSubtreeRequest of an unstarted
// SubtreeProcessor and records the announced ones (SkipNotification false). The
// tests add txs with skipNotification, so only the resize announces.
type blockMaxSizeHarness struct {
	mu        sync.Mutex
	announced []NewSubtreeRequest

	// failAnnounced makes the next N announced requests report a storage error.
	failAnnounced atomic.Int32

	// failAnnouncedIndex makes the announced request with this index (0-based, counted
	// over the harness lifetime) report a storage error; -1 disables it.
	failAnnouncedIndex atomic.Int32
}

func (h *blockMaxSizeHarness) requests() []NewSubtreeRequest {
	h.mu.Lock()
	defer h.mu.Unlock()

	return append([]NewSubtreeRequest(nil), h.announced...)
}

// newBlockMaxSizeProcessor builds an unstarted SubtreeProcessor with the given
// initial subtree size and blockmaxsize. It is not started so the tests can drive
// the processor-goroutine-owned state directly, like TestReChainSubtrees.
func newBlockMaxSizeProcessor(t testing.TB, itemsPerSubtree, blockMaxSize int, opts ...Options) (*SubtreeProcessor, *blockMaxSizeHarness) {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockAssembly.InitialMerkleItemsPerSubtree = itemsPerSubtree
	tSettings.Policy.BlockMaxSize = blockMaxSize

	h := &blockMaxSizeHarness{}
	h.failAnnouncedIndex.Store(-1)

	newSubtreeChan := make(chan NewSubtreeRequest)
	done := make(chan struct{})
	t.Cleanup(func() { close(done) })

	go func() {
		for {
			select {
			case req := <-newSubtreeChan:
				var err error

				if !req.SkipNotification {
					h.mu.Lock()
					idx := len(h.announced)
					h.announced = append(h.announced, req)
					h.mu.Unlock()

					if int32(idx) == h.failAnnouncedIndex.Load() { //nolint:gosec // test counts stay small
						err = errors.NewStorageError("test storage failure")
					}

					if h.failAnnounced.Load() > 0 {
						h.failAnnounced.Add(-1)
						err = errors.NewStorageError("test storage failure")
					}
				}

				if req.ErrChan != nil {
					req.ErrChan <- err
				}
			case <-done:
				return
			}
		}
	}()

	logger := ulogger.NewErrorTestLogger(t)
	subtreeStore, _ := null.New(logger)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(t.Context(), logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	stp, err := NewSubtreeProcessor(t.Context(), ulogger.TestLogger{}, tSettings, subtreeStore, nil, utxoStore, newSubtreeChan, opts...)
	require.NoError(t, err)

	// precomputed mining data is only published once a block header is known
	stp.InitCurrentBlockHeader(&model.BlockHeader{Version: 1, HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}})

	return stp, h
}

// blockMaxSizeTestTxSeq keeps generated tx hashes unique across calls, so repeated
// calls add new txs instead of duplicates that addNode skips.
var blockMaxSizeTestTxSeq atomic.Uint64

// addBlockMaxSizeTestTxs adds one new tx per size, in order, and returns their hashes.
func addBlockMaxSizeTestTxs(t *testing.T, stp *SubtreeProcessor, sizes ...uint64) []chainhash.Hash {
	t.Helper()

	parent := chainhash.HashH([]byte("parent-tx"))
	hashes := make([]chainhash.Hash, 0, len(sizes))

	for i, size := range sizes {
		hash := chainhash.HashH([]byte(fmt.Sprintf("tx-%d", blockMaxSizeTestTxSeq.Add(1))))
		require.NoError(t, stp.addNode(subtreepkg.Node{Hash: hash, Fee: uint64(i), SizeInBytes: size},
			&subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{parent}}, true))

		hashes = append(hashes, hash)
	}

	return hashes
}

// commitTestBlock runs finalizeBlockProcessing, the commit point of a block, with a
// blockchain mock that accepts SetBlockProcessedAt.
func commitTestBlock(t *testing.T, stp *SubtreeProcessor) {
	t.Helper()

	mockClient := &blockchain.Mock{}
	mockClient.On("SetBlockProcessedAt", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	stp.blockchainClient = mockClient

	stp.finalizeBlockProcessing(t.Context(), &model.Block{
		Header: &model.BlockHeader{Version: 1, HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}, Nonce: 1},
	})
}

// sizes returns n copies of size.
func sizes(n int, size uint64) []uint64 {
	s := make([]uint64, n)
	for i := range s {
		s[i] = size
	}

	return s
}

// queuedTxHashes flattens chained + current subtrees into tx order, skipping the coinbase placeholder.
func queuedTxHashes(stp *SubtreeProcessor) []chainhash.Hash {
	var hashes []chainhash.Hash

	for _, st := range append(append([]*subtreepkg.Subtree{}, stp.chainedSubtrees...), stp.currentSubtree.Load()) {
		for _, node := range st.Nodes {
			if node.Hash.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
				continue
			}

			hashes = append(hashes, node.Hash)
		}
	}

	return hashes
}

// requireUniformChain asserts the block layout rule: every chained subtree is complete,
// has the given power-of-two capacity and fits the budget.
func requireUniformChain(t *testing.T, stp *SubtreeProcessor, size int) {
	t.Helper()

	require.NotEmpty(t, stp.chainedSubtrees)

	for i, st := range stp.chainedSubtrees {
		require.Equal(t, size, st.Size(), "chained subtree %d capacity", i)
		require.Len(t, st.Nodes, size, "chained subtree %d must be complete", i)
		require.LessOrEqual(t, st.SizeInBytes, uint64(blockMaxSizeTestBudget), "chained subtree %d must fit the budget", i)
	}

	require.Equal(t, size, stp.currentSubtree.Load().Size())
	require.True(t, stp.chainedSubtrees[0].Nodes[0].Hash.Equal(subtreepkg.CoinbasePlaceholderHashValue),
		"coinbase placeholder must stay at Nodes[0] of the first subtree")
}

// TestEnforceBlockMaxSize_ResizesOversizedChainedSubtrees pins issue 1835: a first
// subtree larger than blockmaxsize must be re-chunked in place into smaller
// power-of-two subtrees rather than stalling mining.
func TestEnforceBlockMaxSize_ResizesOversizedChainedSubtrees(t *testing.T) {
	// 63 txs of 100 bytes + the 0-byte placeholder = 6300 bytes per first subtree
	stp, h := newBlockMaxSizeProcessor(t, 64, model.BlockHeaderSize+blockMaxSizeTestBudget, WithMmapDir(t.TempDir()))

	hashes := addBlockMaxSizeTestTxs(t, stp, sizes(200, blockMaxSizeTestTxSize)...)
	require.Greater(t, stp.chainedSubtrees[0].SizeInBytes, uint64(blockMaxSizeTestBudget), "precondition: first subtree must be oversized")

	oldSubtrees := append(append([]*subtreepkg.Subtree{}, stp.chainedSubtrees...), stp.currentSubtree.Load())

	require.NoError(t, stp.enforceBlockMaxSize(t.Context()))

	// placeholder + 15 txs = 1500 bytes fits, placeholder + 31 txs = 3100 does not
	const expectedSize = 16

	// only this block's layout shrinks; the configured size is what the next block uses
	require.Equal(t, int32(64), stp.currentItemsPerFile.Load())
	requireUniformChain(t, stp, expectedSize)
	require.Equal(t, hashes, queuedTxHashes(stp), "tx order must be preserved")
	require.NoError(t, stp.checkSubtreeProcessor())

	// every rebuilt chained subtree is stored and announced; only the first block's worth
	// is awaited, the rest is sent asynchronously
	require.Eventually(t, func() bool { return len(h.requests()) == len(stp.chainedSubtrees) }, 5*time.Second, 10*time.Millisecond)
	announced := h.requests()

	for i, req := range announced {
		require.True(t, req.Subtree.RootHash().Equal(*stp.chainedSubtrees[i].RootHash()))
	}

	data := stp.GetPrecomputedMiningData()
	require.NotNil(t, data)
	require.Len(t, data.Subtrees, len(stp.chainedSubtrees))
	require.True(t, data.Subtrees[0].RootHash().Equal(*stp.chainedSubtrees[0].RootHash()))

	// the old mmap-backed subtrees may still be read by in-flight storage, so they are
	// retired, not unmapped
	require.Equal(t, oldSubtrees, stp.retiredSubtrees)

	for _, st := range oldSubtrees {
		require.True(t, st.IsMmapBacked())
	}
}

// TestEnforceBlockMaxSize_ResizesOversizedCurrentSubtree covers the incomplete-snapshot
// path: nothing is chained yet but the current subtree already exceeds the budget.
func TestEnforceBlockMaxSize_ResizesOversizedCurrentSubtree(t *testing.T) {
	stp, _ := newBlockMaxSizeProcessor(t, 64, model.BlockHeaderSize+blockMaxSizeTestBudget)

	hashes := addBlockMaxSizeTestTxs(t, stp, sizes(40, blockMaxSizeTestTxSize)...)
	require.Empty(t, stp.chainedSubtrees)
	require.Greater(t, stp.currentSubtree.Load().SizeInBytes, uint64(blockMaxSizeTestBudget))

	require.NoError(t, stp.enforceBlockMaxSize(t.Context()))

	requireUniformChain(t, stp, 16)
	require.Len(t, stp.chainedSubtrees, 2) // 41 nodes incl. placeholder = 2 full subtrees + 9
	require.Len(t, stp.currentSubtree.Load().Nodes, 9)
	require.Equal(t, hashes, queuedTxHashes(stp))
}

// TestEnforceBlockMaxSize_SizesFromBytePrefix checks that the new size is derived from
// the actual bytes at the front of the queue, so a skewed first subtree is fixed in a
// single rebuild instead of repeated halving.
func TestEnforceBlockMaxSize_SizesFromBytePrefix(t *testing.T) {
	stp, h := newBlockMaxSizeProcessor(t, 64, model.BlockHeaderSize+blockMaxSizeTestBudget)

	// large txs at the front, small ones after: the average (~110 bytes) would suggest 16 items
	txSizes := append(sizes(30, 400), sizes(170, 50)...)
	hashes := addBlockMaxSizeTestTxs(t, stp, txSizes...)

	require.NoError(t, stp.enforceBlockMaxSize(t.Context()))

	// placeholder + 3 x 400 = 1200 fits, placeholder + 7 x 400 = 2800 does not
	requireUniformChain(t, stp, 4)
	require.Equal(t, hashes, queuedTxHashes(stp))

	// a second call finds nothing to do
	before := len(h.requests())
	require.NoError(t, stp.enforceBlockMaxSize(t.Context()))
	require.Len(t, h.requests(), before)
}

// TestEnforceBlockMaxSize_NoOp covers the cases where nothing must change.
func TestEnforceBlockMaxSize_NoOp(t *testing.T) {
	t.Run("blockmaxsize disabled", func(t *testing.T) {
		stp, h := newBlockMaxSizeProcessor(t, 64, 0)
		addBlockMaxSizeTestTxs(t, stp, sizes(200, blockMaxSizeTestTxSize)...)

		require.NoError(t, stp.enforceBlockMaxSize(t.Context()))

		require.Equal(t, int32(64), stp.currentItemsPerFile.Load())
		require.Empty(t, h.requests())
	})

	t.Run("first subtree fits", func(t *testing.T) {
		stp, h := newBlockMaxSizeProcessor(t, 64, model.BlockHeaderSize+100_000)
		addBlockMaxSizeTestTxs(t, stp, sizes(200, blockMaxSizeTestTxSize)...)

		require.NoError(t, stp.enforceBlockMaxSize(t.Context()))

		require.Equal(t, int32(64), stp.currentItemsPerFile.Load())
		require.Empty(t, h.requests())
	})

	// A first tx larger than the budget can never be mined under this limit. Rebuilding
	// would only shred the queue into 2-item subtrees without making the block fit.
	t.Run("first tx larger than the budget", func(t *testing.T) {
		stp, h := newBlockMaxSizeProcessor(t, 64, model.BlockHeaderSize+blockMaxSizeTestBudget)
		addBlockMaxSizeTestTxs(t, stp, append([]uint64{5000}, sizes(199, blockMaxSizeTestTxSize)...)...)

		for range 3 {
			require.NoError(t, stp.enforceBlockMaxSize(t.Context()))
		}

		require.Equal(t, int32(64), stp.currentItemsPerFile.Load())
		require.Empty(t, h.requests())
		require.Empty(t, stp.retiredSubtrees)
	})
}

// TestEnforceBlockMaxSize_ReportsFailedAnnouncement checks that a storage failure of the
// awaited subtrees is reported, while the rebuilt layout is still published: the same as
// processCompleteSubtree, which publishes before storage completes and leaves retries of
// failed stores to the storage workers.
func TestEnforceBlockMaxSize_ReportsFailedAnnouncement(t *testing.T) {
	stp, h := newBlockMaxSizeProcessor(t, 64, model.BlockHeaderSize+blockMaxSizeTestBudget, WithMmapDir(t.TempDir()))
	addBlockMaxSizeTestTxs(t, stp, sizes(200, blockMaxSizeTestTxSize)...)

	h.failAnnounced.Store(1)
	require.Error(t, stp.enforceBlockMaxSize(t.Context()))

	requireUniformChain(t, stp, 16)

	data := stp.GetPrecomputedMiningData()
	require.Len(t, data.Subtrees, len(stp.chainedSubtrees))
	require.Same(t, stp.chainedSubtrees[0], data.Subtrees[0])
	require.NotEmpty(t, stp.retiredSubtrees)
}

// TestSubtreeProcessor_RetiredSubtreesClosedAtBlockCommit checks that retired subtrees
// are released once the next block is committed, which gives in-flight storage of the
// replaced layout the whole block to finish.
func TestSubtreeProcessor_RetiredSubtreesClosedAtBlockCommit(t *testing.T) {
	stp, _ := newBlockMaxSizeProcessor(t, 64, model.BlockHeaderSize+blockMaxSizeTestBudget, WithMmapDir(t.TempDir()))
	addBlockMaxSizeTestTxs(t, stp, sizes(200, blockMaxSizeTestTxSize)...)

	require.NoError(t, stp.enforceBlockMaxSize(t.Context()))
	require.NotEmpty(t, stp.retiredSubtrees)

	require.NoError(t, stp.resetSubtreeState(true))
	require.NotEmpty(t, stp.retiredSubtrees)

	commitTestBlock(t, stp)
	require.Empty(t, stp.retiredSubtrees)
}

// TestSubtreeProcessor_RequestBlockMaxSizeCheck checks that a requested check runs on
// the processor goroutine, even with an empty queue.
func TestSubtreeProcessor_RequestBlockMaxSizeCheck(t *testing.T) {
	stp, _ := newBlockMaxSizeProcessor(t, 64, model.BlockHeaderSize+blockMaxSizeTestBudget)
	addBlockMaxSizeTestTxs(t, stp, sizes(200, blockMaxSizeTestTxSize)...)

	stp.RequestBlockMaxSizeCheck()
	stp.Start(t.Context())
	t.Cleanup(func() { stp.Stop(t.Context()) })

	// precomputed mining data is the only state safe to read while the processor runs
	require.Eventually(t, func() bool {
		data := stp.GetPrecomputedMiningData()
		return data != nil && len(data.Subtrees) > 0 && data.Subtrees[0].Size() == 16
	}, 5*time.Second, 10*time.Millisecond)
}

// TestEnforceBlockMaxSize_ResizedBlockSkipsDynamicSizingStats checks that the subtrees of
// a resized block do not feed dynamic sizing: their node counts and the per-subtree
// interval describe the forced size, not the load, and would shrink or grow the
// configured size for no reason.
func TestEnforceBlockMaxSize_ResizedBlockSkipsDynamicSizingStats(t *testing.T) {
	stp, _ := newBlockMaxSizeProcessor(t, 64, model.BlockHeaderSize+blockMaxSizeTestBudget)
	addBlockMaxSizeTestTxs(t, stp, sizes(200, blockMaxSizeTestTxSize)...)

	ringSamples := func() int {
		n := 0
		stp.subtreeNodeCounts.Do(func(v interface{}) {
			if v != nil {
				n++
			}
		})

		return n
	}

	require.NoError(t, stp.enforceBlockMaxSize(t.Context()))
	require.True(t, stp.blockMaxSizeResizedInBlock)

	before := ringSamples()
	addBlockMaxSizeTestTxs(t, stp, sizes(64, blockMaxSizeTestTxSize)...)
	require.Equal(t, before, ringSamples(), "resized subtrees must not enter the utilization ring")

	intervals := len(stp.blockIntervals)
	stp.blockStartTime = time.Now().Add(-time.Second)
	commitTestBlock(t, stp)

	require.Len(t, stp.blockIntervals, intervals, "a resized block must not add an interval sample")
	require.False(t, stp.blockMaxSizeResizedInBlock, "the next block is a normal block again")
}

// TestReChainSubtrees_RetiresReplacedSubtrees checks that a rechain retires the mmap-backed
// subtrees it replaces instead of unmapping them: after a resize most of them may still be
// in flight to storage. Heap subtrees are left to the GC, so retiring them would only hold
// memory until the next block.
func TestReChainSubtrees_RetiresReplacedSubtrees(t *testing.T) {
	t.Run("mmap", func(t *testing.T) {
		stp, _ := newBlockMaxSizeProcessor(t, 64, model.BlockHeaderSize+100_000, WithMmapDir(t.TempDir()))
		addBlockMaxSizeTestTxs(t, stp, sizes(200, blockMaxSizeTestTxSize)...)

		replaced := append(append([]*subtreepkg.Subtree{}, stp.chainedSubtrees[1:]...), stp.currentSubtree.Load())
		for _, st := range replaced {
			require.True(t, st.IsMmapBacked())
		}

		require.NoError(t, stp.reChainSubtrees(1))

		require.Equal(t, replaced, stp.retiredSubtrees)
	})

	t.Run("heap", func(t *testing.T) {
		stp, _ := newBlockMaxSizeProcessor(t, 64, model.BlockHeaderSize+100_000)
		addBlockMaxSizeTestTxs(t, stp, sizes(200, blockMaxSizeTestTxSize)...)

		require.NoError(t, stp.reChainSubtrees(1))

		require.Empty(t, stp.retiredSubtrees)
	})
}

// TestReChainSubtrees_ReadsMmapCurrentSubtree checks that a rechain over an mmap-backed
// current subtree re-adds its nodes before anything unmaps it. Removing a tx from a chained
// subtree leaves the current subtree as the mmap-backed original.
func TestReChainSubtrees_ReadsMmapCurrentSubtree(t *testing.T) {
	stp, _ := newBlockMaxSizeProcessor(t, 64, model.BlockHeaderSize+100_000, WithMmapDir(t.TempDir()))
	hashes := addBlockMaxSizeTestTxs(t, stp, sizes(200, blockMaxSizeTestTxSize)...)
	require.True(t, stp.currentSubtree.Load().IsMmapBacked())

	// hashes[70] lives in the second chained subtree
	require.NoError(t, stp.removeTxFromSubtrees(t.Context(), hashes[70]))

	expected := append(append([]chainhash.Hash{}, hashes[:70]...), hashes[71:]...)
	require.Equal(t, expected, queuedTxHashes(stp))
}

// TestEnforceBlockMaxSize_NextBlockStartsAtPreviousSize checks that a resize only lasts
// for the current block: the next block starts at the subtree size from before the resize.
func TestEnforceBlockMaxSize_NextBlockStartsAtPreviousSize(t *testing.T) {
	stp, _ := newBlockMaxSizeProcessor(t, 64, model.BlockHeaderSize+blockMaxSizeTestBudget)
	addBlockMaxSizeTestTxs(t, stp, sizes(200, blockMaxSizeTestTxSize)...)

	require.NoError(t, stp.enforceBlockMaxSize(t.Context()))
	requireUniformChain(t, stp, 16)

	// a second resize in the same block shrinks the resized layout further
	stp.chainedSubtrees[0].SizeInBytes = blockMaxSizeTestBudget + 1 // force another breach

	require.NoError(t, stp.enforceBlockMaxSize(t.Context()))
	require.Equal(t, 8, stp.currentSubtree.Load().Size())

	// completed subtrees keep the resized size for the rest of the block
	addBlockMaxSizeTestTxs(t, stp, sizes(20, blockMaxSizeTestTxSize)...)
	require.Equal(t, 8, stp.currentSubtree.Load().Size())

	require.NoError(t, stp.resetSubtreeState(true))

	require.Equal(t, int32(64), stp.currentItemsPerFile.Load())
	require.Equal(t, 64, stp.currentSubtree.Load().Size())
}

// TestReChainSubtrees_KeepsCurrentBlockSize checks that rebuilding the tail after a tx
// removal uses the size of the block's subtrees, not the configured size, so a resized
// layout never mixes subtree lengths.
func TestReChainSubtrees_KeepsCurrentBlockSize(t *testing.T) {
	stp, _ := newBlockMaxSizeProcessor(t, 64, model.BlockHeaderSize+blockMaxSizeTestBudget)
	hashes := addBlockMaxSizeTestTxs(t, stp, sizes(200, blockMaxSizeTestTxSize)...)

	require.NoError(t, stp.enforceBlockMaxSize(t.Context()))
	requireUniformChain(t, stp, 16)

	require.NoError(t, stp.reChainSubtrees(1))

	requireUniformChain(t, stp, 16)
	require.Equal(t, hashes, queuedTxHashes(stp))
}

// TestEnforceBlockMaxSize_WaitsOnlyForFirstBlock checks that only the subtrees one block
// can use are stored synchronously; a failure further down the queue does not fail the
// resize.
func TestEnforceBlockMaxSize_WaitsOnlyForFirstBlock(t *testing.T) {
	stp, h := newBlockMaxSizeProcessor(t, 64, model.BlockHeaderSize+blockMaxSizeTestBudget)
	addBlockMaxSizeTestTxs(t, stp, sizes(200, blockMaxSizeTestTxSize)...)

	// with 16-item subtrees of 1500-1600 bytes only the first fits a 2000-byte block
	h.failAnnouncedIndex.Store(2)

	require.NoError(t, stp.enforceBlockMaxSize(t.Context()))
	require.Len(t, stp.GetPrecomputedMiningData().Subtrees, len(stp.chainedSubtrees))
}

// TestReChainSubtrees_RequestsBlockMaxSizeCheck checks that removing a tx, which can
// shift a large tx into the first subtree, schedules a blockmaxsize check.
func TestReChainSubtrees_RequestsBlockMaxSizeCheck(t *testing.T) {
	stp, _ := newBlockMaxSizeProcessor(t, 64, model.BlockHeaderSize+100_000)
	addBlockMaxSizeTestTxs(t, stp, sizes(200, blockMaxSizeTestTxSize)...)

	require.False(t, stp.blockMaxSizeCheckPending.Load())
	require.NoError(t, stp.reChainSubtrees(0))
	require.True(t, stp.blockMaxSizeCheckPending.Load())
}

// BenchmarkCheckBlockMaxSize_NoOp measures the per-iteration cost of the check on the
// dequeue hot path when the first subtree fits, which must stay O(1) and allocation-free.
func BenchmarkCheckBlockMaxSize_NoOp(b *testing.B) {
	stp, _ := newBlockMaxSizeProcessor(b, 1024, model.BlockHeaderSize+10_000_000)

	parent := chainhash.HashH([]byte("parent-tx"))
	for i := 0; i < 5000; i++ {
		hash := chainhash.HashH([]byte(fmt.Sprintf("tx-%d", i)))
		_ = stp.addNode(subtreepkg.Node{Hash: hash, SizeInBytes: blockMaxSizeTestTxSize},
			&subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{parent}}, true)
	}

	ctx := b.Context()

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		stp.checkBlockMaxSize(ctx)
	}
}
