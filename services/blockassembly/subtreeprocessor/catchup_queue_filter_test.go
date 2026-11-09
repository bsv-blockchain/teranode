package subtreeprocessor

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// Issue 2050: a multi-block catch-up drained the intake queue only on its last
// block, filtered against that block alone. A queued tx already mined in an
// earlier catch-up block went back into the subtrees and the node mined it
// again, in an invalid block.

// catchUpQueueFixture is a processor that is not started, so nothing but the
// catch-up itself consumes the queue, with two foreign blocks on top of start.
type catchUpQueueFixture struct {
	stp       *SubtreeProcessor
	blobStore *blob_memory.Memory
	start     *model.BlockHeader
	block1    *model.Block
	block2    *model.Block

	// block2's subtree files, kept so a test can store them late
	block2Key     *chainhash.Hash
	block2Subtree []byte
	block2Meta    []byte

	txA, txB, txC chainhash.Hash
}

func newCatchUpQueueFixture(t *testing.T, storeBlock2 bool, opts ...Options) *catchUpQueueFixture {
	t.Helper()

	ctx := t.Context()

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, ulogger.TestLogger{}, test.CreateBaseTestSettings(t), utxoStoreURL)
	require.NoError(t, err)

	blobStore := blob_memory.New()

	blockchainClient := &blockchain.Mock{}
	blockchainClient.On("GetBlockIsMined", mock.Anything, mock.Anything).Return(true, nil)
	blockchainClient.On("SetBlockProcessedAt", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	newSubtreeChan := make(chan NewSubtreeRequest, 16)
	go func() {
		for req := range newSubtreeChan {
			if req.ErrChan != nil {
				req.ErrChan <- nil
			}
		}
	}()
	t.Cleanup(func() { close(newSubtreeChan) })

	stp, err := NewSubtreeProcessor(ctx, ulogger.TestLogger{}, test.CreateBaseTestSettings(t), blobStore, blockchainClient, utxoStore, newSubtreeChan, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { stp.Stop(context.Background()) })

	f := &catchUpQueueFixture{
		stp:       stp,
		blobStore: blobStore,
		start:     prevBlockHeader,
		txA:       chainhash.HashH([]byte("catchup-mined-in-block1")),
		txB:       chainhash.HashH([]byte("catchup-mined-in-block2")),
		txC:       chainhash.HashH([]byte("catchup-not-mined")),
	}

	block1Key := storeReorgSubtree(t, ctx, blobStore, []subtreepkg.Node{{Hash: f.txA, Fee: 100, SizeInBytes: 250}})

	scratch := blob_memory.New()
	_, f.block2Key, f.block2Meta = buildReorgSubtree(t, ctx, scratch, []subtreepkg.Node{{Hash: f.txB, Fee: 100, SizeInBytes: 250}})
	f.block2Subtree, err = scratch.Get(ctx, f.block2Key[:], fileformat.FileTypeSubtree)
	require.NoError(t, err)

	if storeBlock2 {
		f.storeBlock2(t)
	}

	h1 := childHeader(f.start, 2050_1)
	h2 := childHeader(h1, 2050_2)

	f.block1 = &model.Block{Height: 1, CoinbaseTx: coinbaseTx2, Subtrees: []*chainhash.Hash{block1Key}, Header: h1}
	f.block2 = &model.Block{Height: 2, CoinbaseTx: coinbaseTx3, Subtrees: []*chainhash.Hash{f.block2Key}, Header: h2}

	stp.InitCurrentBlockHeader(f.start)

	// Drain clock 1ms after the enqueue clock, so the drain cutoff admits the batch.
	enqueueAt := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	stp.queue.clock = fixedClock{t: enqueueAt}
	stp.clock = fixedClock{t: enqueueAt.Add(time.Millisecond)}

	// The backlog a stalled intake builds up while other nodes mine its txs.
	stp.queue.enqueueBatch(
		[]subtreepkg.Node{
			{Hash: f.txA, Fee: 100, SizeInBytes: 250},
			{Hash: f.txB, Fee: 100, SizeInBytes: 250},
			{Hash: f.txC, Fee: 100, SizeInBytes: 250},
		},
		[]*subtreepkg.TxInpoints{inpointsPtr(), inpointsPtr(), inpointsPtr()},
	)

	return f
}

func inpointsPtr() *subtreepkg.TxInpoints {
	in := subtreepkg.NewTxInpoints()
	return &in
}

func (f *catchUpQueueFixture) storeBlock2(t *testing.T) {
	t.Helper()

	require.NoError(t, f.blobStore.Set(t.Context(), f.block2Key[:], fileformat.FileTypeSubtree, f.block2Subtree))
	require.NoError(t, f.blobStore.Set(t.Context(), f.block2Key[:], fileformat.FileTypeSubtreeMeta, f.block2Meta))
}

func catchUpTxMapVariants(t *testing.T) map[string][]Options {
	return map[string][]Options{
		"in_memory":   nil,
		"disk_tx_map": {WithTxMapDirs([]string{t.TempDir(), t.TempDir()})},
	}
}

func TestReorgBlocks_CatchUp_FiltersQueueAgainstEveryBlock(t *testing.T) {
	for name, opts := range catchUpTxMapVariants(t) {
		t.Run(name, func(t *testing.T) {
			f := newCatchUpQueueFixture(t, true, opts...)

			require.NoError(t, f.stp.reorgBlocks(t.Context(), []*model.Block{}, []*model.Block{f.block1, f.block2}))

			require.Equal(t, f.block2.Header.Hash(), f.stp.GetCurrentBlockHeader().Hash())
			require.Zero(t, f.stp.queue.length(), "the catch-up must drain the backlog")

			hashes := collectSubtreeHashes(f.stp)
			require.NotContains(t, hashes, f.txA, "queued tx mined in an earlier catch-up block must not re-enter the subtrees")
			require.NotContains(t, hashes, f.txB, "queued tx mined in the last catch-up block must not re-enter the subtrees")
			require.Contains(t, hashes, f.txC, "queued tx mined in no block must be kept")
		})
	}
}

// A catch-up that fails part-way rolls back to its pre-loop state. The drain
// runs before that state is captured, so the rollback keeps what it drained
// instead of losing it, and a retry then filters it against both blocks.
func TestReorgBlocks_CatchUp_RollbackKeepsDrainedBacklog(t *testing.T) {
	for name, opts := range catchUpTxMapVariants(t) {
		t.Run(name, func(t *testing.T) {
			f := newCatchUpQueueFixture(t, false, opts...)

			require.Error(t, f.stp.reorgBlocks(t.Context(), []*model.Block{}, []*model.Block{f.block1, f.block2}),
				"block2's subtree is missing, so the catch-up must fail")

			require.Equal(t, f.start.Hash(), f.stp.GetCurrentBlockHeader().Hash(), "a failed catch-up must not advance the header")

			hashes := collectSubtreeHashes(f.stp)
			require.Contains(t, hashes, f.txA, "the rollback must keep the drained backlog")
			require.Contains(t, hashes, f.txB, "the rollback must keep the drained backlog")
			require.Contains(t, hashes, f.txC, "the rollback must keep the drained backlog")

			f.storeBlock2(t)

			require.NoError(t, f.stp.reorgBlocks(t.Context(), []*model.Block{}, []*model.Block{f.block1, f.block2}))
			require.Equal(t, f.block2.Header.Hash(), f.stp.GetCurrentBlockHeader().Hash())

			hashes = collectSubtreeHashes(f.stp)
			require.NotContains(t, hashes, f.txA)
			require.NotContains(t, hashes, f.txB)
			require.Contains(t, hashes, f.txC)
		})
	}
}
