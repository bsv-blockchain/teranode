package blockvalidation

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	bloboptions "github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// countingSubtreeStore wraps a blob.Store and counts Exists calls so tests can
// assert that writeSubtreeFilesFromTxs reuses the precomputed fullSubtreeExists
// value (computed during prefetch/readSubtree) instead of issuing its own
// existence lookup on the write path.
type countingSubtreeStore struct {
	blob.Store
	existsCalls atomic.Int64
}

func (c *countingSubtreeStore) Exists(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...bloboptions.FileOption) (bool, error) {
	c.existsCalls.Add(1)
	return c.Store.Exists(ctx, key, fileType, opts...)
}

// makeWriteTestBlock builds a minimal block whose Hash() is computable and that
// has room for the given number of subtree slices.
func makeWriteTestBlock(t *testing.T, subtreeSlices int) *model.Block {
	t.Helper()

	return &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Bits:           model.NBit{},
		},
		Height:        1,
		SubtreeSlices: make([]*subtreepkg.Subtree, subtreeSlices),
	}
}

// buildNodeSubtree returns a full (node-only) subtree containing the given tx
// hashes, mirroring what readSubtree deserializes from disk.
func buildNodeSubtree(t *testing.T, txHashes []chainhash.Hash) *subtreepkg.Subtree {
	t.Helper()

	st, err := subtreepkg.NewIncompleteTreeByLeafCount(len(txHashes))
	require.NoError(t, err)

	for _, h := range txHashes {
		require.NoError(t, st.AddNode(h, 0, 0))
	}

	return st
}

// TestWriteSubtreeFilesFromTxs_ReusesPrecomputedExists guards issue #4666: the
// write path must trust the fullSubtreeExists flag plumbed through from the
// prefetch phase and must not issue its own subtreeStore.Exists round-trip.
func TestWriteSubtreeFilesFromTxs_ReusesPrecomputedExists(t *testing.T) {
	ctx := context.Background()

	// Two distinct, non-coinbase txs (varying LockTime => distinct txids, zero fees).
	tx1 := bt.NewTx()
	tx1.LockTime = 1
	tx2 := bt.NewTx()
	tx2.LockTime = 2
	txs := []*bt.Tx{tx1, tx2}
	txHashes := []chainhash.Hash{*tx1.TxIDChainHash(), *tx2.TxIDChainHash()}

	// Use subtree index 1 so the coinbase node is not added and Size() == len(txs).
	const subtreeIdx = 1

	newBV := func(store blob.Store) *BlockValidation {
		return &BlockValidation{
			logger:                      ulogger.TestLogger{},
			subtreeStore:                store,
			subtreeBlockHeightRetention: 10,
		}
	}

	t.Run("does not exist: builds and writes without calling Exists", func(t *testing.T) {
		store := &countingSubtreeStore{Store: blobmemory.New()}
		bv := newBV(store)
		block := makeWriteTestBlock(t, 2)
		nodeSubtree := buildNodeSubtree(t, txHashes)

		err := bv.writeSubtreeFilesFromTxs(ctx, block, subtreeIdx, nodeSubtree, txs, *nodeSubtree.RootHash(), false, false)
		require.NoError(t, err)

		// No existence lookup should have been issued on the write path.
		require.Equal(t, int64(0), store.existsCalls.Load())

		// The full subtree file must have been written.
		exists, err := store.Store.Exists(ctx, nodeSubtree.RootHash()[:], fileformat.FileTypeSubtree)
		require.NoError(t, err)
		require.True(t, exists)

		// The built subtree must be attached to the block for merkle validation.
		require.NotNil(t, block.SubtreeSlices[subtreeIdx])
	})

	t.Run("exists: loads existing file without calling Exists", func(t *testing.T) {
		store := &countingSubtreeStore{Store: blobmemory.New()}
		bv := newBV(store)
		block := makeWriteTestBlock(t, 2)
		nodeSubtree := buildNodeSubtree(t, txHashes)

		// Pre-store a full subtree file (as a prior write would have done).
		fullBytes, err := nodeSubtree.Serialize()
		require.NoError(t, err)
		require.NoError(t, store.Store.Set(ctx, nodeSubtree.RootHash()[:], fileformat.FileTypeSubtree, fullBytes))

		err = bv.writeSubtreeFilesFromTxs(ctx, block, subtreeIdx, nodeSubtree, txs, *nodeSubtree.RootHash(), true, false)
		require.NoError(t, err)

		require.Equal(t, int64(0), store.existsCalls.Load())
		require.NotNil(t, block.SubtreeSlices[subtreeIdx])
	})

	t.Run("exists flag trusted: missing file surfaces as error, not a rebuild", func(t *testing.T) {
		// Passing fullSubtreeExists=true while the file is absent proves the flag
		// is trusted verbatim: the code takes the load branch (which errors) rather
		// than re-deriving existence and rebuilding.
		store := &countingSubtreeStore{Store: blobmemory.New()}
		bv := newBV(store)
		block := makeWriteTestBlock(t, 2)
		nodeSubtree := buildNodeSubtree(t, txHashes)

		err := bv.writeSubtreeFilesFromTxs(ctx, block, subtreeIdx, nodeSubtree, txs, *nodeSubtree.RootHash(), true, false)
		require.Error(t, err)
		require.Equal(t, int64(0), store.existsCalls.Load())
	})
}
