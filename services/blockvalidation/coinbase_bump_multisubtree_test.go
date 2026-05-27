// Package blockvalidation tests the coinbase BUMP that is computed for
// peer-received blocks during validation.
package blockvalidation

import (
	"context"
	"testing"

	bc "github.com/bsv-blockchain/go-bc"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/stretchr/testify/require"
)

// TestComputeAndSetCoinbaseBUMP_Reconciliation verifies the invariant that the
// coinbase BUMP computed by the block-validation path (computeAndSetCoinbaseBUMP)
// always reconciles to the block-header merkle root, regardless of how the block
// is partitioned into subtrees.
//
// The "multi-subtree, smaller final" case reproduces the production defect
// documented in rca-172-datahub-inconsistent-subtrees.md /
// rca-subtree-decomposition-conflict.md: when the final subtree holds fewer
// leaves than the first, the block-level proof must be built from the final
// subtree's root LIFTED to the first subtree's height (exactly what
// model.Block.CheckMerkleRoot and blockassembly.createMerkleTreeFromSubtrees do).
// Before the fix this subtest fails (the BUMP is built from the unlifted root);
// the single- and equal-size cases pass either way and act as regression guards.
func TestComputeAndSetCoinbaseBUMP_Reconciliation(t *testing.T) {
	const height = uint32(800_000)

	t.Run("multi-subtree with smaller final subtree (production repro)", func(t *testing.T) {
		ctx := context.Background()
		store := blobmemory.New()
		cb := newTestCoinbaseTx(t, height)

		// subtree0: full, power-of-two (coinbase placeholder + 3 leaves).
		subtree0 := newCoinbaseSubtree(t, store, 4, []byte{0x11, 0x12, 0x13})
		// final subtree: smaller (2 leaves, height 1 < subtree0 height 2).
		subtree1 := newLeafSubtree(t, store, 2, []byte{0x21, 0x22})

		canonical := canonicalRoot(t, cb, subtree0, subtree1)
		block := newMultiSubtreeBlock(t, ctx, cb, height, canonical, subtree0, subtree1)

		assertValidationBUMPReconciles(t, ctx, store, block, cb, canonical)
	})

	t.Run("single subtree (regression guard)", func(t *testing.T) {
		ctx := context.Background()
		store := blobmemory.New()
		cb := newTestCoinbaseTx(t, height)

		subtree0 := newCoinbaseSubtree(t, store, 4, []byte{0x31, 0x32, 0x33})

		s0corr, err := subtree0.RootHashWithReplaceRootNode(cb.TxIDChainHash(), 0, uint64(cb.Size()))
		require.NoError(t, err)
		canonical := s0corr // single subtree: block merkle root == corrected subtree root

		s0Root := *subtree0.RootHash()
		block := &model.Block{
			Header:           &model.BlockHeader{HashMerkleRoot: canonical, HashPrevBlock: &chainhash.Hash{}},
			CoinbaseTx:       cb,
			Subtrees:         []*chainhash.Hash{&s0Root},
			SubtreeSlices:    []*subtreepkg.Subtree{subtree0},
			TransactionCount: uint64(subtree0.Length()),
			Height:           height,
		}
		require.NoError(t, block.CheckMerkleRoot(ctx))

		assertValidationBUMPReconciles(t, ctx, store, block, cb, canonical)
	})

	t.Run("two equal-size subtrees (regression guard)", func(t *testing.T) {
		ctx := context.Background()
		store := blobmemory.New()
		cb := newTestCoinbaseTx(t, height)

		subtree0 := newCoinbaseSubtree(t, store, 4, []byte{0x41, 0x42, 0x43})
		subtree1 := newLeafSubtree(t, store, 4, []byte{0x51, 0x52, 0x53, 0x54}) // same size, no lift needed

		canonical := canonicalRoot(t, cb, subtree0, subtree1)
		block := newMultiSubtreeBlock(t, ctx, cb, height, canonical, subtree0, subtree1)

		assertValidationBUMPReconciles(t, ctx, store, block, cb, canonical)
	})
}

// assertValidationBUMPReconciles runs the real validation BUMP path and asserts
// the resulting coinbase BUMP computes the canonical (header) merkle root.
func assertValidationBUMPReconciles(t *testing.T, ctx context.Context, store blob.Store, block *model.Block, cb *bt.Tx, canonical *chainhash.Hash) {
	t.Helper()

	bv := &BlockValidation{subtreeStore: store}
	require.NoError(t, bv.computeAndSetCoinbaseBUMP(ctx, block))
	require.NotEmpty(t, block.CoinbaseBUMP, "validation should produce a coinbase BUMP")

	parsed, err := bc.NewBUMPFromBytes(block.CoinbaseBUMP)
	require.NoError(t, err)
	root, err := parsed.CalculateRootGivenTxid(cb.TxID())
	require.NoError(t, err)

	require.Equal(t, canonical.String(), root,
		"coinbase BUMP from the validation path must reconcile to the block-header merkle root")
}

// canonicalRoot computes the block merkle root the way model.Block.CheckMerkleRoot
// does: the top tree over [subtree0-with-real-coinbase, lifted-final-subtree].
func canonicalRoot(t *testing.T, cb *bt.Tx, subtree0, finalSubtree *subtreepkg.Subtree) *chainhash.Hash {
	t.Helper()

	s0corr, err := subtree0.RootHashWithReplaceRootNode(cb.TxIDChainHash(), 0, uint64(cb.Size()))
	require.NoError(t, err)

	finalRoot := finalSubtree.RootHash()
	if finalSubtree.Length() < subtree0.Length() {
		finalRoot, err = finalSubtree.RootHashPadded(subtree0.Height)
		require.NoError(t, err)
	}

	top, err := subtreepkg.NewIncompleteTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, top.AddNode(*s0corr, 1, 0))
	require.NoError(t, top.AddNode(*finalRoot, 1, 0))

	root, err := chainhash.NewHash(top.RootHash()[:])
	require.NoError(t, err)
	return root
}

func newMultiSubtreeBlock(t *testing.T, ctx context.Context, cb *bt.Tx, height uint32, canonical *chainhash.Hash, subtree0, finalSubtree *subtreepkg.Subtree) *model.Block {
	t.Helper()

	s0Root := *subtree0.RootHash()
	sFinalRoot := *finalSubtree.RootHash()

	block := &model.Block{
		Header:           &model.BlockHeader{HashMerkleRoot: canonical, HashPrevBlock: &chainhash.Hash{}},
		CoinbaseTx:       cb,
		Subtrees:         []*chainhash.Hash{&s0Root, &sFinalRoot},
		SubtreeSlices:    []*subtreepkg.Subtree{subtree0, finalSubtree},
		TransactionCount: uint64(subtree0.Length() + finalSubtree.Length()),
		Height:           height,
	}

	// Independent oracle: the canonical root we built is exactly the one the
	// block validator accepts for this subtree decomposition.
	require.NoError(t, block.CheckMerkleRoot(ctx))
	return block
}

// newCoinbaseSubtree builds a power-of-two subtree with a coinbase placeholder at
// index 0 plus the given leaves, serializes it (full format, matching the real
// subtree store) and stores it under its placeholder root hash.
func newCoinbaseSubtree(t *testing.T, store blob.Store, capacity int, leafSeeds []byte) *subtreepkg.Subtree {
	t.Helper()
	st, err := subtreepkg.NewTreeByLeafCount(capacity)
	require.NoError(t, err)
	require.NoError(t, st.AddCoinbaseNode())
	for _, seed := range leafSeeds {
		require.NoError(t, st.AddNode(mkLeaf(seed), 1, 100))
	}
	storeFullSubtree(t, store, st)
	return st
}

// newLeafSubtree builds a subtree of regular leaves (no coinbase) and stores it.
func newLeafSubtree(t *testing.T, store blob.Store, capacity int, leafSeeds []byte) *subtreepkg.Subtree {
	t.Helper()
	st, err := subtreepkg.NewTreeByLeafCount(capacity)
	require.NoError(t, err)
	for _, seed := range leafSeeds {
		require.NoError(t, st.AddNode(mkLeaf(seed), 1, 100))
	}
	storeFullSubtree(t, store, st)
	return st
}

func storeFullSubtree(t *testing.T, store blob.Store, st *subtreepkg.Subtree) {
	t.Helper()
	b, err := st.Serialize()
	require.NoError(t, err)
	require.NoError(t, store.Set(context.Background(), st.RootHash()[:], fileformat.FileTypeSubtree, b))
}

func mkLeaf(seed byte) chainhash.Hash {
	var h chainhash.Hash
	for i := range h {
		h[i] = seed
	}
	return h
}

func newTestCoinbaseTx(t *testing.T, height uint32) *bt.Tx {
	t.Helper()
	cb := bt.NewTx()
	require.NoError(t, cb.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0))
	cb.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x03, byte(height), byte(height >> 8), byte(height >> 16), '/', 'T'})
	require.NoError(t, cb.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 5_000_000_000))
	return cb
}
