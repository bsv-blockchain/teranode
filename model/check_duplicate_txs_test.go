package model

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

// randHash returns a deterministic non-placeholder hash seeded by n.
func randHash(n byte) chainhash.Hash {
	var h chainhash.Hash
	h[0] = n
	h[1] = 0xAB
	return h
}

// subtreeWithHashes builds a *subtreepkg.Subtree whose Nodes contain
// exactly the hashes provided, in order.
func subtreeWithHashes(hashes ...chainhash.Hash) *subtreepkg.Subtree {
	st := &subtreepkg.Subtree{}
	for _, h := range hashes {
		h := h
		st.Nodes = append(st.Nodes, subtreepkg.Node{Hash: h})
	}
	return st
}

// TestCheckSubtreeSlicesForDuplicateTxs_Clean verifies that a set of
// slices with no duplicate hashes passes without error.
func TestCheckSubtreeSlicesForDuplicateTxs_Clean(t *testing.T) {
	slices := []*subtreepkg.Subtree{
		// First subtree: coinbase placeholder + two unique txs
		subtreeWithHashes(subtreepkg.CoinbasePlaceholderHashValue, randHash(1), randHash(2)),
		// Second subtree: two more unique txs
		subtreeWithHashes(randHash(3), randHash(4)),
	}

	err := CheckSubtreeSlicesForDuplicateTxs(slices)
	require.NoError(t, err, "clean slices must pass dedup check")
}

// TestCheckSubtreeSlicesForDuplicateTxs_DuplicateAcrossSubtrees verifies that
// a hash present in two different subtrees is caught.
func TestCheckSubtreeSlicesForDuplicateTxs_DuplicateAcrossSubtrees(t *testing.T) {
	dup := randHash(0x42)

	slices := []*subtreepkg.Subtree{
		subtreeWithHashes(subtreepkg.CoinbasePlaceholderHashValue, dup, randHash(1)),
		subtreeWithHashes(dup, randHash(2)), // dup repeated
	}

	err := CheckSubtreeSlicesForDuplicateTxs(slices)
	require.Error(t, err, "duplicate hash must be detected")

	var terr *errors.Error
	require.ErrorAs(t, err, &terr)
	require.True(t, errors.Is(err, errors.ErrBlockInvalid), "error must be BlockInvalid")
}

// TestCheckSubtreeSlicesForDuplicateTxs_DuplicateWithinOneSubtree verifies that
// two identical hashes inside the same subtree are caught.
func TestCheckSubtreeSlicesForDuplicateTxs_DuplicateWithinOneSubtree(t *testing.T) {
	dup := randHash(0xBE)

	slices := []*subtreepkg.Subtree{
		subtreeWithHashes(subtreepkg.CoinbasePlaceholderHashValue, dup, dup),
	}

	err := CheckSubtreeSlicesForDuplicateTxs(slices)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrBlockInvalid))
}

// TestCheckSubtreeSlicesForDuplicateTxs_CoinbasePlaceholderAllowed verifies that
// the coinbase placeholder appearing exactly once at position [0][0] is not
// treated as a duplicate.
func TestCheckSubtreeSlicesForDuplicateTxs_CoinbasePlaceholderAllowed(t *testing.T) {
	// Only one subtree: coinbase placeholder alone — trivially clean.
	slices := []*subtreepkg.Subtree{
		subtreeWithHashes(subtreepkg.CoinbasePlaceholderHashValue),
	}

	err := CheckSubtreeSlicesForDuplicateTxs(slices)
	require.NoError(t, err, "lone coinbase placeholder must pass")
}

// TestCheckSubtreeSlicesForDuplicateTxs_EmptySlices verifies that nil/empty
// input returns no error.
func TestCheckSubtreeSlicesForDuplicateTxs_EmptySlices(t *testing.T) {
	require.NoError(t, CheckSubtreeSlicesForDuplicateTxs(nil))
	require.NoError(t, CheckSubtreeSlicesForDuplicateTxs([]*subtreepkg.Subtree{}))
}

// TestCheckSubtreeSlicesForDuplicateTxs_NilSubtreeSkipped verifies that a nil
// element inside the slice is skipped gracefully.
func TestCheckSubtreeSlicesForDuplicateTxs_NilSubtreeSkipped(t *testing.T) {
	slices := []*subtreepkg.Subtree{
		subtreeWithHashes(subtreepkg.CoinbasePlaceholderHashValue, randHash(1)),
		nil, // must be skipped, not panic
		subtreeWithHashes(randHash(2)),
	}

	err := CheckSubtreeSlicesForDuplicateTxs(slices)
	require.NoError(t, err)
}

// TestCVE20122459_MerklePassesDedupCatches is the linchpin test for the
// CVE-2012-2459 defence on the unified below-checkpoint route.
//
// # Background — why the merkle root does NOT catch this mutation
//
// go-subtree's BuildMerkleTreeStoreFromBytes computes parent nodes with
// calcMerkle, which, when the right child is the zero hash (returned by
// getNodeHashAt for any index >= len(nodes)), hashes the left child with itself:
//
//	H(left, zeros) → H(left, left)
//
// This is the standard Bitcoin "duplicate-last-when-odd" rule.  It means that
// an honest subtree with N=3 leaf txids  [C, A, B]  and a mutated subtree with
// N=4 leaf txids  [C, A, B, B]  produce IDENTICAL internal merkle hashes and
// therefore IDENTICAL subtree roots:
//
//	N=3: leaves [C,A,B,zeros], pairs → H(C,A), H(B,B*)  → root = H(H(C,A),H(B,B*))
//	N=4: leaves [C,A,B,B],    pairs → H(C,A), H(B,B)   → root = H(H(C,A),H(B,B))
//
// (where B* = B duplicated because B is at an odd position with no right
// neighbour, and zeros is the sentinel returned by getNodeHashAt for i>=length)
//
// The first subtree's node[0] is the coinbase placeholder; CheckMerkleRoot
// replaces it with the real coinbase TXID before computing the root.  That
// replacement is identical in both the honest and mutated cases, so the
// root-preservation property carries through unchanged.
//
// # What the test proves
//
//  1. Build an honest single-subtree block with 3 payload nodes:
//     [coinbase_placeholder, A, B].
//     Record the merkle root produced by CheckMerkleRoot as the header's
//     HashMerkleRoot.
//
//  2. Build a CVE-mutated subtree with 4 payload nodes:
//     [coinbase_placeholder, A, B, B]  (B duplicated to make the count even).
//     Place it on a block whose header carries the HONEST merkle root.
//
//  3. Assert CheckMerkleRoot returns NO error on the mutated block — the
//     duplicate-last rule preserves the root, so the header hash matches.
//     This demonstrates that CheckMerkleRoot alone cannot reject the mutation.
//
//  4. Assert CheckSubtreeSlicesForDuplicateTxs returns a BlockInvalidError on
//     the same mutated subtree slices — dedup is what actually catches it.
//
// The value of having both assertions on the SAME mutated block is that it
// proves the dedup check is load-bearing, not defence-in-depth.
func TestCVE20122459_MerklePassesDedupCatches(t *testing.T) {
	ctx := context.Background()

	// Parse the coinbase transaction used throughout the model tests.
	coinbase, err := bt.NewTxFromString(CoinbaseHex)
	require.NoError(t, err)

	txA := randHash(0x0A)
	txB := randHash(0x0B)

	// ------------------------------------------------------------------ //
	// Step 1: build the honest block (3 payload nodes) and derive the     //
	// header's HashMerkleRoot from CheckMerkleRoot's own computation.     //
	// ------------------------------------------------------------------ //

	honestSubtree := subtreeWithHashes(subtreepkg.CoinbasePlaceholderHashValue, txA, txB)

	// Compute what CheckMerkleRoot will produce for the honest subtree:
	// RootHashWithReplaceRootNode replaces node[0] with the coinbase TXID.
	honestRoot, err := honestSubtree.RootHashWithReplaceRootNode(coinbase.TxIDChainHash(), 0, uint64(coinbase.Size())) //nolint:gosec
	require.NoError(t, err, "honest subtree root must be computable")

	// ------------------------------------------------------------------ //
	// Step 2: build the mutated block — same header merkle root, but the  //
	// subtree contains 4 nodes with B duplicated at the end.              //
	// ------------------------------------------------------------------ //

	// The block header references the honest (3-node) merkle root.
	blockHeader := &BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: honestRoot,
		Bits:           *bits,
	}

	// Fake subtree hash for the Subtrees field (must be non-nil; the exact
	// value does not matter for CheckMerkleRoot — it only uses SubtreeSlices
	// to compute the root, comparing the result against HashMerkleRoot).
	dummyHash := subtreeWithHashes(subtreepkg.CoinbasePlaceholderHashValue, txA, txB, txB).RootHash()

	mutatedBlock := &Block{
		Header:           blockHeader,
		CoinbaseTx:       coinbase,
		TransactionCount: 4,
		SizeInBytes:      256,
		Subtrees:         []*chainhash.Hash{dummyHash},
	}

	// The mutated subtree has B appended a second time.
	mutatedSubtree := subtreeWithHashes(subtreepkg.CoinbasePlaceholderHashValue, txA, txB, txB)
	mutatedBlock.SubtreeSlices = []*subtreepkg.Subtree{mutatedSubtree}

	// ------------------------------------------------------------------ //
	// Step 3: CheckMerkleRoot must PASS — the duplicate-last rule makes   //
	// the 4-node root equal to the 3-node root, so the header matches.   //
	// ------------------------------------------------------------------ //

	err = mutatedBlock.CheckMerkleRoot(ctx)
	require.NoError(t, err,
		"CheckMerkleRoot must not detect the CVE-2012-2459 mutation: "+
			"the duplicate-last-when-odd rule makes the 4-node subtree root "+
			"identical to the honest 3-node subtree root, so the header hash still matches")

	// ------------------------------------------------------------------ //
	// Step 4: CheckSubtreeSlicesForDuplicateTxs must REJECT — B appears   //
	// twice, which is the signal that cannot be faked.                    //
	// ------------------------------------------------------------------ //

	err = CheckSubtreeSlicesForDuplicateTxs(mutatedBlock.SubtreeSlices)
	require.Error(t, err, "dedup check must reject the mutated subtree slices")
	require.True(t, errors.Is(err, errors.ErrBlockInvalid),
		"error must be a BlockInvalidError, got: %v", err)
}
