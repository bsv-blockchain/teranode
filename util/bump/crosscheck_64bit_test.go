package bump_test

import (
	"testing"

	bc "github.com/bsv-blockchain/go-bc"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/util/bump"
	"github.com/bsv-blockchain/teranode/util/merkleproof"
	"github.com/stretchr/testify/require"
)

// TestConvertToBUMP64BitCrosscheck runs an offset above 2^32 (issue 1427) through the
// go-bc reference implementation. It pins two things: that a 9-byte VarInt offset
// survives the wire round trip into go-bc's parser, and that the per-level offsets
// Teranode writes satisfy go-bc's own sibling rule at every level — go-bc derives each
// sibling as (index >> height) ^ 1 and fails outright if the node it needs is not
// where that rule says it should be.
//
// The starting offset below is a hand-derived literal rather than a recomputation of
// the production expression, so a change to that expression fails this test instead of
// moving both sides together. What this test cannot do is validate the offset against a
// real merkle tree: the sibling hashes are arbitrary bytes. That end-to-end check —
// real subtrees, folded to the block header's own merkle root — lives in
// coinbase_placeholder_crosscheck_test.go, at small scale.
func TestConvertToBUMP64BitCrosscheck(t *testing.T) {
	const (
		subtreeLevels = 32
		blockLevels   = 2
		subtreeIndex  = 3
		txIndex       = 5
	)

	mkHash := func(b byte) chainhash.Hash {
		var h chainhash.Hash
		h[0] = b
		h[31] = 0x3c

		return h
	}

	txID := mkHash(0xee)

	subtreeProof := make([]chainhash.Hash, subtreeLevels)
	for i := range subtreeProof {
		subtreeProof[i] = mkHash(byte(i + 1))
	}

	blockProof := make([]chainhash.Hash, blockLevels)
	for i := range blockProof {
		blockProof[i] = mkHash(byte(100 + i))
	}

	proof := &merkleproof.MerkleProof{
		TxID:             txID,
		BlockHeight:      800000,
		SubtreeIndex:     subtreeIndex,
		TxIndexInSubtree: txIndex,
		SubtreeProof:     subtreeProof,
		BlockProof:       blockProof,
	}

	converted, err := bump.ConvertToBUMP(proof)
	require.NoError(t, err)

	binary, err := converted.EncodeBinary()
	require.NoError(t, err)

	// Manual fold: start at the leaf, combine with each sibling by offset
	// parity, exactly as any BRC-74 verifier must. The offset is the hand-derived
	// literal (3 * 2^32 + 5), not the production expression.
	offset := uint64(12884901893)
	working := txID

	fold := func(a, b chainhash.Hash) chainhash.Hash {
		combined := make([]byte, 64)
		copy(combined[:32], a[:])
		copy(combined[32:], b[:])

		return chainhash.DoubleHashH(combined)
	}

	for _, sibling := range append(append([]chainhash.Hash{}, subtreeProof...), blockProof...) {
		if offset%2 == 0 {
			working = fold(working, sibling)
		} else {
			working = fold(sibling, working)
		}

		offset >>= 1
	}

	expectedRoot := working.String()

	// go-bc reference implementation: parse the wire bytes and compute the
	// root from the txid.
	parsed, err := bc.NewBUMPFromBytes(binary)
	require.NoError(t, err)

	gotRoot, err := parsed.CalculateRootGivenTxid(txID.String())
	require.NoError(t, err)
	require.Equal(t, expectedRoot, gotRoot, "go-bc must reconstruct the same root through the >2^32 offsets")
}
