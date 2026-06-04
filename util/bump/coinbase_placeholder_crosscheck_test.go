package bump_test

import (
	"testing"

	bc "github.com/bsv-blockchain/go-bc"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/util/bump"
	"github.com/bsv-blockchain/teranode/util/merkleproof"
	"github.com/stretchr/testify/require"
)

const testCoinbaseHex = "02000000010000000000000000000000000000000000000000000000000000000000000000ffffffff03510101ffffffff0100f2052a01000000232103656065e6886ca1e947de3471c9e723673ab6ba34724476417fa9fcef8bafa604ac00000000"

// mockConstructor implements merkleproof.MerkleProofConstructor for a single in-memory block.
type mockConstructor struct {
	txMeta      *merkleproof.TxMetaData
	block       *model.Block
	blockHeader *model.BlockHeader
	subtrees    map[string]*subtreepkg.Subtree
}

func (m *mockConstructor) GetTxMeta(*chainhash.Hash) (*merkleproof.TxMetaData, error) {
	return m.txMeta, nil
}
func (m *mockConstructor) GetBlockByID(uint64) (*model.Block, error) { return m.block, nil }
func (m *mockConstructor) GetBlockHeader(*chainhash.Hash) (*model.BlockHeader, error) {
	return m.blockHeader, nil
}
func (m *mockConstructor) GetSubtree(h *chainhash.Hash) (*subtreepkg.Subtree, error) {
	return m.subtrees[h.String()], nil
}
func (m *mockConstructor) FindBlocksContainingSubtree(*chainhash.Hash) ([]uint32, []uint32, []int, error) {
	return []uint32{1}, []uint32{100}, []int{0}, nil
}

// canonicalRoot mirrors model.Block.CheckMerkleRoot: coinbase replacement for subtree 0 and padding
// for an incomplete final subtree, then a top-level merkle tree over the resulting roots.
func canonicalRoot(t *testing.T, subtrees []*subtreepkg.Subtree, coinbaseTx *bt.Tx) *chainhash.Hash {
	t.Helper()

	targetLength := subtrees[0].Length()
	targetHeight := subtrees[0].Height
	roots := make([]subtreepkg.Node, len(subtrees))

	for i, st := range subtrees {
		switch {
		case i == 0:
			r, err := st.RootHashWithReplaceRootNode(coinbaseTx.TxIDChainHash(), 0, uint64(coinbaseTx.Size()))
			require.NoError(t, err)
			roots[i] = subtreepkg.Node{Hash: *r}
		case i == len(subtrees)-1 && st.Length() < targetLength:
			r, err := st.RootHashPadded(targetHeight)
			require.NoError(t, err)
			roots[i] = subtreepkg.Node{Hash: *r}
		default:
			roots[i] = subtreepkg.Node{Hash: *st.RootHash()}
		}
	}

	if len(roots) == 1 {
		return &roots[0].Hash
	}

	store, err := subtreepkg.BuildMerkleTreeStoreFromBytes(roots)
	require.NoError(t, err)
	root := (*store)[len(*store)-1]

	return &root
}

func placeholderSubtree(t *testing.T, txs ...*chainhash.Hash) *subtreepkg.Subtree {
	t.Helper()
	st, err := subtreepkg.NewTreeByLeafCount(len(txs) + 1)
	require.NoError(t, err)
	require.NoError(t, st.AddCoinbaseNode())
	for _, h := range txs {
		require.NoError(t, st.AddNode(*h, 1, 1))
	}
	return st
}

func regularSubtree(t *testing.T, txs ...*chainhash.Hash) *subtreepkg.Subtree {
	t.Helper()
	st, err := subtreepkg.NewTreeByLeafCount(len(txs))
	require.NoError(t, err)
	for _, h := range txs {
		require.NoError(t, st.AddNode(*h, 1, 1))
	}
	return st
}

func newMock(t *testing.T, subtrees []*subtreepkg.Subtree, coinbaseTx *bt.Tx, subtreeIdx int) *mockConstructor {
	t.Helper()

	merkleRoot := canonicalRoot(t, subtrees, coinbaseTx)
	hashes := make([]*chainhash.Hash, len(subtrees))
	stMap := make(map[string]*subtreepkg.Subtree, len(subtrees))

	for i, st := range subtrees {
		h := st.RootHash()
		hashes[i] = h
		stMap[h.String()] = st
	}

	prev, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000000")
	header := &model.BlockHeader{Version: 1, HashPrevBlock: prev, HashMerkleRoot: merkleRoot, Timestamp: 1, Nonce: 1}

	return &mockConstructor{
		txMeta:      &merkleproof.TxMetaData{BlockIDs: []uint32{1}, BlockHeights: []uint32{100}, SubtreeIdxs: []int{subtreeIdx}},
		block:       &model.Block{Header: header, CoinbaseTx: coinbaseTx, Subtrees: hashes},
		blockHeader: header,
		subtrees:    stMap,
	}
}

// verifyViaGoBC encodes the proof to a BUMP, parses it with go-bc, and asserts that
// CalculateRootGivenTxid for the proven txid reproduces the block header merkle root.
func verifyViaGoBC(t *testing.T, proof *merkleproof.MerkleProof) {
	t.Helper()

	format, err := bump.ConvertToBUMP(proof)
	require.NoError(t, err)
	require.NoError(t, bump.Validate(format))

	// The served BUMP must never contain the coinbase placeholder.
	placeholder := subtreepkg.CoinbasePlaceholderHashValue.String()
	for _, level := range format.Path {
		for _, node := range level {
			require.NotEqual(t, placeholder, node.Hash, "BUMP must not contain the coinbase placeholder")
		}
	}

	binary, err := format.EncodeBinary()
	require.NoError(t, err)

	parsed, err := bc.NewBUMPFromBytes(binary)
	require.NoError(t, err)

	root, err := parsed.CalculateRootGivenTxid(proof.TxID.String())
	require.NoError(t, err)
	require.Equal(t, proof.MerkleRoot.String(), root,
		"go-bc CalculateRootGivenTxid must reproduce the block header merkle root")
}

// TestBUMPCoinbasePlaceholderEndToEnd is the end-to-end regression test for discussion #989: it runs
// the full Assets-API proof path (ConstructMerkleProof -> ConvertToBUMP -> binary) and verifies the
// result with the go-bc reference implementation.
func TestBUMPCoinbasePlaceholderEndToEnd(t *testing.T) {
	coinbaseTx, err := bt.NewTxFromString(testCoinbaseHex)
	require.NoError(t, err)

	tx := make([]*chainhash.Hash, 8)
	for i := range tx {
		h := chainhash.DoubleHashH([]byte{byte(i + 1), 0xab})
		tx[i] = &h
	}

	t.Run("single subtree, tx#1 adjacent to coinbase", func(t *testing.T) {
		st0 := placeholderSubtree(t, tx[0], tx[1], tx[2])
		mock := newMock(t, []*subtreepkg.Subtree{st0}, coinbaseTx, 0)

		proof, err := merkleproof.ConstructMerkleProof(tx[0], mock)
		require.NoError(t, err)

		verifyViaGoBC(t, proof)
	})

	t.Run("single subtree, deeper tx", func(t *testing.T) {
		st0 := placeholderSubtree(t, tx[0], tx[1], tx[2])
		mock := newMock(t, []*subtreepkg.Subtree{st0}, coinbaseTx, 0)

		proof, err := merkleproof.ConstructMerkleProof(tx[2], mock)
		require.NoError(t, err)

		verifyViaGoBC(t, proof)
	})

	t.Run("multi-subtree complete, tx in subtree 1", func(t *testing.T) {
		st0 := placeholderSubtree(t, tx[0])
		st1 := regularSubtree(t, tx[1], tx[2])
		mock := newMock(t, []*subtreepkg.Subtree{st0, st1}, coinbaseTx, 1)

		proof, err := merkleproof.ConstructMerkleProof(tx[1], mock)
		require.NoError(t, err)

		verifyViaGoBC(t, proof)
	})

	t.Run("incomplete final subtree, tx in final subtree", func(t *testing.T) {
		st0 := placeholderSubtree(t, tx[0], tx[1], tx[2]) // length 4
		st1 := regularSubtree(t, tx[3], tx[4], tx[5], tx[6])
		st2 := regularSubtree(t, tx[7]) // length 1, incomplete -> padded
		mock := newMock(t, []*subtreepkg.Subtree{st0, st1, st2}, coinbaseTx, 2)

		proof, err := merkleproof.ConstructMerkleProof(tx[7], mock)
		require.NoError(t, err)

		verifyViaGoBC(t, proof)
	})
}
