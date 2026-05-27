package bump_test

// These tests reproduce, with literal mainnet block data, a coinbase-BUMP defect
// observed in production (see rca-172-datahub-inconsistent-subtrees.md and
// rca-subtree-decomposition-conflict.md): for blocks served with a MULTI-subtree
// decomposition whose final subtree is smaller than the first, the coinbase BUMP
// does NOT reconcile to the block-header merkle root, while the SINGLE-subtree
// representation of the same block reconciles correctly.
//
// The block binaries in testdata/ were fetched directly from the datahub asset
// servers named in the RCAs (GET /api/v1/block/<hash>, the model.Block.Bytes()
// wire format):
//   - block-9506xx-eks-1subtree.block        from teranode-eks-mainnet-eu-1.bsvb.tech (1 subtree, reconciles)
//   - block-9506xx-gorillanode-2subtree.block from mainnet.gorillanode.io            (2 subtrees, does NOT reconcile)
//
// Root cause: the block-validation path that computes a coinbase BUMP for
// peer-received blocks builds the block-level proof from the RAW subtree roots,
// without lifting the smaller final subtree's root to the first subtree's height
// the way model.Block.CheckMerkleRoot and blockassembly do. TestProductionLift…
// below proves that lifting that final root is exactly what reconciles the real
// production blocks.

import (
	"crypto/sha256"
	"math/bits"
	"os"
	"testing"

	bc "github.com/bsv-blockchain/go-bc"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/stretchr/testify/require"
)

type productionBlockFixture struct {
	name             string
	file             string
	subtreeCount     int
	height           uint32
	txCount          uint64
	headerMerkleRoot string
	coinbaseTxID     string
	storedBUMPRoot   string // root the peer's stored coinbase BUMP actually computes
	reconciles       bool   // whether storedBUMPRoot == headerMerkleRoot
}

// Captured from the production fixtures; every value matches the two RCAs.
var productionBlockFixtures = []productionBlockFixture{
	{
		name:             "950739 eks (1 subtree, correct)",
		file:             "testdata/block-950739-eks-1subtree.block",
		subtreeCount:     1,
		height:           950739,
		txCount:          42634,
		headerMerkleRoot: "a5947038622cdbcdbfcc51d4fc78a125797b37ed94d81f207f456a47a828c29e",
		coinbaseTxID:     "97e1b17f21a34df567346d28015d611b4710f3478f690287d0e926dfa3bb6eb4",
		storedBUMPRoot:   "a5947038622cdbcdbfcc51d4fc78a125797b37ed94d81f207f456a47a828c29e",
		reconciles:       true,
	},
	{
		name:             "950739 gorillanode (2 subtrees, broken)",
		file:             "testdata/block-950739-gorillanode-2subtree.block",
		subtreeCount:     2,
		height:           950739,
		txCount:          42634,
		headerMerkleRoot: "a5947038622cdbcdbfcc51d4fc78a125797b37ed94d81f207f456a47a828c29e",
		coinbaseTxID:     "97e1b17f21a34df567346d28015d611b4710f3478f690287d0e926dfa3bb6eb4",
		storedBUMPRoot:   "257de497d9392f11a202eb495823f20a87ee9abde6b1cdfb64e0389ea8fe8844",
		reconciles:       false,
	},
	{
		name:             "950675 eks (1 subtree, correct)",
		file:             "testdata/block-950675-eks-1subtree.block",
		subtreeCount:     1,
		height:           950675,
		txCount:          39657,
		headerMerkleRoot: "32d293e3c4947fee702128ff1552c85d2959880e03f7f225600ed03ae240a202",
		coinbaseTxID:     "154dd01df47b19b5ae2645c3b3173492a3cee9696325a98bcdc97cb8b3aaa9b3",
		storedBUMPRoot:   "32d293e3c4947fee702128ff1552c85d2959880e03f7f225600ed03ae240a202",
		reconciles:       true,
	},
	{
		name:             "950675 gorillanode (2 subtrees, broken)",
		file:             "testdata/block-950675-gorillanode-2subtree.block",
		subtreeCount:     2,
		height:           950675,
		txCount:          39657,
		headerMerkleRoot: "32d293e3c4947fee702128ff1552c85d2959880e03f7f225600ed03ae240a202",
		coinbaseTxID:     "154dd01df47b19b5ae2645c3b3173492a3cee9696325a98bcdc97cb8b3aaa9b3",
		storedBUMPRoot:   "0a4d286f2b97924ef30c5accee73984e2382dcf9b4a79775a0c28877fb8e40fc",
		reconciles:       false,
	},
}

// TestProductionCoinbaseBUMP_Reconciliation characterizes the literal bytes each
// datahub peer serves. It is independent of any code fix (it walks the peer's
// stored BUMP, not a recomputed one): the 1-subtree peers reconcile, the
// 2-subtree peers do not, reproducing the exact wrong roots from the RCAs.
func TestProductionCoinbaseBUMP_Reconciliation(t *testing.T) {
	for _, f := range productionBlockFixtures {
		t.Run(f.name, func(t *testing.T) {
			block := loadProductionBlock(t, f.file)

			require.Len(t, block.Subtrees, f.subtreeCount, "subtree count")
			require.Equal(t, f.height, block.Height, "height")
			require.Equal(t, f.txCount, block.TransactionCount, "tx count")
			require.Equal(t, f.headerMerkleRoot, block.Header.HashMerkleRoot.String(), "header merkle root")
			require.Equal(t, f.coinbaseTxID, block.CoinbaseTx.TxID(), "coinbase txid")
			require.NotEmpty(t, block.CoinbaseBUMP, "peer should serve a stored coinbase BUMP")

			storedRoot := bumpRootFromTxid(t, block.CoinbaseBUMP, f.coinbaseTxID)
			require.Equal(t, f.storedBUMPRoot, storedRoot, "stored BUMP computes the captured root")

			if f.reconciles {
				require.Equal(t, f.headerMerkleRoot, storedRoot,
					"single-subtree peer: stored coinbase BUMP must reconcile to the header")
			} else {
				require.NotEqual(t, f.headerMerkleRoot, storedRoot,
					"multi-subtree peer: stored coinbase BUMP does NOT reconcile (the production defect)")
			}
		})
	}
}

// TestProductionCoinbaseBUMP_LiftReconcilesFinalSubtree proves, on the literal
// production blocks, that the broken multi-subtree BUMP is fixed by lifting the
// final subtree's root to the first subtree's height before composing the
// block-level proof — i.e. the exact step model.Block.CheckMerkleRoot and
// blockassembly already perform but the validation BUMP path omits.
func TestProductionCoinbaseBUMP_LiftReconcilesFinalSubtree(t *testing.T) {
	for _, f := range productionBlockFixtures {
		if f.subtreeCount != 2 {
			continue
		}
		t.Run(f.name, func(t *testing.T) {
			block := loadProductionBlock(t, f.file)

			parsed, err := bc.NewBUMPFromBytes(block.CoinbaseBUMP)
			require.NoError(t, err)

			// For a 2-subtree block the coinbase (subtree 0) reaches the block root
			// in one block-level step, so the BUMP's top level holds a single
			// sibling: the final subtree's root.
			totalLevels := len(parsed.Path)
			subtree0Height := totalLevels - 1
			finalLeafCount := int(block.TransactionCount) - (1 << subtree0Height)
			liftDepth := subtree0Height - ceilLog2(finalLeafCount)
			require.Greater(t, liftDepth, 0, "final subtree must be shorter than the first")

			rawFinalRoot := block.Subtrees[1].String()
			topLevel := parsed.Path[totalLevels-1]
			require.Len(t, topLevel, 1)
			require.Equal(t, rawFinalRoot, *topLevel[0].Hash,
				"stored BUMP's top sibling is the UNLIFTED final-subtree root (the bug)")

			// Lift the final root and recompute: this must reconcile to the header.
			liftedFinalRoot := liftRootHex(t, rawFinalRoot, liftDepth)
			fixed, err := bc.NewBUMPFromBytes(block.CoinbaseBUMP)
			require.NoError(t, err)
			*fixed.Path[totalLevels-1][0].Hash = liftedFinalRoot

			fixedRoot, err := fixed.CalculateRootGivenTxid(f.coinbaseTxID)
			require.NoError(t, err)
			require.Equal(t, f.headerMerkleRoot, fixedRoot,
				"lifting the final subtree root (depth %d) reconciles the coinbase BUMP to the header", liftDepth)
		})
	}
}

func loadProductionBlock(t *testing.T, file string) *model.Block {
	t.Helper()
	data, err := os.ReadFile(file)
	require.NoError(t, err)
	block, err := model.NewBlockFromBytes(data)
	require.NoError(t, err)
	return block
}

func bumpRootFromTxid(t *testing.T, bumpBytes []byte, txid string) string {
	t.Helper()
	parsed, err := bc.NewBUMPFromBytes(bumpBytes)
	require.NoError(t, err)
	root, err := parsed.CalculateRootGivenTxid(txid)
	require.NoError(t, err)
	return root
}

// liftRootHex lifts a merkle root (display-order hex) by `levels` duplicate-and-hash
// steps — the same padding model.Block.CheckMerkleRoot applies via
// subtree.RootHashPadded when promoting a short final subtree to a full slot.
func liftRootHex(t *testing.T, displayHex string, levels int) string {
	t.Helper()
	h, err := chainhash.NewHashFromStr(displayHex)
	require.NoError(t, err)
	cur := h.CloneBytes() // internal byte order
	for i := 0; i < levels; i++ {
		buf := make([]byte, 0, 64)
		buf = append(buf, cur...)
		buf = append(buf, cur...)
		first := sha256.Sum256(buf)
		second := sha256.Sum256(first[:])
		cur = second[:]
	}
	out, err := chainhash.NewHash(cur)
	require.NoError(t, err)
	return out.String()
}

func ceilLog2(n int) int {
	if n <= 1 {
		return 0
	}
	return bits.Len(uint(n - 1))
}
