package blockvalidation

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestQuickValidateBlock_CoinbaseLengthBinding pins the bitcoin-sv/teranode#4692 fix: the
// quick-validation path never calls block.Valid, so it never ran the bad-coinbase-length check
// (model's step 4b) at all — a merkle-bound block with a one-byte or oversized coinbase scriptSig
// could be silently committed. checkQuickValidationCoinbaseLength closes that gap by running
// model.CoinbaseScriptSigLengthInBounds once subtree processing has returned successfully. Both
// shapes are merkle-bound by the time it runs — subtrees present, by validateSubtrees'
// CheckMerkleRoot; no subtrees, by model.Block.CheckCoinbaseOnlyBodyBound at this route's entry —
// so a bad length is genuine consensus invalidity on either, condemnable once, exactly as model's
// step 4b classifies it. This is defence-in-depth: quick validation only runs for blocks at or
// below the highest hash-verified checkpoint (catchup.go, tryQuickValidation).
func TestQuickValidateBlock_CoinbaseLengthBinding(t *testing.T) {
	t.Run("subtrees present: merkle-bound bad length is condemned invalid, never committed", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()
		setupQuickValidateMocks(suite)

		// Build the same shape of fixture as buildOneSubtreeBlock, but tamper the coinbase's
		// unlocking script to a single byte — below the bad-coinbase-length floor of 2 — BEFORE
		// deriving the header's merkle root, so the body stays genuinely merkle-bound to this
		// (bad-length) coinbase rather than becoming an incidental merkle mismatch.
		txs := transactions.CreateTestTransactionChainWithCount(t, 4)
		coinbaseTx := txs[0]
		regularTxs := txs[1:]
		coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x01})

		block := testhelpers.CreateTestBlocks(t, 1)[0]
		block.Height = 100
		block.CoinbaseTx = coinbaseTx

		subtree, err := subtreepkg.NewIncompleteTreeByLeafCount(3)
		require.NoError(t, err)
		require.NoError(t, subtree.AddCoinbaseNode())
		require.NoError(t, subtree.AddNode(*regularTxs[0].TxIDChainHash(), 1, 1))
		require.NoError(t, subtree.AddNode(*regularTxs[1].TxIDChainHash(), 2, 2))

		subtreeBytes, err := subtree.Serialize()
		require.NoError(t, err)
		require.NoError(t, suite.Server.subtreeStore.Set(t.Context(), subtree.RootHash()[:], fileformat.FileTypeSubtreeToCheck, subtreeBytes))

		subtreeData := subtreepkg.NewSubtreeData(subtree)
		require.NoError(t, subtreeData.AddTx(coinbaseTx, 0))
		require.NoError(t, subtreeData.AddTx(regularTxs[0], 1))
		require.NoError(t, subtreeData.AddTx(regularTxs[1], 2))

		subtreeDataBytes, err := subtreeData.Serialize()
		require.NoError(t, err)
		require.NoError(t, suite.Server.subtreeStore.Set(t.Context(), subtree.RootHash()[:], fileformat.FileTypeSubtreeData, subtreeDataBytes))

		block.Subtrees = []*chainhash.Hash{subtree.RootHash()}
		block.TransactionCount = 3
		block.Header.HashMerkleRoot, err = subtree.RootHashWithReplaceRootNode(coinbaseTx.TxIDChainHash(), 0, 0)
		require.NoError(t, err)

		err = suite.Server.blockValidation.quickValidateBlock(suite.Ctx, block, "test", "")
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrBlockInvalid), "a merkle-bound bad coinbase length must condemn invalid, got: %v", err)
		require.False(t, errors.IsBlockCorrupt(err), "must NOT be corrupt")

		suite.MockBlockchain.AssertNotCalled(t, "AddBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("no subtrees: the coinbase-only binding is asserted, so a bad length condemns invalid", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		suite.MockBlockchain.On("AssignBlockID", mock.Anything, mock.Anything).Return(uint64(1), nil).Maybe()
		suite.MockBlockchain.On("AddBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

		block := testhelpers.CreateTestBlocks(t, 1)[0]
		block.CoinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x01})
		block.Header.HashMerkleRoot = block.CoinbaseTx.TxIDChainHash()

		// The header's merkle root is re-derived from the tampered coinbase, so the body satisfies
		// model's coinbase-only binding rule — which this route now asserts at its entry point. The
		// body is therefore bound, and a bad coinbase length on a bound body is genuine consensus
		// invalidity, condemnable once, exactly as model's step 4b classifies it.
		err := suite.Server.blockValidation.quickValidateBlock(suite.Ctx, block, "test", "")
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrBlockInvalid), "a bound bad coinbase length must condemn invalid, got: %v", err)
		require.False(t, errors.IsBlockCorrupt(err), "must NOT be corrupt")

		suite.MockBlockchain.AssertNotCalled(t, "AddBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	})
}
