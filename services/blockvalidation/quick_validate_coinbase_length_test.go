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
// model.CoinbaseScriptSigLengthInBounds once binding is settled, classifying bind-aware exactly like
// model's own rule: bound (subtrees present, merkle root already verified by validateSubtrees) ->
// invalid; unbound (no subtrees, and nothing on this route asserts model's coinbase-only binding
// rule) -> corrupt. This is defence-in-depth: quick validation only runs for blocks at or below
// the highest hash-verified checkpoint (catchup.go, tryQuickValidation).
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

	t.Run("no subtrees: route asserts no binding, so a bad length stays corrupt, never committed", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		suite.MockBlockchain.On("AssignBlockID", mock.Anything, mock.Anything).Return(uint64(1), nil).Maybe()
		suite.MockBlockchain.On("AddBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

		block := testhelpers.CreateTestBlocks(t, 1)[0]
		block.CoinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x01})
		// Re-derive the header's merkle root from the tampered coinbase, so the body would
		// actually satisfy model's coinbase-only binding rule (header merkle root == coinbase
		// txid) if it were checked here. It never is on this route — proving the corrupt
		// classification comes from that gap, not from an incidental merkle mismatch.
		block.Header.HashMerkleRoot = block.CoinbaseTx.TxIDChainHash()

		err := suite.Server.blockValidation.quickValidateBlock(suite.Ctx, block, "test", "")
		require.Error(t, err)
		require.True(t, errors.IsBlockCorrupt(err), "an unasserted-binding body must stay corrupt, got: %v", err)
		require.False(t, errors.Is(err, errors.ErrBlockInvalid), "must NOT be condemned invalid")

		suite.MockBlockchain.AssertNotCalled(t, "AddBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	})
}
