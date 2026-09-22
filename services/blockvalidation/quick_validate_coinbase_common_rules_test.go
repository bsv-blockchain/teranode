package blockvalidation

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestQuickValidate_CoinbaseCommonRules pins bitcoin-sv/teranode#4835 on the quick-validation path,
// which never calls block.Valid and so has to enforce model.CoinbaseCommonRuleViolation itself. By
// the time the check runs the body is bound on both shapes (see checkQuickValidationCoinbase), so a
// coinbase with no outputs is genuine invalidity and must never be committed.
//
// Mutation proof: removing the CoinbaseCommonRuleViolation call from checkQuickValidationCoinbase
// lets every subtest commit the block, so its invalid assertion and its "AddBlock not called"
// assertion both redden.
func TestQuickValidate_CoinbaseCommonRules(t *testing.T) {
	t.Run("quickValidateBlock: coinbase-only body with no outputs is invalid, never committed", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		// Permissive, so a failure to reject shows up as a COMMIT rather than as a mock panic. A
		// coinbase-only body never reaches the UTXO store, so the subtree-body mocks do not apply.
		suite.MockBlockchain.On("AssignBlockID", mock.Anything, mock.Anything).Return(uint64(1), nil).Maybe()
		suite.MockBlockchain.On("AddBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
		suite.MockBlockchain.On("SetBlockSubtreesSet", mock.Anything, mock.Anything).Return(nil).Maybe()

		block := testhelpers.CreateTestBlocks(t, 1)[0]
		block.CoinbaseTx.Outputs = nil
		block.Header.HashMerkleRoot = block.CoinbaseTx.TxIDChainHash()

		err := suite.Server.blockValidation.quickValidateBlock(suite.Ctx, block, "test", "")
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrBlockInvalid), "a bound coinbase with no outputs must condemn invalid, got: %v", err)
		require.False(t, errors.IsBlockCorrupt(err), "must NOT be corrupt")
		require.Contains(t, err.Error(), "bad-txns-vout-empty")

		suite.MockBlockchain.AssertNotCalled(t, "AddBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("quickValidateBlockAsync: same body, same verdict", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		// Permissive, so a failure to reject shows up as a COMMIT rather than as a mock panic. A
		// coinbase-only body never reaches the UTXO store, so the subtree-body mocks do not apply.
		suite.MockBlockchain.On("AssignBlockID", mock.Anything, mock.Anything).Return(uint64(1), nil).Maybe()
		suite.MockBlockchain.On("AddBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
		suite.MockBlockchain.On("SetBlockSubtreesSet", mock.Anything, mock.Anything).Return(nil).Maybe()

		block := testhelpers.CreateTestBlocks(t, 1)[0]
		block.CoinbaseTx.Outputs = nil
		block.Header.HashMerkleRoot = block.CoinbaseTx.TxIDChainHash()

		// Buffered so the async path never blocks queuing write jobs; this body queues none.
		writeJobsChan := make(chan *SubtreeWriteJob, 16)

		_, _, err := suite.Server.blockValidation.quickValidateBlockAsync(suite.Ctx, block, "test", "", writeJobsChan)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrBlockInvalid), "the catch-up entry point must condemn the same body, got: %v", err)
		require.False(t, errors.IsBlockCorrupt(err), "must NOT be corrupt")
		require.Contains(t, err.Error(), "bad-txns-vout-empty")

		suite.MockBlockchain.AssertNotCalled(t, "AddBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("quickValidateBlock: subtree-carrying body whose coinbase has no outputs is invalid", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()
		setupQuickValidateMocks(suite)

		// Same shape as the coinbase-length fixture: the coinbase is tampered BEFORE the header's
		// merkle root is derived, so the body stays bound to this output-less coinbase.
		txs := transactions.CreateTestTransactionChainWithCount(t, 4)
		coinbaseTx := txs[0]
		regularTxs := txs[1:]
		coinbaseTx.Outputs = nil

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
		require.True(t, errors.Is(err, errors.ErrBlockInvalid), "a bound coinbase with no outputs must condemn invalid, got: %v", err)
		require.False(t, errors.IsBlockCorrupt(err), "must NOT be corrupt")
		require.Contains(t, err.Error(), "bad-txns-vout-empty")

		suite.MockBlockchain.AssertNotCalled(t, "AddBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	})
}
