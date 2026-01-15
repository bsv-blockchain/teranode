package smoke

import (
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/test/utils/aerospike"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/stretchr/testify/require"
)

// TestReorgTransactionPropagation tests that transactions from a fork are properly
// propagated to block assembly when that fork becomes the longest chain after a reorg.
func TestReorgTransactionPropagation(t *testing.T) {
	SharedTestLock.Lock()
	defer SharedTestLock.Unlock()

	// aerospike
	utxoStoreURL, teardown, err := aerospike.InitAerospikeContainer()
	require.NoError(t, err, "Failed to setup Aerospike container")
	parsedURL, err := url.Parse(utxoStoreURL)
	require.NoError(t, err, "Failed to parse UTXO store URL")
	t.Cleanup(func() {
		_ = teardown()
	})

	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableValidator: true,
		EnableP2P:       true,
		SettingsOverrideFunc: func(s *settings.Settings) {
			s.UtxoStore.UtxoStore = parsedURL
		},
	})
	defer td.Stop(t)

	err = td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	// ========================================================================
	// STAGE 1: Mine blocks to maturity (2 blocks with CoinbaseMaturity=1)
	// ========================================================================
	// Chain state:
	//   Genesis -> Block 1 -> Block 2 (*)
	// ========================================================================
	t.Log("STAGE 1: Mining blocks to maturity...")
	coinbaseTxA := td.MineToMaturityAndGetSpendableCoinbaseTx(t, td.Ctx)
	require.NotNil(t, coinbaseTxA)

	heightAfterMaturity, _, err := td.BlockchainClient.GetBestHeightAndTime(td.Ctx)
	require.NoError(t, err)
	t.Logf("Height after maturity: %d", heightAfterMaturity)

	// ========================================================================
	// STAGE 2: Create TXA and mine it in a block (Chain A)
	// ========================================================================
	// Chain state:
	//   Genesis -> Block 1 -> Block 2 -> Block 3 [TXA] (*)
	// ========================================================================
	t.Log("STAGE 2: Creating TXA...")
	txA := td.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTxA, 0),
		transactions.WithP2PKHOutputs(1, 1000000),
	)
	require.NotNil(t, txA)

	err = td.PropagationClient.ProcessTransaction(td.Ctx, txA)
	require.NoError(t, err)
	td.VerifyInBlockAssembly(t, txA)
	td.VerifyNotOnLongestChainInUtxoStore(t, txA) // not mined yet
	t.Logf("TXA created and submitted: %s", txA.TxIDChainHash().String())

	t.Log("Mining block with TXA (chain A)...")
	blockWithTxA := td.MineAndWait(t, 1)
	require.NotNil(t, blockWithTxA)

	heightChainA, _, err := td.BlockchainClient.GetBestHeightAndTime(td.Ctx)
	require.NoError(t, err)
	t.Logf("Chain A height: %d", heightChainA)

	td.WaitForBlock(t, blockWithTxA, 10*time.Second, true)
	td.VerifyNotInBlockAssembly(t, txA)
	td.VerifyOnLongestChainInUtxoStore(t, txA)

	// ========================================================================
	// STAGE 3: Create fork at same height without TXA (Chain B)
	// ========================================================================
	// Chain state:
	//   Genesis -> Block 1 -> Block 2 -> Block 3 [TXA] (*)
	//                                 \
	//                                  -> Block 3B (fork, no TXA)
	// ========================================================================
	forkHeight := heightChainA - 1
	t.Logf("STAGE 3: Creating fork at height %d", forkHeight)
	forkBlock, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, forkHeight)
	require.NoError(t, err)
	require.NotNil(t, forkBlock)

	t.Log("Creating fork block (chain B)...")
	nonce := uint32(200)
	_, block3B := td.CreateTestBlock(t, forkBlock, nonce)
	require.NotNil(t, block3B)

	err = td.BlockValidation.ValidateBlock(td.Ctx, block3B, "")
	require.NoError(t, err)
	t.Logf("Fork block created at height: %d", block3B.Height)

	// ========================================================================
	// STAGE 4: Extend Chain B by 2 blocks to make it longest
	// ========================================================================
	// Chain state:
	//   Genesis -> Block 1 -> Block 2 -> Block 3 [TXA]
	//                                 \
	//                                  -> Block 3B -> Block 4B -> Block 5B (*)
	// ========================================================================
	t.Log("STAGE 4: Mining 2 more blocks on chain B to make it longest...")
	nonce++
	_, block4B := td.CreateTestBlock(t, block3B, nonce)
	require.NotNil(t, block4B)
	err = td.BlockValidation.ValidateBlock(td.Ctx, block4B, "")
	require.NoError(t, err)

	nonce++
	_, block5B := td.CreateTestBlock(t, block4B, nonce)
	require.NotNil(t, block5B)
	err = td.BlockValidation.ValidateBlock(td.Ctx, block5B, "")
	require.NoError(t, err)

	td.WaitForBlock(t, block5B, 10*time.Second, true)
	t.Logf("Chain B extended to height: %d (hash %s)", block5B.Height, block5B.Hash().String())

	// ========================================================================
	// STAGE 5: Create parent TXB and child spending Chain B coinbase
	// ========================================================================
	// Chain state:
	//   Genesis -> Block 1 -> Block 2 -> Block 3 [TXA]
	//                                 \
	//                                  -> Block 3B [coinbase] -> Block 4B -> Block 5B (*)
	//                                      |
	//                                      +-> Parent TXB -> Child TXB (in mempool)
	// ========================================================================
	t.Log("STAGE 5: Creating parent TXB from chain B coinbase...")
	coinbaseTxB := block3B.CoinbaseTx
	require.NotNil(t, coinbaseTxB)

	parentTxB := td.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTxB, 0),
		transactions.WithP2PKHOutputs(1, 2000000),
	)
	require.NotNil(t, parentTxB)

	// err = td.PropagationClient.ProcessTransaction(td.Ctx, parentTxB)
	// require.NoError(t, err)
	// t.Logf("Parent TXB created and submitted: %s", parentTxB.TxIDChainHash().String())

	t.Log("Creating child transaction spending parent TXB...")
	childTxB := td.CreateTransactionWithOptions(t,
		transactions.WithInput(parentTxB, 0),
		transactions.WithP2PKHOutputs(1, 1500000),
	)
	require.NotNil(t, childTxB)

	// err = td.PropagationClient.ProcessTransaction(td.Ctx, childTxB)
	// require.NoError(t, err)
	// t.Logf("Child TxB created and submitted: %s", childTxB.TxIDChainHash().String())
	// Create block4B and mine it
	_, block6B := td.CreateTestBlock(t, block5B, nonce, parentTxB, childTxB)
	require.NotNil(t, block6B)
	err = td.BlockValidation.ValidateBlock(td.Ctx, block6B, "legacy")
	require.NoError(t, err)

	td.WaitForBlock(t, block6B, 10*time.Second, true)

	heightChainB, _, err := td.BlockchainClient.GetBestHeightAndTime(td.Ctx)
	require.NoError(t, err)
	t.Logf("Chain B height: %d", heightChainB)

	td.VerifyNotInBlockAssembly(t, parentTxB)
	td.VerifyNotInBlockAssembly(t, childTxB)
	td.VerifyOnLongestChainInUtxoStore(t, parentTxB)
	td.VerifyOnLongestChainInUtxoStore(t, childTxB)
	td.VerifyInBlockAssembly(t, txA)
	td.VerifyNotOnLongestChainInUtxoStore(t, txA)

	// ========================================================================
	// STAGE 6: Extend Chain A to make it longest again (reorg happens)
	// ========================================================================
	// Chain state:
	//   Genesis -> Block 1 -> Block 2 -> Block 3 [TXA] -> Block 4A -> Block 5A -> Block 6A -> Block 7A (*)
	//                                 \
	//                                  -> Block 3B [coinbase] -> Block 4B -> Block 5B -> Block 6B
	//                                      |
	//                                      +-> Parent TXB -> Child TXB (should be in block assembly)
	// ========================================================================
	t.Log("STAGE 6: Making chain A longer to trigger reorg...")
	// We need to mine enough blocks on chain A to make it longer than chain B
	// Chain B is at height 5 (Block 5B), chain A is at height 3 (Block 3)
	// So we need to mine at least 3 more blocks on chain A (to reach height 6)

	// Mine 4 blocks on chain A to make it longer
	nonce = uint32(300)
	previousBlock := blockWithTxA
	for i := 0; i < 5; i++ {
		nonce++
		_, newBlock := td.CreateTestBlock(t, previousBlock, nonce)
		require.NotNil(t, newBlock)
		err = td.BlockValidation.ValidateBlock(td.Ctx, newBlock, "legacy")
		require.NoError(t, err)
		previousBlock = newBlock
		t.Logf("Mined block %d on chain A at height: %d", i+1, newBlock.Height)
	}

	heightFinal, _, err := td.BlockchainClient.GetBestHeightAndTime(td.Ctx)
	require.NoError(t, err)
	t.Logf("Final best height: %d (reorg complete)", heightFinal)

	// Chain A should now be the active chain
	require.Greater(t, heightFinal, heightChainB, "Chain A should now be longer than chain B")

	// wait for the block to be processed
	td.WaitForBlock(t, previousBlock, 10*time.Second, true)

	// ========================================================================
	// STAGE 7: Verify both parent TXB and child are in block assembly
	// ========================================================================
	// Final chain state after reorg:
	//   Genesis -> Block 1 -> Block 2 -> Block 3 [TXA] -> Block 4A -> Block 5A -> Block 6A -> Block 7A -> Block 8A (*)
	//                                 \
	//                                  -> Block 3B [coinbase] -> Block 4B -> Block 5B (orphaned)
	//
	// After reorg, Chain A is longest. Parent TXB and Child TXB should NOT
	// be in block assembly because their inputs (Block 3B coinbase) are now orphaned.
	// ========================================================================
	t.Log("STAGE 7: Verifying parent TXB and child are NOT in block assembly after reorg...")
	td.VerifyNotInBlockAssembly(t, txA)
	td.VerifyOnLongestChainInUtxoStore(t, txA)
	td.VerifyNotInBlockAssembly(t, parentTxB)
	td.VerifyNotInBlockAssembly(t, childTxB)
	td.VerifyNotInUtxoStore(t, parentTxB)
	td.VerifyNotInUtxoStore(t, childTxB)

	// block9A will be invalid, since parentTxB and childTxB are not valid anymore
	// because their input (coinbase from block3B) is now orphaned.
	_, block9A := td.CreateTestBlock(t, previousBlock, nonce, parentTxB, childTxB)
	require.NotNil(t, block9A)
	err = td.BlockValidation.ValidateBlock(td.Ctx, block9A, "legacy", true)
	require.Error(t, err)
	// td.WaitForBlock(t, block9A, 10*time.Second, true)

	// should revert back to previous block
	// TODO: enable once waiting for block processing is fixed
	td.WaitForBlock(t, previousBlock, 30*time.Second, true)
}
