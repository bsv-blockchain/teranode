package longest_chain

import (
	"testing"

	"github.com/bsv-blockchain/teranode/test/utils/aerospike"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/stretchr/testify/require"
)

func TestLongestChainAerospikeInvalidateFork(t *testing.T) {
	// start an aerospike container
	utxoStore, teardown, err := aerospike.InitAerospikeContainer()
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = teardown()
	})

	t.Run("invalid block with old tx", func(t *testing.T) {
		testLongestChainInvalidateFork(t, utxoStore)
	})
}

func testLongestChainInvalidateFork(t *testing.T, utxoStore string) {
	// Setup test environment
	td, block3 := setupLongestChainTest(t, utxoStore)
	defer func() {
		td.Stop(t)
	}()

	td.Settings.BlockValidation.OptimisticMining = true

	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err)

	parentTxWith3Outputs := td.CreateTransactionWithOptions(t, transactions.WithInput(block1.CoinbaseTx, 0), transactions.WithP2PKHOutputs(3, 100000))
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, parentTxWith3Outputs))

	childTx1 := td.CreateTransactionWithOptions(t, transactions.WithInput(parentTxWith3Outputs, 0), transactions.WithP2PKHOutputs(1, 100000))
	childTx2 := td.CreateTransactionWithOptions(t, transactions.WithInput(parentTxWith3Outputs, 1), transactions.WithP2PKHOutputs(1, 100000))
	childTx3 := td.CreateTransactionWithOptions(t, transactions.WithInput(parentTxWith3Outputs, 2), transactions.WithP2PKHOutputs(1, 100000))
	// create a double spend of tx3
	childTx3DS := td.CreateTransactionWithOptions(t, transactions.WithInput(parentTxWith3Outputs, 2), transactions.WithP2PKHOutputs(2, 50000))

	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, childTx1))
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, childTx2))
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, childTx3))

	_, block4a := td.CreateTestBlock(t, block3, 4001, parentTxWith3Outputs, childTx1, childTx2)
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block4a, "legacy", nil, false, true), "Failed to process block")
	td.WaitForBlockBeingMined(t, block4a)
	t.Logf("WaitForBlock(t, block4a, blockWait): %s", block4a.Hash().String())
	td.WaitForBlock(t, block4a, blockWait)

	// 0 -> 1 ... 2 -> 3 -> 4a (*)

	td.VerifyNotInBlockAssembly(t, parentTxWith3Outputs)
	td.VerifyNotInBlockAssembly(t, childTx1)
	td.VerifyNotInBlockAssembly(t, childTx2)
	td.VerifyInBlockAssembly(t, childTx3)
	td.VerifyOnLongestChainInUtxoStore(t, parentTxWith3Outputs)
	td.VerifyOnLongestChainInUtxoStore(t, childTx1)
	td.VerifyOnLongestChainInUtxoStore(t, childTx2)
	td.VerifyNotOnLongestChainInUtxoStore(t, childTx3)

	// create a block with tx1 and tx2 that will be invalid as tx2 is already on block4a
	_, block4b := td.CreateTestBlock(t, block3, 4002, parentTxWith3Outputs, childTx2, childTx3)
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block4b, "legacy", nil, true), "Failed to process block")
	t.Logf("WaitForBlockBeingMined(t, block4b): %s", block4b.Hash().String())
	td.WaitForBlockBeingMined(t, block4b)

	_, block5b := td.CreateTestBlock(t, block4b, 5001)
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block5b, "legacy", nil, true), "Failed to process block")
	t.Logf("WaitForBlockBeingMined(t, block5b): %s", block5b.Hash().String())
	td.WaitForBlockBeingMined(t, block5b)
	t.Logf("WaitForBlock(t, block5b, blockWait): %s", block5b.Hash().String())
	td.WaitForBlock(t, block5b, blockWait)

	// 0 -> 1 ... 2 -> 3 -> 4b -> 5b (*)
	td.VerifyInBlockAssembly(t, childTx1)
	td.VerifyNotInBlockAssembly(t, childTx2)
	td.VerifyNotInBlockAssembly(t, childTx3)
	td.VerifyNotOnLongestChainInUtxoStore(t, childTx1)
	td.VerifyOnLongestChainInUtxoStore(t, childTx2)
	td.VerifyOnLongestChainInUtxoStore(t, childTx3)

	_, err = td.BlockchainClient.InvalidateBlock(t.Context(), block5b.Hash())
	require.NoError(t, err)

	td.WaitForBlock(t, block4a, blockWait)

	// 0 -> 1 ... 2 -> 3 -> 4a

	td.VerifyNotInBlockAssembly(t, childTx1)
	td.VerifyNotInBlockAssembly(t, childTx2)
	td.VerifyInBlockAssembly(t, childTx3)
	td.VerifyOnLongestChainInUtxoStore(t, childTx1)
	td.VerifyOnLongestChainInUtxoStore(t, childTx2)
	td.VerifyNotOnLongestChainInUtxoStore(t, childTx3)

	// create a new block on 4a with tx3 in it
	_, block5a := td.CreateTestBlock(t, block4a, 6001, childTx3DS)
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block5a, "legacy", nil, false, true), "Failed to process block")
	t.Logf("WaitForBlockBeingMined(t, block5a): %s", block5a.Hash().String())
	td.WaitForBlockBeingMined(t, block5a)
	t.Logf("WaitForBlock(t, block5a, blockWait): %s", block5a.Hash().String())
	td.WaitForBlock(t, block5a, blockWait)

	td.VerifyNotInBlockAssembly(t, childTx1)
	td.VerifyNotInBlockAssembly(t, childTx2)
	td.VerifyInBlockAssembly(t, childTx3)
	td.VerifyNotInBlockAssembly(t, childTx3DS)
	td.VerifyOnLongestChainInUtxoStore(t, childTx1)
	td.VerifyOnLongestChainInUtxoStore(t, childTx2)
	td.VerifyNotOnLongestChainInUtxoStore(t, childTx3)
	td.VerifyOnLongestChainInUtxoStore(t, childTx3DS) // 0 -> 1 ... 2 -> 3 -> 4a -> 6a (*)

	_, err = td.BlockchainClient.InvalidateBlock(t.Context(), block4b.Hash())
	require.NoError(t, err)

	// create a new block on 5a with tx3 in it
	_, block6a := td.CreateTestBlock(t, block5a, 7001, childTx3)
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block6a, "legacy", nil, false, true), "Failed to process block")
	t.Logf("WaitForBlockBeingMined(t, block6a): %s", block6a.Hash().String())
	td.WaitForBlockBeingMined(t, block6a)
	t.Logf("WaitForBlock(t, block6a, blockWait): %s", block6a.Hash().String())
	td.WaitForBlock(t, block6a, blockWait)

	t.Logf("FINAL VERIFICATIONS:")
	td.VerifyNotInBlockAssembly(t, childTx1)
	td.VerifyNotInBlockAssembly(t, childTx2)
	td.VerifyNotInBlockAssembly(t, childTx3)
	td.VerifyNotInBlockAssembly(t, childTx3DS)
	td.VerifyOnLongestChainInUtxoStore(t, childTx1)
	td.VerifyOnLongestChainInUtxoStore(t, childTx2)
	td.VerifyOnLongestChainInUtxoStore(t, childTx3)
	td.VerifyOnLongestChainInUtxoStore(t, childTx3DS)
}
