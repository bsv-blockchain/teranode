package longest_chain

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLongestChainPostgres(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		testLongestChainSimple(t, "postgres")
	})

	t.Run("invalid block", func(t *testing.T) {
		testLongestChainInvalidateBlock(t, "postgres")
	})

	t.Run("invalid block with old tx", func(t *testing.T) {
		t.Skip()
		testLongestChainInvalidateBlockWithOldTx(t, "postgres")
	})
}

func TestLongestChainAerospike(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		testLongestChainSimple(t, "aerospike")
	})

	t.Run("invalid block", func(t *testing.T) {
		testLongestChainInvalidateBlock(t, "aerospike")
	})

	t.Run("invalid block with old tx", func(t *testing.T) {
		testLongestChainInvalidateBlockWithOldTx(t, "aerospike")
	})
}

func testLongestChainSimple(t *testing.T, utxoStore string) {
	// Setup test environment
	td, block3 := setupLongestChainTest(t, utxoStore)
	defer func() {
		td.Stop(t)
	}()

	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err)

	block2, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 2)
	require.NoError(t, err)

	tx1 := td.CreateTransaction(t, block1.CoinbaseTx, 0)
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, tx1))
	require.NoError(t, td.WaitForTransactionInBlockAssembly(tx1, blockWait))

	tx2 := td.CreateTransaction(t, block2.CoinbaseTx, 0)

	td.VerifyInBlockAssembly(t, tx1)
	// td.VerifyInBlockAssembly(t, tx2)

	_, block4a := td.CreateTestBlock(t, block3, 4001, tx1)
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block4a, "legacy", false), "Failed to process block")
	td.WaitForBlock(t, block4a, blockWait)
	td.WaitForBlockBeingMined(t, block4a)

	// 0 -> 1 ... 2 -> 3 -> 4a (*)

	td.VerifyNotInBlockAssembly(t, tx1) // mined and removed from block assembly
	td.VerifyOnLongestChainInUtxoStore(t, tx1)

	_, block4b := td.CreateTestBlock(t, block3, 4002, tx2)
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block4b, "legacy", false), "Failed to process block")
	td.WaitForBlockBeingMined(t, block4b)

	time.Sleep(1 * time.Second) // give some time for the block to be processed

	//                   / 4a (*)
	// 0 -> 1 ... 2 -> 3
	//                   \ 4b

	td.VerifyNotInBlockAssembly(t, tx1) // mined and removed from block assembly
	td.VerifyOnLongestChainInUtxoStore(t, tx1)
	td.VerifyInBlockAssembly(t, tx2)
	td.VerifyNotOnLongestChainInUtxoStore(t, tx2)

	_, block5b := td.CreateTestBlock(t, block4b, 5002) // empty block
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block5b, "legacy", false, false), "Failed to process block")
	td.WaitForBlock(t, block5b, blockWait)
	td.WaitForBlockBeingMined(t, block5b)

	//                   / 4a
	// 0 -> 1 ... 2 -> 3
	//                   \ 4b -> 5b (*)

	// tx1 should now be back in block assembly and marked as not on longest chain in the utxo store
	td.VerifyInBlockAssembly(t, tx1) // added back to block assembly
	td.VerifyNotOnLongestChainInUtxoStore(t, tx1)
	td.VerifyNotInBlockAssembly(t, tx2)
	td.VerifyOnLongestChainInUtxoStore(t, tx2)

	_, block5a := td.CreateTestBlock(t, block4a, 5001) // empty block
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block5a, "legacy", false, false), "Failed to process block")

	_, block6a := td.CreateTestBlock(t, block5a, 6001) // empty block
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block6a, "legacy", false, false), "Failed to process block")
	td.WaitForBlock(t, block6a, blockWait)
	td.WaitForBlockBeingMined(t, block6a)

	//                   / 4a -> 5a -> 6a (*)
	// 0 -> 1 ... 2 -> 3
	//                   \ 4b -> 5b

	td.VerifyNotInBlockAssembly(t, tx1)
	td.VerifyOnLongestChainInUtxoStore(t, tx1)
	td.VerifyInBlockAssembly(t, tx2) // added back to block assembly
	td.VerifyNotOnLongestChainInUtxoStore(t, tx2)
}

func testLongestChainInvalidateBlock(t *testing.T, utxoStore string) {
	// Setup test environment
	td, block3 := setupLongestChainTest(t, utxoStore)
	defer func() {
		td.Stop(t)
	}()

	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err)

	tx1 := td.CreateTransaction(t, block1.CoinbaseTx, 0)
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, tx1))
	require.NoError(t, td.WaitForTransactionInBlockAssembly(tx1, blockWait))

	td.VerifyInBlockAssembly(t, tx1)

	_, block4a := td.CreateTestBlock(t, block3, 4001, tx1)
	// require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block4a, block4a.Height, "legacy", ""))
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block4a, "legacy", false, true), "Failed to process block")
	td.WaitForBlock(t, block4a, blockWait)
	td.WaitForBlockBeingMined(t, block4a)
	t.Logf("block3: %s", block3.Hash().String())
	t.Logf("block4a: %s", block4a.Hash().String())

	// 0 -> 1 ... 2 -> 3 -> 4a (*)

	td.VerifyNotInBlockAssembly(t, tx1) // mined and removed from block assembly
	td.VerifyOnLongestChainInUtxoStore(t, tx1)

	_, err = td.BlockchainClient.InvalidateBlock(t.Context(), block4a.Hash())
	require.NoError(t, err)

	bestBlockHeader, _, err := td.BlockchainClient.GetBestBlockHeader(td.Ctx)
	require.NoError(t, err)
	require.Equal(t, block3.Hash().String(), bestBlockHeader.Hash().String())

	td.WaitForBlock(t, block3, blockWait)

	td.VerifyInBlockAssembly(t, tx1) // re-added to block assembly
	td.VerifyNotOnLongestChainInUtxoStore(t, tx1)
}

func testLongestChainInvalidateBlockWithOldTx(t *testing.T, utxoStore string) {
	// Setup test environment
	td, block3 := setupLongestChainTest(t, utxoStore)
	defer func() {
		td.Stop(t)
	}()

	td.Settings.BlockValidation.OptimisticMining = true

	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err)

	block2, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 2)
	require.NoError(t, err)

	tx1 := td.CreateTransaction(t, block1.CoinbaseTx, 0)
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, tx1))
	require.NoError(t, td.WaitForTransactionInBlockAssembly(tx1, blockWait))

	tx2 := td.CreateTransaction(t, block2.CoinbaseTx, 0)
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, tx2))
	require.NoError(t, td.WaitForTransactionInBlockAssembly(tx2, blockWait))

	td.VerifyInBlockAssembly(t, tx1)
	td.VerifyInBlockAssembly(t, tx2)

	_, block4a := td.CreateTestBlock(t, block3, 4001, tx2)
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block4a, "legacy", false, true), "Failed to process block")
	td.WaitForBlock(t, block4a, blockWait)
	td.WaitForBlockBeingMined(t, block4a)

	// 0 -> 1 ... 2 -> 3 -> 4a (*)

	td.VerifyInBlockAssembly(t, tx1)    // not mined yet
	td.VerifyNotInBlockAssembly(t, tx2) // mined and removed from block assembly
	td.VerifyNotOnLongestChainInUtxoStore(t, tx1)
	td.VerifyOnLongestChainInUtxoStore(t, tx2)

	// create a block with tx1 and tx2 that will be invalid as tx2 is already on block4a
	_, block5a := td.CreateTestBlock(t, block4a, 5001, tx1, tx2)
	// processing the block as "test" will allow us to do optimistic mining
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block5a, "test", false), "Failed to process block")

	td.WaitForBlock(t, block5a, blockWait)

	// should return back to block4a as block5a is invalid

	td.WaitForBlock(t, block4a, blockWait)

	// 0 -> 1 ... 2 -> 3 -> 4a (*)

	td.VerifyInBlockAssembly(t, tx1)    // re-added to block assembly
	td.VerifyNotInBlockAssembly(t, tx2) // removed as already mined in block4a
	td.VerifyNotOnLongestChainInUtxoStore(t, tx1)
	td.VerifyOnLongestChainInUtxoStore(t, tx2)
}
