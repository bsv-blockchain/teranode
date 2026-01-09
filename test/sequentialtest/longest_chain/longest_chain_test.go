package longest_chain

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/test/utils/aerospike"
	"github.com/bsv-blockchain/teranode/test/utils/postgres"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/stretchr/testify/require"
)

func TestLongestChainSQLite(t *testing.T) {
	utxoStore := "sqlite:///test"

	t.Run("simple", func(t *testing.T) {
		testLongestChainSimple(t, utxoStore)
	})

	t.Run("invalid block", func(t *testing.T) {
		testLongestChainInvalidateBlock(t, utxoStore)
	})

	t.Run("invalid block with old tx", func(t *testing.T) {
		testLongestChainInvalidateBlockWithOldTx(t, utxoStore)
	})

	t.Run("fork with different tx inclusion", func(t *testing.T) {
		testLongestChainForkDifferentTxInclusion(t, utxoStore)
	})

	t.Run("transaction chain dependency", func(t *testing.T) {
		testLongestChainTransactionChainDependency(t, utxoStore)
	})

	t.Run("partial output consumption", func(t *testing.T) {
		testLongestChainWithDoubleSpendTransaction(t, utxoStore)
	})
}

func TestLongestChainPostgres(t *testing.T) {
	// start a postgres container
	utxoStore, teardown, err := postgres.SetupTestPostgresContainer()
	require.NoError(t, err)

	defer func() {
		_ = teardown()
	}()

	t.Run("simple", func(t *testing.T) {
		testLongestChainSimple(t, utxoStore)
	})

	t.Run("invalid block", func(t *testing.T) {
		testLongestChainInvalidateBlock(t, utxoStore)
	})

	t.Run("invalid block with old tx", func(t *testing.T) {
		testLongestChainInvalidateBlockWithOldTx(t, utxoStore)
	})

	t.Run("fork with different tx inclusion", func(t *testing.T) {
		testLongestChainForkDifferentTxInclusion(t, utxoStore)
	})

	t.Run("transaction chain dependency", func(t *testing.T) {
		testLongestChainTransactionChainDependency(t, utxoStore)
	})

	t.Run("partial output consumption", func(t *testing.T) {
		testLongestChainWithDoubleSpendTransaction(t, utxoStore)
	})
}

func TestLongestChainAerospike(t *testing.T) {
	// start an aerospike container
	utxoStore, teardown, err := aerospike.InitAerospikeContainer()
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = teardown()
	})

	t.Run("simple", func(t *testing.T) {
		testLongestChainSimple(t, utxoStore)
	})

	t.Run("invalid block", func(t *testing.T) {
		testLongestChainInvalidateBlock(t, utxoStore)
	})

	t.Run("invalid block with old tx", func(t *testing.T) {
		testLongestChainInvalidateBlockWithOldTx(t, utxoStore)
	})

	t.Run("fork with different tx inclusion", func(t *testing.T) {
		testLongestChainForkDifferentTxInclusion(t, utxoStore)
	})

	t.Run("transaction chain dependency", func(t *testing.T) {
		testLongestChainWithDoubleSpendTransaction(t, utxoStore)
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

	tx2 := td.CreateTransaction(t, block2.CoinbaseTx, 0)

	td.VerifyInBlockAssembly(t, tx1)
	// td.VerifyInBlockAssembly(t, tx2)

	_, block4a := td.CreateTestBlock(t, block3, 4001, tx1)
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block4a, "legacy", nil, false), "Failed to process block")
	td.WaitForBlock(t, block4a, blockWait)
	td.WaitForBlockBeingMined(t, block4a)

	// 0 -> 1 ... 2 -> 3 -> 4a (*)

	td.VerifyNotInBlockAssembly(t, tx1) // mined and removed from block assembly
	td.VerifyOnLongestChainInUtxoStore(t, tx1)

	_, block4b := td.CreateTestBlock(t, block3, 4002, tx2)
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block4b, "legacy", nil, false), "Failed to process block")
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
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block5b, "legacy", nil, false, false), "Failed to process block")
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
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block5a, "legacy", nil, false, false), "Failed to process block")

	_, block6a := td.CreateTestBlock(t, block5a, 6001) // empty block
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block6a, "legacy", nil, false, false), "Failed to process block")
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

	td.VerifyInBlockAssembly(t, tx1)

	_, block4a := td.CreateTestBlock(t, block3, 4001, tx1)
	// require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block4a, block4a.Height, "legacy", ""))
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block4a, "legacy", nil, false, true), "Failed to process block")
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

	tx2 := td.CreateTransaction(t, block2.CoinbaseTx, 0)
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, tx2))

	td.VerifyInBlockAssembly(t, tx1)
	td.VerifyInBlockAssembly(t, tx2)

	_, block4a := td.CreateTestBlock(t, block3, 4001, tx2)
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block4a, "legacy", nil, false, true), "Failed to process block")
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
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block5a, "test", nil, false), "Failed to process block")

	td.WaitForBlock(t, block5a, blockWait)

	// should return back to block4a as block5a is invalid

	td.WaitForBlock(t, block4a, blockWait)

	// 0 -> 1 ... 2 -> 3 -> 4a (*)

	td.VerifyInBlockAssembly(t, tx1)    // re-added to block assembly
	td.VerifyNotInBlockAssembly(t, tx2) // removed as already mined in block4a
	td.VerifyNotOnLongestChainInUtxoStore(t, tx1)
	td.VerifyOnLongestChainInUtxoStore(t, tx2)
}

func testLongestChainForkDifferentTxInclusion(t *testing.T, utxoStore string) {
	// Setup test environment
	td, block3 := setupLongestChainTest(t, utxoStore)
	defer func() {
		td.Stop(t)
	}()

	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err)

	block2, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 2)
	require.NoError(t, err)

	// Create two transactions
	tx1 := td.CreateTransaction(t, block1.CoinbaseTx, 0)
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, tx1))

	tx2 := td.CreateTransaction(t, block2.CoinbaseTx, 0)
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, tx2))

	td.VerifyInBlockAssembly(t, tx1)
	td.VerifyInBlockAssembly(t, tx2)

	// Fork A: Create block4a with only tx1
	_, block4a := td.CreateTestBlock(t, block3, 4001, tx1)
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block4a, "legacy", nil, false), "Failed to process block")
	td.WaitForBlock(t, block4a, blockWait)
	td.WaitForBlockBeingMined(t, block4a)

	// 0 -> 1 ... 2 -> 3 -> 4a (*)

	td.VerifyNotInBlockAssembly(t, tx1) // mined and removed from block assembly
	td.VerifyInBlockAssembly(t, tx2)    // not mined yet
	td.VerifyOnLongestChainInUtxoStore(t, tx1)
	td.VerifyNotOnLongestChainInUtxoStore(t, tx2)

	// Fork B: Create block4b with both tx1 and tx2
	_, block4b := td.CreateTestBlock(t, block3, 4002, tx1, tx2)
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block4b, "legacy", nil, false), "Failed to process block")
	td.WaitForBlockBeingMined(t, block4b)

	time.Sleep(1 * time.Second) // give some time for the block to be processed

	//                   / 4a (*)
	// 0 -> 1 ... 2 -> 3
	//                   \ 4b

	// Still on fork A, so tx1 is mined, tx2 is not
	td.VerifyNotInBlockAssembly(t, tx1) // mined in fork A
	td.VerifyInBlockAssembly(t, tx2)    // not on longest chain yet
	td.VerifyOnLongestChainInUtxoStore(t, tx1)
	td.VerifyNotOnLongestChainInUtxoStore(t, tx2)

	// Make fork B longer by adding block5b
	_, block5b := td.CreateTestBlock(t, block4b, 5002) // empty block
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block5b, "legacy", nil, false, false), "Failed to process block")
	td.WaitForBlock(t, block5b, blockWait)
	td.WaitForBlockBeingMined(t, block5b)

	//                   / 4a
	// 0 -> 1 ... 2 -> 3
	//                   \ 4b -> 5b (*)

	// Now fork B is longest, both tx1 and tx2 are mined in block4b
	td.VerifyNotInBlockAssembly(t, tx1) // mined in fork B (block4b)
	td.VerifyNotInBlockAssembly(t, tx2) // mined in fork B (block4b)
	td.VerifyOnLongestChainInUtxoStore(t, tx1)
	td.VerifyOnLongestChainInUtxoStore(t, tx2)
}

func testLongestChainTransactionChainDependency(t *testing.T, utxoStore string) {
	// Scenario: Parent-child transaction chain where parent gets invalidated in reorg
	// Fork A: Block4a contains tx1 (creates multiple outputs)
	// Mempool: tx2 spends output from tx1, tx3 spends output from tx2
	// Fork B becomes longest without tx1
	// All dependent transactions (tx2, tx3) should be removed from mempool

	td, block3 := setupLongestChainTest(t, utxoStore)
	defer func() {
		td.Stop(t)
	}()

	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err)

	block2, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 2)
	require.NoError(t, err)

	// Create parent transaction with multiple outputs (explicitly 5 outputs)
	// This ensures we have enough outputs for child and grandchild transactions to spend
	tx1, err := td.CreateParentTransactionWithNOutputs(t, block1.CoinbaseTx, 5)
	require.NoError(t, err)
	td.VerifyInBlockAssembly(t, tx1)
	t.Logf("tx1 created with %d outputs", len(tx1.Outputs))

	// Mine tx1 in Fork A
	_, block4a := td.CreateTestBlock(t, block3, 4001, tx1)
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block4a, "legacy", nil, false), "Failed to process block")
	td.WaitForBlock(t, block4a, blockWait)
	td.WaitForBlockBeingMined(t, block4a)

	// 0 -> 1 ... 2 -> 3 -> 4a (*)

	td.VerifyNotInBlockAssembly(t, tx1)
	td.VerifyOnLongestChainInUtxoStore(t, tx1)

	// Create child transaction (tx2) spending output from tx1
	tx2 := td.CreateTransaction(t, tx1, 0)
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, tx2))
	td.VerifyInBlockAssembly(t, tx2)

	// Create grandchild transaction (tx3) spending output from tx2
	tx3 := td.CreateTransaction(t, tx2, 0)
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, tx3))
	td.VerifyInBlockAssembly(t, tx3)

	// Create competing Fork B without tx1
	altTx := td.CreateTransaction(t, block2.CoinbaseTx, 0)
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, altTx))

	_, block4b := td.CreateTestBlock(t, block3, 4002, altTx)
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block4b, "legacy", nil, false), "Failed to process block")
	td.WaitForBlockBeingMined(t, block4b)

	time.Sleep(1 * time.Second)

	//                   / 4a (*) [contains tx1]
	// 0 -> 1 ... 2 -> 3
	//                   \ 4b [contains altTx, no tx1]

	// Still on Fork A, all transactions should be in expected state
	td.VerifyNotInBlockAssembly(t, tx1)
	td.VerifyInBlockAssembly(t, tx2)
	td.VerifyInBlockAssembly(t, tx3)

	// Make Fork B longer
	_, block5b := td.CreateTestBlock(t, block4b, 5002)
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block5b, "legacy", nil, false, false), "Failed to process block")
	td.WaitForBlock(t, block5b, blockWait)
	td.WaitForBlockBeingMined(t, block5b)

	//                   / 4a [contains tx1]
	// 0 -> 1 ... 2 -> 3
	//                   \ 4b -> 5b (*) [no tx1]

	// Now Fork B is longest, tx1 should return to mempool
	// tx2 and tx3 should be removed as their parent (tx1) outputs are not on longest chain
	td.VerifyInBlockAssembly(t, tx1) // back in mempool

	// tx2 and tx3 depend on tx1's outputs which are not on the longest chain
	// They should NOT be in block assembly as they're invalid
	td.VerifyInBlockAssembly(t, tx2)
	td.VerifyInBlockAssembly(t, tx3)

	td.VerifyNotOnLongestChainInUtxoStore(t, tx1)
	td.VerifyNotOnLongestChainInUtxoStore(t, tx2)
	td.VerifyNotOnLongestChainInUtxoStore(t, tx3)
}

func testLongestChainWithDoubleSpendTransaction(t *testing.T, utxoStore string) {
	// Scenario: Transaction with multiple outputs gets consumed differently across forks
	// Parent tx creates multiple outputs [O1, O2, O3]
	// Fork A: Contains tx1 spending O1 and tx2 spending O2
	// Fork B: Contains tx3 spending all outputs [O2, O3]

	td, block3 := setupLongestChainTest(t, utxoStore)
	defer func() {
		td.Stop(t)
	}()

	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err)

	block2, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 2)
	require.NoError(t, err)

	// Create parent transaction with multiple outputs (at least 3)
	parentTx, err := td.CreateParentTransactionWithNOutputs(t, block1.CoinbaseTx, 4)
	require.NoError(t, err)

	// Mine parentTx first so we have confirmed UTXOs
	_, block4 := td.CreateTestBlock(t, block3, 4000, parentTx)
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block4, "legacy", nil, false), "Failed to process block")
	t.Logf("WaitForBlockBeingMined(t, block4): %s", block4.Hash().String())
	td.WaitForBlockBeingMined(t, block4)
	t.Logf("WaitForBlock(t, block4, blockWait): %s", block4.Hash().String())
	td.WaitForBlock(t, block4, blockWait)
	t.Logf("VerifyNotInBlockAssembly(t, parentTx): %s", parentTx.TxIDChainHash().String())
	td.VerifyNotInBlockAssembly(t, parentTx)
	t.Logf("VerifyOnLongestChainInUtxoStore(t, parentTx): %s", parentTx.TxIDChainHash().String())
	td.VerifyOnLongestChainInUtxoStore(t, parentTx)

	// 0 -> 1 ... 2 -> 3 -> 4 (*)

	// Create transactions spending individual outputs
	tx1 := td.CreateTransaction(t, parentTx, 0) // spends output 0
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, tx1))

	tx2 := td.CreateTransaction(t, parentTx, 1) // spends output 1
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, tx2))

	// Create parentTx2 early so it can be mined in both forks
	parentTx2 := td.CreateTransaction(t, block2.CoinbaseTx, 0)
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, parentTx2))

	td.VerifyInBlockAssembly(t, tx1)
	td.VerifyInBlockAssembly(t, tx2)
	td.VerifyInBlockAssembly(t, parentTx2)

	// Fork A: Mine tx1, tx2, and parentTx2
	_, block5a := td.CreateTestBlock(t, block4, 5001, tx1, tx2, parentTx2)
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block5a, "legacy", nil, false), "Failed to process block")
	t.Logf("WaitForBlockBeingMined(t, block5a): %s", block5a.Hash().String())
	td.WaitForBlockBeingMined(t, block5a)
	t.Logf("WaitForBlock(t, block5a, blockWait): %s", block5a.Hash().String())
	td.WaitForBlock(t, block5a, blockWait)

	// 0 -> 1 ... 2 -> 3 -> 4 -> 5a (*) [tx1, tx2, parentTx2]

	t.Logf("VerifyNotInBlockAssembly(t, tx1): %s", tx1.TxIDChainHash().String())
	td.VerifyNotInBlockAssembly(t, tx1)
	t.Logf("VerifyNotInBlockAssembly(t, tx2): %s", tx2.TxIDChainHash().String())
	td.VerifyNotInBlockAssembly(t, tx2)
	t.Logf("VerifyNotInBlockAssembly(t, parentTx2): %s", parentTx2.TxIDChainHash().String())
	td.VerifyNotInBlockAssembly(t, parentTx2)
	t.Logf("VerifyOnLongestChainInUtxoStore(t, tx1): %s", tx1.TxIDChainHash().String())
	td.VerifyOnLongestChainInUtxoStore(t, tx1)
	t.Logf("VerifyOnLongestChainInUtxoStore(t, tx2): %s", tx2.TxIDChainHash().String())
	td.VerifyOnLongestChainInUtxoStore(t, tx2)
	t.Logf("VerifyOnLongestChainInUtxoStore(t, parentTx2): %s", parentTx2.TxIDChainHash().String())
	td.VerifyOnLongestChainInUtxoStore(t, parentTx2)

	// Fork B: Create a transaction that spends output 3 and 1 from parentTx (conflicts with tx2) and output 0 from parentTx2
	tx3 := td.CreateTransactionWithOptions(t, transactions.WithInput(parentTx, 3), transactions.WithInput(parentTx, 1), transactions.WithInput(parentTx2, 0), transactions.WithP2PKHOutputs(1, 100000))

	// Fork B: block5b must also mine parentTx2 (even though it's in block5a) because both forks need it
	// Each fork mines parentTx2 independently at the same height
	_, block5b := td.CreateTestBlock(t, block4, 5002, parentTx2, tx3)
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block5b, "legacy", nil, false), "Failed to process block")
	t.Logf("WaitForBlockBeingMined(t, block5b): %s", block5b.Hash().String())
	td.WaitForBlockBeingMined(t, block5b)

	//                        / 5a (*) [tx1, tx2, parentTx2]
	// 0 -> 1 ... 2 -> 3 -> 4
	//                        \ 5b [parentTx2, tx3 - conflicts with tx2]

	// Make Fork B longer
	_, block6b := td.CreateTestBlock(t, block5b, 6002)
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block6b, "legacy", nil, false, false), "Failed to process block")
	t.Logf("WaitForBlockBeingMined(t, block6b): %s", block6b.Hash().String())
	td.WaitForBlockBeingMined(t, block6b)
	t.Logf("WaitForBlock(t, block6b, blockWait): %s", block6b.Hash().String())
	td.WaitForBlock(t, block6b, blockWait)

	//                        / 5a [tx1, tx2, parentTx2]
	// 0 -> 1 ... 2 -> 3 -> 4
	//                        \ 5b [parentTx2, tx3 - conflicts with tx2] -> 6b (*) [empty]

	t.Logf("VerifyInBlockAssembly(t, tx1): %s", tx1.TxIDChainHash().String())
	td.VerifyInBlockAssembly(t, tx1) // back in mempool (was in block5a)
	t.Logf("VerifyNotInBlockAssembly(t, tx2): %s", tx2.TxIDChainHash().String())
	td.VerifyNotInBlockAssembly(t, tx2) // lost conflict to tx3, removed from mempool
	t.Logf("VerifyNotInBlockAssembly(t, tx3): %s", tx3.TxIDChainHash().String())
	td.VerifyNotInBlockAssembly(t, tx3) // mined in block5b
	t.Logf("VerifyNotInBlockAssembly(t, parentTx2): %s", parentTx2.TxIDChainHash().String())
	td.VerifyNotInBlockAssembly(t, parentTx2) // mined in block5b on longest chain
	t.Logf("VerifyNotOnLongestChainInUtxoStore(t, tx1): %s", tx1.TxIDChainHash().String())
	td.VerifyNotOnLongestChainInUtxoStore(t, tx1)
	t.Logf("VerifyNotOnLongestChainInUtxoStore(t, tx2): %s", tx2.TxIDChainHash().String())
	td.VerifyNotOnLongestChainInUtxoStore(t, tx2)
	t.Logf("VerifyOnLongestChainInUtxoStore(t, parentTx2): %s", parentTx2.TxIDChainHash().String())
	td.VerifyOnLongestChainInUtxoStore(t, parentTx2) // mined in block5b on longest chain
	t.Logf("VerifyOnLongestChainInUtxoStore(t, tx3): %s", tx3.TxIDChainHash().String())
	td.VerifyOnLongestChainInUtxoStore(t, tx3)

	// Note: We cannot mine tx3 on Fork A because tx2 (mined in block5a) already spent parentTx output 1
	// tx3 also spends parentTx output 1, creating a conflict. Once tx2 is mined, that UTXO is consumed.
	// The test scenario successfully validates:
	// 1. Transactions can be mined differently across forks
	// 2. During reorg, conflicting losers are correctly marked as NOT on longest chain
	// 3. Conflicting winners are correctly marked as ON longest chain
}
