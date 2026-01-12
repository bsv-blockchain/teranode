package doublespendtest

import (
	"testing"

	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/stretchr/testify/require"
)

// TestSameChainGrandparentDoubleSpend tests a scenario where a child transaction
// on the same chain attempts to spend a grandparent output that was already
// consumed by the parent transaction.
//
// This should result in an INVALID block because the child is attempting to
// double-spend an already-consumed UTXO within the same chain.
//
// Transaction Structure:
//
//	grandparent (5 outputs: 0,1,2,3,4)
//	├── parent (spends grandparent:0 and grandparent:1) -> creates 5 outputs
//	│
//	└── invalidChild (spends parent:0, grandparent:0, grandparent:3)
//	                                   ^^^^^^^^^^^^^^
//	                                   INVALID! grandparent:0 already spent by parent
//
// Block Structure:
//
//	0 -> 1 -> ... -> 101 -> 102 [grandparent] -> 103 [parent] -> 104 [invalidChild] (SHOULD FAIL)
func TestSameChainGrandparentDoubleSpendPostgres(t *testing.T) {
	t.Run("same_chain_grandparent_double_spend", func(t *testing.T) {
		testSameChainGrandparentDoubleSpend(t, "postgres")
	})
}

func TestSameChainGrandparentDoubleSpendAerospike(t *testing.T) {
	t.Run("same_chain_grandparent_double_spend", func(t *testing.T) {
		testSameChainGrandparentDoubleSpend(t, "aerospike")
	})
}

// testSameChainGrandparentDoubleSpend verifies that a child transaction cannot
// spend a grandparent output that was already consumed by its parent on the same chain.
//
// This is a fundamental UTXO validation rule - once an output is spent, it cannot
// be spent again, even by a descendant transaction on the same chain.
func testSameChainGrandparentDoubleSpend(t *testing.T, utxoStore string) {
	// Setup test environment with external transactions
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		UTXOStoreType:        utxoStore,
		SettingsOverrideFunc: externalTxSettingsFunc(),
	})
	defer func() {
		td.Stop(t)
	}()

	// Initialize blockchain
	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	// Generate initial blocks and get spendable coinbase
	coinbaseTx := td.MineToMaturityAndGetSpendableCoinbaseTx(t, td.Ctx)

	// Create grandparent with 5 outputs (external tx due to low utxoBatchSize)
	grandparent := td.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTx, 0),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, coinbaseTx.Outputs[0].Satoshis/numOutputsForExternalTx-100),
	)
	t.Logf("Grandparent: %s (%d outputs)", grandparent.TxIDChainHash().String(), len(grandparent.Outputs))
	t.Logf("  Outputs: [0]=%d, [1]=%d, [2]=%d, [3]=%d, [4]=%d sats",
		grandparent.Outputs[0].Satoshis,
		grandparent.Outputs[1].Satoshis,
		grandparent.Outputs[2].Satoshis,
		grandparent.Outputs[3].Satoshis,
		grandparent.Outputs[4].Satoshis)

	// Submit and mine grandparent
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, grandparent))
	td.MineAndWait(t, 1)

	// Verify grandparent is mined
	block3, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 3)
	require.NoError(t, err)
	require.Equal(t, uint32(3), block3.Height)

	// 0 -> 1 -> 2 -> 3 [grandparent]

	// Create parent that spends grandparent outputs 0 and 1
	// After this, grandparent:0 and grandparent:1 are SPENT
	parent := td.CreateTransactionWithOptions(t,
		transactions.WithInput(grandparent, 0), // Spend output 0
		transactions.WithInput(grandparent, 1), // Spend output 1
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, grandparent.Outputs[0].Satoshis/numOutputsForExternalTx-100),
	)
	t.Logf("Parent: %s (%d outputs) - spends grandparent:0 and grandparent:1",
		parent.TxIDChainHash().String(), len(parent.Outputs))

	// Submit and mine parent
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, parent))
	td.MineAndWait(t, 1)

	block4, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 4)
	require.NoError(t, err)
	require.Equal(t, uint32(4), block4.Height)

	// 0 -> 1 -> 2 -> 3 [grandparent] -> 4 [parent]

	td.VerifyOnLongestChainInUtxoStore(t, grandparent)
	td.VerifyOnLongestChainInUtxoStore(t, parent)

	t.Log("Chain established: grandparent -> parent")
	t.Log("grandparent:0 and grandparent:1 are now SPENT by parent")

	// Now create an INVALID child transaction that attempts to:
	// 1. Spend parent:0 (valid - this output exists and is unspent)
	// 2. Spend grandparent:0 (INVALID - already spent by parent!)
	// 3. Spend grandparent:3 (valid - this output is still unspent)
	//
	// This transaction should be rejected because grandparent:0 is already spent
	invalidChild := td.CreateTransactionWithOptions(t,
		transactions.WithInput(parent, 0),      // Valid: parent output 0
		transactions.WithInput(grandparent, 0), // INVALID: already spent by parent!
		transactions.WithInput(grandparent, 4), // Valid: still unspent
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, grandparent.Outputs[0].Satoshis/numOutputsForExternalTx-100),
	)
	t.Logf("InvalidChild: %s (%d outputs) - attempts to spend parent:0, grandparent:0 (INVALID!), grandparent:3",
		invalidChild.TxIDChainHash().String(), len(invalidChild.Outputs))

	// Attempt to submit the invalid child transaction via propagation
	// This should FAIL because grandparent:0 is already spent
	t.Log("Attempting to submit invalidChild via propagation (should fail)...")
	err = td.PropagationClient.ProcessTransaction(td.Ctx, invalidChild)
	require.Error(t, err, "Expected invalidChild to be rejected - grandparent:0 is already spent by parent")
	t.Logf("Propagation correctly rejected invalidChild: %v", err)

	// Also verify that a block containing this transaction would be invalid
	// Create a block with the invalid child transaction
	t.Log("Attempting to create block with invalidChild (should fail validation)...")
	_, block5Invalid := td.CreateTestBlock(t, block4, 10501, invalidChild)

	// The block validation should fail because it contains a transaction
	// that attempts to spend an already-spent UTXO
	err = td.BlockValidationClient.ProcessBlock(td.Ctx, block5Invalid, block5Invalid.Height, "", "legacy")
	require.Error(t, err, "Expected block with invalidChild to be rejected - contains double spend")
	t.Logf("Block validation correctly rejected block with invalidChild: %v", err)

	// Verify the blockchain state hasn't changed
	currentTip, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 4)
	require.NoError(t, err)
	require.Equal(t, block4.Header.Hash(), currentTip.Header.Hash(),
		"Blockchain tip should still be block4")

	// Verify that a valid child transaction CAN spend unspent grandparent outputs
	t.Log("Verifying that a valid child CAN spend unspent grandparent outputs...")
	validChild := td.CreateTransactionWithOptions(t,
		transactions.WithInput(parent, 0),      // Valid: parent output 0
		transactions.WithInput(grandparent, 3), // Valid: unspent grandparent output
		transactions.WithInput(grandparent, 4), // Valid: unspent grandparent output
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, 50000),
	)
	t.Logf("ValidChild: %s (%d outputs) - spends parent:0, grandparent:3, grandparent:4 (all valid)",
		validChild.TxIDChainHash().String(), len(validChild.Outputs))

	// This should succeed
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, validChild),
		"ValidChild should be accepted - it only spends unspent outputs")
	td.MineAndWait(t, 1)

	block5, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 5)
	require.NoError(t, err)
	require.Equal(t, uint32(5), block5.Height)

	// 0 -> 1 -> 2 -> 3 [grandparent] -> 4 [parent] -> 5 [validChild]

	td.VerifyOnLongestChainInUtxoStore(t, validChild)

	t.Log("Successfully verified:")
	t.Log("  - Invalid child (spending already-spent grandparent output) is rejected")
	t.Log("  - Valid child (spending unspent grandparent outputs) is accepted")
}
