package smoke

import (
	"testing"

	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/test"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/stretchr/testify/require"
)

func TestBlockValidationWithParentAndChildrenTxs(t *testing.T) {
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableValidator: true,
		UTXOStoreType:   "aerospike",
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
		),
	})
	defer td.Stop(t)

	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err, "failed to initialize blockchain")

	t.Log("Mining to coinbase maturity...")
	coinbaseTx := td.MineToMaturityAndGetSpendableCoinbaseTx(t, td.Ctx)

	t.Log("Creating parent transaction with multiple outputs...")
	parentTx := td.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTx, 0),
		transactions.WithP2PKHOutputs(1, 10000), // output 0
		transactions.WithP2PKHOutputs(1, 10000), // output 1
		transactions.WithP2PKHOutputs(1, 10000), // output 2
	)

	t.Log("Creating valid child transactions spending different outputs...")
	childTx1 := td.CreateTransactionWithOptions(t,
		transactions.WithInput(parentTx, 0), // Spend output 0
		transactions.WithP2PKHOutputs(1, 9000),
	)
	childTx2 := td.CreateTransactionWithOptions(t,
		transactions.WithInput(parentTx, 1), // Spend output 1 (different from childTx1)
		transactions.WithP2PKHOutputs(1, 9000),
	)

	t.Log("Testing valid block with parent and child transactions...")
	// Submit all transactions to mempool first
	err = td.PropagationClient.ProcessTransaction(td.Ctx, parentTx)
	require.NoError(t, err, "failed to submit parent transaction")

	err = td.PropagationClient.ProcessTransaction(td.Ctx, childTx1)
	require.NoError(t, err, "failed to submit child transaction 1")

	err = td.PropagationClient.ProcessTransaction(td.Ctx, childTx2)
	require.NoError(t, err, "failed to submit child transaction 2")

	t.Log("Mining block containing parent and child transactions...")
	block := td.MineAndWait(t, 1)
	require.NotNil(t, block, "failed to mine block")

	t.Log("Verifying all transactions are included in the block...")
	subtrees, err := block.GetSubtrees(td.Ctx, td.Logger, td.SubtreeStore, td.Settings.Block.GetAndValidateSubtreesConcurrency)
	require.NoError(t, err)

	parentFound := false
	child1Found := false
	child2Found := false

	for _, subtree := range subtrees {
		for _, node := range subtree.Nodes {
			nodeHashStr := node.Hash.String()
			if nodeHashStr == parentTx.TxIDChainHash().String() {
				parentFound = true
			}
			if nodeHashStr == childTx1.TxIDChainHash().String() {
				child1Found = true
			}
			if nodeHashStr == childTx2.TxIDChainHash().String() {
				child2Found = true
			}
		}
	}

	require.True(t, parentFound, "Parent transaction not found in block")
	require.True(t, child1Found, "Child transaction 1 not found in block")
	require.True(t, child2Found, "Child transaction 2 not found in block")

	t.Log("Block validation successful - all transactions properly included")
}

func TestBlockValidationWithDoubleSpend(t *testing.T) {
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableValidator: true,
		UTXOStoreType:   "aerospike",
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
		),
	})
	defer td.Stop(t)

	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err, "failed to initialize blockchain")

	t.Log("Mining to coinbase maturity...")
	coinbaseTx := td.MineToMaturityAndGetSpendableCoinbaseTx(t, td.Ctx)

	t.Log("Creating parent transaction...")
	parentTx := td.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTx, 0),
		transactions.WithP2PKHOutputs(1, 10000), // Single output that will be double-spent
	)

	t.Log("Creating two child transactions that double-spend the same output...")
	childTx1 := td.CreateTransactionWithOptions(t,
		transactions.WithInput(parentTx, 0), // Spend output 0
		transactions.WithP2PKHOutputs(1, 9000),
	)
	childTx2 := td.CreateTransactionWithOptions(t,
		transactions.WithInput(parentTx, 0), // Also spend output 0 (double spend!)
		transactions.WithP2PKHOutputs(1, 8000),
	)

	t.Log("Submitting parent transaction...")
	err = td.PropagationClient.ProcessTransaction(td.Ctx, parentTx)
	require.NoError(t, err, "failed to submit parent transaction")

	t.Log("Submitting first child transaction...")
	err = td.PropagationClient.ProcessTransaction(td.Ctx, childTx1)
	require.NoError(t, err, "failed to submit first child transaction")

	t.Log("Attempting to submit second child transaction (double spend)...")
	err = td.PropagationClient.ProcessTransaction(td.Ctx, childTx2)
	require.Error(t, err, "expected second child transaction to be rejected due to double spend")
	t.Logf("Correctly rejected double spend transaction: %v", err)

	t.Log("Mining block with valid transactions...")
	block := td.MineAndWait(t, 1)
	require.NotNil(t, block, "failed to mine block")

	t.Log("Verifying only valid transactions are in the block...")
	subtrees, err := block.GetSubtrees(td.Ctx, td.Logger, td.SubtreeStore, td.Settings.Block.GetAndValidateSubtreesConcurrency)
	require.NoError(t, err)

	parentFound := false
	child1Found := false
	child2Found := false

	for _, subtree := range subtrees {
		for _, node := range subtree.Nodes {
			nodeHashStr := node.Hash.String()
			if nodeHashStr == parentTx.TxIDChainHash().String() {
				parentFound = true
			}
			if nodeHashStr == childTx1.TxIDChainHash().String() {
				child1Found = true
			}
			if nodeHashStr == childTx2.TxIDChainHash().String() {
				child2Found = true
			}
		}
	}

	require.True(t, parentFound, "Parent transaction should be in block")
	require.True(t, child1Found, "First child transaction should be in block")
	require.False(t, child2Found, "Second child transaction should NOT be in block due to double spend rejection")

	t.Log("Block validation correctly handled double spend scenario")
}

func TestBlockValidationWithDuplicateTransaction(t *testing.T) {
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableValidator: true,
		UTXOStoreType:   "aerospike",
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
		),
	})
	defer td.Stop(t)

	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err, "failed to initialize blockchain")

	t.Log("Mining to coinbase maturity...")
	coinbaseTx := td.MineToMaturityAndGetSpendableCoinbaseTx(t, td.Ctx)
	block2, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 2)
	require.NoError(t, err)

	t.Log("Creating parent transaction...")
	parentTx := td.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTx, 0),
		transactions.WithP2PKHOutputs(1, 10000), // Single output that will be duplicated
	)

	t.Log("Creating two child transactions that are the same...")
	childTx1 := td.CreateTransactionWithOptions(t,
		transactions.WithInput(parentTx, 0), // Spend output 0
		transactions.WithP2PKHOutputs(1, 9000),
	)

	t.Log("Submitting parent transaction...")
	err = td.PropagationClient.ProcessTransaction(td.Ctx, parentTx)
	require.NoError(t, err, "failed to submit parent transaction")

	t.Log("Submitting first child transaction...")
	err = td.PropagationClient.ProcessTransaction(td.Ctx, childTx1)
	require.NoError(t, err, "failed to submit first child transaction")

	_, block3 := td.CreateTestBlock(t, block2, 101, parentTx, childTx1, childTx1)
	err = td.BlockValidation.ValidateBlock(td.Ctx, block3, "legacy", nil, true)
	require.Error(t, err)

	t.Log("Block validation correctly handled duplicate transaction scenario")
}
