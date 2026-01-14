package smoke

import (
	"encoding/json"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/test/utils/aerospike"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/stretchr/testify/require"
)

func TestParentNotFullySpentNotMinedonSameChain(t *testing.T) {
	t.Skip("Skipping test, bug with invalid block error")
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

	// Start NodeA
	t.Log("Starting NodeA...")
	nodeA := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC: true,
		EnableP2P: true,
		SettingsOverrideFunc: func(settings *settings.Settings) {
			settings.Asset.HTTPPort = 18090
			settings.Block.GetAndValidateSubtreesConcurrency = 1
			settings.UtxoStore.UtxoStore = parsedURL
		},
		FSMState: blockchain.FSMStateRUNNING,
	})
	defer nodeA.Stop(t)

	err = nodeA.BlockchainClient.Run(nodeA.Ctx, "test")
	require.NoError(t, err, "Failed to initialize NodeA blockchain")

	// Generate blocks to have coinbase maturity
	t.Log("Generating blocks for coinbase maturity on NodeA...")
	coinbaseTx := nodeA.MineToMaturityAndGetSpendableCoinbaseTx(t, nodeA.Ctx)
	t.Logf("Coinbase transaction created: %s", coinbaseTx.TxIDChainHash().String())

	// Create a parent transaction with 6 outputs
	t.Log("Creating parent transaction with 6 outputs...")
	outputAmount := uint64(8_000_000) // 0.08 BSV per output
	parentTx := nodeA.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTx, 0),
		transactions.WithP2PKHOutputs(1, outputAmount),
		transactions.WithP2PKHOutputs(1, outputAmount),
		transactions.WithP2PKHOutputs(1, outputAmount),
		transactions.WithP2PKHOutputs(1, outputAmount),
		transactions.WithP2PKHOutputs(1, outputAmount),
		transactions.WithP2PKHOutputs(1, outputAmount),
	)
	t.Logf("Parent transaction created with %d outputs: %s", len(parentTx.Outputs), parentTx.TxIDChainHash().String())

	// Send the parent transaction
	t.Log("Sending parent transaction...")
	err = nodeA.PropagationClient.ProcessTransaction(nodeA.Ctx, parentTx)
	require.NoError(t, err, "Failed to send parent transaction")

	// Verify parent transaction is in mempool
	t.Log("Verifying parent transaction is in mempool...")
	resp, err := nodeA.CallRPC(nodeA.Ctx, "getrawmempool", []interface{}{})
	require.NoError(t, err, "Failed to get mempool")

	var mempoolResp struct {
		Result []string `json:"result"`
	}
	err = json.Unmarshal([]byte(resp), &mempoolResp)
	require.NoError(t, err, "Failed to parse mempool response")

	parentFound := false
	for _, txID := range mempoolResp.Result {
		if txID == parentTx.TxIDChainHash().String() {
			parentFound = true
			break
		}
	}
	require.True(t, parentFound, "Parent transaction not found in mempool")
	t.Log("Parent transaction confirmed in mempool")

	// Create a child transaction which spends one output of the parent
	t.Log("Creating child transaction spending output 0 of parent...")
	childAmount := outputAmount - 1000 // Leave 1000 satoshis for fee
	childTx := nodeA.CreateTransactionWithOptions(t,
		transactions.WithInput(parentTx, 0),
		transactions.WithP2PKHOutputs(1, childAmount),
	)
	t.Logf("Child transaction created: %s", childTx.TxIDChainHash().String())

	// Send the child transaction
	t.Log("Sending child transaction...")
	err = nodeA.PropagationClient.ProcessTransaction(nodeA.Ctx, childTx)
	require.NoError(t, err, "Failed to send child transaction")

	// Verify child transaction is in mempool
	t.Log("Verifying child transaction is in mempool...")
	resp, err = nodeA.CallRPC(nodeA.Ctx, "getrawmempool", []interface{}{})
	require.NoError(t, err, "Failed to get mempool after child transaction")

	err = json.Unmarshal([]byte(resp), &mempoolResp)
	require.NoError(t, err, "Failed to parse mempool response after child transaction")

	childFound := false
	for _, txID := range mempoolResp.Result {
		if txID == childTx.TxIDChainHash().String() {
			childFound = true
			break
		}
	}
	require.True(t, childFound, "Child transaction not found in mempool")
	t.Log("Child transaction confirmed in mempool")

	// Create a block with the child transaction
	t.Log("Mining block to include parent and child transactions...")
	// block := nodeA.MineAndWait(t, 1)
	bestBlockHeader, _, err := nodeA.BlockchainClient.GetBestBlockHeader(nodeA.Ctx)
	require.NoError(t, err)
	bestBlock, err := nodeA.BlockchainClient.GetBlock(nodeA.Ctx, bestBlockHeader.Hash())
	require.NoError(t, err)
	_, block := nodeA.CreateTestBlock(t, bestBlock, 0, childTx)
	err = nodeA.BlockValidation.ValidateBlock(nodeA.Ctx, block, "legacy")
	require.Error(t, err)
	require.ErrorContains(t, err, fmt.Sprintf("parent transaction %s of tx %s has no block IDs", parentTx.TxIDChainHash().String(), childTx.TxIDChainHash().String()))

	// Verify transactions are no longer in mempool (they should be mined)
	t.Log("Verifying transactions are still in mempool...")
	resp, err = nodeA.CallRPC(nodeA.Ctx, "getrawmempool", []interface{}{})
	require.NoError(t, err, "Failed to get mempool after mining")

	err = json.Unmarshal([]byte(resp), &mempoolResp)
	require.NoError(t, err, "Failed to parse mempool response after mining")

	for _, txID := range mempoolResp.Result {
		if txID == "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff" {
			continue
		}
		if txID == parentTx.TxIDChainHash().String() || txID == childTx.TxIDChainHash().String() {
			continue
		}
		require.Fail(t, "Transaction still in mempool")
	}
	t.Log("Confirmed transactions are no longer in mempool")
}

func TestParentSpentNotMinedonSameChain(t *testing.T) {
	t.Skip("Skipping test, bug with invalid block error")
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

	// Start NodeA
	t.Log("Starting NodeA...")
	nodeA := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC: true,
		EnableP2P: true,
		SettingsOverrideFunc: func(settings *settings.Settings) {
			settings.Asset.HTTPPort = 18090
			settings.Block.GetAndValidateSubtreesConcurrency = 1
			settings.GlobalBlockHeightRetention = 1
			settings.UtxoStore.UtxoStore = parsedURL
			settings.BlockValidation.OptimisticMining = true
		},
		FSMState:           blockchain.FSMStateRUNNING,
		EnableDebugLogging: true,
	})
	defer nodeA.Stop(t)

	// Generate blocks to have coinbase maturity
	t.Log("Generating blocks for coinbase maturity on NodeA...")
	coinbaseTx := nodeA.MineToMaturityAndGetSpendableCoinbaseTx(t, nodeA.Ctx)
	t.Logf("Coinbase transaction created: %s", coinbaseTx.TxIDChainHash().String())

	// Create a parent transaction with 6 outputs
	t.Log("Creating parent transaction with 6 outputs...")
	outputAmount := uint64(8_000_000) // 0.08 BSV per output
	parentTx := nodeA.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTx, 0),
		transactions.WithP2PKHOutputs(1, outputAmount),
	)
	t.Logf("Parent transaction created with %d outputs: %s", len(parentTx.Outputs), parentTx.TxIDChainHash().String())

	// Send the parent transaction
	t.Log("Sending parent transaction...")
	err = nodeA.PropagationClient.ProcessTransaction(nodeA.Ctx, parentTx)
	require.NoError(t, err, "Failed to send parent transaction")

	// Verify parent transaction is in mempool
	t.Log("Verifying parent transaction is in mempool...")
	resp, err := nodeA.CallRPC(nodeA.Ctx, "getrawmempool", []interface{}{})
	require.NoError(t, err, "Failed to get mempool")

	var mempoolResp struct {
		Result []string `json:"result"`
	}
	err = json.Unmarshal([]byte(resp), &mempoolResp)
	require.NoError(t, err, "Failed to parse mempool response")

	parentFound := false
	for _, txID := range mempoolResp.Result {
		if txID == parentTx.TxIDChainHash().String() {
			parentFound = true
			break
		}
	}
	require.True(t, parentFound, "Parent transaction not found in mempool")
	t.Log("Parent transaction confirmed in mempool")

	// Create a child transaction which spends one output of the parent
	t.Log("Creating child transaction spending output 0 of parent...")
	childAmount := outputAmount - 1000 // Leave 1000 satoshis for fee
	childTx := nodeA.CreateTransactionWithOptions(t,
		transactions.WithInput(parentTx, 0),
		transactions.WithP2PKHOutputs(1, childAmount),
	)
	t.Logf("Child transaction created: %s", childTx.TxIDChainHash().String())

	// Send the child transaction
	t.Log("Sending child transaction...")
	err = nodeA.PropagationClient.ProcessTransaction(nodeA.Ctx, childTx)
	require.NoError(t, err, "Failed to send child transaction")

	// Verify child transaction is in mempool
	t.Log("Verifying child transaction is in mempool...")
	resp, err = nodeA.CallRPC(nodeA.Ctx, "getrawmempool", []interface{}{})
	require.NoError(t, err, "Failed to get mempool after child transaction")

	err = json.Unmarshal([]byte(resp), &mempoolResp)
	require.NoError(t, err, "Failed to parse mempool response after child transaction")

	childFound := false
	for _, txID := range mempoolResp.Result {
		if txID == childTx.TxIDChainHash().String() {
			childFound = true
			break
		}
	}
	require.True(t, childFound, "Child transaction not found in mempool")
	t.Log("Child transaction confirmed in mempool")

	// Create a block with the child transaction
	t.Log("Mining block to include parent and child transactions...")
	// block := nodeA.MineAndWait(t, 1)
	bestBlockHeader, _, err := nodeA.BlockchainClient.GetBestBlockHeader(nodeA.Ctx)
	require.NoError(t, err)
	block2, err := nodeA.BlockchainClient.GetBlock(nodeA.Ctx, bestBlockHeader.Hash())
	require.NoError(t, err)
	_, block3 := nodeA.CreateTestBlock(t, block2, 0, childTx)
	err = nodeA.BlockValidation.ValidateBlock(nodeA.Ctx, block3, "legacy")
	require.NoError(t, err)
	_, block4 := nodeA.CreateTestBlock(t, block3, 0)
	err = nodeA.BlockValidation.ValidateBlock(nodeA.Ctx, block4, "legacy")
	require.NoError(t, err)
	nodeA.WaitForBlock(t, block4, 10*time.Second, true)

	// verify parent tx is in mempool
	nodeA.VerifyNotInBlockAssembly(t, childTx)
	nodeA.VerifyInBlockAssembly(t, parentTx)
}
