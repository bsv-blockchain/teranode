package smoke

import (
	"encoding/json"
	"fmt"
	"net/url"
	"testing"
	"time"

	aerospikeclient "github.com/aerospike/aerospike-client-go/v8"
	"github.com/bitcoin-sv/teranode/daemon"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	aerospikestore "github.com/bitcoin-sv/teranode/stores/utxo/aerospike"
	"github.com/bitcoin-sv/teranode/stores/utxo/fields"
	"github.com/bitcoin-sv/teranode/test/utils/aerospike"
	"github.com/bitcoin-sv/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParentNotFullySpentNotMinedonSameChain(t *testing.T) {
	SharedTestLock.Lock()
	defer SharedTestLock.Unlock()

	// Start NodeA
	t.Log("Starting NodeA...")
	nodeA := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableP2P:       true,
		SettingsContext: "docker.host.teranode1.daemon",
		SettingsOverrideFunc: func(settings *settings.Settings) {
			settings.Asset.HTTPPort = 18090
			settings.Block.GetAndValidateSubtreesConcurrency = 1
		},
		FSMState: blockchain.FSMStateRUNNING,
	})
	defer nodeA.Stop(t)

	err := nodeA.BlockchainClient.Run(nodeA.Ctx, "test")
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
	err = nodeA.BlockchainClient.AddBlock(nodeA.Ctx, block, "")
	require.NoError(t, err)
	// t.Logf("Block mined at height %d with hash: %s", block.Height, block.Header.Hash().String())
	// t.Log("Validating the mined block...")
	// err = block.GetAndValidateSubtrees(nodeA.Ctx, nodeA.Logger, nodeA.SubtreeStore)
	// require.NoError(t, err, "Failed to validate block subtrees")
	err = nodeA.BlockValidationClient.ValidateBlock(nodeA.Ctx, block)
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
	SharedTestLock.Lock()
	defer SharedTestLock.Unlock()

	// Start NodeA
	t.Log("Starting NodeA...")
	nodeA := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableP2P:       true,
		SettingsContext: "docker.host.teranode1.daemon",
		SettingsOverrideFunc: func(settings *settings.Settings) {
			settings.Asset.HTTPPort = 18090
			settings.Block.GetAndValidateSubtreesConcurrency = 1
			settings.GlobalBlockHeightRetention = 1
		},
		FSMState: blockchain.FSMStateRUNNING,
	})
	defer nodeA.Stop(t)

	err := nodeA.BlockchainClient.Run(nodeA.Ctx, "test")
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
	err = nodeA.BlockchainClient.AddBlock(nodeA.Ctx, block, "")
	require.NoError(t, err)
	err = nodeA.BlockValidation.ValidateBlock(nodeA.Ctx, block, "legacy", nil, false)
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

func TestParentNotMinedonFork(t *testing.T) {
	SharedTestLock.Lock()
	defer SharedTestLock.Unlock()

	// Start NodeA
	t.Log("Starting NodeA...")
	nodeA := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableP2P:       true,
		SettingsContext: "docker.host.teranode1.daemon",
		SettingsOverrideFunc: func(settings *settings.Settings) {
			settings.Asset.HTTPPort = 18090
			settings.Block.GetAndValidateSubtreesConcurrency = 1
		},
		FSMState: blockchain.FSMStateRUNNING,
	})
	defer nodeA.Stop(t)

	err := nodeA.BlockchainClient.Run(nodeA.Ctx, "test")
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
	_, blockA := nodeA.CreateTestBlock(t, bestBlock, 0, parentTx)
	err = nodeA.BlockchainClient.AddBlock(nodeA.Ctx, blockA, "")
	require.NoError(t, err)
	err = nodeA.BlockValidationClient.ValidateBlock(nodeA.Ctx, blockA)
	require.NoError(t, err)

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
		require.NotEqual(t, parentTx.TxIDChainHash().String(), txID, "Child transaction still in mempool after mining")
		require.Equal(t, childTx.TxIDChainHash().String(), txID, "Parent transaction still in mempool after mining")
	}
	t.Log("Confirmed transactions are no longer in mempool")

	// create a fork
	_, blockB := nodeA.CreateTestBlock(t, bestBlock, 0, childTx)
	err = nodeA.BlockchainClient.AddBlock(nodeA.Ctx, blockB, "")
	require.NoError(t, err)
	err = nodeA.BlockValidationClient.ValidateBlock(nodeA.Ctx, blockB)
	require.Error(t, err)
	require.ErrorContains(t, err, fmt.Sprintf("parent transaction %s of tx %s is not valid on our current chain", parentTx.TxIDChainHash().String(), childTx.TxIDChainHash().String()))
}

// 🧨the test fails with sqlite utxo store, but passes with aerospike
func TestParentDeletedFromUtxoStoreBeforeMined(t *testing.T) {
	// t.Skip("Skipping test , bug with invalid block error")
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
		EnableRPC:       true,
		EnableP2P:       true,
		SettingsContext: "docker.host.teranode1.daemon",
		SettingsOverrideFunc: func(settings *settings.Settings) {
			settings.UtxoStore.UtxoStore = parsedURL
			settings.Asset.HTTPPort = 18090
			settings.Block.GetAndValidateSubtreesConcurrency = 1
			settings.GlobalBlockHeightRetention = 2
			settings.BlockValidation.OptimisticMining = false
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
	t.Log("Creating parent transaction with 1 outputs...")
	outputAmount := uint64(1e8) // 1 BSV per output
	parentTx := nodeA.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTx, 0),
		transactions.WithP2PKHOutputs(1, outputAmount),
	)
	t.Logf("Parent transaction created with %d outputs: %s", len(parentTx.Outputs), parentTx.TxIDChainHash().String())

	// Send the parent transaction
	t.Log("Sending parent transaction...")
	err = nodeA.PropagationClient.ProcessTransaction(nodeA.Ctx, parentTx)
	require.NoError(t, err, "Failed to send parent transaction")
	assert.Nil(t, getRawTx(t, nodeA.UtxoStore, *parentTx.TxIDChainHash(), fields.DeleteAtHeight.String()))

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

	// Create a block with no transactions
	bestBlockHeader, _, err := nodeA.BlockchainClient.GetBestBlockHeader(nodeA.Ctx)
	require.NoError(t, err)
	block2, err := nodeA.BlockchainClient.GetBlock(nodeA.Ctx, bestBlockHeader.Hash())
	require.NoError(t, err)

	_, block3 := nodeA.CreateTestBlock(t, block2, 1000)
	err = nodeA.BlockValidation.ValidateBlock(nodeA.Ctx, block3, "legacy", nil)
	require.NoError(t, err)
	nodeA.WaitForBlock(t, block3, 10*time.Second, true)

	// mine upto GlobalBlockHeightRetention
	_, block4 := nodeA.CreateTestBlock(t, block3, 1001)
	err = nodeA.BlockValidation.ValidateBlock(nodeA.Ctx, block4, "legacy", nil)
	require.NoError(t, err)
	nodeA.WaitForBlock(t, block4, 10*time.Second, true)

	_, block5 := nodeA.CreateTestBlock(t, block4, 1002)
	err = nodeA.BlockValidation.ValidateBlock(nodeA.Ctx, block5, "legacy", nil)
	require.NoError(t, err)
	nodeA.WaitForBlock(t, block5, 10*time.Second, true)

	_, invalidblock6 := nodeA.CreateTestBlock(t, block5, 1003, childTx)
	err = nodeA.BlockchainClient.AddBlock(nodeA.Ctx, invalidblock6, "")
	require.NoError(t, err)
	// err = nodeA.BlockValidation.ValidateBlock(nodeA.Ctx, invalidblock6, "legacy", nil)
	// require.NoError(t, err)

	// create a block with both transactions
	_, block6 := nodeA.CreateTestBlock(t, block5, 1004, parentTx, childTx)
	err = nodeA.BlockValidation.ValidateBlock(nodeA.Ctx, block6, "legacy", nil)
	require.NoError(t, err)
	nodeA.WaitForBlockHeight(t, block6, 10*time.Second, true)
	nodeA.WaitForBlock(t, block6, 10*time.Second, true)

	assert.Equal(t, 8, getRawTx(t, nodeA.UtxoStore, *parentTx.TxIDChainHash(), fields.DeleteAtHeight.String()))
}

func getRawTx(t *testing.T, store utxo.Store, txID chainhash.Hash, fields ...string) map[string]interface{} {
	switch store.(type) {
	case *aerospikestore.Store:
		aeroStore := store.(*aerospikestore.Store)
		namespace := aeroStore.GetNamespace()
		setName := aeroStore.GetName()
		key, err := aerospikeclient.NewKey(namespace, setName, txID.CloneBytes())
		require.NoError(t, err)

		record, err := aeroStore.GetClient().Get(nil, key, fields...)
		require.NoError(t, err)
		require.NotNil(t, record)

		m := make(map[string]interface{})
		for k, v := range record.Bins {
			m[k] = v
		}
		return m
	default:
		t.Logf("Unsupported store type: %T", store)
		t.FailNow()
	}
	return nil
}

func printRawTx(t *testing.T, label string, rawTx map[string]interface{}) {
	fmt.Printf("%s:\n", label)
	for k, v := range rawTx {
		switch k {
		case fields.Utxos.String():
			printUtxos(v)
		case fields.TxID.String():
			printTxID(v)

		default:
			var b []byte
			if arr, ok := v.([]interface{}); ok {
				printArray(k, arr)
			} else if b, ok = v.([]byte); ok {
				fmt.Printf("%-14s: %x\n", k, b)
			} else {
				fmt.Printf("%-14s: %v\n", k, v)
			}
		}
	}
}

func printArray(name string, value interface{}) {
	fmt.Printf("%-14s:", name)

	if value == nil {
		fmt.Printf(" <nil>\n")
		return
	}

	arr, ok := value.([]interface{})
	if !ok {
		fmt.Printf(" <not array>\n")
		return
	}

	if len(arr) == 0 {
		fmt.Printf(" <empty>\n")
		return
	}

	for i, item := range arr {
		if b, found := item.([]byte); found {
			if i == 0 {
				fmt.Printf(" %-5d : %x\n", i, b)
			} else {
				fmt.Printf("              : %-5d : %x\n", i, b)
			}
		} else {
			if i == 0 {
				fmt.Printf(" %-5d : %v\n", i, item)
			} else {
				fmt.Printf("              : %-5d : %v\n", i, item)
			}
		}
	}
}

func printUtxos(value interface{}) {
	fmt.Printf("%-14s:", fields.Utxos.String())

	if value == nil {
		fmt.Printf(" <nil>\n")
		return
	}

	arr, ok := value.([]interface{})
	if !ok {
		fmt.Printf(" <not array>\n")
		return
	}

	if len(arr) == 0 {
		fmt.Printf(" <empty>\n")
		return
	}

	for i, item := range arr {
		if b, found := item.([]byte); found {
			// Format the hex string with spaces after byte 32 and 64
			hexStr := formatUtxoHex(b)
			if i == 0 {
				fmt.Printf(" %-5d : %s\n", i, hexStr)
			} else {
				fmt.Printf("              : %-5d : %s\n", i, hexStr)
			}
		} else {
			if i == 0 {
				fmt.Printf(" %-5d : %v\n", i, item)
			} else {
				fmt.Printf("              : %-5d : %v\n", i, item)
			}
		}
	}
}
func formatUtxoHex(b []byte) string {
	if len(b) == 32 {
		// Reverse 32-byte hash from little-endian to big-endian
		reversed := make([]byte, 32)
		for i := 0; i < 32; i++ {
			reversed[i] = b[31-i]
		}
		return fmt.Sprintf("%x", reversed)
	}

	if len(b) < 32 {
		return fmt.Sprintf("%x", b)
	}

	// For values > 32 bytes, reverse the first 32 bytes (hash part)
	reversed := make([]byte, 32)
	for i := 0; i < 32; i++ {
		reversed[i] = b[31-i]
	}
	result := fmt.Sprintf("%x", reversed)

	if len(b) > 32 && len(b) <= 64 {
		result += " " + fmt.Sprintf("%x", b[32:])
	} else if len(b) > 64 {
		result += " " + fmt.Sprintf("%x", b[32:64]) + " " + fmt.Sprintf("%x", b[64:])
	}

	return result
}
func printTxID(value interface{}) {
	fmt.Printf("%-14s:", fields.TxID.String())

	if value == nil {
		fmt.Printf(" <nil>\n")
		return
	}

	b, ok := value.([]byte)
	if !ok {
		fmt.Printf(" <not bytes>\n")
		return
	}

	if len(b) != 32 {
		fmt.Printf(" %x (invalid length: %d bytes)\n", b, len(b))
		return
	}

	// Reverse the 32-byte hash from little-endian to big-endian
	reversed := make([]byte, 32)
	for i := 0; i < 32; i++ {
		reversed[i] = b[31-i]
	}
	fmt.Printf(" %x\n", reversed)
}
