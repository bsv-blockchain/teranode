//go:build test_smoke || test_utxo || debug

// How to run each test:
// Clean up docker containers before running the test manually
// $ cd test/smoke/
// $ go test -v -run "^TestUtxoTestSuite$/TestShouldAllowToSpendUtxos$" -tags test_utxo
// $ go test -v -run "^TestUtxoTestSuite$/TestShouldAllowSpendAllUtxos$" -tags test_utxo
// $ go test -v -run "^TestUtxoTestSuite$/TestDeleteParentTx$" -tags test_utxo
// $ go test -v -run "^TestUtxoTestSuite$/TestFreezeAndUnfreezeUtxos$" -tags test_utxo
// $ go test -v -run "^TestUtxoTestSuite$/TestShouldAllowSaveUTXOsIfExtStoreHasTXs$" -tags test_utxo
// $ go test -v -run "^TestUtxoTestSuite$/TestShouldAllowReassign$" -tags test_utxo
// $ go test -v -run "^TestUtxoTestSuite$/TestShouldAllowSpendAllUtxosWithAerospikeFailure$" -tags test_utxo
// $ go test -v -run "^TestUtxoTestSuite$/TestConnectionPoolLimiting$" -tags test_utxo
package smoke

import (
	"crypto/rand"
	"fmt"
	"math"
	"math/big"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/unlocker"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/teranode/errors"
	helper "github.com/bsv-blockchain/teranode/test/utils"
	"github.com/bsv-blockchain/teranode/test/utils/tstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type UtxoTestSuite struct {
	helper.TeranodeTestSuite
}

func TestUtxoTestSuite(t *testing.T) {
	suite.Run(t, &UtxoTestSuite{})
}

/*
TestShouldAllowSpendAllUtxosWithAerospikeFailure tests the system's resilience when Aerospike fails during UTXO processing.

Test Steps:
1. Initial Setup
  - Create private keys and addresses for transaction testing
  - Record initial UTXO balance for verification

2. Faucet Transaction
  - Request funds from faucet to get initial UTXOs
  - Send faucet transaction to network
  - Split faucet outputs into two sets for separate transactions

3. First Transaction
  - Create and send transaction using first set of UTXOs
  - Verify successful transmission

4. Concurrent Operations with Second Transaction
  - Create second transaction using remaining UTXOs
  - Execute two concurrent operations:
    a. Send the second transaction to the network
    b. Shutdown Aerospike node with small delay (100ms) to ensure transaction processing has started
  - Wait for both operations to complete

5. Recovery Phase
  - Wait 2 seconds before attempting Aerospike restart
  - Restart Aerospike node
  - Wait 5 seconds for Aerospike to fully initialize

6. Verification Phase
  - Mine new blocks to include pending transactions
  - Verify both transactions are included in blocks:
    a. Check first transaction inclusion
    b. Check second transaction inclusion
  - Verify final UTXO balance matches expected state

Expected Outcomes:
- Both transactions should eventually be included in blocks
- System should handle Aerospike failure gracefully
- UTXO state should remain consistent after recovery

Settings used in this test:
- expiration=1
- utxostore_utxoBatchSize=128
- utxostore_utxoBatchSize=50
*/
func (suite *UtxoTestSuite) TestShouldAllowSpendAllUtxosWithAerospikeFailure() {
	t := suite.T()
	framework := suite.TeranodeTestEnv
	logger := framework.Logger
	ctx := framework.Context
	url := "http://" + framework.Nodes[0].AssetURL
	rpcEndpoint := "http://" + framework.Nodes[0].RPCURL

	node1 := framework.Nodes[0]
	txDistributor := node1.PropagationClient

	// Generate private keys and addresses
	privateKey0, err := bec.NewPrivateKey()
	require.NoError(t, err, "Failed to generate private key")

	privateKey1, err := bec.NewPrivateKey()
	require.NoError(t, err, "Failed to generate private key")

	address1, err := bscript.NewAddressFromPublicKey(privateKey1.PubKey(), true)
	require.NoError(t, err, "Failed to create address")

	// Get coinbase transaction from block 1
	block1, err := node1.BlockchainClient.GetBlockByHeight(ctx, 1)
	require.NoError(t, err, "Failed to get block 1")

	parentTx := block1.CoinbaseTx

	// Split outputs into two parts
	firstSet := len(parentTx.Outputs) - 2
	secondSet := firstSet

	createTx := func(outputs []*bt.Output, startIndex int) (*bt.Tx, error) {
		logger.Infof("Creating transaction with %d outputs", len(outputs))

		spendingTx := bt.NewTx()
		totalSatoshis := uint64(0)
		utxos := make([]*bt.UTXO, 0)

		for i, output := range outputs {
			idx := i + startIndex
			utxo := &bt.UTXO{
				TxIDHash:      parentTx.TxIDChainHash(),
				Vout:          uint32(idx),
				LockingScript: output.LockingScript,
				Satoshis:      output.Satoshis,
			}
			utxos = append(utxos, utxo)
			totalSatoshis += output.Satoshis
		}

		err := spendingTx.FromUTXOs(utxos...)
		if err != nil {
			return nil, errors.NewProcessingError("error adding UTXOs to transaction", err)
		}

		fee := uint64(1000)
		amountToSend := totalSatoshis - fee

		err = spendingTx.AddP2PKHOutputFromAddress(address1.AddressString, amountToSend)
		if err != nil {
			return nil, errors.NewProcessingError("error adding output to transaction", err)
		}

		err = spendingTx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privateKey0})
		if err != nil {
			return nil, errors.NewProcessingError("error filling transaction inputs", err)
		}

		return spendingTx, nil
	}

	// Create and send first transaction
	tx1, err := createTx(parentTx.Outputs[:firstSet], 0)
	require.NoError(t, err, "Failed to create first transaction")

	err = txDistributor.ProcessTransaction(ctx, tx1)
	require.NoError(t, err, "Failed to send first transaction")
	logger.Infof("First Transaction sent: %s %s", tx1.TxIDChainHash(), tx1.TxID())

	// Create second transaction
	tx2, err := createTx(parentTx.Outputs[secondSet:], secondSet)
	require.NoError(t, err, "Failed to create second transaction")

	// Channel to coordinate operations
	done := make(chan struct{})
	errChan := make(chan error, 2)

	// Start Aerospike shutdown goroutine
	go func() {
		defer close(done)
		time.Sleep(100 * time.Millisecond) // Small delay to ensure transaction starts sending
		logger.Infof("Stopping Aerospike node")

		if err := framework.StopNode("aerospike-1"); err != nil {
			errChan <- errors.NewProcessingError("failed to stop aerospike", err)
			return
		}
		errChan <- nil
	}()

	// Send second transaction
	go func() {
		logger.Infof("Sending second transaction %s %s", tx2.TxIDChainHash(), tx2.TxID())

		err = txDistributor.ProcessTransaction(ctx, tx2)
		if err != nil {
			errChan <- errors.NewProcessingError("failed to send second transaction", err)
			return
		}
		errChan <- nil
	}()

	// Wait for both operations to complete
	for i := 0; i < 2; i++ {
		if err := <-errChan; err != nil {
			logger.Warnf("Operation error: %v", err)
		}
	}

	<-done

	// Restart Aerospike
	time.Sleep(2 * time.Second) // Wait before restart

	err = framework.StartNode("aerospike-1")
	require.NoError(t, err, "Failed to restart Aerospike")
	time.Sleep(5 * time.Second) // Wait for Aerospike to fully start

	// Mine blocks and verify transactions
	height, _ := helper.GetBlockHeight(url)
	logger.Infof("Block height before mining: %d", height)

	_, err = helper.MineBlockWithRPC(ctx, framework.Nodes[0], logger)
	require.NoError(t, err, "Failed to mine block")

	blockStore := framework.Nodes[0].ClientBlockstore
	subtreeStore := framework.Nodes[0].ClientSubtreestore
	blockchainClient := framework.Nodes[0].BlockchainClient

	// Verify both transactions are in blocks
	verifyTxInBlock := func(tx *bt.Tx, desc string) bool {
		targetHeight := height + 1
		for i := 0; i < 5; i++ {
			err := helper.WaitForBlockHeight(url, targetHeight, 60)
			require.NoError(t, err, "Failed to wait for block height")

			_, err = helper.CallRPC(rpcEndpoint, "generate", []interface{}{101})
			require.NoError(t, err, "Failed to generate block")

			header, meta, _ := blockchainClient.GetBlockHeadersFromHeight(ctx, targetHeight, 1)
			logger.Infof("Checking %s in block at height %d", desc, targetHeight)

			found, err := helper.TestTxInBlock(ctx, logger, blockStore, subtreeStore, header[0].Hash()[:], *tx.TxIDChainHash())
			if err != nil {
				t.Errorf("error checking if tx exists in block: %v, error %v", meta[0].Height, err)
			}

			if found {
				logger.Infof("Found %s in block at height %d", desc, targetHeight)
				return true
			}

			targetHeight++

			_, err = helper.MineBlockWithRPC(ctx, framework.Nodes[0], logger)
			if err != nil {
				logger.Warnf("Failed to mine block: %v", err)
			}
		}

		return false
	}

	assert.True(t, verifyTxInBlock(tx2, "second transaction"), "Second transaction not found in block")
}

// nolint: gocognit
func (suite *UtxoTestSuite) TestDeleteParentTx() {
	t := suite.T()
	framework := suite.TeranodeTestEnv
	ctx := framework.Context
	logger := framework.Logger

	url := "http://" + framework.Nodes[0].AssetURL
	rpcEndpoint := "http://" + framework.Nodes[0].RPCURL

	node1 := framework.Nodes[0]
	txDistributor := node1.PropagationClient

	// Generate private keys and addresses
	privateKey, err := bec.NewPrivateKey()
	require.NoError(t, err, "Failed to generate private key")

	address, err := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
	require.NoError(t, err, "Failed to create address")

	// Get coinbase transaction from block 1
	block1, err := node1.BlockchainClient.GetBlockByHeight(ctx, 1)
	require.NoError(t, err, "Failed to get block 1")

	parentTx := block1.CoinbaseTx

	// Create first transaction using coinbase UTXO
	utxo := &bt.UTXO{
		TxIDHash:      parentTx.TxIDChainHash(),
		Vout:          uint32(0),
		LockingScript: parentTx.Outputs[0].LockingScript,
		Satoshis:      parentTx.Outputs[0].Satoshis,
	}

	newTx := bt.NewTx()
	err = newTx.FromUTXOs(utxo)
	require.NoError(t, err, "Error adding UTXO to transaction")

	err = newTx.AddP2PKHOutputFromAddress(address.AddressString, 10000)
	require.NoError(t, err, "Error adding output to transaction")

	err = newTx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privateKey})
	require.NoError(t, err, "Error filling transaction inputs")

	err = txDistributor.ProcessTransaction(ctx, newTx)
	require.NoError(t, err, "Failed to send transaction")

	logger.Infof("Transaction sent: %s %s", newTx.TxIDChainHash(), newTx.TxID())

	height, _ := helper.GetBlockHeight(url)
	logger.Infof("Block height before mining: %d", height)

	err = framework.Nodes[0].UtxoStore.Delete(framework.Context, parentTx.TxIDChainHash())
	require.NoError(t, err, "Failed to delete parent tx")

	_, err = helper.MineBlockWithRPC(ctx, framework.Nodes[0], logger)
	require.NoError(t, err, "Failed to mine block")

	blockStore := framework.Nodes[0].ClientBlockstore
	subtreeStore := framework.Nodes[0].ClientSubtreestore
	blockchainClient := framework.Nodes[0].BlockchainClient
	bl := false
	targetHeight := height + 1

	for i := 0; i < 5; i++ {
		err := helper.WaitForBlockHeight(url, targetHeight, 60)
		require.NoError(t, err, "Failed to wait for block height")

		_, err = helper.CallRPC(rpcEndpoint, "generate", []interface{}{101})
		require.NoError(t, err, "Failed to generate block")

		header, meta, _ := blockchainClient.GetBlockHeadersFromHeight(ctx, targetHeight, 1)
		logger.Infof("Testing on Best block header: %v", header[0].Hash())

		bl, err = helper.TestTxInBlock(ctx, logger, blockStore, subtreeStore, header[0].Hash()[:], *newTx.TxIDChainHash())
		if err != nil {
			t.Errorf("error checking if tx exists in block: %v, error %v", meta[0].Height, err)
		}

		if bl {
			break
		}

		targetHeight++
	}

	assert.Equal(t, true, bl, "Test Tx not found in block")
}

func (suite *UtxoTestSuite) TestShouldAllowSaveUTXOsIfExtStoreHasTXs() {
	t := suite.T()
	framework := suite.TeranodeTestEnv
	logger := framework.Logger
	ctx := framework.Context

	framework.StopNode("teranode2")

	node1 := framework.Nodes[0]
	txDistributor := node1.PropagationClient

	// Generate private key and address
	privateKey0, err := bec.NewPrivateKey()
	require.NoError(t, err, "Failed to generate private key")

	// Get coinbase transaction from block 1
	block1, err := node1.BlockchainClient.GetBlockByHeight(ctx, 1)
	require.NoError(t, err, "Failed to get block 1")

	parentTx := block1.CoinbaseTx

	// Create first transaction using coinbase UTXO
	utxo := &bt.UTXO{
		TxIDHash:      parentTx.TxIDChainHash(),
		Vout:          uint32(0),
		LockingScript: parentTx.Outputs[0].LockingScript,
		Satoshis:      parentTx.Outputs[0].Satoshis,
	}

	newTx := bt.NewTx()
	err = newTx.FromUTXOs(utxo)
	require.NoError(t, err, "Error adding UTXO to transaction")

	err = newTx.AddP2PKHOutputFromPubKeyBytes(privateKey0.PubKey().Compressed(), 10000)
	require.NoError(t, err, "Error adding output to transaction")

	err = newTx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privateKey0})
	require.NoError(t, err, "Error filling transaction inputs")

	err = txDistributor.ProcessTransaction(ctx, newTx)
	require.NoError(t, err, "Failed to send transaction")

	logger.Infof("Transaction sent: %s with %d outputs", newTx.TxIDChainHash(), len(newTx.Outputs))

	time.Sleep(10 * time.Second)

	srcFile := fmt.Sprintf("teranode1/external/%s.tx", newTx.TxID())
	destFile := fmt.Sprintf("teranode2/external/%s.tx", newTx.TxID())
	resp, err := suite.TeranodeTestEnv.ComposeSharedStorage.Copy(
		ctx,
		&tstore.CopyRequest{
			SrcPath:  srcFile,
			DestPath: destFile,
		},
	)

	if err != nil || !resp.Ok {
		t.Errorf("Failed to copy file from %s to %s: %v", srcFile, destFile, err)
	}

	framework.StartNode("teranode2")

	err = framework.InitializeTeranodeTestClients()
	require.NoError(t, err, "Failed to initialize teranode test clients")
	time.Sleep(10 * time.Second)

	err = framework.Nodes[1].BlockchainClient.Run(framework.Context, "test")
	require.NoError(t, err, "Failed to run blockchain client")

	// mine a block
	_, err = helper.MineBlockWithRPC(ctx, framework.Nodes[0], logger)
	require.NoError(t, err, "Failed to mine block")

	time.Sleep(30 * time.Second)

	md, err := framework.Nodes[1].UtxoStore.Get(ctx, newTx.TxIDChainHash())
	require.NoError(t, err, "Failed to get UTXO")

	assert.NotNil(t, md.Tx.TxID(), "Failed to get UTXO")
}

func (suite *UtxoTestSuite) TestConnectionPoolLimiting() {
	t := suite.T()
	framework := suite.TeranodeTestEnv
	logger := framework.Logger
	ctx := framework.Context
	txDistributor := framework.Nodes[0].PropagationClient
	coinbaseClient := framework.Nodes[0].CoinbaseClient

	// Generate keys and address
	privateKey, err := bec.NewPrivateKey()
	require.NoError(t, err)
	address, err := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
	require.NoError(t, err)

	// Request funds with many outputs
	faucetTx, err := coinbaseClient.RequestFunds(ctx, address.AddressString, true)
	require.NoError(t, err)
	err = txDistributor.ProcessTransaction(ctx, faucetTx)
	require.NoError(t, err)

	// Get the connection queue size from settings
	utxoStoreURL := framework.Nodes[0].Settings.UtxoStore.UtxoStore
	qValues := utxoStoreURL.Query()

	connQueueSize := 32 // Default to 32 if not specified

	if qsStr := qValues.Get("ConnectionQueueSize"); qsStr != "" {
		qs, err := strconv.Atoi(qsStr)
		require.NoError(t, err)

		connQueueSize = qs
	}

	logger.Infof("Using connection queue size: %d", connQueueSize)

	// Create 2x ConnectionQueueSize transactions to ensure queuing occurs
	numTx := connQueueSize * 2
	txs := make([]*bt.Tx, numTx)

	// Split faucet outputs into small chunks
	outputsPerTx := len(faucetTx.Outputs) / numTx

	for i := 0; i < numTx; i++ {
		startIdx := i * outputsPerTx
		endIdx := startIdx + outputsPerTx

		if i == numTx-1 {
			endIdx = len(faucetTx.Outputs) // Use all remaining outputs in last tx
		}

		// Create transaction with subset of outputs
		tx := bt.NewTx()
		utxos := make([]*bt.UTXO, 0)
		totalSats := uint64(0)

		for j, output := range faucetTx.Outputs[startIdx:endIdx] {
			if startIdx+j > math.MaxUint32 {
				t.Fatal("UTXO index exceeds uint32 maximum")
			}

			utxo := &bt.UTXO{
				TxIDHash:      faucetTx.TxIDChainHash(),
				Vout:          uint32(startIdx + j), //nolint:gosec // G115: already checked for overflow above
				LockingScript: output.LockingScript,
				Satoshis:      output.Satoshis,
			}

			utxos = append(utxos, utxo) // Collect UTXOs
			totalSats += output.Satoshis
		}

		err = tx.FromUTXOs(utxos...)
		require.NoError(t, err)

		// Add output back to same address (minus fee)
		fee := uint64(1000)
		err = tx.AddP2PKHOutputFromAddress(address.AddressString, totalSats-fee)
		require.NoError(t, err)

		err = tx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privateKey})
		require.NoError(t, err)

		txs[i] = tx
	}

	// Send all transactions concurrently and measure timing
	errChan := make(chan error, numTx)
	timingChan := make(chan time.Duration, numTx)

	start := time.Now()

	for i := 0; i < numTx; i++ {
		go func(tx *bt.Tx, idx int) {
			txStart := time.Now()
			err = txDistributor.ProcessTransaction(ctx, tx)
			timingChan <- time.Since(txStart)
			errChan <- err

			logger.Infof("Transaction %d sent in %v", idx, time.Since(txStart))
		}(txs[i], i)
	}

	// Collect results and timing
	var errors []error

	timings := make([]time.Duration, numTx)

	for i := 0; i < numTx; i++ {
		if err := <-errChan; err != nil {
			errors = append(errors, err)
		}

		timings[i] = <-timingChan
	}

	totalTime := time.Since(start)
	logger.Infof("All transactions processed in %v", totalTime)

	// Verify no connection pool errors occurred
	for _, err := range errors {
		assert.NotContains(t, err.Error(), "NO_AVAILABLE_CONNECTIONS_TO_NODE",
			"Connection pool exhaustion occurred")
	}

	// Analyze timing distribution
	sort.Slice(timings, func(i, j int) bool { return timings[i] < timings[j] })
	medianTime := timings[numTx/2]
	p90Time := timings[int(float64(numTx)*0.9)]

	logger.Infof("Transaction timing statistics:")
	logger.Infof("Median processing time: %v", medianTime)
	logger.Infof("90th percentile processing time: %v", p90Time)

	// Verify timing shows proper queuing behavior
	fastProcessed := 0           // Count of quickly processed transactions
	slowProcessed := 0           // Count of queued transactions
	queueThreshold := medianTime // Transactions taking > 2x median are considered queued

	// First ConnectionQueueSize (32) transactions should process quickly
	for i := 0; i < connQueueSize; i++ {
		if timings[i] < medianTime {
			fastProcessed++
		}
	}

	// Transactions beyond ConnectionQueueSize should show queuing
	for i := connQueueSize; i < numTx; i++ {
		if timings[i] > queueThreshold {
			slowProcessed++
		}
	}

	// Verify queuing behavior
	assert.GreaterOrEqual(t, fastProcessed, connQueueSize/2,
		"Expected majority of first %d transactions to process quickly (<%v)", connQueueSize, medianTime)
	assert.GreaterOrEqual(t, slowProcessed, (numTx-connQueueSize)/2,
		"Expected at least half of transactions beyond connection pool size to be queued (>%v)", queueThreshold)

	// Verify timing distribution shows clear queuing
	earlyTxMedian := timings[connQueueSize/2]       // Median of first 32 tx (middle of 0-31)
	laterTxMedian := timings[numTx-connQueueSize/2] // Median of second 32 tx (middle of 32-63)
	logger.Infof("Early transactions median: %v, Later transactions median: %v", earlyTxMedian, laterTxMedian)
	assert.Greater(t, laterTxMedian, earlyTxMedian,
		"Expected later transactions to take significantly longer, showing queuing behavior")

	// Mine blocks and verify all transactions
	height, _ := helper.GetBlockHeight("http://" + framework.Nodes[0].AssetURL)
	_, err = helper.MineBlockWithRPC(ctx, framework.Nodes[0], logger)
	require.NoError(t, err)

	blockStore := framework.Nodes[0].ClientBlockstore
	subtreeStore := framework.Nodes[0].ClientSubtreestore
	blockchainClient := framework.Nodes[0].BlockchainClient

	// Function to verify a transaction is in a block
	verifyTxInBlock := func(tx *bt.Tx, desc string) bool {
		targetHeight := height + 1
		for i := 0; i < 5; i++ {
			err := helper.WaitForBlockHeight("http://"+framework.Nodes[0].AssetURL, targetHeight, 60)
			assert.NoError(t, err, "Failed to wait for block height")

			_, err = helper.CallRPC("http://"+framework.Nodes[0].RPCURL, "generate", []interface{}{101})
			assert.NoError(t, err, "Failed to generate block")

			header, meta, _ := blockchainClient.GetBlockHeadersFromHeight(ctx, targetHeight, 1)
			logger.Infof("Checking %s in block at height %d", desc, targetHeight)

			found, err := helper.TestTxInBlock(ctx, logger, blockStore, subtreeStore, header[0].Hash()[:], *tx.TxIDChainHash())
			if err != nil {
				t.Errorf("error checking if tx exists in block: %v, error %v", meta[0].Height, err)
			}

			if found {
				logger.Infof("Found %s in block at height %d", desc, targetHeight)
				return true
			}

			targetHeight++

			_, err = helper.MineBlockWithRPC(ctx, framework.Nodes[0], logger)
			if err != nil {
				logger.Warnf("Failed to mine block: %v", err)
			}
		}

		return false
	}

	// Verify a random sample of transactions made it into blocks
	numSamples := 3
	seen := make(map[int]bool)

	for i := 0; i < numSamples; i++ {
		// Get random index we haven't checked yet
		idx := -1

		for {
			max := big.NewInt(int64(len(txs)))

			n, err := rand.Int(rand.Reader, max)
			if err != nil {
				t.Fatal("Failed to generate random number:", err)
			}

			idx = int(n.Int64())
			if !seen[idx] {
				seen[idx] = true
				break
			}
		}

		logger.Infof("Verifying random transaction %d", idx)
		assert.True(t, verifyTxInBlock(txs[idx], fmt.Sprintf("transaction %d", idx)),
			"Transaction %d (%s) not found in block", idx, txs[idx].TxID())
	}
}
