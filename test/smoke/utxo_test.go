//go:build test_all || test_smoke || test_utxo

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
package smoke

import (
	"fmt"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/libsv/go-bk/bec"
	"github.com/libsv/go-bk/wif"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/unlocker"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type UtxoTestSuite struct {
	helper.TeranodeTestSuite
}

func (suite *UtxoTestSuite) TearDownTest() {
}

/* TestShouldAllowToSpendUtxos tests that a UTXO can be spent */
// Request funds from the coinbase wallet
// Create a new transaction from the first output of the faucet transaction
// Send the transaction
// Mine a block
// Verify the transaction is in the block
// Create another transaction from the second output of the faucet transaction
// Send the transaction
// Mine a block
// Verify the transaction is in the block
func (suite *UtxoTestSuite) TestShouldAllowToSpendUtxos() {
	t := suite.T()
	framework := suite.TeranodeTestEnv
	logger := framework.Logger
	ctx := framework.Context

	url := "http://" + framework.Nodes[0].AssetURL
	rpcEndpoint := "http://" + framework.Nodes[0].RPCURL

	txDistributor := &framework.Nodes[0].DistributorClient

	coinbaseClient := framework.Nodes[0].CoinbaseClient
	utxoBalanceBefore, _, _ := coinbaseClient.GetBalance(ctx)
	logger.Infof("utxoBalanceBefore: %d\n", utxoBalanceBefore)

	privateKey0, err := bec.NewPrivateKey(bec.S256())
	if err != nil {
		t.Errorf("Failed to generate private key: %v", err)
	}

	privateKey1, err := bec.NewPrivateKey(bec.S256())
	if err != nil {
		t.Errorf("Failed to generate private key: %v", err)
	}

	address0, err := bscript.NewAddressFromPublicKey(privateKey0.PubKey(), true)
	if err != nil {
		t.Errorf("Failed to create address: %v", err)
	}

	address1, err := bscript.NewAddressFromPublicKey(privateKey1.PubKey(), true)
	if err != nil {
		t.Errorf("Failed to create address: %v", err)
	}

	faucetTx, err := coinbaseClient.RequestFunds(ctx, address0.AddressString, true)
	if err != nil {
		t.Errorf("Failed to request funds: %v", err)
	}

	t.Logf("Transaction: %s %s\n", faucetTx.TxIDChainHash(), faucetTx.TxID())

	_, err = txDistributor.SendTransaction(ctx, faucetTx)
	if err != nil {
		t.Errorf("Failed to send transaction: %v", err)
	}

	logger.Infof("Request funds Transaction sent: %s %v\n", faucetTx.TxIDChainHash(), len(faucetTx.Outputs))
	output := faucetTx.Outputs[0]
	utxo := &bt.UTXO{
		TxIDHash:      faucetTx.TxIDChainHash(),
		Vout:          uint32(0),
		LockingScript: output.LockingScript,
		Satoshis:      output.Satoshis,
	}

	firstTx := bt.NewTx()

	err = firstTx.FromUTXOs(utxo)
	if err != nil {
		t.Errorf("Error adding UTXO to transaction: %s\n", err)
	}

	err = firstTx.AddP2PKHOutputFromAddress(address1.AddressString, 10000)
	if err != nil {
		t.Errorf("Error adding output to transaction: %v", err)
	}

	err = firstTx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privateKey0})
	if err != nil {
		t.Errorf("Error filling transaction inputs: %v", err)
	}

	_, err = txDistributor.SendTransaction(ctx, firstTx)
	if err != nil {
		t.Errorf("Failed to send new transaction: %v", err)
	}

	logger.Infof("First Transaction created with output[0] of faucet sent: %s %s\n", firstTx.TxIDChainHash(), firstTx.TxID())

	height, _ := helper.GetBlockHeight(url)
	logger.Infof("Block height before mining: %d\n", height)

	utxoBalanceAfter, _, _ := coinbaseClient.GetBalance(ctx)
	logger.Infof("utxoBalanceBefore: %d, utxoBalanceAfter: %d\n", utxoBalanceBefore, utxoBalanceAfter)

	_, err = helper.MineBlockWithRPC(ctx, framework.Nodes[0], logger)

	if err != nil {
		t.Errorf("Failed to mine block: %v", err)
	}

	output = faucetTx.Outputs[1]
	utxo = &bt.UTXO{
		TxIDHash:      faucetTx.TxIDChainHash(),
		Vout:          uint32(1),
		LockingScript: output.LockingScript,
		Satoshis:      output.Satoshis,
	}

	secondTx := bt.NewTx()

	err = secondTx.FromUTXOs(utxo)
	if err != nil {
		t.Errorf("Error adding UTXO to transaction: %s\n", err)
	}

	err = secondTx.AddP2PKHOutputFromAddress(address1.AddressString, 10000)
	if err != nil {
		t.Errorf("Error adding output to transaction: %v", err)
	}

	err = secondTx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privateKey0})
	if err != nil {
		t.Errorf("Error filling transaction inputs: %v", err)
	}

	_, err = txDistributor.SendTransaction(ctx, secondTx)
	if err != nil {
		t.Errorf("Failed to send new transaction: %v", err)
	}

	logger.Infof("Second Transaction created with output[1] of faucet sent %s %s\n", secondTx.TxIDChainHash(), secondTx.TxID())

	blockStore := framework.Nodes[0].Blockstore
	blockchainClient := framework.Nodes[0].BlockchainClient
	bl := false
	targetHeight := height + 1

	for i := 0; i < 5; i++ {
		_, err = helper.CallRPC(rpcEndpoint, "generate", []interface{}{101})
		require.NoError(t, err)

		err := helper.WaitForBlockHeight(url, targetHeight, 60)
		if err != nil {
			t.Errorf("Failed to wait for block height: %v", err)
		}

		header, meta, _ := blockchainClient.GetBlockHeadersFromHeight(ctx, targetHeight, 1)
		logger.Infof("Testing on Best block header: %v", header[0].Hash())
		bl, err = helper.CheckIfTxExistsInBlock(ctx, blockStore, framework.Nodes[0].BlockstoreURL, header[0].Hash()[:], meta[0].Height, *secondTx.TxIDChainHash(), framework.Logger)

		if err != nil {
			t.Errorf("error checking if tx exists in block: %v", err)
		}

		if bl {
			break
		}

		targetHeight++
		// _, err = helper.MineBlockWithRPC(ctx, framework.Nodes[0], logger)

		if err != nil {
			t.Errorf("Failed to mine block: %v", err)
		}
	}

	assert.Equal(t, true, bl, "Test Tx not found in block")
}

/* TestShouldAllowSpendAllUtxos tests that we can spend all UTXOs with multiple transactions */
// Request Tx from faucet, it has 100+ outputs
// Split the outputs into two parts
// Create and send two transactions
// Mine a block
// Verify both transactions are in blocks
// Settings used in this test:
// expiration=1
// utxostore_utxoBatchSize=128
// utxostore_utxoBatchSize=50
func (suite *UtxoTestSuite) TestShouldAllowSpendAllUtxos() {
	t := suite.T()
	framework := suite.TeranodeTestEnv
	logger := framework.Logger
	ctx := framework.Context
	url := "http://" + framework.Nodes[0].AssetURL
	rpcEndpoint := "http://" + framework.Nodes[0].RPCURL

	txDistributor := &framework.Nodes[0].DistributorClient

	coinbaseClient := framework.Nodes[0].CoinbaseClient
	utxoBalanceBefore, _, _ := coinbaseClient.GetBalance(ctx)
	logger.Infof("utxoBalanceBefore: %d", utxoBalanceBefore)

	privateKey0, err := bec.NewPrivateKey(bec.S256())
	assert.NoError(t, err, "Failed to generate private key")

	privateKey1, err := bec.NewPrivateKey(bec.S256())
	assert.NoError(t, err, "Failed to generate private key")

	address0, err := bscript.NewAddressFromPublicKey(privateKey0.PubKey(), true)
	assert.NoError(t, err, "Failed to create address")

	address1, err := bscript.NewAddressFromPublicKey(privateKey1.PubKey(), true)
	assert.NoError(t, err, "Failed to create address")

	faucetTx, err := coinbaseClient.RequestFunds(ctx, address0.AddressString, true)
	assert.NoError(t, err, "Failed to request funds")

	logger.Infof("Faucet Transaction: %s %s", faucetTx.TxIDChainHash(), faucetTx.TxID())

	_, err = txDistributor.SendTransaction(ctx, faucetTx)
	assert.NoError(t, err, "Failed to send faucet transaction")

	logger.Infof("Faucet Transaction sent: %s with %d outputs", faucetTx.TxIDChainHash(), len(faucetTx.Outputs))

	blockStore := framework.Nodes[0].Blockstore
	blockchainClient := framework.Nodes[0].BlockchainClient

	// Split outputs into two parts
	firstSet := len(faucetTx.Outputs) - 2
	secondSet := firstSet

	createAndSendTx := func(outputs []*bt.Output, startIndex int) (*bt.Tx, error) {
		logger.Infof("Creating and sending transaction with %d outputs", len(outputs))

		spendingTx := bt.NewTx()
		totalSatoshis := uint64(0)

		utxos := make([]*bt.UTXO, 0)

		const maxUint32 = 1<<32 - 1

		for i, output := range outputs {
			idx := i + startIndex
			if idx < 0 || idx > maxUint32 {
				return nil, errors.NewProcessingError("Vout index out of range")
			}

			// nolint: gosec
			utxo := &bt.UTXO{
				TxIDHash:      faucetTx.TxIDChainHash(),
				Vout:          uint32(idx),
				LockingScript: output.LockingScript,
				Satoshis:      output.Satoshis,
			}
			utxos = append(utxos, utxo) // Collect UTXOs
			totalSatoshis += output.Satoshis
		}

		// Add UTXOs to the transaction
		err := spendingTx.FromUTXOs(utxos...)
		assert.NoError(t, err, "Error adding UTXOs to transaction")

		// Subtract a small fee
		fee := uint64(1000)
		amountToSend := totalSatoshis - fee

		err = spendingTx.AddP2PKHOutputFromAddress(address1.AddressString, amountToSend)
		assert.NoError(t, err, "Error adding output to transaction")

		err = spendingTx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privateKey0})
		assert.NoError(t, err, "Error filling transaction inputs")

		_, err = txDistributor.SendTransaction(ctx, spendingTx)
		assert.NoError(t, err, "Failed to send spending transaction")

		return spendingTx, nil
	}

	// Create and send first transaction
	tx1, err := createAndSendTx(faucetTx.Outputs[:firstSet], 0)
	assert.NoError(t, err, "Failed to create and send first transaction")
	logger.Infof("First Transaction sent: %s %s", tx1.TxIDChainHash(), tx1.TxID())

	// Create and send second transaction
	tx2, err := createAndSendTx(faucetTx.Outputs[secondSet:], secondSet)
	assert.NoError(t, err, "Failed to create and send second transaction")
	logger.Infof("Second Transaction sent: %s %s", tx2.TxIDChainHash(), tx2.TxID())

	height, _ := helper.GetBlockHeight(url)
	logger.Infof("Block height before mining: %d", height)

	_, err = helper.MineBlockWithRPC(ctx, framework.Nodes[0], logger)
	assert.NoError(t, err, "Failed to mine block")

	// Verify both transactions are in blocks
	blTx1 := false
	blTx2 := false
	targetHeight := height + 1

	for j := 0; j < 5; j++ {
		err := helper.WaitForBlockHeight(url, targetHeight, 60)
		assert.NoError(t, err, "Failed to wait for block height")

		_, err = helper.CallRPC(rpcEndpoint, "generate", []interface{}{101})
		assert.NoError(t, err, "Failed to generate blocks")

		header, meta, _ := blockchainClient.GetBlockHeadersFromHeight(ctx, targetHeight, 1)
		logger.Infof("Testing on Best block header: %v", header[0].Hash())

		if !blTx1 {
			blTx1, err = helper.CheckIfTxExistsInBlock(ctx, blockStore, framework.Nodes[0].BlockstoreURL, header[0].Hash()[:], meta[0].Height, *tx1.TxIDChainHash(), framework.Logger)
		}

		if !blTx2 {
			blTx2, err = helper.CheckIfTxExistsInBlock(ctx, blockStore, framework.Nodes[0].BlockstoreURL, header[0].Hash()[:], meta[0].Height, *tx2.TxIDChainHash(), framework.Logger)
		}

		if err != nil {
			logger.Errorf("Error checking if tx exists in block: %v", err)
		}

		if blTx1 && blTx2 {
			break
		}

		targetHeight++
		_, err = helper.MineBlockWithRPC(ctx, framework.Nodes[0], logger)
		assert.NoError(t, err, "Failed to mine block")
	}

	assert.True(t, blTx1, "Transaction %d not found in block", tx1.TxIDChainHash())
	assert.True(t, blTx2, "Transaction %d not found in block", tx2.TxIDChainHash())

	utxoBalanceAfter, _, _ := coinbaseClient.GetBalance(ctx)
	logger.Infof("utxoBalanceBefore: %d, utxoBalanceAfter: %d", utxoBalanceBefore, utxoBalanceAfter)
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

	txDistributor := &framework.Nodes[0].DistributorClient
	coinbaseClient := framework.Nodes[0].CoinbaseClient
	utxoBalanceBefore, _, _ := coinbaseClient.GetBalance(ctx)
	logger.Infof("utxoBalanceBefore: %d", utxoBalanceBefore)

	// Generate keys and addresses
	privateKey0, err := bec.NewPrivateKey(bec.S256())
	assert.NoError(t, err, "Failed to generate private key")

	privateKey1, err := bec.NewPrivateKey(bec.S256())
	assert.NoError(t, err, "Failed to generate private key")

	address0, err := bscript.NewAddressFromPublicKey(privateKey0.PubKey(), true)
	assert.NoError(t, err, "Failed to create address")

	address1, err := bscript.NewAddressFromPublicKey(privateKey1.PubKey(), true)
	assert.NoError(t, err, "Failed to create address")

	// Request funds from faucet
	faucetTx, err := coinbaseClient.RequestFunds(ctx, address0.AddressString, true)
	assert.NoError(t, err, "Failed to request funds")

	_, err = txDistributor.SendTransaction(ctx, faucetTx)
	assert.NoError(t, err, "Failed to send faucet transaction")
	logger.Infof("Faucet Transaction sent: %s with %d outputs", faucetTx.TxIDChainHash(), len(faucetTx.Outputs))

	// Split outputs into two parts
	firstSet := len(faucetTx.Outputs) - 2
	secondSet := firstSet

	createTx := func(outputs []*bt.Output, startIndex int) (*bt.Tx, error) {
		logger.Infof("Creating transaction with %d outputs", len(outputs))

		spendingTx := bt.NewTx()
		totalSatoshis := uint64(0)
		utxos := make([]*bt.UTXO, 0)

		// nolint: gosec
		for i, output := range outputs {
			idx := i + startIndex
			utxo := &bt.UTXO{
				TxIDHash:      faucetTx.TxIDChainHash(),
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
	tx1, err := createTx(faucetTx.Outputs[:firstSet], 0)
	assert.NoError(t, err, "Failed to create first transaction")

	_, err = txDistributor.SendTransaction(ctx, tx1)
	assert.NoError(t, err, "Failed to send first transaction")
	logger.Infof("First Transaction sent: %s %s", tx1.TxIDChainHash(), tx1.TxID())

	// Create second transaction
	tx2, err := createTx(faucetTx.Outputs[secondSet:], secondSet)
	assert.NoError(t, err, "Failed to create second transaction")

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

		_, err = txDistributor.SendTransaction(ctx, tx2)
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
	assert.NoError(t, err, "Failed to restart Aerospike")
	time.Sleep(5 * time.Second) // Wait for Aerospike to fully start

	// Mine blocks and verify transactions
	height, _ := helper.GetBlockHeight(url)
	logger.Infof("Block height before mining: %d", height)

	_, err = helper.MineBlockWithRPC(ctx, framework.Nodes[0], logger)
	assert.NoError(t, err, "Failed to mine block")

	blockStore := framework.Nodes[0].Blockstore
	blockchainClient := framework.Nodes[0].BlockchainClient

	// Verify both transactions are in blocks
	verifyTxInBlock := func(tx *bt.Tx, desc string) bool {
		targetHeight := height + 1
		for i := 0; i < 5; i++ {
			err := helper.WaitForBlockHeight(url, targetHeight, 60)
			assert.NoError(t, err, "Failed to wait for block height")

			_, err = helper.CallRPC(rpcEndpoint, "generate", []interface{}{101})
			assert.NoError(t, err, "Failed to generate block")

			header, meta, _ := blockchainClient.GetBlockHeadersFromHeight(ctx, targetHeight, 1)
			logger.Infof("Checking %s in block at height %d", desc, targetHeight)

			found, err := helper.CheckIfTxExistsInBlock(ctx, blockStore, framework.Nodes[0].BlockstoreURL,
				header[0].Hash()[:], meta[0].Height, *tx.TxIDChainHash(), framework.Logger)

			if err != nil {
				logger.Warnf("Error checking if tx exists in block: %v", err)
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

	// assert.True(t, verifyTxInBlock(tx1, "first transaction"), "First transaction not found in block")
	assert.True(t, verifyTxInBlock(tx2, "second transaction"), "Second transaction not found in block")

	utxoBalanceAfter, _, _ := coinbaseClient.GetBalance(ctx)
	logger.Infof("utxoBalanceBefore: %d, utxoBalanceAfter: %d", utxoBalanceBefore, utxoBalanceAfter)
}

// nolint: gocognit
func (suite *UtxoTestSuite) TestDeleteParentTx() {
	t := suite.T()
	framework := suite.TeranodeTestEnv
	ctx := framework.Context
	logger := framework.Logger

	url := "http://" + framework.Nodes[0].AssetURL
	rpcEndpoint := "http://" + framework.Nodes[0].RPCURL

	txDistributor := framework.Nodes[0].DistributorClient

	coinbaseClient := framework.Nodes[0].CoinbaseClient
	utxoBalanceBefore, _, _ := coinbaseClient.GetBalance(ctx)
	logger.Infof("utxoBalanceBefore: %d\n", utxoBalanceBefore)

	coinbasePrivKey, _ := gocore.Config().Get("coinbase_wallet_private_key")

	coinbasePrivateKey, err := wif.DecodeWIF(coinbasePrivKey)

	if err != nil {
		t.Errorf("Failed to decode Coinbase private key: %v", err)
	}

	coinbaseAddr, _ := bscript.NewAddressFromPublicKey(coinbasePrivateKey.PrivKey.PubKey(), true)

	privateKey, err := bec.NewPrivateKey(bec.S256())
	if err != nil {
		t.Errorf("Failed to generate private key: %v", err)
	}

	address, err := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
	if err != nil {
		t.Errorf("Failed to create address: %v", err)
	}

	tx, err := coinbaseClient.RequestFunds(ctx, address.AddressString, true)
	if err != nil {
		t.Errorf("Failed to request funds: %v", err)
	}

	t.Logf("Transaction: %s %s\n", tx.TxIDChainHash(), tx.TxID())

	_, err = txDistributor.SendTransaction(ctx, tx)
	if err != nil {
		t.Errorf("Failed to send transaction: %v", err)
	}

	logger.Infof("Transaction sent: %s %v\n", tx.TxIDChainHash(), len(tx.Outputs))
	output := tx.Outputs[0]
	utxo := &bt.UTXO{
		TxIDHash:      tx.TxIDChainHash(),
		Vout:          uint32(0),
		LockingScript: output.LockingScript,
		Satoshis:      output.Satoshis,
	}

	newTx := bt.NewTx()

	err = newTx.FromUTXOs(utxo)
	if err != nil {
		t.Errorf("Error adding UTXO to transaction: %s\n", err)
	}

	err = newTx.AddP2PKHOutputFromAddress(coinbaseAddr.AddressString, 10000)
	if err != nil {
		t.Errorf("Error adding output to transaction: %v", err)
	}

	err = newTx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privateKey})
	if err != nil {
		t.Errorf("Error filling transaction inputs: %v", err)
	}

	_, err = txDistributor.SendTransaction(ctx, newTx)
	if err != nil {
		t.Errorf("Failed to send new transaction: %v", err)
	}

	logger.Infof("Transaction sent: %s %s\n", newTx.TxIDChainHash(), newTx.TxID())

	height, _ := helper.GetBlockHeight(url)
	logger.Infof("Block height before mining: %d\n", height)

	utxoBalanceAfter, _, _ := coinbaseClient.GetBalance(ctx)
	logger.Infof("utxoBalanceBefore: %d, utxoBalanceAfter: %d\n", utxoBalanceBefore, utxoBalanceAfter)

	err = framework.Nodes[0].UtxoStore.Delete(framework.Context, tx.TxIDChainHash())
	if err != nil {
		t.Errorf("Failed to delete parent tx: %v", err)
	}

	_, err = helper.MineBlockWithRPC(ctx, framework.Nodes[0], logger)

	if err != nil {
		t.Errorf("Failed to mine block: %v", err)
	}

	blockStore := framework.Nodes[0].Blockstore
	blockchainClient := framework.Nodes[0].BlockchainClient
	bl := false
	targetHeight := height + 1

	for i := 0; i < 5; i++ {
		err := helper.WaitForBlockHeight(url, targetHeight, 60)
		assert.NoError(t, err, "Failed to wait for block height")

		_, err = helper.CallRPC(rpcEndpoint, "generate", []interface{}{101})
		assert.NoError(t, err, "Failed to generate block")

		header, meta, _ := blockchainClient.GetBlockHeadersFromHeight(ctx, targetHeight, 1)
		logger.Infof("Testing on Best block header: %v", header[0].Hash())

		bl, err = helper.CheckIfTxExistsInBlock(ctx, blockStore, framework.Nodes[0].BlockstoreURL, header[0].Hash()[:], meta[0].Height, *newTx.TxIDChainHash(), framework.Logger)
		if err != nil {
			t.Errorf("error checking if tx exists in block: %v", err)
		}

		if bl {
			break
		}

		targetHeight++

		if err != nil {
			t.Errorf("Failed to mine block: %v", err)
		}
	}

	assert.Equal(t, true, bl, "Test Tx not found in block")
}

// TestFreezeAndUnfreezeUtxos tests that we can freeze and unfreeze UTXOs
// Request Tx from faucet, it has 100+ Outputs
// Split the outputs into two parts
// Freeze the first set of outputs
// Create and send a transaction with the frozen outputs
// Expect freeze to be successful
// Create a TX from the frozen outputs
// Expect validation to fail
func (suite *UtxoTestSuite) TestFreezeAndUnfreezeUtxos() {
	t := suite.T()
	framework := suite.TeranodeTestEnv
	settingsMap := suite.SettingsMap
	logger := framework.Logger
	ctx := framework.Context

	settingsMap["SETTINGS_CONTEXT_1"] = "docker.teranode1.test.TestFreezeAndUnfreezeUtxos"
	if err := framework.RestartDockerNodes(settingsMap); err != nil {
		t.Errorf("Failed to restart nodes: %v", err)
	}

	err := framework.InitializeTeranodeTestClients()
	require.NoError(t, err, "Failed to initialize Teranode test clients")

	url := "http://" + framework.Nodes[0].AssetURL

	txDistributor := framework.Nodes[0].DistributorClient

	coinbaseClient := framework.Nodes[0].CoinbaseClient
	utxoBalanceBefore, _, _ := coinbaseClient.GetBalance(ctx)
	logger.Infof("utxoBalanceBefore: %d", utxoBalanceBefore)

	privateKey0, err := bec.NewPrivateKey(bec.S256())
	assert.NoError(t, err, "Failed to generate private key")

	privateKey1, err := bec.NewPrivateKey(bec.S256())
	assert.NoError(t, err, "Failed to generate private key")

	address0, err := bscript.NewAddressFromPublicKey(privateKey0.PubKey(), true)
	assert.NoError(t, err, "Failed to create address")

	address1, err := bscript.NewAddressFromPublicKey(privateKey1.PubKey(), true)
	assert.NoError(t, err, "Failed to create address")

	faucetTx, err := coinbaseClient.RequestFunds(ctx, address0.AddressString, false)
	assert.NoError(t, err, "Failed to request funds")

	logger.Infof("Faucet Transaction: %s %s", faucetTx.TxIDChainHash(), faucetTx.TxID())

	_, err = txDistributor.SendTransaction(ctx, faucetTx)
	assert.NoError(t, err, "Failed to send faucet transaction")

	logger.Infof("Faucet Transaction sent: %s with %d outputs", faucetTx.TxIDChainHash(), len(faucetTx.Outputs))

	blockStore := framework.Nodes[0].Blockstore
	blockchainClient := framework.Nodes[0].BlockchainClient

	// Split outputs into two parts
	firstSet := len(faucetTx.Outputs) - 2

	getSpends := func(outputs []*bt.Output, startIndex int) []*utxo.Spend {
		logger.Infof("Creating spends with %d outputs", len(outputs))

		spends := make([]*utxo.Spend, 0)

		const maxUint32 = 1<<32 - 1

		for i, output := range outputs {
			idx := i + startIndex
			if idx < 0 || idx > maxUint32 {
				return nil
			}

			// nolint: gosec
			utxoHash, _ := util.UTXOHashFromOutput(faucetTx.TxIDChainHash(), output, uint32(idx))
			// nolint: gosec
			spend := &utxo.Spend{
				TxID:     faucetTx.TxIDChainHash(),
				Vout:     uint32(idx),
				UTXOHash: utxoHash,
			}
			spends = append(spends, spend)
		}

		return spends
	}

	createAndSendTx := func(outputs []*bt.Output, startIndex int) (*bt.Tx, error) {
		logger.Infof("Creating and sending transaction with %d outputs", len(outputs))

		spendingTx := bt.NewTx()
		totalSatoshis := uint64(0)

		utxos := make([]*bt.UTXO, 0)

		const maxUint32 = 1<<32 - 1

		for i, output := range outputs {
			idx := i + startIndex
			if idx < 0 || idx > maxUint32 {
				return nil, errors.NewProcessingError("Vout index out of range")
			}

			// nolint: gosec
			utxo := &bt.UTXO{
				TxIDHash:      faucetTx.TxIDChainHash(),
				Vout:          uint32(idx),
				LockingScript: output.LockingScript,
				Satoshis:      output.Satoshis,
			}
			utxos = append(utxos, utxo) // Collect UTXOs
			totalSatoshis += output.Satoshis
		}

		// Add UTXOs to the transaction
		err := spendingTx.FromUTXOs(utxos...)
		assert.NoError(t, err, "Error adding UTXOs to transaction")

		// Subtract a small fee
		fee := uint64(1000)
		amountToSend := totalSatoshis - fee

		err = spendingTx.AddP2PKHOutputFromAddress(address1.AddressString, amountToSend)
		assert.NoError(t, err, "Error adding output to transaction")

		err = spendingTx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privateKey0})
		assert.NoError(t, err, "Error filling transaction inputs")

		_, err = txDistributor.SendTransaction(ctx, spendingTx)
		if err != nil {
			return nil, err
		}

		return spendingTx, nil
	}

	// TODO: which settings do we use?

	// get spends for the first set of outputs
	spends := getSpends(faucetTx.Outputs[:firstSet], 0)
	err = framework.Nodes[0].UtxoStore.FreezeUTXOs(ctx, spends, framework.Nodes[0].Settings)
	assert.NoError(t, err, "Failed to freeze UTXOs")
	// Create and send transaction with the frozen spends
	_, err = createAndSendTx(faucetTx.Outputs[:firstSet], 0)
	assert.Error(t, err, "Should not allow to spend frozen UTXOs")
	// Unfreeze the UTXOs
	err = framework.Nodes[0].UtxoStore.UnFreezeUTXOs(ctx, spends, framework.Nodes[0].Settings)
	assert.NoError(t, err, "Failed to unfreeze UTXOs")
	// Create and send transaction with the unfrozen spends
	tx1, err := createAndSendTx(faucetTx.Outputs[:firstSet], 0)
	assert.NoError(t, err, "Failed to create and send first transaction")

	height, _ := helper.GetBlockHeight(url)
	logger.Infof("Block height before mining: %d", height)

	_, err = helper.MineBlockWithRPC(ctx, framework.Nodes[0], logger)
	assert.NoError(t, err, "Failed to mine block")

	rpcEndpoint := "http://" + framework.Nodes[0].RPCURL
	// Verify both transactions are in blocks
	for i, tx := range []*bt.Tx{tx1} {
		bl := false
		targetHeight := height + 1

		for j := 0; j < 5; j++ {
			err := helper.WaitForBlockHeight(url, targetHeight, 60)
			assert.NoError(t, err, "Failed to wait for block height")

			_, err = helper.CallRPC(rpcEndpoint, "generate", []interface{}{101})
			assert.NoError(t, err, "Failed to generate block")

			header, meta, _ := blockchainClient.GetBlockHeadersFromHeight(ctx, targetHeight, 1)
			logger.Infof("Testing on Best block header: %v", header[0].Hash())
			bl, err = helper.CheckIfTxExistsInBlock(ctx, blockStore, framework.Nodes[0].BlockstoreURL, header[0].Hash()[:], meta[0].Height, *tx.TxIDChainHash(), framework.Logger)

			if err != nil {
				logger.Errorf("Error checking if tx exists in block: %v", err)
			}

			if bl {
				break
			}

			targetHeight++
		}

		assert.True(t, bl, "Transaction %d not found in block", i+1)
	}

	utxoBalanceAfter, _, _ := coinbaseClient.GetBalance(ctx)
	logger.Infof("utxoBalanceBefore: %d, utxoBalanceAfter: %d", utxoBalanceBefore, utxoBalanceAfter)
}

func (suite *UtxoTestSuite) TestShouldAllowSaveUTXOsIfExtStoreHasTXs() {
	t := suite.T()
	framework := suite.TeranodeTestEnv
	logger := framework.Logger
	ctx := framework.Context

	framework.StopNode("teranode2")

	txDistributor := &framework.Nodes[0].DistributorClient

	coinbaseClient := framework.Nodes[0].CoinbaseClient
	utxoBalanceBefore, _, _ := coinbaseClient.GetBalance(ctx)
	logger.Infof("utxoBalanceBefore: %d", utxoBalanceBefore)

	privateKey0, err := bec.NewPrivateKey(bec.S256())
	assert.NoError(t, err, "Failed to generate private key")

	address0, err := bscript.NewAddressFromPublicKey(privateKey0.PubKey(), true)
	assert.NoError(t, err, "Failed to create address")

	faucetTx, err := coinbaseClient.RequestFunds(ctx, address0.AddressString, true)
	assert.NoError(t, err, "Failed to request funds")

	logger.Infof("Faucet Transaction: %s %s", faucetTx.TxIDChainHash(), faucetTx.TxID())

	_, err = txDistributor.SendTransaction(ctx, faucetTx)
	assert.NoError(t, err, "Failed to send faucet transaction")

	logger.Infof("Faucet Transaction sent: %s with %d outputs", faucetTx.TxIDChainHash(), len(faucetTx.Outputs))

	time.Sleep(10 * time.Second)

	srcFile := fmt.Sprintf("../../data/test/%s/teranode1/external/%s.tx", framework.TestID, faucetTx.TxID())

	destFile := fmt.Sprintf("../../data/test/%s/teranode2/external/%s.tx", framework.TestID, faucetTx.TxID())

	err = helper.CopyFile(srcFile, destFile)
	if err != nil {
		t.Errorf("Failed to copy file from %s to %s: %v", srcFile, destFile, err)
	}

	framework.StartNode("teranode2")

	err = framework.InitializeTeranodeTestClients()
	if err != nil {
		t.Errorf("Failed to initialize teranode test clients: %v", err)
	}
	time.Sleep(10 * time.Second)

	err = framework.Nodes[1].BlockchainClient.Run(framework.Context, "test")
	if err != nil {
		t.Errorf("Failed to run blockchain client: %v", err)
	}

	// mine a block
	_, err = helper.MineBlockWithRPC(ctx, framework.Nodes[0], logger)
	if err != nil {
		t.Errorf("Failed to mine block: %v", err)
	}

	time.Sleep(30 * time.Second)

	md, err := framework.Nodes[1].UtxoStore.Get(ctx, faucetTx.TxIDChainHash())
	if err != nil {
		t.Errorf("Failed to get UTXO: %v", err)
	}

	assert.NotNil(t, md.Tx.TxID(), "Failed to get UTXO")
}

// TestShouldAllowReassign tests that we can reassign UTXOs
// Request Tx from faucet - output to pubkey of address0
// Create a new transaction (TX0) from the first output vout[0] of the faucet transaction (TXf) - output to pubkey of address1
// Send the transaction
// Mine a block
// Freeze the UTXO of TX0
// Create a dummy transaction (throwawayTx) from the same output vout[0] of the faucet transaction, but this time assign to pubkey of address2
// Create another transaction (reassignTx) from the output vout[0] of the throwawayTx transaction, as if address2 is spending it
// Calculate the UTXO hash for TX0
// Create the Spend object (Spend0) for the UTXO hash of TX0
// Create another Spend object (Spend1) but this time replace the locking script with the public key of address2
// UtxoStore.ReAssignUTXO(ctx, Spend0, Spend1)
// Send the transaction
// Expect that the tx will have to wait till a reassign height of 1000 blocks
func (suite *UtxoTestSuite) TestShouldAllowReassign() {
	t := suite.T()
	testenv := suite.TeranodeTestEnv
	logger := testenv.Logger
	ctx := testenv.Context

	teranode1RPCEndpoint := "http://" + testenv.Nodes[0].RPCURL

	node1 := testenv.Nodes[0]

	utxoBalanceBefore := helper.GetUtxoBalance(ctx, node1)
	logger.Infof("utxoBalanceBefore: %d\n", utxoBalanceBefore)

	// Generate private keys and addresses
	alicePrivateKey, alice, _ := helper.GeneratePrivateKeyAndAddress()
	_, bob, _ := helper.GeneratePrivateKeyAndAddress()
	charlesPrivatekey, charles, _ := helper.GeneratePrivateKeyAndAddress()

	// Request funds and create a transaction
	faucetTx, _ := helper.RequestFunds(ctx, node1, alice.AddressString)
	_, err := helper.SendTransaction(ctx, node1, faucetTx)
	assert.NoError(t, err, "Failed to send transaction")
	logger.Infof("Request funds Transaction sent", faucetTx.TxID())

	// Create and send the first transaction
	faucetUtxo := helper.CreateUtxoFromTransaction(faucetTx, 0)
	aliceToBobTx, err := helper.CreateTransaction(faucetUtxo, bob.AddressString, 10000, alicePrivateKey)
	assert.NoError(t, err, "Failed to create transaction")
	_, err = helper.SendTransaction(ctx, node1, aliceToBobTx)
	assert.NoError(t, err, "Failed to send transaction")
	logger.Infof("Alice sends to Bob output[0] of faucet sent", aliceToBobTx.TxID())

	// Mine a block
	// _, err = helper.MineBlock(ctx, node1.BlockassemblyClient, logger)
	// assert.NoError(t, err, "Failed to mine block")
	_, err = helper.CallRPC(teranode1RPCEndpoint, "generate", []interface{}{1})
	require.NoError(t, err, "Failed to generate blocks")
	time.Sleep(1 * time.Second)

	// Create throwaway and reassignment transactions, do not send these transactions
	// assumed alice would have sent this to charles instead of bob
	// so creating an alice-charles transaction (throwaway tx)
	// and creating another where charles could have spent that tx (reassign tx)
	// the input of the reassign tx has the public key of charles
	throwawayTx, err := helper.CreateTransaction(faucetUtxo, charles.AddressString, 10000, alicePrivateKey)
	assert.NoError(t, err, "Failed to create transaction")

	newUtxo := helper.CreateUtxoFromTransaction(throwawayTx, 0)
	reassignTx, err := helper.CreateTransaction(newUtxo, alice.AddressString, 10000, charlesPrivatekey)
	assert.NoError(t, err, "Failed to create transaction")

	time.Sleep(10 * time.Second)
	// Freeze UTXO of the Alice-Bob transaction
	err = helper.FreezeUtxos(ctx, *testenv, aliceToBobTx, logger, testenv.Nodes[0].Settings)
	assert.NoError(t, err, "Failed to freeze UTXOs")

	// Reassign the UTXO to Charles address
	err = helper.ReassignUtxo(ctx, *testenv, aliceToBobTx, reassignTx, logger, testenv.Nodes[0].Settings)
	assert.NoError(t, err, "Failed to reassign UTXOs")
	logger.Infof("Alice to Bob Transaction reassigned to Charles", reassignTx.TxID())

	_, err = helper.CallRPC(teranode1RPCEndpoint, "generate", []interface{}{100})
	require.NoError(t, err, "Failed to generate blocks")
	time.Sleep(60 * time.Second)

	// Spend the reassigned UTXO
	aliceToCharlesReassignedTxUtxo := helper.CreateUtxoFromTransaction(aliceToBobTx, 0)
	charlestoAliceTx, err := helper.CreateTransaction(aliceToCharlesReassignedTxUtxo, alice.AddressString, 10000, charlesPrivatekey)
	assert.NoError(t, err, "Failed to create transaction")
	_, err = helper.SendTransaction(ctx, node1, charlestoAliceTx)
	assert.Error(t, err, "TX should be rejected since UTXO is not spendable until block 1000")

	// mine more 1000 blocks
	url := "http://" + testenv.Nodes[0].AssetURL

	_, err = helper.CallRPC(teranode1RPCEndpoint, "generate", []interface{}{1000})
	require.NoError(t, err, "Failed to generate blocks")

	err = helper.WaitForBlockHeight(url, 1000, 60*time.Second)
	assert.NoError(t, err, "Failed to generate blocks")

	_, err = helper.SendTransaction(ctx, node1, charlestoAliceTx)
	assert.NoError(t, err, "Failed to send transaction")
}

func TestUtxoTestSuite(t *testing.T) {
	suite.Run(t, new(UtxoTestSuite))
}
