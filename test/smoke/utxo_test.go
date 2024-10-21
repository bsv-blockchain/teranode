//go:build utxo

// How to run each test:
// Clean up docker containers before running the test manually
// $ cd test/smoke/
// $ go test -v -run "^TestUtxoTestSuite$/TestShouldAllowToSpendUtxos$" -tags utxo
// $ go test -v -run "^TestUtxoTestSuite$/TestShouldAllowSpendAllUtxos$" -tags utxo
// $ go test -v -run "^TestUtxoTestSuite$/TestDeleteParentTx$" -tags utxo
// $ go test -v -run "^TestUtxoTestSuite$/TestFreezeAndUnfreezeUtxos$" -tags utxo
// $ go test -v -run "^TestUtxoTestSuite$/TestShouldAllowToSpendUtxosAfterReassignment$" -tags utxo
// $ go test -v -run "^TestUtxoTestSuite$/TestShouldAllowSaveUTXOsIfExtStoreHasTXs$" -tags utxo
package test

import (
	"fmt"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	arrange "github.com/bitcoin-sv/ubsv/test/fixtures"
	helper "github.com/bitcoin-sv/ubsv/test/utils"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/bitcoin-sv/ubsv/util/distributor"
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
	arrange.TeranodeTestSuite
}

func (suite *UtxoTestSuite) TearDownTest() {
}

const url = "http://localhost:10090"

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

	baClient := framework.Nodes[0].BlockassemblyClient
	_, err = helper.MineBlock(ctx, baClient, logger)

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

	for i := 0; i < 30; i++ {
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
		_, err = helper.MineBlock(ctx, baClient, logger)

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

	baClient := framework.Nodes[0].BlockassemblyClient
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

	_, err = helper.MineBlock(ctx, baClient, logger)
	assert.NoError(t, err, "Failed to mine block")

	// Verify both transactions are in blocks
	blTx1 := false
	blTx2 := false
	targetHeight := height + 1

	for j := 0; j < 30; j++ {
		err := helper.WaitForBlockHeight(url, targetHeight, 60)
		assert.NoError(t, err, "Failed to wait for block height")

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
		_, err = helper.MineBlock(ctx, baClient, logger)
		assert.NoError(t, err, "Failed to mine block")
	}

	assert.True(t, blTx1, "Transaction %d not found in block", tx1.TxIDChainHash())
	assert.True(t, blTx2, "Transaction %d not found in block", tx2.TxIDChainHash())

	utxoBalanceAfter, _, _ := coinbaseClient.GetBalance(ctx)
	logger.Infof("utxoBalanceBefore: %d, utxoBalanceAfter: %d", utxoBalanceBefore, utxoBalanceAfter)
}

// nolint: gocognit
func (suite *UtxoTestSuite) TestDeleteParentTx() {
	t := suite.T()
	framework := suite.TeranodeTestEnv
	ctx := framework.Context
	logger := framework.Logger

	url := "http://localhost:10090"

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

	baClient := framework.Nodes[0].BlockassemblyClient
	_, err = helper.MineBlock(ctx, baClient, logger)

	if err != nil {
		t.Errorf("Failed to mine block: %v", err)
	}

	blockStore := framework.Nodes[0].Blockstore
	blockchainClient := framework.Nodes[0].BlockchainClient
	bl := false
	targetHeight := height + 1

	for i := 0; i < 30; i++ {
		err := helper.WaitForBlockHeight(url, targetHeight, 60)
		if err != nil {
			t.Errorf("Failed to wait for block height: %v", err)
		}

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
		_, err = helper.MineBlock(ctx, baClient, logger)

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

	settingsMap["SETTINGS_CONTEXT_1"] = "docker.ubsv1.test.TestFreezeAndUnfreezeUtxos"
	if err := framework.RestartDockerNodes(settingsMap); err != nil {
		t.Errorf("Failed to restart nodes: %v", err)
	}

	err := framework.InitializeTeranodeTestClients()
	require.NoError(t, err, "Failed to initialize Teranode test clients")

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

	baClient := framework.Nodes[0].BlockassemblyClient
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

	// get spends for the first set of outputs
	spends := getSpends(faucetTx.Outputs[:firstSet], 0)
	err = framework.Nodes[0].UtxoStore.FreezeUTXOs(ctx, spends)
	assert.NoError(t, err, "Failed to freeze UTXOs")
	// Create and send transaction with the frozen spends
	_, err = createAndSendTx(faucetTx.Outputs[:firstSet], 0)
	assert.Error(t, err, "Should not allow to spend frozen UTXOs")
	// Unfreeze the UTXOs
	err = framework.Nodes[0].UtxoStore.UnFreezeUTXOs(ctx, spends)
	assert.NoError(t, err, "Failed to unfreeze UTXOs")
	// Create and send transaction with the unfrozen spends
	tx1, err := createAndSendTx(faucetTx.Outputs[:firstSet], 0)
	assert.NoError(t, err, "Failed to create and send first transaction")

	height, _ := helper.GetBlockHeight(url)
	logger.Infof("Block height before mining: %d", height)

	_, err = helper.MineBlock(ctx, baClient, logger)
	assert.NoError(t, err, "Failed to mine block")

	// Verify both transactions are in blocks
	for i, tx := range []*bt.Tx{tx1} {
		bl := false
		targetHeight := height + 1

		for j := 0; j < 30; j++ {
			err := helper.WaitForBlockHeight(url, targetHeight, 60)
			assert.NoError(t, err, "Failed to wait for block height")

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
			_, err = helper.MineBlock(ctx, baClient, logger)
			assert.NoError(t, err, "Failed to mine block")
		}

		assert.True(t, bl, "Transaction %d not found in block", i+1)
	}

	utxoBalanceAfter, _, _ := coinbaseClient.GetBalance(ctx)
	logger.Infof("utxoBalanceBefore: %d, utxoBalanceAfter: %d", utxoBalanceBefore, utxoBalanceAfter)
}

// TestShouldAllowToSpendUtxosAfterReassignment tests that we can spend UTXOs after reassignment
// Request TXf from faucet
// Create a new transaction (TX0) from the first 5 outputs vout[0..5] of the faucet transaction (TXf) - output to pubkey of address0
// Create the spend object (Spend0) for the UTXO hash of TX0
// Send the transaction
// Mine a block
// Verify the transaction (TX0) is in the block
// Freeze the UTXO of TX0
// Create a new transaction (TX1) from the same outputs vout[0..5] of the faucet transaction, but this time assign to pubkey of address1
// Calculate the UTXO hash for TX1
// Create the Spend object (Spend1) for the UTXO hash of TX1
// UtxoStore.ReAssignUTXO(ctx, Spend0, Spend1)

func (suite *UtxoTestSuite) TestShouldAllowToSpendUtxosAfterReassignment() {
	t := suite.T()
	framework := suite.TeranodeTestEnv
	settingsMap := suite.SettingsMap
	logger := framework.Logger
	ctx := framework.Context

	settingsMap["SETTINGS_CONTEXT_1"] = "docker.ubsv1.TestFreezeAndUnfreezeUtxos"
	if err := framework.RestartDockerNodes(settingsMap); err != nil {
		t.Errorf("Failed to restart nodes: %v", err)
	}

	txDistributor, _ := distributor.NewDistributor(ctx, logger,
		distributor.WithBackoffDuration(200*time.Millisecond),
		distributor.WithRetryAttempts(3),
		distributor.WithFailureTolerance(0),
	)

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
	oldUtxo := &bt.UTXO{
		TxIDHash:      faucetTx.TxIDChainHash(),
		Vout:          uint32(0),
		LockingScript: output.LockingScript,
		Satoshis:      output.Satoshis,
	}

	firstTx := bt.NewTx()

	err = firstTx.FromUTXOs(oldUtxo)
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

	baClient := framework.Nodes[0].BlockassemblyClient
	_, err = helper.MineBlock(ctx, baClient, logger)

	if err != nil {
		t.Errorf("Failed to mine block: %v", err)
	}

	// Create a reassign transaction from the same output of the faucet transaction
	output = faucetTx.Outputs[0]
	newUtxo := &bt.UTXO{
		TxIDHash:      faucetTx.TxIDChainHash(),
		Vout:          uint32(1),
		LockingScript: output.LockingScript,
		Satoshis:      output.Satoshis,
	}

	reassignTx := bt.NewTx()

	err = reassignTx.FromUTXOs(newUtxo)
	if err != nil {
		t.Errorf("Error adding UTXO to transaction: %s\n", err)
	}

	err = reassignTx.AddP2PKHOutputFromAddress(address1.AddressString, 10000)
	if err != nil {
		t.Errorf("Error adding output to transaction: %v", err)
	}

	err = reassignTx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privateKey0})
	if err != nil {
		t.Errorf("Error filling transaction inputs: %v", err)
	}

	// Freeze the UTXO of the first transaction
	oldUtxoHash, _ := util.UTXOHashFromOutput(firstTx.TxIDChainHash(), firstTx.Outputs[0], 0)
	spend := &utxo.Spend{
		TxID:     firstTx.TxIDChainHash(),
		Vout:     0,
		UTXOHash: oldUtxoHash,
	}
	err = framework.Nodes[0].UtxoStore.FreezeUTXOs(ctx, []*utxo.Spend{spend})
	require.NoError(t, err, "Failed to freeze UTXOs")

	newUtxoHash, _ := util.UTXOHashFromOutput(reassignTx.TxIDChainHash(), reassignTx.Outputs[0], 0)
	// nolint: gosec
	newSpend := &utxo.Spend{
		TxID:     reassignTx.TxIDChainHash(),
		Vout:     0,
		UTXOHash: newUtxoHash,
	}

	err = framework.Nodes[0].UtxoStore.ReAssignUTXO(ctx, spend, newSpend)
	require.NoError(t, err, "Failed to reassign UTXOs")

	// try to unfreeze the old utxo
	err = framework.Nodes[0].UtxoStore.UnFreezeUTXOs(ctx, []*utxo.Spend{spend})
	require.Error(t, err, "Should not allow to unfreeze UTXOs as it has been reassigned")

	// send the reassign transaction
	_, err = txDistributor.SendTransaction(ctx, reassignTx)
	require.NoError(t, err, "Failed to send reassign transaction")

	logger.Infof("Reassignment Transaction created with output[0] of faucet sent: %s %s\n", reassignTx.TxIDChainHash(), reassignTx.TxID())

	height, _ = helper.GetBlockHeight(url)
	logger.Infof("Block height before mining: %d\n", height)

	_, err = helper.MineBlock(ctx, baClient, logger)

	if err != nil {
		t.Errorf("Failed to mine block: %v", err)
	}

	blockStore := framework.Nodes[0].Blockstore
	blockchainClient := framework.Nodes[0].BlockchainClient
	bl := false
	targetHeight := height + 1

	for i := 0; i < 30; i++ {
		err := helper.WaitForBlockHeight(url, targetHeight, 60)
		if err != nil {
			t.Errorf("Failed to wait for block height: %v", err)
		}

		header, meta, _ := blockchainClient.GetBlockHeadersFromHeight(ctx, targetHeight, 1)
		logger.Infof("Testing on Best block header: %v", header[0].Hash())
		bl, err = helper.CheckIfTxExistsInBlock(ctx, blockStore, framework.Nodes[0].BlockstoreURL, header[0].Hash()[:], meta[0].Height, *reassignTx.TxIDChainHash(), framework.Logger)

		if err != nil {
			t.Errorf("error checking if tx exists in block: %v", err)
		}

		if bl {
			break
		}

		targetHeight++
		_, err = helper.MineBlock(ctx, baClient, logger)

		if err != nil {
			t.Errorf("Failed to mine block: %v", err)
		}
	}

	assert.Equal(t, true, bl, "Test Tx not found in block")
}

func (suite *UtxoTestSuite) TestShouldAllowSaveUTXOsIfExtStoreHasTXs() {
	t := suite.T()
	framework := suite.TeranodeTestEnv
	logger := framework.Logger
	ctx := framework.Context

	framework.StopNode("ubsv2")

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

	srcFile := fmt.Sprintf("../../data/test/ubsv1/external/%s.tx", faucetTx.TxID())

	destFile := fmt.Sprintf("../../data/test/ubsv2/external/%s.tx", faucetTx.TxID())

	err = helper.CopyFile(srcFile, destFile)
	if err != nil {
		t.Errorf("Failed to copy file from %s to %s: %v", srcFile, destFile, err)
	}

	framework.StartNode("ubsv2")
	time.Sleep(10 * time.Second)

	err = framework.Nodes[1].BlockchainClient.Run(framework.Context)
	if err != nil {
		t.Errorf("Failed to run blockchain client: %v", err)
	}

	time.Sleep(30 * time.Second)

	md, err := framework.Nodes[1].UtxoStore.Get(ctx, faucetTx.TxIDChainHash())
	if err != nil {
		t.Errorf("Failed to get UTXO: %v", err)
	}
	assert.NotNil(t, md.Tx.TxID(), "Failed to get UTXO")
	t.Logf("UTXO: %s %s\n", md.Tx.TxIDChainHash(), md.Tx.TxID())
}

func TestUtxoTestSuite(t *testing.T) {
	suite.Run(t, new(UtxoTestSuite))
}
