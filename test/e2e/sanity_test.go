////go:build e2eTest

// How to run this test:
// $ unzip data.zip
// $ cd test/functional/
// $ `SETTINGS_CONTEXT=docker.ci.tc1.run go test -run TestShouldAllowFairTx`

package test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	blockchain_store "github.com/bitcoin-sv/ubsv/stores/blockchain"
	tf "github.com/bitcoin-sv/ubsv/test/test_framework"
	helper "github.com/bitcoin-sv/ubsv/test/utils"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util/distributor"
	"github.com/libsv/go-bk/bec"
	"github.com/libsv/go-bk/wif"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/libsv/go-bt/v2/unlocker"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/assert"
)

var (
	framework *tf.BitcoinTestFramework
)

func TestMain(m *testing.M) {
	setupBitcoinTestFramework()
	defer tearDownBitcoinTestFramework()

	m.Run()

	// os.Exit(exitCode)
}

func setupBitcoinTestFramework() {
	framework = tf.NewBitcoinTestFramework([]string{"../../docker-compose.yml", "../../docker-compose.aerospike.override.yml", "../../docker-compose.e2etest.override.yml"})
	m := map[string]string{
		"SETTINGS_CONTEXT_1": "docker.ci.ubsv1.tc1",
		"SETTINGS_CONTEXT_2": "docker.ci.ubsv2.tc1",
		"SETTINGS_CONTEXT_3": "docker.ci.ubsv3.tc1",
	}
	if err := framework.SetupNodes(m); err != nil {
		fmt.Printf("Error setting up nodes: %v\n", err)
		os.Exit(1)
	}
}

func tearDownBitcoinTestFramework() {
	if err := framework.StopNodes(); err != nil {
		fmt.Printf("Error stopping nodes: %v\n", err)
	}
	_ = os.RemoveAll("../../data")
}

func TestShouldAllowFairTx(t *testing.T) {
	ctx := context.Background()
	url := "http://localhost:18090"

	var logLevelStr, _ = gocore.Config().Get("logLevel", "INFO")
	logger := ulogger.New("test", ulogger.WithLevel(logLevelStr))

	txDistributor, _ := distributor.NewDistributor(ctx, logger,
		distributor.WithBackoffDuration(200*time.Millisecond),
		distributor.WithRetryAttempts(3),
		distributor.WithFailureTolerance(0),
	)

	coinbaseClient := framework.Nodes[0].CoinbaseClient
	utxoBalanceBefore, _, _ := coinbaseClient.GetBalance(ctx)
	fmt.Printf("utxoBalanceBefore: %d\n", utxoBalanceBefore)

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
	fmt.Printf("Transaction: %s %s\n", tx.TxIDChainHash(), tx.TxID())

	_, err = txDistributor.SendTransaction(ctx, tx)
	if err != nil {
		t.Errorf("Failed to send transaction: %v", err)
	}

	fmt.Printf("Transaction sent: %s %v\n", tx.TxIDChainHash(), len(tx.Outputs))
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

	fmt.Printf("Transaction sent: %s %s\n", newTx.TxIDChainHash(), newTx.TxID())
	time.Sleep(10 * time.Second)

	height, _ := helper.GetBlockHeight(url)
	fmt.Printf("Block height: %d\n", height)

	utxoBalanceAfter, _, _ := coinbaseClient.GetBalance(ctx)
	fmt.Printf("utxoBalanceBefore: %d, utxoBalanceAfter: %d\n", utxoBalanceBefore, utxoBalanceAfter)
	// assert.Less(t, utxoBalanceAfter, utxoBalanceBefore)

	baClient := framework.Nodes[0].BlockassemblyClient
	blockHash, err := helper.MineBlock(ctx, baClient, logger)
	if err != nil {
		t.Errorf("Failed to mine block: %v", err)
	}

	fmt.Printf("Block height: %d\n", height)
	var newHeight int
	for i := 0; i < 30; i++ {
		newHeight, _ = helper.GetBlockHeight(url)
		if newHeight > height {
			break
		}
		time.Sleep(1 * time.Second)
	}
	if newHeight <= height {
		t.Errorf("Block height did not increase after mining block")
	}

	blockchainStoreURL, _, found := gocore.Config().GetURL("blockchain_store.docker.ci.chainintegrity.ubsv1")
	blockchainDB, err := blockchain_store.NewStore(logger, blockchainStoreURL)
	logger.Debugf("blockchainStoreURL: %v", blockchainStoreURL)
	if err != nil {
		panic(err.Error())
	}
	if !found {
		panic("no blockchain_store setting found")
	}
	b, _, _ := blockchainDB.GetBlock(ctx, (*chainhash.Hash)(blockHash))
	fmt.Printf("Block: %v\n", b)

	blockStore := framework.Nodes[0].Blockstore
	var o []options.Options
	o = append(o, options.WithFileExtension("block"))
	//wait
	time.Sleep(10 * time.Second)
	blockchainClient := framework.Nodes[0].BlockchainClient
	header, meta, _ := blockchainClient.GetBestBlockHeader(ctx)
	fmt.Printf("Best block header: %v\n", header.Hash())

	r, err := blockStore.GetIoReader(ctx, header.Hash()[:], o...)
	// t.Errorf("error getting block reader: %v", err)
	if err != nil {
		t.Errorf("error getting block reader: %v", err)
	}
	if err == nil {
		if bl, err := helper.ReadFile(ctx, "block", logger, r, *newTx.TxIDChainHash(), framework.Nodes[0].BlockstoreUrl); err != nil {
			t.Errorf("error reading block: %v", err)
		} else {
			fmt.Printf("Block at height (%d): was tested for the test Tx\n", meta.Height)
			assert.Equal(t, true, bl, "Test Tx not found in block")
		}
	}

}

func TestTxsReceivedAllNodes(t *testing.T) {
	ctx := context.Background()

	var logLevelStr, _ = gocore.Config().Get("logLevel", "INFO")
	logger := ulogger.New("test", ulogger.WithLevel(logLevelStr))

	// Send transactions

	txDistributor, _ := distributor.NewDistributor(ctx, logger,
		distributor.WithBackoffDuration(200*time.Millisecond),
		distributor.WithRetryAttempts(3),
		distributor.WithFailureTolerance(0),
	)

	coinbaseClient := framework.Nodes[0].CoinbaseClient
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
	fmt.Printf("Transaction: %s %s\n", tx.TxIDChainHash(), tx.TxID())

	_, err = txDistributor.SendTransaction(ctx, tx)
	if err != nil {
		t.Errorf("Failed to send transaction: %v", err)
	}

	fmt.Printf("Transaction sent: %s %v\n", tx.TxIDChainHash(), len(tx.Outputs))

	utxo := &bt.UTXO{
		TxIDHash:      tx.TxIDChainHash(),
		Vout:          uint32(0),
		LockingScript: tx.Outputs[0].LockingScript,
		Satoshis:      tx.Outputs[0].Satoshis,
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

	fmt.Printf("Transaction sent: %s %s\n", newTx.TxIDChainHash(), newTx.TxID())
	time.Sleep(5 * time.Second)

	// If Block Assembly removes correctly the Tx, it means that Tx exists

	errTx0 := framework.Nodes[0].BlockassemblyClient.RemoveTx(ctx, newTx.TxIDChainHash())

	if errTx0 != nil {
		t.Errorf("Error Tx0 not present: %v", errTx0)
	}

	errTx1 := framework.Nodes[1].BlockassemblyClient.RemoveTx(ctx, newTx.TxIDChainHash())

	if errTx1 != nil {
		t.Errorf("Error Tx1 not present: %v", errTx1)
	}

	errTx2 := framework.Nodes[2].BlockassemblyClient.RemoveTx(ctx, newTx.TxIDChainHash())

	if errTx2 != nil {
		t.Errorf("Error Tx2 not present: %v", errTx2)
	}
}

func TestSameTxsMultipleBlocks(t *testing.T) {
	ctx := context.Background()

	var logLevelStr, _ = gocore.Config().Get("logLevel", "INFO")
	logger := ulogger.New("test", ulogger.WithLevel(logLevelStr))

	// Send transactions

	txDistributor, _ := distributor.NewDistributor(ctx, logger,
		distributor.WithBackoffDuration(200*time.Millisecond),
		distributor.WithRetryAttempts(3),
		distributor.WithFailureTolerance(0),
	)

	coinbaseClient := framework.Nodes[0].CoinbaseClient
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
	fmt.Printf("Transaction: %s %s\n", tx.TxIDChainHash(), tx.TxID())

	_, err = txDistributor.SendTransaction(ctx, tx)
	if err != nil {
		t.Errorf("Failed to send transaction: %v", err)
	}

	fmt.Printf("Transaction sent: %s %v\n", tx.TxIDChainHash(), len(tx.Outputs))

	utxo := &bt.UTXO{
		TxIDHash:      tx.TxIDChainHash(),
		Vout:          uint32(0),
		LockingScript: tx.Outputs[0].LockingScript,
		Satoshis:      tx.Outputs[0].Satoshis,
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

	fmt.Printf("Transaction sent: %s %s\n", newTx.TxIDChainHash(), newTx.TxID())
	time.Sleep(5 * time.Second)

	// Mine block on each node and check if the block exists

	for i := 0; i < 3; i++ {
		baClient := framework.Nodes[i].BlockassemblyClient
		blockHash, err := helper.MineBlock(ctx, baClient, logger)
		if err != nil {
			t.Errorf("Failed to mine block: %v", err)
		}

		time.Sleep(5 * time.Second)

		blockNode, blockErr := framework.Nodes[i].BlockchainClient.GetBlockExists(ctx, (*chainhash.Hash)(blockHash))

		if blockErr != nil {
			t.Errorf("Failure on blockchain on Node0: %v", err)
		}

		if !blockNode {
			t.Errorf("Failed to retrieve new mined block on Node0: %v", blockErr)
		}
	}
}

func TestMultipleMining(t *testing.T) {
	// Test setup
	ctx := context.Background()
	var logLevelStr, _ = gocore.Config().Get("logLevel", "INFO")
	var logger = ulogger.New("testRun", ulogger.WithLevel(logLevelStr))

	hashes0, err := helper.CreateAndSendRawTxs(ctx, framework.Nodes[0], 10)
	if err != nil {
		t.Errorf("Failed to create and send raw txs: %v", err)
	}
	fmt.Printf("Hashes in created block: %v\n", hashes0)

	baClient := framework.Nodes[0].BlockassemblyClient
	var block0, blockerr0 = helper.MineBlock(ctx, baClient, logger)
	if blockerr0 != nil {
		t.Errorf("Failed to mine block: %v", err)
	}

	hashes1, err := helper.CreateAndSendRawTxs(ctx, framework.Nodes[1], 10)
	if err != nil {
		t.Errorf("Failed to create and send raw txs: %v", err)
	}
	fmt.Printf("Hashes in created block: %v\n", hashes1)

	baClient1 := framework.Nodes[1].BlockassemblyClient
	var block1, blockerr1 = helper.MineBlock(ctx, baClient1, logger)
	if blockerr1 != nil {
		t.Errorf("Failed to mine block: %v", err)
	}

	hashes2, err := helper.CreateAndSendRawTxs(ctx, framework.Nodes[2], 10)
	if err != nil {
		t.Errorf("Failed to create and send raw txs: %v", err)
	}
	fmt.Printf("Hashes in created block: %v\n", hashes2)

	baClient2 := framework.Nodes[2].BlockassemblyClient
	var block2, blockerr2 = helper.MineBlock(ctx, baClient2, logger)
	if blockerr2 != nil {
		t.Errorf("Failed to mine block: %v", err)
	}

	blockNode0, blockErr0 := framework.Nodes[0].BlockchainClient.GetBlockExists(ctx, (*chainhash.Hash)(block0))
	blockNode1, blockErr1 := framework.Nodes[1].BlockChainDB.GetBlockExists(ctx, (*chainhash.Hash)(block1))
	blockNode2, blockErr2 := framework.Nodes[2].BlockchainClient.GetBlockExists(ctx, (*chainhash.Hash)(block2))

	if blockErr0 != nil {
		t.Errorf("Failure on blockchain on Node0: %v", err)
	}

	if blockErr1 != nil {
		t.Errorf("Failure on blockchain on Node1: %v", err)
	}

	if blockErr2 != nil {
		t.Errorf("Failure on blockchain on Node2: %v", err)
	}

	if !blockNode0 {
		t.Errorf("Failed to retrieve new mined block on Node0: %v", blockErr0)
	}
	if !blockNode1 {
		t.Errorf("Failed to retrieve new mined block on Node1: %v", blockErr1)
	}
	if !blockNode2 {
		t.Errorf("Failed to retrieve new mined block on Node2: %v", blockErr2)
	}
}

func TestBroadcastPoW(t *testing.T) {
	// Test setup
	ctx := context.Background()
	var logLevelStr, _ = gocore.Config().Get("logLevel", "INFO")
	var logger = ulogger.New("testRun", ulogger.WithLevel(logLevelStr))

	hashes, err := helper.CreateAndSendRawTxs(ctx, framework.Nodes[0], 10)
	if err != nil {
		t.Errorf("Failed to create and send raw txs: %v", err)
	}
	fmt.Printf("Hashes in created block: %v\n", hashes)

	baClient := framework.Nodes[0].BlockassemblyClient
	var block, blockerr = helper.MineBlock(ctx, baClient, logger)
	if blockerr != nil {
		t.Errorf("Failed to mine block: %v", err)
	}

	time.Sleep(10 * time.Second)

	blockNode0, blockErr0 := framework.Nodes[0].BlockchainClient.GetBlockExists(ctx, (*chainhash.Hash)(block))

	blockNode1, blockErr1 := framework.Nodes[1].BlockChainDB.GetBlockExists(ctx, (*chainhash.Hash)(block))

	blockNode2, blockErr2 := framework.Nodes[2].BlockchainClient.GetBlockExists(ctx, (*chainhash.Hash)(block))

	if blockErr0 != nil {
		t.Errorf("Failure on blockchain on Node0: %v", err)
	}

	if blockErr1 != nil {
		t.Errorf("Failure on blockchain on Node1: %v", err)
	}

	if blockErr2 != nil {
		t.Errorf("Failure on blockchain on Node2: %v", err)
	}

	if !blockNode0 {
		t.Errorf("Failed to retrieve new mined block on Node0: %v", blockErr0)
	}
	if !blockNode1 {
		t.Errorf("Failed to retrieve new mined block on Node1: %v", blockErr1)
	}
	if !blockNode2 {
		t.Errorf("Failed to retrieve new mined block on Node2: %v", blockErr2)
	}
}

func TestShouldNotAllowDoubleSpend(t *testing.T) {
	ctx := context.Background()
	url := "http://localhost:18090"

	var logLevelStr, _ = gocore.Config().Get("logLevel", "INFO")
	logger := ulogger.New("testRun", ulogger.WithLevel(logLevelStr))

	txDistributor, _ := distributor.NewDistributor(ctx, logger,
		distributor.WithBackoffDuration(200*time.Millisecond),
		distributor.WithRetryAttempts(3),
		distributor.WithFailureTolerance(0),
	)

	txNode1Distributor, _ := distributor.NewDistributorFromAddress(ctx, logger, "localhost:18084",
		distributor.WithBackoffDuration(200*time.Millisecond),
		distributor.WithRetryAttempts(3),
		distributor.WithFailureTolerance(0),
	)

	txNode2Distributor, _ := distributor.NewDistributorFromAddress(ctx, logger, "localhost:28084",
		distributor.WithBackoffDuration(200*time.Millisecond),
		distributor.WithRetryAttempts(3),
		distributor.WithFailureTolerance(0),
	)

	coinbaseClient := framework.Nodes[0].CoinbaseClient

	utxoBalanceBefore, _, _ := coinbaseClient.GetBalance(ctx)
	fmt.Printf("utxoBalanceBefore: %d\n", utxoBalanceBefore)

	coinbasePrivKey, _ := gocore.Config().Get("coinbase_wallet_private_key")
	coinbasePrivateKey, err := wif.DecodeWIF(coinbasePrivKey)
	if err != nil {
		t.Errorf("Failed to decode Coinbase private key: %v", err)
	}

	coinbaseAddr, _ := bscript.NewAddressFromPublicKey(coinbasePrivateKey.PrivKey.PubKey(), true)
	coinbaseAddrDouble, _ := bscript.NewAddressFromPublicKey(coinbasePrivateKey.PrivKey.PubKey(), true)

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

	_, err = txDistributor.SendTransaction(ctx, tx)
	if err != nil {
		t.Errorf("Failed to send transaction: %v", err)
	}

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
	newTx.LockTime = 0

	newTxDouble := bt.NewTx()
	err = newTxDouble.FromUTXOs(utxo)
	if err != nil {
		t.Errorf("Error adding UTXO to transaction: %s\n", err)
	}

	newTxDouble.LockTime = 1

	err = newTx.AddP2PKHOutputFromAddress(coinbaseAddr.AddressString, 10000)
	if err != nil {
		t.Errorf("Error adding output to transaction: %v", err)
	}

	err = newTxDouble.AddP2PKHOutputFromAddress(coinbaseAddrDouble.AddressString, 10000)
	if err != nil {
		t.Errorf("Error adding output to transaction: %v", err)
	}

	err = newTx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privateKey})
	if err != nil {
		t.Errorf("Error filling transaction inputs: %v", err)
	}

	err = newTxDouble.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privateKey})
	if err != nil {
		t.Errorf("Error filling transaction inputs: %v", err)
	}

	_, err = txNode1Distributor.SendTransaction(ctx, newTx)
	if err != nil {
		t.Errorf("Failed to send new transaction: %v", err)
	}

	_, err = txNode2Distributor.SendTransaction(ctx, newTxDouble)
	if err != nil {
		t.Errorf("Failed to send new transaction: %v", err)
	}

	fmt.Printf("Transaction sent: %s %s\n", newTx.TxIDChainHash(), newTx.TxID())
	time.Sleep(5 * time.Second)

	fmt.Printf("Double spend Transaction sent: %s %s\n", newTxDouble.TxIDChainHash(), newTxDouble.TxID())
	time.Sleep(5 * time.Second)

	height, _ := helper.GetBlockHeight(url)
	fmt.Printf("Block height: %d\n", height)

	utxoBalanceAfter, _, _ := coinbaseClient.GetBalance(ctx)
	fmt.Printf("utxoBalanceBefore: %d, utxoBalanceAfter: %d\n", utxoBalanceBefore, utxoBalanceAfter)
	// assert.Less(t, utxoBalanceAfter, utxoBalanceBefore)

	baClientNode1 := framework.Nodes[0].BlockassemblyClient
	miningCandidate, err := baClientNode1.GetMiningCandidate(ctx)
	if err != nil {
		t.Errorf("Failed to get mining candidate: %v", err)
	}
	fmt.Printf("Mining candidate: %d\n", miningCandidate.SubtreeCount)

	baClientNode2 := framework.Nodes[1].BlockassemblyClient
	miningCandidate2, err := baClientNode2.GetMiningCandidate(ctx)
	if err != nil {
		t.Errorf("Failed to get mining candidate: %v", err)
	}
	fmt.Printf("Mining candidate: %d\n", miningCandidate2.SubtreeCount)

	blockHash, err := helper.MineBlock(ctx, baClientNode1, logger)
	if err != nil {
		t.Errorf("Failed to mine block: %v with error: %v", err, blockHash)
	}

	blockHashNode2, err := helper.MineBlock(ctx, baClientNode2, logger)
	if err != nil {
		t.Errorf("error getting block: %v", blockHashNode2)
	}

	time.Sleep(30 * time.Second)

	fmt.Printf("Block height: %d\n", height)
	var newHeight int
	for i := 0; i < 30; i++ {
		newHeight, _ = helper.GetBlockHeight(url)
		if newHeight > height {
			break
		}
		time.Sleep(1 * time.Second)
	}
	if newHeight <= height {
		t.Errorf("Block height did not increase after mining block")
	}

	blockStore := framework.Nodes[0].Blockstore
	var o []options.Options
	o = append(o, options.WithFileExtension("block"))

	blockchainC, _ := blockchain.NewClient(ctx, logger)
	header, meta, _ := blockchainC.GetBestBlockHeader(ctx)

	r, err := blockStore.GetIoReader(ctx, header.Hash()[:], o...)
	if err == nil {
		if _, err := helper.ReadFile(ctx, "block", logger, r, *newTx.TxIDChainHash(), framework.Nodes[0].BlockstoreUrl); err != nil {
			t.Errorf("error reading block: %v", err)
		} else {
			fmt.Printf("Block at height (%d): was tested for the test Tx\n", meta.Height)
			// assert.Equal(t, true, bl, "Test Tx not found in block")
		}
	}
	//TODO: Add test to check if the double spend transaction is not in the block
}
