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

func getFutureLockTime(daysAhead int) uint32 {
    // Get the current time
    now := time.Now()
    
    // Add the number of seconds in `daysAhead` days
    futureTime := now.Add(time.Duration(daysAhead*24) * time.Hour)
    
    // Return the Unix timestamp for the future time
    return uint32(futureTime.Unix())
}

func TestMain(m *testing.M) {
	setupBitcoinTestFramework()
	defer tearDownBitcoinTestFramework()

	m.Run()

	// os.Exit(exitCode)
}

func setupBitcoinTestFramework() {
	framework = tf.NewBitcoinTestFramework([]string{"../../docker-compose.yml", "../../docker-compose.aerospike.override.yml", "../../docker-compose.e2etest.override.yml"})
	// framework = tf.NewBitcoinTestFramework([]string{"../../docker-compose.yml", "../../docker-compose.e2etest.override.yml"})
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
	if err := framework.StopNodesWithRmVolume(); err != nil {
		fmt.Printf("Error stopping nodes: %v\n", err)
	}
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

func TestShouldAllowFairTx_UseRpc(t *testing.T) {
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
	miningCandidate, err := baClient.GetMiningCandidate(ctx)
	if err != nil {
		t.Errorf("Failed to get mining candidate: %v", err)
	}

	framework.Logger.Infof("RPC Url: %d\n", framework.Nodes[0].RPC_URL)
	blockHash, err := helper.MineBlockWithCandidate_rpc(ctx, framework.Nodes[0].RPC_URL, miningCandidate, logger)
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

func TestLocktimeFutureHeight(t *testing.T) {
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

	err = newTx.AddP2PKHOutputFromAddress(coinbaseAddr.AddressString, 10000)
	if err != nil {
		t.Errorf("Error adding output to transaction: %v", err)
	}

	newTx.LockTime = 350
	newTx.LockTime = getFutureLockTime(10)
	err = newTx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privateKey})
	if err != nil {
		t.Errorf("Error filling transaction inputs: %v", err)
	}

	_, err = txDistributor.SendTransaction(ctx, newTx)
	// require.Error(t, err)
	if err != nil {
		t.Errorf("Failed to send new transaction: %v", err)
	}

	fmt.Printf("Transaction sent: %s %s\n", newTx.TxIDChainHash(), newTx.TxID())
	time.Sleep(10 * time.Second)

	height, _ := helper.GetBlockHeight(url)

	utxoBalanceAfter, _, _ := coinbaseClient.GetBalance(ctx)
	logger.Infof("utxoBalanceBefore: %d, utxoBalanceAfter: %d\n", utxoBalanceBefore, utxoBalanceAfter)

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
	logger.Infof("Block: %v\n", b)

	blockStore := framework.Nodes[0].Blockstore
	var o []options.Options
	o = append(o, options.WithFileExtension("block"))
	//wait
	time.Sleep(10 * time.Second)
	blockchainClient := framework.Nodes[0].BlockchainClient
	header, meta, _ := blockchainClient.GetBestBlockHeader(ctx)
	logger.Infof("Best block header: %v\n", header.Hash())

	r, err := blockStore.GetIoReader(ctx, header.Hash()[:], o...)
	if err != nil {
		t.Errorf("error getting block reader: %v", err)
	}
	if err == nil {
		if bl, err := helper.ReadFile(ctx, "block", logger, r, *newTx.TxIDChainHash(), framework.Nodes[0].BlockstoreUrl); err != nil {
			t.Errorf("error reading block: %v", err)
		} else {
			fmt.Printf("Block at height (%d): was tested for the test Tx %v\n", meta.Height, newTx)
			logger.Infof("Block at height (%d): was tested for the test Tx %v\n", meta.Height, newTx)
			logger.Infof("Tx locktime: %d\n", newTx.LockTime)
			logger.Infof("Tx hash: %s\n", newTx.TxIDChainHash())
			logger.Infof("Tx inputs: %v\n", newTx.Inputs)
			logger.Infof("Tx outputs: %v\n", newTx.Outputs)
			assert.Equal(t, true, bl, "Test Tx was not expected to be found in the block")
		}
	}

}

func TestLocktimeFutureTimeStamp(t *testing.T) {
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

	err = newTx.AddP2PKHOutputFromAddress(coinbaseAddr.AddressString, 10000)
	if err != nil {
		t.Errorf("Error adding output to transaction: %v", err)
	}

	newTx.LockTime = getFutureLockTime(10)
	// newTx.Inputs[0].SequenceNumber = 0xFFFFFFFF
	logger.Infof("Future time: %d\n", newTx.LockTime)
	err = newTx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privateKey})
	if err != nil {
		t.Errorf("Error filling transaction inputs: %v", err)
	}

	_, err = txDistributor.SendTransaction(ctx, newTx)
	if err != nil {
		t.Errorf("Failed to send new transaction: %v", err)
	}

	fmt.Printf("Transaction sent: %s %s\n", newTx.TxIDChainHash(), newTx.TxID())
	time.Sleep(20 * time.Second)

	height, _ := helper.GetBlockHeight(url)

	utxoBalanceAfter, _, _ := coinbaseClient.GetBalance(ctx)
	logger.Infof("utxoBalanceBefore: %d, utxoBalanceAfter: %d\n", utxoBalanceBefore, utxoBalanceAfter)

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
	logger.Infof("Block: %v\n", b)

	blockStore := framework.Nodes[0].Blockstore
	var o []options.Options
	o = append(o, options.WithFileExtension("block"))
	//wait
	time.Sleep(10 * time.Second)
	blockchainClient := framework.Nodes[0].BlockchainClient
	header, meta, _ := blockchainClient.GetBestBlockHeader(ctx)
	logger.Infof("Best block header: %v\n", header.Hash())

	r, err := blockStore.GetIoReader(ctx, header.Hash()[:], o...)
	if err != nil {
		t.Errorf("error getting block reader: %v", err)
	}
	if err == nil {
		if bl, err := helper.ReadFile(ctx, "block", logger, r, *newTx.TxIDChainHash(), framework.Nodes[0].BlockstoreUrl); err != nil {
			t.Errorf("error reading block: %v", err)
		} else {
			fmt.Printf("Block at height (%d): was tested for the test Tx %v\n", meta.Height, newTx)
			logger.Infof("Block at height (%d): was tested for the test Tx %v\n", meta.Height, newTx)
			logger.Infof("Tx locktime: %d\n", newTx.LockTime)
			logger.Infof("Tx hash: %s\n", newTx.TxIDChainHash())
			logger.Infof("Tx inputs: %v\n", newTx.Inputs)
			logger.Infof("Tx outputs: %v\n", newTx.Outputs)
			assert.Equal(t, true, bl, "Test Tx was not expected to be found in the block")
		}
	}

}