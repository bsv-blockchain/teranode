//go:build tnjtests

// How to run this test:
// $ unzip data.zip
// $ cd test/functional/
// $ `SETTINGS_CONTEXT=docker.ci.tc1.run go test -run TestShouldAllowFairTx`

package tnj

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	blockchain_store "github.com/bitcoin-sv/ubsv/stores/blockchain"
	setup "github.com/bitcoin-sv/ubsv/test/setup"
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
	"github.com/stretchr/testify/suite"
)

type TNJLockTimeTestSuite struct {
	setup.BitcoinTestSuite
}

func getFutureLockTime(daysAhead int) uint32 {
	now := time.Now()
	futureTime := now.Add(time.Duration(daysAhead*24) * time.Hour)
	return uint32(futureTime.Unix())
}

func (suite *TNJLockTimeTestSuite) TestLocktimeFutureHeight() {
	t := suite.T()
	framework := suite.Framework
	ctx := context.Background()
	url := "http://localhost:18090"

	helper.GetBestBlock(ctx, framework.Nodes[0])

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

func (suite *TNJLockTimeTestSuite) TestLocktimeFutureTimeStamp() {
	ctx := context.Background()
	url := "http://localhost:18090"
	t := suite.T()
	framework := suite.Framework

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

func TestTNJLockTimeTestSuite(t *testing.T) {
	suite.Run(t, new(TNJLockTimeTestSuite))
}
