//go:build functional

// How to run each test:
// Clean up docker containers before running the test manually
// $ cd test/smoke/
// $ SETTINGS_CONTEXT=docker.ci.tc1.run go test -v -run "^TestSanityTestSuite$/TestShouldAllowFairTx$" -tags functional
// $ SETTINGS_CONTEXT=docker.ci.tc1.run go test -v -run "^TestSanityTestSuite$/TestShouldAllowFairTx_UseRpc$" -tags functional

package test

import (
	"fmt"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/test/setup"
	helper "github.com/bitcoin-sv/ubsv/test/utils"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util/distributor"
	"github.com/libsv/go-bk/bec"
	"github.com/libsv/go-bk/wif"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/unlocker"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type SanityTestSuite struct {
	setup.BitcoinTestSuite
}

// func (suite *SanityTestSuite) TearDownTest() {
// }

func (suite *SanityTestSuite) TestShouldAllowFairTx() {
	t := suite.T()
	framework := suite.Framework
	ctx := framework.Context
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

	t.Logf("Transaction: %s %s\n", tx.TxIDChainHash(), tx.TxID())

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
	logger.Infof("Block height before mining: %d\n", height)

	utxoBalanceAfter, _, _ := coinbaseClient.GetBalance(ctx)
	logger.Infof("utxoBalanceBefore: %d, utxoBalanceAfter: %d\n", utxoBalanceBefore, utxoBalanceAfter)

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

// func (suite *SanityTestSuite) TestShouldAllowFairTx_UseRpc() {
// 	ctx := context.Background()
// 	t := suite.T()
// 	framework := suite.Framework
// 	logger := framework.Logger
// 	url := "http://localhost:18090"

// 	txDistributor, _ := distributor.NewDistributor(ctx, logger,
// 		distributor.WithBackoffDuration(200*time.Millisecond),
// 		distributor.WithRetryAttempts(3),
// 		distributor.WithFailureTolerance(0),
// 	)

// 	coinbaseClient := framework.Nodes[0].CoinbaseClient
// 	utxoBalanceBefore, _, _ := coinbaseClient.GetBalance(ctx)
// 	fmt.Printf("utxoBalanceBefore: %d\n", utxoBalanceBefore)

// 	coinbasePrivKey, _ := gocore.Config().Get("coinbase_wallet_private_key")
// 	coinbasePrivateKey, err := wif.DecodeWIF(coinbasePrivKey)
// 	if err != nil {
// 		t.Errorf("Failed to decode Coinbase private key: %v", err)
// 	}

// 	coinbaseAddr, _ := bscript.NewAddressFromPublicKey(coinbasePrivateKey.PrivKey.PubKey(), true)

// 	privateKey, err := bec.NewPrivateKey(bec.S256())
// 	if err != nil {
// 		t.Errorf("Failed to generate private key: %v", err)
// 	}

// 	address, err := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
// 	if err != nil {
// 		t.Errorf("Failed to create address: %v", err)
// 	}

// 	tx, err := coinbaseClient.RequestFunds(ctx, address.AddressString, true)
// 	if err != nil {
// 		t.Errorf("Failed to request funds: %v", err)
// 	}
// 	fmt.Printf("Transaction: %s %s\n", tx.TxIDChainHash(), tx.TxID())

// 	_, err = txDistributor.SendTransaction(ctx, tx)
// 	if err != nil {
// 		t.Errorf("Failed to send transaction: %v", err)
// 	}

// 	fmt.Printf("Transaction sent: %s %v\n", tx.TxIDChainHash(), len(tx.Outputs))
// 	output := tx.Outputs[0]
// 	utxo := &bt.UTXO{
// 		TxIDHash:      tx.TxIDChainHash(),
// 		Vout:          uint32(0),
// 		LockingScript: output.LockingScript,
// 		Satoshis:      output.Satoshis,
// 	}

// 	newTx := bt.NewTx()
// 	err = newTx.FromUTXOs(utxo)
// 	if err != nil {
// 		t.Errorf("Error adding UTXO to transaction: %s\n", err)
// 	}

// 	err = newTx.AddP2PKHOutputFromAddress(coinbaseAddr.AddressString, 10000)
// 	if err != nil {
// 		t.Errorf("Error adding output to transaction: %v", err)
// 	}

// 	err = newTx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privateKey})
// 	if err != nil {
// 		t.Errorf("Error filling transaction inputs: %v", err)
// 	}

// 	_, err = txDistributor.SendTransaction(ctx, newTx)
// 	if err != nil {
// 		t.Errorf("Failed to send new transaction: %v", err)
// 	}

// 	logger.Infof("Transaction sent: %s %s\n", newTx.TxIDChainHash(), newTx.TxID())
// 	time.Sleep(10 * time.Second)

// 	height, _ := helper.GetBlockHeight(url)
// 	fmt.Printf("Block height: %d\n", height)

// 	utxoBalanceAfter, _, _ := coinbaseClient.GetBalance(ctx)
// 	logger.Infof("utxoBalanceBefore: %d, utxoBalanceAfter: %d\n", utxoBalanceBefore, utxoBalanceAfter)

// 	baClient := framework.Nodes[0].BlockassemblyClient
// 	miningCandidate, err := baClient.GetMiningCandidate(ctx)

// 	if err != nil {
// 		t.Errorf("Failed to get mining candidate: %v", err)
// 	}

// 	framework.Logger.Infof("RPC Url: %d\n", framework.Nodes[0].RPC_URL)
// 	_, err = helper.MineBlockWithCandidate_rpc(ctx, framework.Nodes[0].RPC_URL, miningCandidate, logger)

// 	if err != nil {
// 		t.Errorf("Failed to mine block: %v", err)
// 	}

// 	blockStore := framework.Nodes[0].Blockstore
// 	blockchainClient := framework.Nodes[0].BlockchainClient
// 	bl := false
// 	targetHeight := height + 1
// 	for i := 0; i < 30; i++ {

// 		err := helper.WaitForBlockHeight(url, targetHeight, 60)
// 		if err != nil {
// 			t.Errorf("Failed to wait for block height: %v", err)
// 		}

// 		header, meta, _ := blockchainClient.GetBlockHeadersFromHeight(ctx, targetHeight, 1)
// 		logger.Infof("Tetsing on Best block header: %v", header[0].Hash())
// 		bl, err = helper.CheckIfTxExistsInBlock(ctx, blockStore, framework.Nodes[0].BlockstoreUrl, header[0].Hash()[:], meta[0].Height, *newTx.TxIDChainHash(), framework.Logger)

// 		if err != nil {
// 			t.Errorf("error checking if tx exists in block: %v", err)
// 		}

// 		if bl {
// 			break
// 		}

// 		miningCandidate, err := baClient.GetMiningCandidate(ctx)
// 		if err != nil {
// 			t.Errorf("Failed to get mining candidate: %v", err)
// 		}

// 		_, err = helper.MineBlockWithCandidate_rpc(ctx, framework.Nodes[0].RPC_URL, miningCandidate, logger)
// 		if err != nil {
// 			t.Errorf("Failed to mine block: %v", err)
// 		}

// 		targetHeight++
// 	}

// 	assert.Equal(t, true, bl, "Test Tx not found in block")

// }

func TestSanityTestSuite(t *testing.T) {
	suite.Run(t, new(SanityTestSuite))
}
