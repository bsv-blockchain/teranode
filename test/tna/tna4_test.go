//go:build tnatests

package tna

import (
	"fmt"
	"testing"
	"time"

	arrange "github.com/bitcoin-sv/ubsv/test/fixtures"
	helper "github.com/bitcoin-sv/ubsv/test/utils"
	"github.com/libsv/go-bk/bec"
	"github.com/libsv/go-bk/wif"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/libsv/go-bt/v2/unlocker"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/suite"
)

type TNA4TestSuite struct {
	arrange.TeranodeTestSuite
}

func (suite *TNA4TestSuite) InitSuite() {
	suite.SettingsMap = map[string]string{
		"SETTINGS_CONTEXT_1": "docker.test.ubsv1.tna1Test",
		"SETTINGS_CONTEXT_2": "docker.test.ubsv2.tna1Test",
		"SETTINGS_CONTEXT_3": "docker.test.ubsv3.tna1Test",
	}
}

func (suite *TNA4TestSuite) SetupTest() {
	suite.InitSuite()
	suite.SetupTestEnv(suite.SettingsMap, suite.DefaultComposeFiles(), false)
}

func (suite *TNA4TestSuite) TestBroadcastPoW() {
	// Test setup
	testEnv := suite.TeranodeTestEnv
	ctx := testEnv.Context
	t := suite.T()
	logger := testEnv.Logger

	hashes, err := helper.CreateAndSendTxs(ctx, testEnv.Nodes[0], 10)

	if err != nil {
		t.Errorf("Failed to create and send raw txs: %v", err)
	}

	fmt.Printf("Hashes in created block: %v\n", hashes)

	baClient := testEnv.Nodes[0].BlockassemblyClient

	var block, blockerr = helper.MineBlock(ctx, baClient, logger)

	if blockerr != nil {
		t.Errorf("Failed to mine block: %v", err)
	}

	time.Sleep(10 * time.Second)

	blockNode0, blockErr0 := testEnv.Nodes[0].BlockchainClient.GetBlockExists(ctx, (*chainhash.Hash)(block))

	blockNode1, blockErr1 := testEnv.Nodes[1].BlockChainDB.GetBlockExists(ctx, (*chainhash.Hash)(block))

	blockNode2, blockErr2 := testEnv.Nodes[2].BlockchainClient.GetBlockExists(ctx, (*chainhash.Hash)(block))

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

func (suite *TNA4TestSuite) TestSameTxsMultipleBlocks() {
	testEnv := suite.TeranodeTestEnv
	ctx := testEnv.Context
	t := suite.T()
	logger := testEnv.Logger

	// Send transactions
	txDistributor := testEnv.Nodes[0].DistributorClient

	coinbaseClient := testEnv.Nodes[0].CoinbaseClient
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
		baClient := testEnv.Nodes[i].BlockassemblyClient
		blockHash, err := helper.MineBlock(ctx, baClient, logger)

		if err != nil {
			t.Errorf("Failed to mine block: %v", err)
		}

		time.Sleep(5 * time.Second)

		blockNode, blockErr := testEnv.Nodes[i].BlockchainClient.GetBlockExists(ctx, (*chainhash.Hash)(blockHash))

		if blockErr != nil {
			t.Errorf("Failure on blockchain on Node0: %v", err)
		}

		if !blockNode {
			t.Errorf("Failed to retrieve new mined block on Node0: %v", blockErr)
		}
	}
}

func (suite *TNA4TestSuite) TestMultipleMining() {
	// Test setup
	testEnv := suite.TeranodeTestEnv
	ctx := testEnv.Context
	t := suite.T()
	logger := testEnv.Logger

	hashes0, err := helper.CreateAndSendTxs(ctx, testEnv.Nodes[0], 10)
	if err != nil {
		t.Errorf("Failed to create and send raw txs: %v", err)
	}

	fmt.Printf("Hashes in created block: %v\n", hashes0)

	baClient := testEnv.Nodes[0].BlockassemblyClient

	var block0, blockerr0 = helper.MineBlock(ctx, baClient, logger)

	if blockerr0 != nil {
		t.Errorf("Failed to mine block: %v", err)
	}

	hashes1, err := helper.CreateAndSendTxs(ctx, testEnv.Nodes[1], 10)
	if err != nil {
		t.Errorf("Failed to create and send raw txs: %v", err)
	}

	fmt.Printf("Hashes in created block: %v\n", hashes1)

	baClient1 := testEnv.Nodes[1].BlockassemblyClient

	var block1, blockerr1 = helper.MineBlock(ctx, baClient1, logger)

	if blockerr1 != nil {
		t.Errorf("Failed to mine block: %v", err)
	}

	hashes2, err := helper.CreateAndSendTxs(ctx, testEnv.Nodes[2], 10)

	if err != nil {
		t.Errorf("Failed to create and send raw txs: %v", err)
	}

	fmt.Printf("Hashes in created block: %v\n", hashes2)

	baClient2 := testEnv.Nodes[2].BlockassemblyClient

	var block2, blockerr2 = helper.MineBlock(ctx, baClient2, logger)

	if blockerr2 != nil {
		t.Errorf("Failed to mine block: %v", err)
	}

	blockNode0, blockErr0 := testEnv.Nodes[0].BlockchainClient.GetBlockExists(ctx, (*chainhash.Hash)(block0))
	blockNode1, blockErr1 := testEnv.Nodes[1].BlockChainDB.GetBlockExists(ctx, (*chainhash.Hash)(block1))
	blockNode2, blockErr2 := testEnv.Nodes[2].BlockchainClient.GetBlockExists(ctx, (*chainhash.Hash)(block2))

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

func TestTNA4TestSuite(t *testing.T) {
	suite.Run(t, new(TNA4TestSuite))
}
