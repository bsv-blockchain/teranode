//go:build tnj

// How to run this test manually:
// $ cd test/tnj
// $ go test -v -run "^TestTNJDoubleSpendTestSuite$/TestDoubleSpendMultipleUtxos$" -tags tnj
package tnj

import (
	"testing"
	"time"

	arrange "github.com/bitcoin-sv/ubsv/test/fixtures"
	"github.com/bitcoin-sv/ubsv/test/testenv"
	helper "github.com/bitcoin-sv/ubsv/test/utils"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/unlocker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type TNJDoubleSpendTestSuite struct {
	arrange.TeranodeTestSuite
}

func (suite *TNJDoubleSpendTestSuite) TearDownTest() {
}

func (suite *TNJDoubleSpendTestSuite) TestRejectLongerChainWithDoubleSpend() {
	t := suite.T()
	testEnv := suite.TeranodeTestEnv
	settingsMap := suite.SettingsMap
	logger := testEnv.Logger
	ctx := testEnv.Context

	node1 := testEnv.Nodes[0]
	node2 := testEnv.Nodes[1]
	blockchainNode1 := testEnv.Nodes[0].BlockchainClient
	blockchainNode2 := testEnv.Nodes[1].BlockchainClient

	settingsMap["SETTINGS_CONTEXT_2"] = "docker.ubsv2.test.stopP2P"
	if err := testEnv.RestartDockerNodes(settingsMap); err != nil {
		t.Errorf("Failed to restart nodes: %v", err)
	}

	for i := 0; i < 5; i++ {
		hashes, err := helper.CreateAndSendTxs(ctx, node1, 1)
		if err != nil {
			t.Errorf("Failed to create and send raw txs: %v", err)
		}

		logger.Infof("Hashes: %v\n", hashes)

		baClient := node1.BlockassemblyClient
		_, err = helper.MineBlock(ctx, baClient, logger)

		if err != nil {

			t.Errorf("Failed to mine block: %v", err)

		}
		time.Sleep(5 * time.Second)
	}

	// Send a double spend transaction
	arrayOfNodes := []testenv.TeranodeTestClient{node1, node2}
	_, err := helper.CreateAndSendDoubleSpendTx(ctx, arrayOfNodes)
	if err != nil {
		t.Errorf("Failed to create and send double spend tx: %v", err)
	}
	baClient := arrayOfNodes[0].BlockassemblyClient
	_, err = helper.MineBlock(ctx, baClient, logger)
	if err != nil {
		t.Errorf("Failed to mine block: %v", err)
	}
	baClient = arrayOfNodes[1].BlockassemblyClient
	_, err = helper.MineBlock(ctx, baClient, logger)
	if err != nil {
		t.Errorf("Failed to mine block: %v", err)
	}

	settingsMap["SETTINGS_CONTEXT_2"] = "docker.ubsv2.test"
	if err := testEnv.RestartDockerNodes(settingsMap); err != nil {
		t.Errorf("Failed to restart nodes: %v", err)
	}

	time.Sleep(60 * time.Second)

	headerNode1, _, _ := blockchainNode1.GetBestBlockHeader(ctx)
	headerNode2, _, _ := blockchainNode2.GetBestBlockHeader(ctx)
	assert.Equal(t, headerNode1.Hash(), headerNode2.Hash(), "Best block headers are not equal")
}

/*
TestDoubleSpendMultipleUtxos tests double spending of multiple UTXOs across nodes

	Test flow:
	1. Request funds from faucet to get initial UTXOs
	2. Create two competing transactions spending the same UTXOs
	3. Send transactions to different nodes
	4. Mine blocks and verify network convergence
*/
func (suite *TNJDoubleSpendTestSuite) TestDoubleSpendMultipleUtxos() {
	t := suite.T()
	testEnv := suite.TeranodeTestEnv
	settingsMap := suite.SettingsMap
	logger := testEnv.Logger
	ctx := testEnv.Context

	node1 := testEnv.Nodes[0]
	node2 := testEnv.Nodes[1]
	node3 := testEnv.Nodes[2]

	// Generate keys for participants
	alicePrivateKey, alice, err := helper.GeneratePrivateKeyAndAddress()
	assert.NoError(t, err, "Failed to generate Alice's keys")

	_, bob, err := helper.GeneratePrivateKeyAndAddress()
	assert.NoError(t, err, "Failed to generate Bob's keys")

	_, charles, err := helper.GeneratePrivateKeyAndAddress()
	assert.NoError(t, err, "Failed to generate Charles's keys")

	// Request funds from faucet to Alice's address
	faucetTx, err := helper.RequestFunds(ctx, node1, alice.AddressString)
	assert.NoError(t, err, "Failed to request funds from faucet")

	sent, err := helper.SendTransaction(ctx, node1, faucetTx)
	assert.NoError(t, err, "Failed to send faucet transaction")
	assert.True(t, sent)
	logger.Infof("Faucet transaction sent: %s", faucetTx.TxIDChainHash())

	// Mine a block to confirm faucet transaction
	_, err = helper.MineBlock(ctx, node1.BlockassemblyClient, logger)
	assert.NoError(t, err, "Failed to mine block for faucet transaction")

	// Disable P2P on second node to create network partition
	settingsMap["SETTINGS_CONTEXT_2"] = "docker.ubsv2.test.stopP2P"
	if err := testEnv.RestartDockerNodes(settingsMap); err != nil {
		t.Fatalf("Failed to restart nodes: %v", err)
	}

	// Wait for network synchronization
	time.Sleep(10 * time.Second)

	// set to run on node 1
	_ = node1.BlockchainClient.Run(ctx, "test")
	_ = node2.BlockchainClient.Run(ctx, "test")
	_ = node3.BlockchainClient.Run(ctx, "test")

	// Wait for network synchronization
	time.Sleep(10 * time.Second)

	// Collect all UTXOs from faucet transaction
	var utxos []*bt.UTXO

	totalSatoshis := uint64(0)

	// nolint: gosec
	for i := 0; i < len(faucetTx.Outputs); i++ {
		output := faucetTx.Outputs[i]
		utxo := helper.CreateUtxoFromTransaction(faucetTx, uint32(i))
		utxos = append(utxos, utxo)
		totalSatoshis += output.Satoshis
	}

	// Create first transaction spending all UTXOs to Bob
	tx1 := bt.NewTx()
	err = tx1.FromUTXOs(utxos...)
	assert.NoError(t, err, "Failed to add UTXOs to first transaction")

	fee := uint64(1000)
	amountForBob := totalSatoshis - fee

	err = tx1.AddP2PKHOutputFromAddress(bob.AddressString, amountForBob)
	assert.NoError(t, err, "Failed to add output to first transaction")

	err = tx1.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: alicePrivateKey})
	assert.NoError(t, err, "Failed to fill inputs for first transaction")

	// Create double-spend transaction to Charles using same UTXOs
	tx2 := bt.NewTx()
	err = tx2.FromUTXOs(utxos...)
	assert.NoError(t, err, "Failed to add UTXOs to double-spend transaction")

	amountForCharles := totalSatoshis - fee
	err = tx2.AddP2PKHOutputFromAddress(charles.AddressString, amountForCharles)
	assert.NoError(t, err, "Failed to add output to double-spend transaction")

	err = tx2.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: alicePrivateKey})
	assert.NoError(t, err, "Failed to fill inputs for double-spend transaction")

	// Send tx1 to first node
	sent, err = helper.SendTransaction(ctx, node1, tx1)
	assert.NoError(t, err, "Failed to send first transaction")
	assert.True(t, sent)
	logger.Infof("First transaction sent to node1: %s", tx1.TxIDChainHash())

	// Send tx2 to second node
	sent, err = helper.SendTransaction(ctx, node2, tx2)
	assert.NoError(t, err, "Failed to send double-spend transaction")
	assert.True(t, sent)
	logger.Infof("Double-spend transaction sent to node2: %s", tx2.TxIDChainHash())

	// Mine blocks on both nodes to create competing chains
	_, err = helper.MineBlock(ctx, node1.BlockassemblyClient, logger)
	assert.NoError(t, err, "Failed to mine block on node1")

	_, err = helper.MineBlock(ctx, node2.BlockassemblyClient, logger)
	assert.NoError(t, err, "Failed to mine block on node2")

	// Re-enable P2P on second node
	settingsMap["SETTINGS_CONTEXT_2"] = "docker.ubsv2.test"
	if err := testEnv.RestartDockerNodes(settingsMap); err != nil {
		t.Errorf("Failed to restart nodes: %v", err)
	}

	// Wait for network synchronization
	time.Sleep(10 * time.Second)

	// Run
	_ = node1.BlockchainClient.Run(ctx, "test")
	_ = node2.BlockchainClient.Run(ctx, "test")
	_ = node3.BlockchainClient.Run(ctx, "test")

	// Wait for network synchronization
	time.Sleep(10 * time.Second)

	// mine blocks
	_, err = helper.MineBlock(ctx, node1.BlockassemblyClient, logger)
	assert.NoError(t, err, "Failed to mine block on node1")

	// Verify both nodes converged to same chain tip
	header1, _, err := node1.BlockchainClient.GetBestBlockHeader(ctx)
	assert.NoError(t, err, "Failed to get best block header from node1")

	header2, _, err := node2.BlockchainClient.GetBestBlockHeader(ctx)
	assert.NoError(t, err, "Failed to get best block header from node2")

	assert.Equal(t, header1.Hash().String(), header2.Hash().String(),
		"Best block headers should be equal after synchronization")

	// Verify UTXO state
	// Check which transaction was accepted on node1
	tx1OnNode1, _ := node1.UtxoStore.Get(ctx, tx1.TxIDChainHash())

	tx2OnNode1, _ := node1.UtxoStore.Get(ctx, tx2.TxIDChainHash())

	// Check which transaction was accepted on node2
	tx1OnNode2, _ := node2.UtxoStore.Get(ctx, tx1.TxIDChainHash())

	tx2OnNode2, _ := node2.UtxoStore.Get(ctx, tx2.TxIDChainHash())

	// Only one transaction should exist in the UTXO store on each node
	assert.True(t, (tx1OnNode1 != nil && tx2OnNode1 == nil) || (tx1OnNode1 == nil && tx2OnNode1 != nil),
		"Node1 should have exactly one transaction in UTXO store")
	assert.True(t, (tx1OnNode2 != nil && tx2OnNode2 == nil) || (tx1OnNode2 == nil && tx2OnNode2 != nil),
		"Node2 should have exactly one transaction in UTXO store")

	// Both nodes should agree on which transaction was accepted
	if tx1OnNode1 != nil {
		assert.NotNil(t, tx1OnNode2.Tx, "Node2 should also accept tx1")
		assert.Nil(t, tx2OnNode1.Tx, "Node1 should reject tx2")
		assert.Nil(t, tx2OnNode2.Tx, "Node2 should reject tx2")
		logger.Infof("Transaction 1 was accepted, Transaction 2 was rejected")
	} else {
		assert.NotNil(t, tx2OnNode2.Tx, "Node2 should also accept tx2")
		assert.Nil(t, tx1OnNode1.Tx, "Node1 should reject tx1")
		assert.Nil(t, tx1OnNode2.Tx, "Node2 should reject tx1")
		logger.Infof("Transaction 2 was accepted, Transaction 1 was rejected")
	}
}

func TestTNJDoubleSpendTestSuite(t *testing.T) {
	suite.Run(t, new(TNJDoubleSpendTestSuite))
}
