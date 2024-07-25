package tna

import (
	"context"
	"fmt"
	"testing"
	"time"

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
)

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
