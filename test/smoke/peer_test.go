//go:build test_all || test_smoke || test_peer

// How to run each test:
// Clean up docker containers before running the test manually
// $ cd test/smoke/
// $ go test -v -run "^TestPeerTestSuite$/TestBanPeerList$" -tags peer

package smoke

import (
	"fmt"
	"testing"
	"time"

	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/libsv/go-bk/bec"
	"github.com/libsv/go-bk/wif"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/unlocker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type PeerTestSuite struct {
	helper.TeranodeTestSuite
}

func TestPeerTestSuite(t *testing.T) {
	suite.Run(t, &PeerTestSuite{})
}

func (suite *PeerTestSuite) TestBanPeerList() {
	t := suite.T()
	testEnv := suite.TeranodeTestEnv
	ctx := testEnv.Context
	logger := testEnv.Logger
	url := "http://localhost:10090"
	node1 := testEnv.Nodes[0]
	node2 := testEnv.Nodes[1]
	node3 := testEnv.Nodes[2]

	tSettings := test.CreateBaseTestSettings()

	_, err := helper.CallRPC(node1.RPCURL, "setban", []interface{}{node2.IPAddress, "add", 180, false})
	require.NoError(t, err)

	_, err = helper.CallRPC(node3.RPCURL, "setban", []interface{}{node2.IPAddress, "add", 180, false})
	require.NoError(t, err)

	txDistributor := testEnv.Nodes[0].DistributorClient

	coinbaseClient := testEnv.Nodes[0].CoinbaseClient
	utxoBalanceBefore, _, _ := coinbaseClient.GetBalance(ctx)
	t.Logf("utxoBalanceBefore: %d\n", utxoBalanceBefore)

	coinbasePrivKey := tSettings.Coinbase.WalletPrivateKey
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

	txDistributor.TriggerBatcher() // just in case there is a delay in processing txs

	t.Logf("Transaction sent: %s %s\n", newTx.TxIDChainHash(), newTx.TxID())

	delay := tSettings.BlockAssembly.DoubleSpendWindow
	if delay != 0 {
		t.Logf("Waiting %dms [block assembly has delay processing txs to catch double spends]\n", delay)

		time.Sleep(time.Duration(delay) * time.Millisecond)
	}

	height, _ := helper.GetBlockHeight(url)
	t.Logf("Block height before mining: %d\n", height)

	utxoBalanceAfter, _, _ := coinbaseClient.GetBalance(ctx)
	t.Logf("utxoBalanceBefore: %d, utxoBalanceAfter: %d\n", utxoBalanceBefore, utxoBalanceAfter)

	_, err = helper.MineBlockWithRPC(ctx, testEnv.Nodes[0], logger)

	if err != nil {
		t.Errorf("Failed to mine block: %v", err)
	}

	blockchainClient := testEnv.Nodes[0].BlockchainClient
	blNode1 := false
	blNode3 := false

	targetHeight := height + 1

	for i := 0; i < 30; i++ {
		err := helper.WaitForBlockHeight(url, targetHeight, 60)

		if err != nil {
			t.Errorf("Failed to wait for block height: %v", err)
		}

		header, meta, err := blockchainClient.GetBlockHeadersFromHeight(ctx, targetHeight, 1)

		if err != nil {
			t.Errorf("Failed to get block headers: %v", err)
		}

		t.Logf("Testing on Best block header: %v", header[0].Hash())

		if !blNode1 {
			blockStore := testEnv.Nodes[0].ClientBlockstore
			subtreeStore := testEnv.Nodes[0].ClientSubtreestore
			blNode1, err = helper.TestTxInBlock(ctx, logger, blockStore, subtreeStore, header[0].Hash()[:], *newTx.TxIDChainHash())

			if err != nil {
				t.Errorf("error checking if tx exists in block: %v, error %v", meta[0].Height, err)
			}
		}

		if !blNode3 {
			blockStore := testEnv.Nodes[2].ClientBlockstore
			subtreeStore := testEnv.Nodes[2].ClientSubtreestore
			blNode3, err = helper.TestTxInBlock(ctx, logger, blockStore, subtreeStore, header[0].Hash()[:], *newTx.TxIDChainHash())

			if err != nil {
				t.Errorf("error checking if tx exists in block: %v, error %v", meta[0].Height, err)
			}
		}

		if blNode1 && blNode3 {
			break
		}

		targetHeight++
		_, err = helper.MineBlockWithRPC(ctx, testEnv.Nodes[0], logger)

		if err != nil {
			t.Errorf("Failed to mine block: %v", err)
		}
	}

	assert.Equal(t, true, blNode1, "Test Tx not found in block")
	assert.Equal(t, false, blNode3, "Test Tx found in block")

	bestBlockHeightNode1, _, _ := node1.BlockchainClient.GetBestBlockHeader(ctx)
	bestBlockHeightNode2, _, _ := node2.BlockchainClient.GetBestBlockHeader(ctx)
	bestBlockHeightNode3, _, _ := node3.BlockchainClient.GetBestBlockHeader(ctx)

	assert.NotEqual(t, bestBlockHeightNode1, bestBlockHeightNode2, "Best block height mismatch")
	assert.NotEqual(t, bestBlockHeightNode2, bestBlockHeightNode3, "Best block height mismatch")
	assert.Equal(t, bestBlockHeightNode1, bestBlockHeightNode3, "Best block height mismatch")
}
