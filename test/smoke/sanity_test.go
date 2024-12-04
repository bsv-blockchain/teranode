//go:build test_all || test_smoke || test_functional

// How to run each test:
// Clean up docker containers before running the test manually
// $ cd test/smoke/
// $ go test -v -run "^TestSanityTestSuite$/TestShouldAllowFairTx$" -tags functional

package smoke

import (
	"fmt"
	"testing"
	"time"

	helper "github.com/bitcoin-sv/ubsv/test/utils"
	"github.com/libsv/go-bk/bec"
	"github.com/libsv/go-bk/wif"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/unlocker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type SanityTestSuite struct {
	helper.TeranodeTestSuite
}

// func (suite *SanityTestSuite) TearDownTest() {
// }

func (suite *SanityTestSuite) TestShouldAllowFairTx() {
	t := suite.T()
	testEnv := suite.TeranodeTestEnv
	ctx := testEnv.Context
	logger := testEnv.Logger
	url := "http://localhost:10090"

	txDistributor := testEnv.Nodes[0].DistributorClient

	coinbaseClient := testEnv.Nodes[0].CoinbaseClient
	utxoBalanceBefore, _, _ := coinbaseClient.GetBalance(ctx)
	t.Logf("utxoBalanceBefore: %d\n", utxoBalanceBefore)

	coinbasePrivKey := testEnv.Nodes[0].Settings.Coinbase.WalletPrivateKey
	if coinbasePrivKey == "" {
		t.Errorf("Coinbase private key is not set")
	}

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

	delay := testEnv.Nodes[0].Settings.BlockAssembly.DoubleSpendWindow
	if delay != 0 {
		t.Logf("Waiting %dms [block assembly has delay processing txs to catch double spends]\n", delay)
		time.Sleep(time.Duration(delay) * time.Millisecond)
	}

	height, _ := helper.GetBlockHeight(url)
	t.Logf("Block height before mining: %d\n", height)

	utxoBalanceAfter, _, _ := coinbaseClient.GetBalance(ctx)
	t.Logf("utxoBalanceBefore: %d, utxoBalanceAfter: %d\n", utxoBalanceBefore, utxoBalanceAfter)

	const ubsv1RPCEndpoint = "http://localhost:11292"
	// Generate blocks
	_, err = helper.CallRPC(ubsv1RPCEndpoint, "generate", []interface{}{101})
	// wait for the blocks to be generated
	time.Sleep(5 * time.Second)
	require.NoError(t, err, "Failed to generate blocks")

	blockStore := testEnv.Nodes[0].Blockstore
	blockchainClient := testEnv.Nodes[0].BlockchainClient
	bl := false
	targetHeight := height + 1

	for i := 0; i < 5; i++ {
		err := helper.WaitForBlockHeight(url, targetHeight, 60)
		if err != nil {
			t.Errorf("Failed to wait for block height: %v", err)
		}

		header, meta, err := blockchainClient.GetBlockHeadersFromHeight(ctx, targetHeight, 1)
		if err != nil {
			t.Errorf("Failed to get block headers: %v", err)
		}

		t.Logf("Testing on Best block header: %v", header[0].Hash())

		t.Logf("Testing on Best block meta: %v", meta[0].Height)
		t.Logf("Testing on BlockstoreURL: %v", testEnv.Nodes[0].BlockstoreURL)
		bl, err = helper.CheckIfTxExistsInBlock(ctx, blockStore, testEnv.Nodes[0].BlockstoreURL, header[0].Hash()[:], meta[0].Height, *newTx.TxIDChainHash(), logger)
		if err != nil {
			t.Errorf("error checking if tx exists in block: %v", err)
		}

		if bl {
			break
		}

		targetHeight++
	}

	assert.Equal(t, true, bl, "Test Tx not found in block")
}

func TestSanityTestSuite(t *testing.T) {
	suite.Run(t, new(SanityTestSuite))
}
