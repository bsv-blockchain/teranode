//go:build test_all || test_tna

// How to run this test manually:
// $ cd test/tna
// $ go test -v -run "^TestTNA2TestSuite$/TestTxsReceivedAllNodes$" -tags test_tna

package tna

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
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/suite"
)

type TNA2TestSuite struct {
	helper.TeranodeTestSuite
}

func (suite *TNA2TestSuite) InitSuite() {
	suite.SettingsMap = map[string]string{
		"SETTINGS_CONTEXT_1": "docker.ubsv1.test.tna1Test",
		"SETTINGS_CONTEXT_2": "docker.ubsv2.test.tna1Test",
		"SETTINGS_CONTEXT_3": "docker.ubsv3.test.tna1Test",
	}
}

func (suite *TNA2TestSuite) SetupTest() {
	suite.InitSuite()
	suite.SetupTestEnv(suite.SettingsMap, suite.DefaultComposeFiles(), false)
}

func (suite *TNA2TestSuite) TestTxsReceivedAllNodes() {
	testEnv := suite.TeranodeTestEnv
	ctx := testEnv.Context
	t := suite.T()

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

	// If Block Assembly removes correctly the Tx, it means that Tx exists

	errTx0 := testEnv.Nodes[0].BlockassemblyClient.RemoveTx(ctx, newTx.TxIDChainHash())

	if errTx0 != nil {
		t.Errorf("Error Tx0 not present: %v", errTx0)
	}

	errTx1 := testEnv.Nodes[1].BlockassemblyClient.RemoveTx(ctx, newTx.TxIDChainHash())

	if errTx1 != nil {
		t.Errorf("Error Tx1 not present: %v", errTx1)
	}

	errTx2 := testEnv.Nodes[2].BlockassemblyClient.RemoveTx(ctx, newTx.TxIDChainHash())

	if errTx2 != nil {
		t.Errorf("Error Tx2 not present: %v", errTx2)
	}
}

func TestTNA2TestSuite(t *testing.T) {
	suite.Run(t, new(TNA2TestSuite))
}
