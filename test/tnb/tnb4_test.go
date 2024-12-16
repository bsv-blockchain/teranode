//go:build test_all || test_tnb

package tnb

import (
	"testing"

	helper "github.com/bitcoin-sv/ubsv/test/utils"
	"github.com/libsv/go-bk/bec"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/unlocker"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// TNB4TestSuite contains tests for TNB-4: Script Validation
// These tests verify that Teranode correctly validates transaction scripts
type TNB4TestSuite struct {
	helper.TeranodeTestSuite
}

func (suite *TNB4TestSuite) InitSuite() {
	suite.SettingsMap = map[string]string{
		"SETTINGS_CONTEXT_1": "docker.ubsv1.test",
		"SETTINGS_CONTEXT_2": "docker.ubsv2.test",
		"SETTINGS_CONTEXT_3": "docker.ubsv3.test",
	}
}

func (suite *TNB4TestSuite) SetupTest() {
	suite.InitSuite()
	suite.SetupTestEnv(suite.SettingsMap, suite.DefaultComposeFiles(), false)
}

func (suite *TNB4TestSuite) TearDownTest() {
}

// TestP2PKHScriptValidation verifies that Teranode correctly validates P2PKH transaction scripts.
// This test ensures that:
// 1. Invalid signatures are rejected
// 2. Script execution returns false for invalid signatures
//
// To run the test:
// $ cd test/tnb/
// $ go test -v -run "^TestTNB4TestSuite$/TestP2PKHScriptValidation$" -tags test_tnb
func (suite *TNB4TestSuite) TestP2PKHScriptValidation() {
	testEnv := suite.TeranodeTestEnv
	ctx := testEnv.Context
	t := suite.T()
	node1 := testEnv.Nodes[0]

	// Create a valid key pair for testing
	privateKey, _ := bec.NewPrivateKey(bec.S256())
	address, _ := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)

	// Get some funds from coinbase
	coinbaseClient := node1.CoinbaseClient
	faucetTx, err := coinbaseClient.RequestFunds(ctx, address.AddressString, true)
	require.NoError(t, err, "Failed to request funds")

	_, err = node1.DistributorClient.SendTransaction(ctx, faucetTx)
	require.NoError(t, err, "Failed to send faucet transaction")

	output := faucetTx.Outputs[0]
	utxo := &bt.UTXO{
		TxIDHash:      faucetTx.TxIDChainHash(),
		Vout:          uint32(0),
		LockingScript: output.LockingScript,
		Satoshis:      output.Satoshis,
	}

	// Create a transaction with invalid signature
	invalidTx := bt.NewTx()
	err = invalidTx.FromUTXOs(utxo)
	require.NoError(t, err)

	err = invalidTx.AddP2PKHOutputFromAddress(address.AddressString, 10000)
	require.NoError(t, err)

	// Use a different private key to create an invalid signature
	wrongPrivateKey, _ := bec.NewPrivateKey(bec.S256())
	err = invalidTx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: wrongPrivateKey})
	require.NoError(t, err)

	_, err = node1.DistributorClient.SendTransaction(ctx, invalidTx)
	t.Logf("Transaction with invalid signature: %v", invalidTx.TxIDChainHash())
	require.NoError(t, err, "Failed to send transaction")
	require.Error(t, err, "Transaction with invalid signature should be rejected")
	require.Contains(t, err.Error(), "script validation failed", "Error should indicate script validation failure")

	t.Log("P2PKH script validation test completed successfully")
}

func TestTNB4TestSuite(t *testing.T) {
	suite.Run(t, new(TNB4TestSuite))
}
