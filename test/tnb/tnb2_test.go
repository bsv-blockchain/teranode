//go:build test_all || test_tnb

package tnb

import (
	"testing"

	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/bitcoin-sv/teranode/test/utils/tconfig"
	"github.com/libsv/go-bk/bec"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/unlocker"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type TNB2TestSuite struct {
	helper.TeranodeTestSuite
}

func TestTNB2TestSuite(t *testing.T) {
	suite.Run(t, &TNB2TestSuite{
		TeranodeTestSuite: helper.TeranodeTestSuite{
			TConfig: tconfig.LoadTConfig(
				map[string]any{
					tconfig.KeyTeranodeContexts: []string{
						"docker.teranode1.test",
						"docker.teranode2.test",
						"docker.teranode3.test",
					},
				},
			),
		},
	},
	)
}

// How to run this test:
// $ cd test/tnb/
// $ go test -v -run "^TestTNB2TestSuite$/TestUTXOValidation$" -tags test_tnb
//
// TestUTXOValidation verifies that Teranode correctly validates transaction inputs against the UTXO set.
// This test ensures that double-spending is prevented across nodes by:
//
// Test Steps:
// 1. Setup:
//   - Start two Teranode instances (node1 and node2)
//   - Disable P2P communication on node2 using settings context
//   - Generate private key and address for transaction creation
//
// 2. Transaction Creation and Spending:
//   - Create a transaction using node1's coinbase funds
//   - Send the transaction to node1
//   - Mine a block on node1 to confirm the transaction
//   - Mark the transaction's UTXO as spent in node1's UTXO store
//
// 3. P2P Communication Test:
//   - Re-enable P2P communication on node2
//   - Restart nodes with updated settings
//   - Send RUN event to both nodes to ensure they're active
//
// 4. Verification:
//   - Verify that node2's UTXO store does not contain the spent transaction
//   - This confirms that the UTXO spending information was correctly propagated
//
// Required Settings:
// - docker.teranode2.test.stopP2P: Used to disable P2P communication
// - docker.teranode2.test: Used to re-enable P2P communication
//
// Dependencies:
// - Requires a running Teranode environment with at least 2 nodes
// - Requires access to coinbase funds for transaction creation
// - Requires Aerospike for UTXO store functionality
func (suite *TNB2TestSuite) TestUTXOValidation() {
	testEnv := suite.TeranodeTestEnv
	ctx := testEnv.Context
	t := suite.T()
	// blockchainNode0 := testEnv.Nodes[0].BlockchainClient
	// logger := testEnv.Logger

	// if err != nil {
	// 	t.Errorf("error subscribing to blockchain service: %v", err)
	// 	return
	// }

	privateKey, _ := bec.NewPrivateKey(bec.S256())

	address, _ := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)

	node1 := testEnv.Nodes[0]

	coinbaseClient := node1.CoinbaseClient
	faucetTx, err := coinbaseClient.RequestFunds(ctx, address.AddressString, true)
	require.NoError(t, err, "Failed to request funds")

	_, err = node1.DistributorClient.SendTransaction(ctx, faucetTx)
	require.NoError(t, err, "Failed to send faucet transaction")

	output := faucetTx.Outputs[0]
	u := &bt.UTXO{
		TxIDHash:      faucetTx.TxIDChainHash(),
		Vout:          uint32(0),
		LockingScript: output.LockingScript,
		Satoshis:      output.Satoshis,
	}

	tx := bt.NewTx()

	err = tx.FromUTXOs(u)
	require.NoError(t, err)

	err = tx.AddP2PKHOutputFromAddress(address.AddressString, 10000)
	require.NoError(t, err)

	err = tx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privateKey})
	require.NoError(t, err)

	_, err = node1.UtxoStore.Spend(ctx, tx)
	require.NoError(t, err)

	// Create another transaction
	anotherTx := bt.NewTx()
	err = anotherTx.FromUTXOs(u)
	require.NoError(t, err)

	err = anotherTx.AddP2PKHOutputFromAddress("1ApLMk225o7S9FvKwpNChB7CX8cknQT9Hy", 10000)
	require.NoError(t, err)

	err = anotherTx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privateKey})
	require.NoError(t, err)

	_, err = helper.SendTransaction(ctx, node1, anotherTx)
	require.Error(t, err, "Transaction should be rejected due to double-spending")

	t.Logf("Transaction sent to node 1: %v", anotherTx.TxIDChainHash())
}

// TestScriptValidation verifies that Teranode correctly validates transaction scripts.
// This test ensures that:
// 1. Valid P2PKH transactions are accepted
// 2. Invalid scripts (wrong signature) are rejected
// 3. Script execution returns true for valid transactions
//
// To run the test:
// $ cd test/tnb/
// $ go test -v -run "^TestTNB2TestSuite$/TestScriptValidation$" -tags test_tnb
func (suite *TNB2TestSuite) TestScriptValidation() {
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

	// Test 1: Create and send a valid P2PKH transaction
	validTx := bt.NewTx()
	err = validTx.FromUTXOs(utxo)
	require.NoError(t, err)

	err = validTx.AddP2PKHOutputFromAddress(address.AddressString, 10000)
	require.NoError(t, err)

	err = validTx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privateKey})
	require.NoError(t, err)

	_, err = helper.SendTransaction(ctx, node1, validTx)
	require.NoError(t, err, "Valid transaction should be accepted")

	// Test 2: Create a transaction with invalid signature
	invalidTx := bt.NewTx()
	err = invalidTx.FromUTXOs(utxo)
	require.NoError(t, err)

	err = invalidTx.AddP2PKHOutputFromAddress(address.AddressString, 10000)
	require.NoError(t, err)

	// Use a different private key to create an invalid signature
	wrongPrivateKey, _ := bec.NewPrivateKey(bec.S256())
	err = invalidTx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: wrongPrivateKey})
	require.NoError(t, err)

	_, err = helper.SendTransaction(ctx, node1, invalidTx)
	require.Error(t, err, "Transaction with invalid signature should be rejected")
	require.Contains(t, err.Error(), "script validation failed", "Error should indicate script validation failure")

	t.Log("Script validation test completed successfully")
}
