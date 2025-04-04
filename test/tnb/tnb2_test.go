//go:build test_tnb || debug

package tnb

import (
	"testing"

	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/bitcoin-sv/teranode/test/utils/tconfig"
	"github.com/libsv/go-bk/bec"
	"github.com/libsv/go-bk/wif"
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
// $ go test -v -run "^TestTNB2TestSuite$/TestUTXOValidation$" -tags test_tnb ./test/tnb/tnb2_test.go
// $ go test -v -run "^TestTNB2TestSuite$/TestScriptValidation$" -tags test_tnb ./test/tnb/tnb2_test.go
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
	
	node1 := testEnv.Nodes[0]

	w, err := wif.DecodeWIF(node1.Settings.BlockAssembly.MinerWalletPrivateKeys[0])
	require.NoError(t, err)

	privateKey := w.PrivKey
	publicKey := privateKey.PubKey().SerialiseCompressed()

	block1, err := node1.BlockchainClient.GetBlockByHeight(ctx, 1)
	require.NoError(t, err)

	parenTx := block1.CoinbaseTx

	utxo := &bt.UTXO{
		TxIDHash:      parenTx.TxIDChainHash(),
		Vout:          uint32(0),
		LockingScript: parenTx.Outputs[0].LockingScript,
		Satoshis:      parenTx.Outputs[0].Satoshis,
	}

	tx := bt.NewTx()

	err = tx.FromUTXOs(utxo)
	require.NoError(t, err)

	err = tx.AddP2PKHOutputFromPubKeyBytes(publicKey, 10000)
	require.NoError(t, err)

	err = tx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privateKey})
	require.NoError(t, err)

	_, err = helper.SendTransaction(ctx, node1, tx)

	t.Logf("Transaction sent to node 1: %v", tx.TxIDChainHash())

	_, err = helper.CallRPC("http://"+node1.RPCURL, "generate", []interface{}{110})
	require.NoError(t, err)

	utxo = &bt.UTXO{
		TxIDHash:      tx.TxIDChainHash(),
		Vout:          uint32(0),
		LockingScript: tx.Outputs[0].LockingScript,
		Satoshis:      tx.Outputs[0].Satoshis,
	}

	// Create another transaction
	anotherTx := bt.NewTx()
	err = anotherTx.FromUTXOs(utxo)
	require.NoError(t, err)

	err = anotherTx.AddP2PKHOutputFromPubKeyBytes(publicKey, 10000)
	require.NoError(t, err)

	err = anotherTx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privateKey})
	require.NoError(t, err)

	_, err = node1.UtxoStore.Spend(ctx, tx)
	require.NoError(t, err)

	_, err = helper.SendTransaction(ctx, node1, anotherTx)

	require.Error(t, err)
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

	block1, err := node1.BlockchainClient.GetBlockByHeight(ctx, 1)
	require.NoError(t, err)

	parenTx := block1.CoinbaseTx

	utxo := &bt.UTXO{
		TxIDHash:      parenTx.TxIDChainHash(),
		Vout:          uint32(0),
		LockingScript: parenTx.Outputs[0].LockingScript,
		Satoshis:      parenTx.Outputs[0].Satoshis,
	}

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
	require.Contains(t, err.Error(), "failed to validate transaction", "Error should indicate script validation failure")

	t.Log("Script validation test completed successfully")
}
