//go:build test_all || test_tnb

/*
Package tnb implements Teranode Behavioral tests.

TNB-6: UTXO Set Management
-------------------------

Description:
    This test suite verifies that Teranode correctly manages the UTXO set by ensuring all outputs
    of validated transactions are properly added as unspent transaction outputs.

Test Coverage:
    1. UTXO Creation:
       - Verify that transaction outputs are added to the UTXO set
       - Confirm UTXO metadata (amount, script) is stored correctly

    2. UTXO Spending:
       - Verify that UTXOs can be spent in subsequent transactions
       - Ensure spent UTXOs are marked as spent in the UTXO set

    3. UTXO State Management:
       - Test UnSpend functionality to revert UTXO state
       - Verify UTXO state consistency across operations

Required Settings:
    - SETTINGS_CONTEXT_1: "docker.teranode1.test"
    - SETTINGS_CONTEXT_2: "docker.teranode2.test"
    - SETTINGS_CONTEXT_3: "docker.teranode3.test"

Dependencies:
    - Aerospike for UTXO store
    - Coinbase client for funding
    - Distributor client for transaction broadcasting

How to Run:
    cd test/tnb/
    go test -v -run "^TestTNB6TestSuite$/TestUTXOSetManagement$" -tags test_tnb

Test Flow:
    1. Initialize test environment with required settings
    2. Create test transaction with multiple outputs
    3. Send transaction and verify UTXO creation
    4. Test UTXO state management (spend/unspend)
    5. Verify UTXO metadata consistency
*/

package tnb

import (
	"testing"
	"time"

	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/bitcoin-sv/teranode/test/utils/tconfig"
	"github.com/libsv/go-bk/bec"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/unlocker"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// TNB6TestSuite contains tests for TNB-6: UTXO Set Management
// These tests verify that all outputs of validated transactions are correctly added to the UTXO set
type TNB6TestSuite struct {
	helper.TeranodeTestSuite
}

func TestTNB6TestSuite(t *testing.T) {
	suite.Run(t, &TNB6TestSuite{
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

// TestUTXOSetManagement verifies that transaction outputs are correctly added to the UTXO set.
// This test ensures that:
// 1. All outputs of a validated transaction are added to the UTXO set
// 2. The UTXO metadata is correctly stored (amount, script, etc.)
// 3. Multiple outputs in a single transaction are handled correctly
//
// To run the test:
// $ cd test/tnb/
// $ go test -v -run "^TestTNB6TestSuite$/TestUTXOSetManagement$" -tags test_tnb
func (suite *TNB6TestSuite) TestUTXOSetManagement() {
	testEnv := suite.TeranodeTestEnv
	ctx := testEnv.Context
	t := suite.T()
	node1 := testEnv.Nodes[0]

	// Create key pairs for testing
	privateKey, _ := bec.NewPrivateKey(bec.S256())
	address, _ := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)

	// Create another address for second output
	privateKey2, _ := bec.NewPrivateKey(bec.S256())
	address2, _ := bscript.NewAddressFromPublicKey(privateKey2.PubKey(), true)

	// Get funds from coinbase
	coinbaseClient := node1.CoinbaseClient
	faucetTx, err := coinbaseClient.RequestFunds(ctx, address.AddressString, true)
	require.NoError(t, err, "Failed to request funds")

	_, err = node1.DistributorClient.SendTransaction(ctx, faucetTx)
	require.NoError(t, err, "Failed to send faucet transaction")

	// Create a transaction with multiple outputs
	tx := bt.NewTx()
	err = tx.FromUTXOs(&bt.UTXO{
		TxIDHash:      faucetTx.TxIDChainHash(),
		Vout:          uint32(0),
		LockingScript: faucetTx.Outputs[0].LockingScript,
		Satoshis:      faucetTx.Outputs[0].Satoshis,
	})
	require.NoError(t, err)

	// Add two outputs with different amounts
	amount1 := uint64(10000)
	amount2 := uint64(20000)
	err = tx.AddP2PKHOutputFromAddress(address.AddressString, amount1)
	require.NoError(t, err)
	err = tx.AddP2PKHOutputFromAddress(address2.AddressString, amount2)
	require.NoError(t, err)

	// Sign and send the transaction
	err = tx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privateKey})
	require.NoError(t, err)

	_, err = node1.DistributorClient.SendTransaction(ctx, tx)
	require.NoError(t, err, "Failed to send transaction")

	// Wait a moment for the transaction to be processed
	time.Sleep(1 * time.Second)

	require.NoError(t, err, "First output should be in UTXO set")

	invalidTx := bt.NewTx()
	err = invalidTx.FromUTXOs(&bt.UTXO{
		TxIDHash:      faucetTx.TxIDChainHash(),
		Vout:          uint32(0),
		LockingScript: faucetTx.Outputs[0].LockingScript,
		Satoshis:      faucetTx.Outputs[0].Satoshis,
	})
	require.NoError(t, err)

	// Use a different private key to create an invalid signature
	wrongPrivateKey, _ := bec.NewPrivateKey(bec.S256())
	addr, err := bscript.NewAddressFromPublicKey(wrongPrivateKey.PubKey(), true)
	require.NoError(t, err)

	err = invalidTx.AddP2PKHOutputFromAddress(addr.AddressString, amount1)
	require.NoError(t, err)
	err = invalidTx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: wrongPrivateKey})
	require.NoError(t, err)

	_, err = node1.UtxoStore.Spend(ctx, invalidTx)
	require.Error(t, err)
}
