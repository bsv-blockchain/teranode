//go:build test_tnb || debug

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
       - Test Unspend functionality to revert UTXO state
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
    go test -v -run "^TestTNB6TestSuite$/TestUTXOSetManagement$" -tags test_tnb ./test/tnb/tnb6_test.go

Test Flow:
    1. Initialize test environment with required settings
    2. Create test transaction with multiple outputs
    3. Send transaction and verify UTXO creation
    4. Test UTXO state management (spend/unspend)
    5. Verify UTXO metadata consistency
*/

package tnb

import (
	"context"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/stores/utxo/meta"
	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/bitcoin-sv/teranode/test/utils/tconfig"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/libsv/go-bk/bec"
	"github.com/libsv/go-bk/wif"
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
// $ go test -v -run "^TestTNB6TestSuite$/TestUTXOSetManagement$" -tags test_tnb ./test/tnb/tnb6_test.go

func (suite *TNB6TestSuite) TestUnspentTransactionOutputs() {
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
	block1, err := node1.BlockchainClient.GetBlockByHeight(ctx, 1)
	require.NoError(t, err)

	coinbaseTx := block1.CoinbaseTx

	w, err := wif.DecodeWIF(node1.Settings.BlockAssembly.MinerWalletPrivateKeys[0])
	require.NoError(t, err)

	coinbaseTxPrivateKey := w.PrivKey

	// Create a transaction with multiple outputs
	tx := bt.NewTx()
	err = tx.FromUTXOs(&bt.UTXO{
		TxIDHash:      coinbaseTx.TxIDChainHash(),
		Vout:          uint32(0),
		LockingScript: coinbaseTx.Outputs[0].LockingScript,
		Satoshis:      coinbaseTx.Outputs[0].Satoshis,
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
	err = tx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: coinbaseTxPrivateKey})
	require.NoError(t, err)

	_, err = node1.DistributorClient.SendTransaction(ctx, tx)
	require.NoError(t, err, "Failed to send transaction")

	ctxTimeout, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	utxoReady := make(chan *meta.Data)
	go func() {
		defer close(utxoReady)
		for {
			utxos, err := node1.UtxoStore.Get(ctx, tx.TxIDChainHash())
			if err == nil && utxos != nil {
				utxoReady <- utxos
				return
			}
			select {
			case <-ctxTimeout.Done():
				return
			default:
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()

	select {
	case utxos := <-utxoReady:
		t.Logf("UTXO meta data: %+v\n", utxos)

		if utxos.Tx != nil {
			for i, out := range utxos.Tx.Outputs {
				t.Logf("UTXO #%d: Value=%d Satoshis, Script=%x\n", i, out.Satoshis, *out.LockingScript)
				utxoHash, _ := util.UTXOHashFromOutput(tx.TxIDChainHash(), out, uint32(i))
				spend := &utxo.Spend{
					TxID:     tx.TxIDChainHash(),
					Vout:     uint32(i),
					UTXOHash: utxoHash,
				}
				spendStatus, err := node1.UtxoStore.GetSpend(ctx, spend)
				require.NoError(t, err)
				t.Logf("UTXO #%d spend status: %+v\n", i, spendStatus)
				require.Equal(t, spendStatus.Status, 0)
				require.Nil(t, spendStatus.SpendingTxID)
			}
		} else {
			t.Logf("No tx found into meta.Data")
		}

	case <-ctxTimeout.Done():
		t.Fatalf("Timeout waiting for transaction to be processed")
	}
}
