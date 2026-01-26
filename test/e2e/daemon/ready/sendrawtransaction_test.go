package smoke

import (
	"encoding/hex"
	"encoding/json"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/test"
	"github.com/bsv-blockchain/teranode/test/utils/aerospike"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/stretchr/testify/require"
)

// TestSendRawTransaction tests the sendrawtransaction RPC endpoint with a complete daemon setup.
// This test verifies that transactions can be successfully submitted via RPC and validates that
// the txStore and validatorClient dependencies are properly initialized.
func TestSendRawTransaction(t *testing.T) {
	// Create test daemon with RPC and validator enabled
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableValidator: true,
		UTXOStoreType:   "aerospike",
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
			func(s *settings.Settings) {
				s.TracingEnabled = true
				s.TracingSampleRate = 1.0
				s.ChainCfgParams.CoinbaseMaturity = 2
			},
		),
		FSMState: blockchain.FSMStateRUNNING,
	})

	defer td.Stop(t, true)

	// Mine blocks to maturity and get a spendable coinbase transaction
	coinbaseTx := td.MineToMaturityAndGetSpendableCoinbaseTx(t, td.Ctx)

	// Create a transaction that spends the coinbase
	tx := td.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTx, 0),
		transactions.WithP2PKHOutputs(1, 50000), // 50000 satoshis
	)

	// Encode transaction to hex
	txHex := hex.EncodeToString(tx.ExtendedBytes())
	t.Logf("Transaction hex: %s", txHex)

	// Send the transaction via RPC
	resp, err := td.CallRPC(td.Ctx, "sendrawtransaction", []any{txHex})
	require.NoError(t, err, "sendrawtransaction should succeed")

	// Parse the response
	var sendRawTxResp struct {
		Result string      `json:"result"`
		Error  interface{} `json:"error"`
		ID     int         `json:"id"`
	}
	err = json.Unmarshal([]byte(resp), &sendRawTxResp)
	require.NoError(t, err, "Failed to parse response")
	require.Nil(t, sendRawTxResp.Error, "RPC should not return an error")

	// Verify the returned transaction ID matches
	expectedTxID := tx.TxID()
	require.Equal(t, expectedTxID, sendRawTxResp.Result, "Returned txid should match")

	t.Logf("Successfully sent transaction: %s", sendRawTxResp.Result)

	// Wait for block assembly to process the transaction
	td.WaitForBlockAssemblyToProcessTx(t, expectedTxID)

	// Verify we can retrieve the transaction via getrawtransaction
	getRawResp, err := td.CallRPC(td.Ctx, "getrawtransaction", []any{expectedTxID, 0})
	require.NoError(t, err, "getrawtransaction should succeed")

	var getRawTxResp struct {
		Result string      `json:"result"`
		Error  interface{} `json:"error"`
		ID     int         `json:"id"`
	}
	err = json.Unmarshal([]byte(getRawResp), &getRawTxResp)
	require.NoError(t, err, "Failed to parse getrawtransaction response")
	require.Nil(t, getRawTxResp.Error, "getrawtransaction should not return an error")
	require.NotEmpty(t, getRawTxResp.Result, "getrawtransaction should return transaction hex")

	t.Logf("Retrieved transaction hex length: %d", len(getRawTxResp.Result))
}

// TestSendRawTransactionInvalidTx tests that sendrawtransaction properly rejects invalid transactions.
func TestSendRawTransactionInvalidTx(t *testing.T) {
	// aerospike
	utxoStoreURL, teardown, err := aerospike.InitAerospikeContainer()
	require.NoError(t, err, "Failed to setup Aerospike container")
	parsedURL, err := url.Parse(utxoStoreURL)
	require.NoError(t, err, "Failed to parse UTXO store URL")
	t.Cleanup(func() {
		_ = teardown()
	})

	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableValidator: true,
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
			func(s *settings.Settings) {
				s.UtxoStore.UtxoStore = parsedURL
			},
		),
	})

	defer td.Stop(t, true)

	// Test with invalid hex - CallRPC returns error when RPC response has error field
	invalidHex := "not_valid_hex"
	_, err = td.CallRPC(td.Ctx, "sendrawtransaction", []any{invalidHex})
	require.Error(t, err, "Should return an error for invalid hex")
	t.Logf("Invalid hex rejected with error: %v", err)

	// Test with valid hex but invalid transaction format
	invalidTxHex := "0102030405060708"
	_, err = td.CallRPC(td.Ctx, "sendrawtransaction", []any{invalidTxHex})
	require.Error(t, err, "Should return an error for invalid transaction")
	t.Logf("Invalid transaction rejected with error: %v", err)
}

// TestSendRawTransactionDoubleSpend tests that sendrawtransaction rejects double-spend attempts.
func TestSendRawTransactionDoubleSpend(t *testing.T) {
	// FLAKY test
	t.Skip()

	// aerospike
	utxoStoreURL, teardown, err := aerospike.InitAerospikeContainer()
	require.NoError(t, err, "Failed to setup Aerospike container")
	parsedURL, err := url.Parse(utxoStoreURL)
	require.NoError(t, err, "Failed to parse UTXO store URL")
	t.Cleanup(func() {
		_ = teardown()
	})

	// Create test daemon with RPC and validator enabled
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableValidator: true,
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
			func(s *settings.Settings) {
				s.TracingEnabled = true
				s.TracingSampleRate = 1.0
				s.ChainCfgParams.CoinbaseMaturity = 2
				s.UtxoStore.UtxoStore = parsedURL
			},
		),
		FSMState: blockchain.FSMStateRUNNING,
	})

	defer td.Stop(t, true)

	// Mine blocks and get spendable coinbase
	coinbaseTx := td.MineToMaturityAndGetSpendableCoinbaseTx(t, td.Ctx)

	// Create and send first transaction
	tx1 := td.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTx, 0),
		transactions.WithP2PKHOutputs(1, 50000),
	)

	txHex1 := hex.EncodeToString(tx1.ExtendedBytes())
	resp, err := td.CallRPC(td.Ctx, "sendrawtransaction", []any{txHex1})
	require.NoError(t, err)

	var sendRawTxResp struct {
		Result string      `json:"result"`
		Error  interface{} `json:"error"`
		ID     int         `json:"id"`
	}
	err = json.Unmarshal([]byte(resp), &sendRawTxResp)
	require.NoError(t, err)
	require.Nil(t, sendRawTxResp.Error, "First transaction should succeed")

	t.Logf("First transaction sent: %s", sendRawTxResp.Result)

	// Wait for transaction to be processed
	td.WaitForBlockAssemblyToProcessTx(t, tx1.TxID())

	// Create second transaction that spends the same UTXO (double spend)
	tx2 := td.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTx, 0),   // Same input as tx1
		transactions.WithP2PKHOutputs(1, 40000), // Different amount
	)

	txHex2 := hex.EncodeToString(tx2.ExtendedBytes())
	_, err = td.CallRPC(td.Ctx, "sendrawtransaction", []any{txHex2})
	require.Error(t, err, "Double spend should be rejected")

	t.Logf("Double spend rejected with error: %v", err)
}
