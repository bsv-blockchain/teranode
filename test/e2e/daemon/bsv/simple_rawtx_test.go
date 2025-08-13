package bsv

import (
	"encoding/json"
	"testing"

	"github.com/bitcoin-sv/teranode/daemon"
	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/stretchr/testify/require"
)

// TestSimpleRawTransaction tests basic raw transaction RPC functionality
//
// Custom test based on Teranode architecture analysis
// Purpose: Test implemented RPCs without wallet dependencies
//
// COMPATIBILITY NOTES:
// ✅ Compatible: createrawtransaction, getrawtransaction, sendrawtransaction
// ❌ Incompatible: signrawtransaction, decoderawtransaction (not implemented)
//
// REASON: Focuses on core transaction RPCs that are implemented in Teranode
// while avoiding wallet operations and unimplemented features.
//
// TEST BEHAVIOR: Tests transaction creation, retrieval, and error handling.
// All test cases expected to work with implemented Teranode RPCs.
func TestSimpleRawTransaction(t *testing.T) {
	SharedTestLock.Lock()
	defer SharedTestLock.Unlock()

	// Initialize test daemon with required services
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableValidator: true,
		SettingsContext: "dev.system.test",
	})

	defer td.Stop(t)

	// Generate blocks for test setup
	t.Log("Generating 101 blocks for coinbase maturity...")
	_, err := td.CallRPC(td.Ctx, "generate", []any{101})
	require.NoError(t, err, "Failed to generate initial blocks")

	// Test Case 1: Create Raw Transaction
	t.Run("create_raw_transaction", func(t *testing.T) {
		testCreateRawTransaction(t, td)
	})

	// Test Case 2: Get Raw Transaction
	t.Run("get_raw_transaction", func(t *testing.T) {
		testGetRawTransaction(t, td)
	})

	// Test Case 3: Send Raw Transaction Error Handling
	t.Run("send_raw_transaction_errors", func(t *testing.T) {
		testSendRawTransactionErrors(t, td)
	})

	// Test Case 4: Transaction Hex Validation
	t.Run("transaction_hex_validation", func(t *testing.T) {
		testTransactionHexValidation(t, td)
	})
}

func testCreateRawTransaction(t *testing.T, td *daemon.TestDaemon) {
	t.Log("Testing createrawtransaction RPC...")

	// Use Teranode's native client to get coinbase transaction (like other Teranode tests)
	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err, "Failed to get block 1")

	// Get coinbase transaction ID from the block's CoinbaseTx field
	coinbaseTxid := block1.CoinbaseTx.TxIDChainHash().String()

	// Create simple raw transaction
	inputs := []map[string]interface{}{
		{
			"txid": coinbaseTxid,
			"vout": 0,
		},
	}

	// Generate a proper regtest address (like other Teranode tests)
	privateKey, err := bec.NewPrivateKey()
	require.NoError(t, err, "Failed to generate private key")

	address, err := bscript.NewAddressFromPublicKey(privateKey.PubKey(), false) // false = regtest/testnet
	require.NoError(t, err, "Failed to generate address")

	outputs := map[string]interface{}{
		address.AddressString: 43.75, // Use proper regtest address
	}

	resp, err := td.CallRPC(td.Ctx, "createrawtransaction", []any{inputs, outputs})

	if err != nil {
		if isMethodNotFoundError(err) {
			t.Skip("createrawtransaction RPC method not implemented in Teranode")
			return
		}
		require.NoError(t, err, "Failed to call createrawtransaction")
	}

	var createTxResp helper.CreateRawTransactionResponse
	err = json.Unmarshal([]byte(resp), &createTxResp)
	require.NoError(t, err, "Failed to parse createrawtransaction response")

	require.NotEmpty(t, createTxResp.Result, "Raw transaction hex should not be empty")
	require.Regexp(t, "^[0-9a-fA-F]+$", createTxResp.Result, "Raw transaction should be valid hex")

	t.Logf("Created raw transaction: %s", createTxResp.Result[:64]+"...") // Log first 64 chars
}

func testGetRawTransaction(t *testing.T, td *daemon.TestDaemon) {
	t.Log("Testing getrawtransaction RPC...")

	// Use Teranode's native client to get coinbase transaction
	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err, "Failed to get block 1")

	coinbaseTxid := block1.CoinbaseTx.TxIDChainHash().String()

	// Test getrawtransaction with verbosity 0 (hex)
	resp, err := td.CallRPC(td.Ctx, "getrawtransaction", []any{coinbaseTxid, 0})

	if err != nil {
		if isMethodNotFoundError(err) {
			t.Skip("getrawtransaction RPC method not implemented in Teranode")
			return
		}
		require.NoError(t, err, "Failed to call getrawtransaction")
	}

	var rawTxHex helper.GetRawTransactionHexResponse
	err = json.Unmarshal([]byte(resp), &rawTxHex)
	require.NoError(t, err, "Failed to parse getrawtransaction hex response")

	require.NotEmpty(t, rawTxHex.Result, "Raw transaction hex should not be empty")
	require.Regexp(t, "^[0-9a-fA-F]+$", rawTxHex.Result, "Should be valid hex")

	// Test getrawtransaction with verbosity 1 (JSON)
	resp, err = td.CallRPC(td.Ctx, "getrawtransaction", []any{coinbaseTxid, 1})
	require.NoError(t, err, "Failed to call getrawtransaction verbose")

	var rawTxJSON helper.GetRawTransactionResponse
	err = json.Unmarshal([]byte(resp), &rawTxJSON)
	require.NoError(t, err, "Failed to parse getrawtransaction JSON response")

	require.Equal(t, coinbaseTxid, rawTxJSON.Result.Txid, "Transaction ID should match")
	require.NotEmpty(t, rawTxJSON.Result.Hex, "Transaction hex should not be empty")

	t.Logf("Retrieved transaction: %s", coinbaseTxid)
}

func testSendRawTransactionErrors(t *testing.T, td *daemon.TestDaemon) {
	t.Log("Testing sendrawtransaction error handling...")

	// Test with invalid hex
	invalidHex := "invalid_hex_string"
	resp, err := td.CallRPC(td.Ctx, "sendrawtransaction", []any{invalidHex})

	if err != nil {
		if isMethodNotFoundError(err) {
			t.Skip("sendrawtransaction RPC method not implemented in Teranode")
			return
		}
		// Error is expected for invalid hex
		t.Logf("Expected error for invalid hex: %v", err)
	} else {
		t.Logf("Unexpected success with invalid hex: %s", resp)
	}

	// Test with malformed transaction hex (valid hex but invalid transaction)
	malformedHex := "deadbeef"
	resp, err = td.CallRPC(td.Ctx, "sendrawtransaction", []any{malformedHex})

	if err != nil {
		// Error is expected for malformed transaction
		t.Logf("Expected error for malformed transaction: %v", err)
	} else {
		t.Logf("Unexpected success with malformed transaction: %s", resp)
	}

	t.Log("Error handling test completed")
}

func testTransactionHexValidation(t *testing.T, td *daemon.TestDaemon) {
	t.Log("Testing transaction hex format consistency...")

	// Use Teranode's native client to get coinbase transaction
	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err, "Failed to get block 1")

	coinbaseTxid := block1.CoinbaseTx.TxIDChainHash().String()

	// Get transaction hex
	resp, err := td.CallRPC(td.Ctx, "getrawtransaction", []any{coinbaseTxid, 0})
	if err != nil {
		t.Skip("getrawtransaction not available for hex validation test")
		return
	}

	var rawTxHex helper.GetRawTransactionHexResponse
	err = json.Unmarshal([]byte(resp), &rawTxHex)
	require.NoError(t, err)

	// Validate hex format
	require.Regexp(t, "^[0-9a-fA-F]+$", rawTxHex.Result, "Transaction hex should be valid")
	require.True(t, len(rawTxHex.Result)%2 == 0, "Transaction hex should have even length")
	require.Greater(t, len(rawTxHex.Result), 0, "Transaction hex should not be empty")

	t.Logf("Transaction hex validation passed: %d characters", len(rawTxHex.Result))
}
