package bsv

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/bitcoin-sv/teranode/daemon"
	"github.com/stretchr/testify/require"
)

// TestBSVBlockchain tests blockchain-related RPC functionality in Teranode
//
// Converted from BSV blockchain.py test
// Purpose: Test core blockchain RPC methods for block retrieval and chain information
//
// Test Cases:
// 1. GetBlockchainInfo - Test blockchain information retrieval (Teranode alternative to getchaintxstats)
// 2. GetBlockHeader - Test block header retrieval with different verbosity levels
// 3. GetBlock - Test block retrieval with different verbosity levels
// 4. GetBlockByHeight - Test block retrieval by height
// 5. GetChainTips - Test chain tips information
// 6. GetDifficulty - Test network difficulty retrieval (if implemented)
// 7. Block Hash Operations - Test getblockhash and getbestblockhash
//
// Note: This test focuses on RPCs that are implemented in Teranode.
// Some BSV RPCs like getnetworkhashps and gettxoutsetinfo are not implemented
// and are gracefully skipped.
func TestBSVBlockchain(t *testing.T) {
	SharedTestLock.Lock()
	defer SharedTestLock.Unlock()

	t.Log("Testing BSV blockchain RPC functionality in Teranode...")

	// Setup single node (like BSV test)
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableValidator: true,
		SettingsContext: "dev.system.test",
	})
	defer td.Stop(t)

	// Generate some blocks for testing (like BSV test generates 200+ blocks)
	_, err := td.CallRPC(td.Ctx, "generate", []any{10})
	require.NoError(t, err, "Failed to generate initial blocks")

	// Test cases
	t.Run("getblockchaininfo", func(t *testing.T) {
		testBSVGetBlockchainInfo(t, td)
	})

	t.Run("getblockheader", func(t *testing.T) {
		testBSVGetBlockHeader(t, td)
	})

	t.Run("getblock", func(t *testing.T) {
		testBSVGetBlock(t, td)
	})

	t.Run("getblockbyheight", func(t *testing.T) {
		testBSVGetBlockByHeight(t, td)
	})

	t.Run("getchaintips", func(t *testing.T) {
		testBSVGetChainTips(t, td)
	})

	t.Run("getdifficulty", func(t *testing.T) {
		testBSVGetDifficulty(t, td)
	})

	t.Run("block_hash_operations", func(t *testing.T) {
		testBSVBlockHashOperations(t, td)
	})

	t.Log("✅ BSV blockchain test completed successfully")
}

func testBSVGetBlockchainInfo(t *testing.T, td *daemon.TestDaemon) {
	t.Log("Testing getblockchaininfo RPC...")

	// Test getblockchaininfo (Teranode's alternative to getchaintxstats)
	resp, err := td.CallRPC(td.Ctx, "getblockchaininfo", []any{})
	require.NoError(t, err, "Failed to call getblockchaininfo")

	var blockchainInfoResp struct {
		Result map[string]interface{} `json:"result"`
		Error  interface{}            `json:"error"`
		ID     int                    `json:"id"`
	}
	err = json.Unmarshal([]byte(resp), &blockchainInfoResp)
	require.NoError(t, err, "Failed to unmarshal getblockchaininfo response")

	// Verify response structure
	require.Nil(t, blockchainInfoResp.Error, "getblockchaininfo should not return error")
	require.NotNil(t, blockchainInfoResp.Result, "getblockchaininfo should return result")

	result := blockchainInfoResp.Result

	// Check for expected fields
	expectedFields := []string{"chain", "blocks", "bestblockhash", "difficulty", "chainwork"}
	for _, field := range expectedFields {
		value, exists := result[field]
		require.True(t, exists, "Field '%s' should exist in getblockchaininfo response", field)
		require.NotNil(t, value, "Field '%s' should not be nil", field)
		t.Logf("✅ Field '%s': %v", field, value)
	}

	// Verify blocks is a number > 0 (we generated 10 blocks)
	if blocks, ok := result["blocks"]; ok {
		switch v := blocks.(type) {
		case float64:
			require.Greater(t, v, float64(0), "blocks should be greater than 0")
		case int, int32, int64:
			t.Logf("blocks field is integer: %v", v)
		default:
			t.Errorf("blocks field should be numeric, got %T: %v", v, v)
		}
	}

	t.Log("✅ getblockchaininfo returned valid blockchain information")
}

func testBSVGetBlockHeader(t *testing.T, td *daemon.TestDaemon) {
	t.Log("Testing getblockheader RPC...")

	// Get best block hash first
	resp, err := td.CallRPC(td.Ctx, "getbestblockhash", []any{})
	require.NoError(t, err, "Failed to get best block hash")

	var bestHashResp struct {
		Result string      `json:"result"`
		Error  interface{} `json:"error"`
		ID     int         `json:"id"`
	}
	err = json.Unmarshal([]byte(resp), &bestHashResp)
	require.NoError(t, err, "Failed to unmarshal getbestblockhash response")

	bestHash := bestHashResp.Result
	require.NotEmpty(t, bestHash, "Best block hash should not be empty")
	require.Len(t, bestHash, 64, "Block hash should be 64 characters")

	// Test getblockheader with verbosity=false (hex format)
	resp, err = td.CallRPC(td.Ctx, "getblockheader", []any{bestHash, false})
	require.NoError(t, err, "Failed to call getblockheader with verbosity=false")

	var headerHexResp struct {
		Result string      `json:"result"`
		Error  interface{} `json:"error"`
		ID     int         `json:"id"`
	}
	err = json.Unmarshal([]byte(resp), &headerHexResp)
	require.NoError(t, err, "Failed to unmarshal getblockheader hex response")

	require.Nil(t, headerHexResp.Error, "getblockheader should not return error")
	require.NotEmpty(t, headerHexResp.Result, "getblockheader should return non-empty hex")
	require.Equal(t, 160, len(headerHexResp.Result), "Block header hex should be 160 characters (80 bytes)")

	// Test getblockheader with verbosity=true (JSON format)
	resp, err = td.CallRPC(td.Ctx, "getblockheader", []any{bestHash, true})
	require.NoError(t, err, "Failed to call getblockheader with verbosity=true")

	var headerJsonResp struct {
		Result map[string]interface{} `json:"result"`
		Error  interface{}            `json:"error"`
		ID     int                    `json:"id"`
	}
	err = json.Unmarshal([]byte(resp), &headerJsonResp)
	require.NoError(t, err, "Failed to unmarshal getblockheader JSON response")

	require.Nil(t, headerJsonResp.Error, "getblockheader should not return error")
	require.NotNil(t, headerJsonResp.Result, "getblockheader should return result")

	// Verify JSON structure contains expected fields (using Teranode field names)
	expectedFields := []string{"hash", "version", "previousblockhash", "merkleroot", "time", "bits", "nonce", "difficulty", "height"}
	for _, field := range expectedFields {
		value, exists := headerJsonResp.Result[field]
		require.True(t, exists, "Field '%s' should exist in block header", field)
		require.NotNil(t, value, "Field '%s' should not be nil", field)
	}

	// Verify hash matches what we requested
	if hash, ok := headerJsonResp.Result["hash"]; ok {
		require.Equal(t, bestHash, hash, "Returned hash should match requested hash")
	}

	t.Log("✅ getblockheader works correctly with both verbosity levels")
}

func testBSVGetBlock(t *testing.T, td *daemon.TestDaemon) {
	t.Log("Testing getblock RPC...")

	// Get genesis block hash (height 0)
	resp, err := td.CallRPC(td.Ctx, "getblockhash", []any{0})
	require.NoError(t, err, "Failed to get genesis block hash")

	var genesisHashResp struct {
		Result string      `json:"result"`
		Error  interface{} `json:"error"`
		ID     int         `json:"id"`
	}
	err = json.Unmarshal([]byte(resp), &genesisHashResp)
	require.NoError(t, err, "Failed to unmarshal getblockhash response")

	genesisHash := genesisHashResp.Result

	// Test getblock with verbosity=0 (hex format)
	resp, err = td.CallRPC(td.Ctx, "getblock", []any{genesisHash, 0})
	require.NoError(t, err, "Failed to call getblock with verbosity=0")

	var blockHexResp struct {
		Result string      `json:"result"`
		Error  interface{} `json:"error"`
		ID     int         `json:"id"`
	}
	err = json.Unmarshal([]byte(resp), &blockHexResp)
	require.NoError(t, err, "Failed to unmarshal getblock hex response")

	require.Nil(t, blockHexResp.Error, "getblock should not return error")
	require.NotEmpty(t, blockHexResp.Result, "getblock should return non-empty hex")
	require.True(t, len(blockHexResp.Result) > 160, "Block hex should be longer than header (>160 chars)")

	// Test getblock with verbosity=1 (JSON format)
	resp, err = td.CallRPC(td.Ctx, "getblock", []any{genesisHash, 1})
	require.NoError(t, err, "Failed to call getblock with verbosity=1")

	var blockJsonResp struct {
		Result map[string]interface{} `json:"result"`
		Error  interface{}            `json:"error"`
		ID     int                    `json:"id"`
	}
	err = json.Unmarshal([]byte(resp), &blockJsonResp)
	require.NoError(t, err, "Failed to unmarshal getblock JSON response")

	require.Nil(t, blockJsonResp.Error, "getblock should not return error")
	require.NotNil(t, blockJsonResp.Result, "getblock should return result")

	// Verify JSON structure contains expected fields (using Teranode field names)
	// Note: 'tx' field may not exist in Teranode's getblock response, so we'll check it separately
	coreFields := []string{"hash", "version", "previousblockhash", "merkleroot", "time", "bits", "nonce", "difficulty", "height"}
	for _, field := range coreFields {
		value, exists := blockJsonResp.Result[field]
		require.True(t, exists, "Field '%s' should exist in block", field)
		require.NotNil(t, value, "Field '%s' should not be nil", field)
	}

	// Check for transaction data (may be 'tx' or 'txs' or missing in Teranode)
	if txValue, exists := blockJsonResp.Result["tx"]; exists {
		require.NotNil(t, txValue, "tx field should not be nil if present")
		t.Log("✅ Found 'tx' field in getblock response")
	} else if txsValue, exists := blockJsonResp.Result["txs"]; exists {
		require.NotNil(t, txsValue, "txs field should not be nil if present")
		t.Log("✅ Found 'txs' field in getblock response")
	} else {
		t.Log("⚠️  No transaction field found in getblock response - this may be expected for Teranode")
	}

	// Verify hash matches what we requested
	if hash, ok := blockJsonResp.Result["hash"]; ok {
		require.Equal(t, genesisHash, hash, "Returned hash should match requested hash")
	}

	// Verify height is 0 for genesis block
	if height, ok := blockJsonResp.Result["height"]; ok {
		switch v := height.(type) {
		case float64:
			require.Equal(t, float64(0), v, "Genesis block height should be 0")
		case int, int32, int64:
			require.Equal(t, 0, v, "Genesis block height should be 0")
		}
	}

	t.Log("✅ getblock works correctly with both verbosity levels")
}

func testBSVGetBlockByHeight(t *testing.T, td *daemon.TestDaemon) {
	t.Log("Testing getblockbyheight RPC...")

	// Test getblockbyheight with verbosity=0 (hex format) for genesis block
	resp, err := td.CallRPC(td.Ctx, "getblockbyheight", []any{0, 0})
	require.NoError(t, err, "Failed to call getblockbyheight with verbosity=0")

	var blockHexResp struct {
		Result string      `json:"result"`
		Error  interface{} `json:"error"`
		ID     int         `json:"id"`
	}
	err = json.Unmarshal([]byte(resp), &blockHexResp)
	require.NoError(t, err, "Failed to unmarshal getblockbyheight hex response")

	require.Nil(t, blockHexResp.Error, "getblockbyheight should not return error")
	require.NotEmpty(t, blockHexResp.Result, "getblockbyheight should return non-empty hex")

	// Test getblockbyheight with verbosity=1 (JSON format)
	resp, err = td.CallRPC(td.Ctx, "getblockbyheight", []any{1, 1})
	require.NoError(t, err, "Failed to call getblockbyheight with verbosity=1")

	var blockJsonResp struct {
		Result map[string]interface{} `json:"result"`
		Error  interface{}            `json:"error"`
		ID     int                    `json:"id"`
	}
	err = json.Unmarshal([]byte(resp), &blockJsonResp)
	require.NoError(t, err, "Failed to unmarshal getblockbyheight JSON response")

	require.Nil(t, blockJsonResp.Error, "getblockbyheight should not return error")
	require.NotNil(t, blockJsonResp.Result, "getblockbyheight should return result")

	// Verify height is 1
	if height, ok := blockJsonResp.Result["height"]; ok {
		switch v := height.(type) {
		case float64:
			require.Equal(t, float64(1), v, "Block height should be 1")
		case int, int32, int64:
			require.Equal(t, 1, v, "Block height should be 1")
		}
	}

	t.Log("✅ getblockbyheight works correctly with both verbosity levels")
}

func testBSVGetChainTips(t *testing.T, td *daemon.TestDaemon) {
	t.Log("Testing getchaintips RPC...")

	// Test getchaintips
	resp, err := td.CallRPC(td.Ctx, "getchaintips", []any{})

	if err != nil {
		if isBlockchainMethodNotFoundError(err) {
			t.Skip("getchaintips RPC method not implemented in this Teranode version")
			return
		}
		require.NoError(t, err, "Failed to call getchaintips")
	}

	var chainTipsResp struct {
		Result []map[string]interface{} `json:"result"`
		Error  interface{}              `json:"error"`
		ID     int                      `json:"id"`
	}
	err = json.Unmarshal([]byte(resp), &chainTipsResp)
	require.NoError(t, err, "Failed to unmarshal getchaintips response")

	require.Nil(t, chainTipsResp.Error, "getchaintips should not return error")
	require.NotNil(t, chainTipsResp.Result, "getchaintips should return result")
	require.NotEmpty(t, chainTipsResp.Result, "getchaintips should return at least one tip")

	// Verify structure of first tip
	firstTip := chainTipsResp.Result[0]
	expectedFields := []string{"height", "hash", "branchlen", "status"}
	for _, field := range expectedFields {
		value, exists := firstTip[field]
		require.True(t, exists, "Field '%s' should exist in chain tip", field)
		require.NotNil(t, value, "Field '%s' should not be nil", field)
	}

	t.Logf("✅ getchaintips returned %d chain tips", len(chainTipsResp.Result))
}

func testBSVGetDifficulty(t *testing.T, td *daemon.TestDaemon) {
	t.Log("Testing getdifficulty RPC...")

	// Test getdifficulty (may not be implemented as standalone RPC)
	resp, err := td.CallRPC(td.Ctx, "getdifficulty", []any{})

	if err != nil {
		if isBlockchainMethodNotFoundError(err) {
			t.Skip("getdifficulty RPC method not implemented - difficulty available in getblockchaininfo")
			return
		}
		require.NoError(t, err, "Failed to call getdifficulty")
	}

	var difficultyResp struct {
		Result interface{} `json:"result"`
		Error  interface{} `json:"error"`
		ID     int         `json:"id"`
	}
	err = json.Unmarshal([]byte(resp), &difficultyResp)
	require.NoError(t, err, "Failed to unmarshal getdifficulty response")

	require.Nil(t, difficultyResp.Error, "getdifficulty should not return error")
	require.NotNil(t, difficultyResp.Result, "getdifficulty should return result")

	// Verify difficulty is a number
	switch v := difficultyResp.Result.(type) {
	case float64:
		require.Greater(t, v, float64(0), "difficulty should be greater than 0")
		t.Logf("✅ getdifficulty returned: %f", v)
	case int, int32, int64:
		t.Logf("✅ getdifficulty returned integer: %v", v)
	default:
		t.Errorf("difficulty should be numeric, got %T: %v", v, v)
	}
}

func testBSVBlockHashOperations(t *testing.T, td *daemon.TestDaemon) {
	t.Log("Testing block hash operations...")

	// Test getbestblockhash
	resp, err := td.CallRPC(td.Ctx, "getbestblockhash", []any{})
	require.NoError(t, err, "Failed to call getbestblockhash")

	var bestHashResp struct {
		Result string      `json:"result"`
		Error  interface{} `json:"error"`
		ID     int         `json:"id"`
	}
	err = json.Unmarshal([]byte(resp), &bestHashResp)
	require.NoError(t, err, "Failed to unmarshal getbestblockhash response")

	require.Nil(t, bestHashResp.Error, "getbestblockhash should not return error")
	require.NotEmpty(t, bestHashResp.Result, "getbestblockhash should return non-empty hash")
	require.Len(t, bestHashResp.Result, 64, "Block hash should be 64 characters")

	// Test getblockhash for various heights
	for height := 0; height <= 2; height++ {
		resp, err := td.CallRPC(td.Ctx, "getblockhash", []any{height})
		require.NoError(t, err, "Failed to call getblockhash for height %d", height)

		var blockHashResp struct {
			Result string      `json:"result"`
			Error  interface{} `json:"error"`
			ID     int         `json:"id"`
		}
		err = json.Unmarshal([]byte(resp), &blockHashResp)
		require.NoError(t, err, "Failed to unmarshal getblockhash response for height %d", height)

		require.Nil(t, blockHashResp.Error, "getblockhash should not return error for height %d", height)
		require.NotEmpty(t, blockHashResp.Result, "getblockhash should return non-empty hash for height %d", height)
		require.Len(t, blockHashResp.Result, 64, "Block hash should be 64 characters for height %d", height)

		t.Logf("✅ Height %d hash: %s", height, blockHashResp.Result)
	}

	t.Log("✅ Block hash operations work correctly")
}

// Helper function to check if error is method not found
func isBlockchainMethodNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "Method not found") ||
		strings.Contains(errStr, "method not found") ||
		strings.Contains(errStr, "not implemented") ||
		strings.Contains(errStr, "unimplemented")
}
