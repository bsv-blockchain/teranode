package bsv

import (
	"encoding/json"
	"testing"

	"github.com/bitcoin-sv/teranode/daemon"
	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBlockchainRPC tests blockchain-related RPC methods
//
// Converted from: bitcoin-sv/test/functional/blockchain.py
// Purpose: Verify that blockchain-related RPC methods return correct information about the blockchain state
//
// COMPATIBILITY NOTES:
// ✅ Compatible: getdifficulty, getblock, getblockheader, getblockchaininfo
// ❌ Incompatible: getchaintxstats (not implemented), gettxoutsetinfo (wallet command),
//
//	getnetworkhashps (unimplemented)
//
// REASON:
//
// TEST BEHAVIOR: Focuses on compatible RPCs only. All 4 test cases pass successfully.
func TestOriginalBlockchainRPC(t *testing.T) {
	SharedTestLock.Lock()
	defer SharedTestLock.Unlock()

	// Initialize test daemon with required services
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableValidator: true,
		SettingsContext: "dev.system.test",
	})

	defer td.Stop(t)

	// Set up initial blockchain state - generate 200 blocks
	t.Log("Generating 200 blocks for test setup...")
	_, err := td.CallRPC(td.Ctx, "generate", []any{200})
	require.NoError(t, err, "Failed to generate initial blocks")

	// Test Case 1: getdifficulty RPC (Compatible)
	t.Run("getdifficulty", func(t *testing.T) {
		testOriginalGetDifficulty(t, td)
	})

	// Test Case 2: getblock RPC with different verbosity (Compatible)
	t.Run("getblock_verbosity", func(t *testing.T) {
		testOriginalGetBlockVerbosity(t, td)
	})

	// Test Case 3: getblockheader RPC (Compatible)
	t.Run("getblockheader", func(t *testing.T) {
		testOriginalGetBlockHeader(t, td)
	})

	// Test Case 4: getblockchaininfo RPC (Teranode alternative to getchaintxstats)
	t.Run("getblockchaininfo", func(t *testing.T) {
		testOriginalGetBlockchainInfo(t, td)
	})

	// Note: The following BSV RPCs are not implemented in Teranode:
	// - getchaintxstats (not implemented)
	// - gettxoutsetinfo (wallet command, not supported)
	// - getnetworkhashps (command unimplemented)
}

func testOriginalGetBlockchainInfo(t *testing.T, td *daemon.TestDaemon) {
	t.Log("Testing getblockchaininfo RPC (Teranode alternative to getchaintxstats)...")

	resp, err := td.CallRPC(td.Ctx, "getblockchaininfo", []any{})
	require.NoError(t, err, "Failed to call getblockchaininfo")

	var blockchainInfo helper.BlockchainInfo
	err = json.Unmarshal([]byte(resp), &blockchainInfo)
	require.NoError(t, err, "Failed to parse getblockchaininfo response")

	// Verify blockchain information
	assert.Equal(t, 200, blockchainInfo.Result.Blocks, "Expected 200 blocks")
	assert.Equal(t, "regtest", blockchainInfo.Result.Chain, "Expected regtest chain")
	assert.NotEmpty(t, blockchainInfo.Result.BestBlockHash, "Best block hash should not be empty")
	assert.Len(t, blockchainInfo.Result.BestBlockHash, 64, "Best block hash should be 64 characters")
	assert.Greater(t, blockchainInfo.Result.Headers, 0, "Headers should be positive")

	t.Logf("Blockchain info verified: blocks=%d, chain=%s, best_hash=%s",
		blockchainInfo.Result.Blocks,
		blockchainInfo.Result.Chain,
		blockchainInfo.Result.BestBlockHash)
}

func testOriginalGetDifficulty(t *testing.T, td *daemon.TestDaemon) {
	t.Log("Testing getdifficulty RPC...")

	resp, err := td.CallRPC(td.Ctx, "getdifficulty", []any{})
	require.NoError(t, err, "Failed to call getdifficulty")

	var difficulty helper.GetDifficultyResponse
	err = json.Unmarshal([]byte(resp), &difficulty)
	require.NoError(t, err, "Failed to parse getdifficulty response")

	// Verify difficulty is a reasonable value for regtest
	assert.Greater(t, difficulty.Result, float64(0), "Difficulty should be positive")
	assert.Less(t, difficulty.Result, float64(1), "Difficulty should be low for regtest")

	t.Logf("Difficulty verified: %f", difficulty.Result)
}

func testOriginalGetBlockVerbosity(t *testing.T, td *daemon.TestDaemon) {
	t.Log("Testing getblock RPC with different verbosity levels...")

	// Get a block hash to test with
	resp, err := td.CallRPC(td.Ctx, "getblockhash", []any{100})
	require.NoError(t, err, "Failed to get block hash")

	var blockHashResp helper.GetBlockHashResponse
	err = json.Unmarshal([]byte(resp), &blockHashResp)
	require.NoError(t, err)

	blockHash := blockHashResp.Result

	// Test verbosity 0 (raw hex)
	resp, err = td.CallRPC(td.Ctx, "getblock", []any{blockHash, 0})
	require.NoError(t, err, "Failed to call getblock with verbosity 0")

	var blockHex helper.GetBlockHexResponse
	err = json.Unmarshal([]byte(resp), &blockHex)
	require.NoError(t, err)

	assert.NotEmpty(t, blockHex.Result, "Block hex should not be empty")
	assert.Regexp(t, "^[0-9a-fA-F]+$", blockHex.Result, "Block should be valid hex")

	// Test verbosity 1 (JSON with tx IDs)
	resp, err = td.CallRPC(td.Ctx, "getblock", []any{blockHash, 1})
	require.NoError(t, err, "Failed to call getblock with verbosity 1")

	var blockJSON helper.GetBlockByHeightResponse
	err = json.Unmarshal([]byte(resp), &blockJSON)
	require.NoError(t, err)

	assert.Equal(t, blockHash, blockJSON.Result.Hash, "Block hash should match")
	assert.Greater(t, blockJSON.Result.Height, uint32(0), "Block height should be positive")
	assert.NotEmpty(t, blockJSON.Result.Merkleroot, "Merkle root should not be empty")

	t.Logf("Block verbosity tests passed for block %s at height %d",
		blockHash, blockJSON.Result.Height)
}

func testOriginalGetBlockHeader(t *testing.T, td *daemon.TestDaemon) {
	t.Log("Testing getblockheader RPC...")

	// Get a block hash to test with
	resp, err := td.CallRPC(td.Ctx, "getblockhash", []any{50})
	require.NoError(t, err, "Failed to get block hash")

	var blockHashResp helper.GetBlockHashResponse
	err = json.Unmarshal([]byte(resp), &blockHashResp)
	require.NoError(t, err)

	blockHash := blockHashResp.Result

	// Test verbose=true (default)
	resp, err = td.CallRPC(td.Ctx, "getblockheader", []any{blockHash, true})
	require.NoError(t, err, "Failed to call getblockheader verbose")

	var headerJSON helper.BlockHeaderResponse
	err = json.Unmarshal([]byte(resp), &headerJSON)
	require.NoError(t, err)

	assert.Equal(t, blockHash, headerJSON.Result.Hash, "Header hash should match")
	assert.Greater(t, headerJSON.Result.Height, uint32(0), "Header height should be positive")
	assert.NotEmpty(t, headerJSON.Result.Merkleroot, "Merkle root should not be empty")
	assert.Greater(t, headerJSON.Result.Time, int64(0), "Time should be positive")

	// Test verbose=false (hex)
	resp, err = td.CallRPC(td.Ctx, "getblockheader", []any{blockHash, false})
	require.NoError(t, err, "Failed to call getblockheader hex")

	var headerHex helper.GetBlockHeaderHexResponse
	err = json.Unmarshal([]byte(resp), &headerHex)
	require.NoError(t, err)

	assert.NotEmpty(t, headerHex.Result, "Header hex should not be empty")
	assert.Regexp(t, "^[0-9a-fA-F]+$", headerHex.Result, "Header should be valid hex")
	assert.Equal(t, 160, len(headerHex.Result), "Header should be 80 bytes (160 hex chars)")

	t.Logf("Block header tests passed for block %s", blockHash)
}
