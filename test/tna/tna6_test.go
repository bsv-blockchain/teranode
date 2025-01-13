//go:build test_all || test_tna

// How to run this test manually:
// $ cd test/tna
// $ go test -v -run "^TestTNA6TestSuite$/TestAcceptanceNextBlock$" -tags test_tna
//
// To run all TNA tests:
// $ go test -v -tags test_tna ./...
//
// Prerequisites:
// 1. Docker must be running
// 2. Docker compose must be installed
// 3. The following ports must be available:
//    - 16090-16092: Node API ports
//
// Test Description:
// This test verifies TNA-6 requirement: Teranode must express its acceptance of a block
// by working on creating the next block in the chain, using the hash of the accepted
// block as the previous hash.
//
// The test uses three Teranode instances in a Docker environment to verify that:
// 1. A block can be successfully mined
// 2. The block is accepted by the network
// 3. All nodes demonstrate acceptance by using the block's hash as their previous hash
//    when working on the next block

package tna

import (
	"testing"
	"time"

	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// TNA6TestSuite tests that Teranode expresses block acceptance by using the accepted
// block's hash as the previous hash when working on the next block. This verifies
// that Teranode is actively building on top of blocks it has accepted.
type TNA6TestSuite struct {
	helper.TeranodeTestSuite
}

// InitSuite initializes the test suite with configuration settings for three test nodes.
// Each node is configured with a unique docker context for isolated testing.
func (suite *TNA6TestSuite) InitSuite() {
	suite.SettingsMap = map[string]string{
		"SETTINGS_CONTEXT_1": "docker.teranode1.test.tna1Test",
		"SETTINGS_CONTEXT_2": "docker.teranode2.test.tna1Test",
		"SETTINGS_CONTEXT_3": "docker.teranode3.test.tna1Test",
	}
}

// SetupTest sets up the test environment with the initialized settings and default compose files.
func (suite *TNA6TestSuite) SetupTest() {
	suite.InitSuite()
	suite.SetupTestEnv(suite.SettingsMap, suite.DefaultComposeFiles(), false)
}

// TestAcceptanceNextBlock verifies that Teranode expresses block acceptance by using
// the accepted block's hash as the previous hash in its next mining candidate.
// This test specifically addresses TNA-6 requirement by:
// 1. Mining an initial block
// 2. Verifying the block is accepted
// 3. Getting a new mining candidate
// 4. Confirming that the mining candidate's previous hash matches the accepted block's hash
//
// This ensures that Teranode is actively working on extending the chain from blocks it has accepted.
func (suite *TNA6TestSuite) TestAcceptanceNextBlock() {
	testEnv := suite.TeranodeTestEnv
	ctx := testEnv.Context
	t := suite.T()
	logger := testEnv.Logger

	// Get the first node's clients
	ba := testEnv.Nodes[0].BlockassemblyClient
	bc := testEnv.Nodes[0].BlockchainClient

	// Mine a block
	blockHash, err := helper.MineBlock(ctx, testEnv.Nodes[0].Settings, ba, logger)
	require.NoError(t, err, "Failed to mine block")
	require.NotNil(t, blockHash, "Block hash should not be nil")

	// Wait for the block to be accepted and processed
	time.Sleep(2 * time.Second)

	// Verify the block was accepted by checking the best block
	bestBlockHeader, _, err := bc.GetBestBlockHeader(ctx)
	require.NoError(t, err, "Failed to get best block header")

	require.Equal(t, blockHash, bestBlockHeader.Hash().CloneBytes(),
		"Best block hash should match mined block hash")

	// Get a new mining candidate - this is the key test for TNA-6
	// It verifies that Teranode is working on the next block using the accepted block's hash
	miningCandidate, err := ba.GetMiningCandidate(ctx)
	require.NoError(t, err, "Failed to get mining candidate")

	// Convert hashes for comparison
	prevHash, err := chainhash.NewHash(miningCandidate.PreviousHash)
	require.NoError(t, err, "Failed to parse mining candidate's previous hash")

	// Verify that Teranode is building on top of the accepted block
	require.Equal(t, bestBlockHeader.Hash().String(), prevHash.String(),
		"Mining candidate's previous hash should match the accepted block's hash")

	// Verify all nodes are working on extending the same chain
	for i, node := range testEnv.Nodes {
		nodeMiningCandidate, err := node.BlockassemblyClient.GetMiningCandidate(ctx)
		require.NoError(t, err, "Failed to get mining candidate from node %d", i)

		nodePrevHash, err := chainhash.NewHash(nodeMiningCandidate.PreviousHash)
		require.NoError(t, err, "Failed to parse mining candidate's previous hash for node %d", i)

		require.Equal(t, bestBlockHeader.Hash().String(), nodePrevHash.String(),
			"Node %d should be building on top of the accepted block", i)
	}
}

// TestTNA6TestSuite runs the TNA6TestSuite test suite.
func TestTNA6TestSuite(t *testing.T) {
	suite.Run(t, new(TNA6TestSuite))
}
