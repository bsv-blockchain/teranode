//go:build test_all || test_tnc

// TNC-1.2 Test Suite
// Requirement: Teranode must refer to the hash of the previous block upon which the candidate block is being built.
//
// How to run these tests:
//
// 1. Run all TNC-1.2 tests:
//    $ cd test/tnc
//    $ go test -v -run "^TestTNC1_2TestSuite$" -tags test_tnc
//
// 2. Run specific test cases:
//    $ cd test/tnc
//    $ go test -v -run "^TestTNC1_2TestSuite$/TestCheckPrevBlockHash$" -tags test_tnc
//    $ go test -v -run "^TestTNC1_2TestSuite$/TestPrevBlockHashAfterReorg$" -tags test_tnc
//
// Prerequisites:
// - Go 1.19 or later
// - Docker and Docker Compose
// - Access to required Docker images
//
// For more details, see README.md in the same directory.

package tnc

import (
	"testing"

	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// TNC1_2TestSuite is a test suite for TNC-1.2 requirement:
// Teranode must refer to the hash of the previous block upon which the candidate block is being built.
type TNC1_2TestSuite struct {
	helper.TeranodeTestSuite
}

func (suite *TNC1_2TestSuite) InitSuite() {
	suite.SettingsMap = map[string]string{
		"SETTINGS_CONTEXT_1": "docker.teranode1.test.tnc1_2Test",
		"SETTINGS_CONTEXT_2": "docker.teranode2.test.tnc1_2Test",
		"SETTINGS_CONTEXT_3": "docker.teranode3.test.tnc1_2Test",
	}
}

func (suite *TNC1_2TestSuite) SetupTest() {
	suite.InitSuite()
	suite.SetupTestEnv(suite.SettingsMap, suite.DefaultComposeFiles(), false)
}

// func (suite *TNC1_2TestSuite) TearDownTest() {
// }

// TestCheckPrevBlockHash verifies that the mining candidate correctly references
// the hash of the previous block
func (suite *TNC1_2TestSuite) TestCheckPrevBlockHash() {
	testEnv := suite.TeranodeTestEnv
	ctx := testEnv.Context
	t := suite.T()
	logger := testEnv.Logger
	node := testEnv.Nodes[0]

	// Send some transactions and mine a block to have a known state
	_, err := helper.SendTXsWithDistributorV2(ctx, node, logger, node.Settings, 10000)
	require.NoError(t, err, "Failed to send transactions")

	// Mine the block with our transactions
	_, err = helper.MineBlockWithRPC(ctx, node, logger)
	require.NoError(t, err, "Failed to mine block")

	// Get the current best block header
	bestBlockHeader, _, err := node.BlockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err, "Failed to get best block header")

	// Get a new mining candidate
	mc, err := node.BlockassemblyClient.GetMiningCandidate(ctx)
	require.NoError(t, err, "Failed to get mining candidate")

	// Convert the previous hash from the mining candidate to a chainhash.Hash
	prevHash, err := chainhash.NewHash(mc.PreviousHash)
	require.NoError(t, err, "Failed to create hash from previous hash bytes")

	// Verify that the mining candidate's previous hash matches the current best block
	require.Equal(t, bestBlockHeader.String(), prevHash.String(),
		"Mining candidate's previous hash does not match current best block")
}

// TestPrevBlockHashAfterReorg verifies that the mining candidate correctly updates
// its previous block reference after a chain reorganization
// Todo: Add proper wait mechanism for reorg completion
func (suite *TNC1_2TestSuite) TestPrevBlockHashAfterReorg() {
	testEnv := suite.TeranodeTestEnv
	ctx := testEnv.Context
	t := suite.T()
	logger := testEnv.Logger
	node0 := testEnv.Nodes[0]
	node1 := testEnv.Nodes[1]

	// Create and mine a block on node0
	_, err := helper.SendTXsWithDistributorV2(ctx, node0, logger, node0.Settings, 10000)
	require.NoError(t, err, "Failed to send transactions to node0")

	_, err = helper.MineBlockWithRPC(ctx, node0, logger)
	require.NoError(t, err, "Failed to mine block on node0")

	// Create a longer chain on node1 (two blocks)
	_, err = helper.SendTXsWithDistributorV2(ctx, node1, logger, node1.Settings, 10000)
	require.NoError(t, err, "Failed to send transactions to node1")

	_, err = helper.MineBlockWithRPC(ctx, node1, logger)
	require.NoError(t, err, "Failed to mine first block on node1")

	_, err = helper.SendTXsWithDistributorV2(ctx, node1, logger, node1.Settings, 10000)
	require.NoError(t, err, "Failed to send more transactions to node1")

	_, err = helper.MineBlockWithRPC(ctx, node1, logger)
	require.NoError(t, err, "Failed to mine second block on node1")

	// Wait for node0 to reorganize to the longer chain
	// TODO: Add proper wait mechanism for reorg completion

	// Get the current best block header from node0 after reorg
	bestBlockHeader, _, err := node0.BlockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err, "Failed to get best block header after reorg")

	// Get a new mining candidate from node0
	mc, err := node0.BlockassemblyClient.GetMiningCandidate(ctx)
	require.NoError(t, err, "Failed to get mining candidate after reorg")

	// Convert the previous hash from the mining candidate to a chainhash.Hash
	prevHash, err := chainhash.NewHash(mc.PreviousHash)
	require.NoError(t, err, "Failed to create hash from previous hash bytes")

	// Verify that node0's mining candidate now references the tip of the longer chain
	require.Equal(t, bestBlockHeader.String(), prevHash.String(),
		"Mining candidate's previous hash does not match the new best block after reorg")
}

func TestTNC1_2TestSuite(t *testing.T) {
	suite.Run(t, new(TNC1_2TestSuite))
}
