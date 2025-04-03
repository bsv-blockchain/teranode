//go:build test_all || test_tnc || debug

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
//    $ go test -v -run "^TestTNC1_2TestSuite$/TestCheckPrevBlockHash$" -tags test_tnc ./test/tnc/tnc1_2_test.go
//    $ go test -v -run "^TestTNC1_2TestSuite$/TestPrevBlockHashAfterReorg$" -tags test_tnc ./test/tnc/tnc1_2_test.go
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
	"github.com/bitcoin-sv/teranode/test/utils/tconfig"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// TNC1_2TestSuite is a test suite for TNC-1.2 requirement:
// Teranode must refer to the hash of the previous block upon which the candidate block is being built.
type TNC1_2TestSuite struct {
	helper.TeranodeTestSuite
}

func TestTNC1_2TestSuite(t *testing.T) {
	suite.Run(t, &TNC1_2TestSuite{
		TeranodeTestSuite: helper.TeranodeTestSuite{
			TConfig: tconfig.LoadTConfig(
				map[string]any{
					tconfig.KeyTeranodeContexts: []string{
						"docker.teranode1.test.tnc1_2Test",
						"docker.teranode2.test.tnc1_2Test",
						"docker.teranode3.test.tnc1_2Test",
					},
				},
			),
		},
	},
	)
}

// TestCheckPrevBlockHash verifies that the mining candidate correctly references
// the hash of the previous block
func (suite *TNC1_2TestSuite) TestCheckPrevBlockHash() {
	testEnv := suite.TeranodeTestEnv
	ctx := testEnv.Context
	t := suite.T()
	logger := testEnv.Logger
	node := testEnv.Nodes[0]

	block1, err := node.BlockchainClient.GetBlockByHeight(ctx, 1)
	require.NoError(t, err)
	parenTx := block1.CoinbaseTx
	_, err = node.CreateAndSendTx(t, ctx, parenTx)
	require.NoError(t, err, "Failed to send transactions")

	// Mine the block with our transactions
	_, err = helper.GenerateBlocks(ctx, node, 1, logger)
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
func (suite *TNC1_2TestSuite) TestPrevBlockHashAfterReorg() {
	testEnv := suite.TeranodeTestEnv
	ctx := testEnv.Context
	t := suite.T()
	logger := testEnv.Logger
	node1 := testEnv.Nodes[0]
	node2 := testEnv.Nodes[1]

	block1, err := node1.BlockchainClient.GetBlockByHeight(ctx, 1)
	require.NoError(t, err)
	parenTx := block1.CoinbaseTx
	_, err = node1.CreateAndSendTx(t, ctx, parenTx)
	require.NoError(t, err, "Failed to send transactions")

	_, err = helper.GenerateBlocks(ctx, node1, 1, logger)
	require.NoError(t, err, "Failed to mine block on node0")

	_, err = helper.GenerateBlocks(ctx, node2, 5, logger)
	require.NoError(t, err, "Failed to mine block on node2")

	// Get the current best block header from node1 after reorg
	bestBlockHeader, _, err := node2.BlockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err, "Failed to get best block header after reorg")

	// Get a new mining candidate from node1
	mc, err := node1.BlockassemblyClient.GetMiningCandidate(ctx)
	require.NoError(t, err, "Failed to get mining candidate after reorg")

	// Convert the previous hash from the mining candidate to a chainhash.Hash
	prevHash, err := chainhash.NewHash(mc.PreviousHash)
	require.NoError(t, err, "Failed to create hash from previous hash bytes")

	// Verify that node0's mining candidate now references the tip of the longer chain
	require.Equal(t, bestBlockHeader.String(), prevHash.String(),
		"Mining candidate's previous hash does not match the new best block after reorg")
}
