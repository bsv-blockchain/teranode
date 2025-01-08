//go:build test_all || test_tnc

// TNC-1.1 Test Suite
// Requirement: Teranode must calculate the Merkle root of all the transactions included in the candidate block.
//
// How to run these tests:
//
// 1. Run all TNC-1.1 tests:
//    $ cd test/tnc
//    $ go test -v -run "^TestTNC1_1TestSuite$" -tags tnc
//
// 2. Run specific test cases:
//    $ cd test/tnc
//    $ go test -v -run "^TestTNC1_1TestSuite$/TestVerifyMerkleRootCalculation$" -tags test_tnc
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
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// TNC1_1TestSuite is a test suite for TNC-1.1 requirement:
// Teranode must calculate the Merkle root of all the transactions included in the candidate block.
type TNC1_1TestSuite struct {
	helper.TeranodeTestSuite
}

func (suite *TNC1_1TestSuite) InitSuite() {
	suite.SettingsMap = map[string]string{
		"SETTINGS_CONTEXT_1": "docker.teranode1.test.tnc1_1Test",
		"SETTINGS_CONTEXT_2": "docker.teranode2.test.tnc1_1Test",
		"SETTINGS_CONTEXT_3": "docker.teranode3.test.tnc1_1Test",
	}
}

func (suite *TNC1_1TestSuite) SetupTest() {
	suite.InitSuite()
	suite.SetupTestEnv(suite.SettingsMap, suite.DefaultComposeFiles(), false)
}

// func (suite *TNC1_1TestSuite) TearDownTest() {
// }

func (suite *TNC1_1TestSuite) TestVerifyMerkleRootCalculation() {
	testEnv := suite.TeranodeTestEnv
	ctx := testEnv.Context
	t := suite.T()
	node := testEnv.Nodes[0]
	logger := testEnv.Logger

	// Get mining candidate with no additional transactions
	_, err := helper.GetMiningCandidate(ctx, node.BlockassemblyClient, logger)
	require.NoError(t, err, "Failed to get mining candidate")

	// generate block
	blockHash, err := helper.MineBlock(ctx, node.BlockassemblyClient, logger)
	require.NoError(t, err, "Failed to mine block")
	require.NotNil(t, blockHash, "Block hash should not be nil")

	// Get Merkle branches from the mining candidate (should be empty for coinbase-only block)
	block, err := helper.GetBestBlockV2(ctx, node)
	require.NoError(t, err)
	require.NotNil(t, block)
	err = block.CheckMerkleRoot(ctx) // broken
	require.NoError(t, err)
}

func TestTNC1_1TestSuite(t *testing.T) {
	suite.Run(t, new(TNC1_1TestSuite))
}
