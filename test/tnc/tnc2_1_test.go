//go:build test_all || test_tnc

package tnc

// TNC-2.1 Test Suite
// Requirement: Each candidate block must have a unique identifier
//
// How to run these tests:
//
// 1. Run all TNC-2.1 tests:
//    $ cd test/tnc
//    $ go test -v -run "^TestTNC2_1TestSuite$" -tags test_tnc
//
// 2. Run specific test cases:
//    $ cd test/tnc
//    $ go test -v -run "^TestTNC2_1TestSuite$/TestUniqueCandidateIdentifiers$" -tags test_tnc
//    $ go test -v -run "^TestTNC2_1TestSuite$/TestConcurrentCandidateIdentifiers$" -tags test_tnc
//
// Prerequisites:
// - Go 1.19 or later
// - Docker and Docker Compose
// - Access to required Docker images
//
// For more details, see README.md in the same directory.

import (
	"sync"
	"testing"
	"time"

	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// TNC2_1TestSuite is a test suite for TNC-2.1 requirement:
// Each candidate block must have a unique identifier
type TNC2_1TestSuite struct {
	helper.TeranodeTestSuite
}

func (suite *TNC2_1TestSuite) InitSuite() {
	suite.SettingsMap = map[string]string{
		"SETTINGS_CONTEXT_1": "docker.teranode1.test.tnc2_1Test",
		"SETTINGS_CONTEXT_2": "docker.teranode2.test.tnc2_1Test",
		"SETTINGS_CONTEXT_3": "docker.teranode3.test.tnc2_1Test",
	}
}

func (suite *TNC2_1TestSuite) SetupTest() {
	suite.InitSuite()
	suite.SetupTestEnv(suite.SettingsMap, suite.DefaultComposeFiles(), false)
}

func (suite *TNC2_1TestSuite) TearDownTest() {
}

// TestUniqueCandidateIdentifiers verifies that each mining candidate has a unique identifier
func (suite *TNC2_1TestSuite) TestUniqueCandidateIdentifiers() {
	testEnv := suite.TeranodeTestEnv
	ctx := testEnv.Context
	t := suite.T()
	logger := testEnv.Logger
	node := testEnv.Nodes[0]

	// Get initial mining candidate
	mc1, err := helper.GetMiningCandidate(ctx, node.BlockassemblyClient, logger)
	require.NoError(t, err, "Failed to get first mining candidate")
	require.NotNil(t, mc1, "First mining candidate should not be nil")

	// Get another mining candidate
	mc2, err := helper.GetMiningCandidate(ctx, node.BlockassemblyClient, logger)
	require.NoError(t, err, "Failed to get second mining candidate")
	require.NotNil(t, mc2, "Second mining candidate should not be nil")

	// Verify that the candidates have different IDs
	require.NotEqual(t, mc1.GetId(), mc2.GetId(),
		"Mining candidates should have unique identifiers")

	// Mine a block and get another candidate to verify uniqueness across blocks
	_, err = helper.MineBlockWithRPC(ctx, node, logger)
	require.NoError(t, err, "Failed to mine block")

	mc3, err := helper.GetMiningCandidate(ctx, node.BlockassemblyClient, logger)
	require.NoError(t, err, "Failed to get third mining candidate")
	require.NotNil(t, mc3, "Third mining candidate should not be nil")

	// Verify the new candidate has a different ID
	require.NotEqual(t, mc1.GetId(), mc3.GetId(),
		"Mining candidate after new block should have unique identifier")
	require.NotEqual(t, mc2.GetId(), mc3.GetId(),
		"Mining candidate after new block should have unique identifier")
}

// TestConcurrentCandidateIdentifiers verifies that candidate identifiers remain unique
// when requesting multiple candidates concurrently
func (suite *TNC2_1TestSuite) TestConcurrentCandidateIdentifiers() {
	testEnv := suite.TeranodeTestEnv
	ctx := testEnv.Context
	t := suite.T()
	logger := testEnv.Logger
	node := testEnv.Nodes[0]

	// Number of concurrent requests
	numRequests := 10

	// Channel to collect mining candidate IDs
	candidateIds := make(chan []byte, numRequests)

	var wg sync.WaitGroup

	// Request mining candidates concurrently
	for i := 0; i < numRequests; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			mc, err := helper.GetMiningCandidate(ctx, node.BlockassemblyClient, logger)
			require.NoError(t, err, "Failed to get mining candidate in concurrent request")
			require.NotNil(t, mc, "Mining candidate should not be nil")

			candidateIds <- mc.GetId()
		}()
	}

	// Small delay to ensure we're not getting the same candidate
	time.Sleep(100 * time.Millisecond)

	wg.Wait()
	close(candidateIds)

	// Collect all IDs and verify uniqueness
	ids := make(map[string]bool)

	for id := range candidateIds {
		idStr := string(id)
		require.False(t, ids[idStr], "Duplicate mining candidate ID found: %s", idStr)
		ids[idStr] = true
	}

	// Verify we got the expected number of unique IDs
	require.Equal(t, numRequests, len(ids),
		"Expected %d unique mining candidate IDs, got %d", numRequests, len(ids))
}

func TestTNC2_1TestSuite(t *testing.T) {
	suite.Run(t, new(TNC2_1TestSuite))
}
