//go:build 1.24 || test_tnc2_1_sync

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
//    $ GOEXPERIMENT=synctest go test -v -run "^TestTNC2_1TestSuite$/TestConcurrentCandidateIdentifiers$" -tags=test_tnc2_1_sync
//
//
// Prerequisites:
// - Go 1.24 or later
// - Docker and Docker Compose
// - Access to required Docker images
//
// For more details, see README.md in the same directory.

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"

	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/bitcoin-sv/teranode/test/utils/tconfig"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// TNC2_1TestSuite is a test suite for TNC-2.1 requirement:
// Each candidate block must have a unique identifier
type TNC2_1SyncTestSuite struct {
	helper.TeranodeTestSuite
}

func TestTNC2_1SyncTestSuite(t *testing.T) {
	suite.Run(t, &TNC2_1SyncTestSuite{
		TeranodeTestSuite: helper.TeranodeTestSuite{
			TConfig: tconfig.LoadTConfig(
				map[string]any{
					tconfig.KeyTeranodeContexts: []string{
						"docker.teranode1.test.tnc2_1Test",
						"docker.teranode2.test.tnc2_1Test",
						"docker.teranode3.test.tnc2_1Test",
					},
				},
			),
		},
	},
	)
}

// TestUniqueCandidateIdentifiers verifies that each mining candidate has a unique identifier
func (suite *TNC2_1SyncTestSuite) TestUniqueCandidateIdentifiers() {
	synctest.Run(func() {
		// Set up the test context from the test environment.
		testEnv := suite.TeranodeTestEnv
		ctx, cancel := context.WithCancel(testEnv.Context)
		defer cancel()
		t := suite.T()
		logger := testEnv.Logger
		node := testEnv.Nodes[0]

		// Number of concurrent requests
		numRequests := 10

		// Channel to collect mining candidate IDs
		candidateIds := make(chan []byte, numRequests)

		var wg sync.WaitGroup

		// Launch concurrent requests for mining candidates.
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

		// Instead of time.Sleep, simulate a small delay using AfterFunc.
		// Create a separate context for the delay simulation.
		delayCtx, delayCancel := context.WithCancel(ctx)
		var delayTriggered bool
		// Register an AfterFunc that will set delayTriggered to true when delayCtx is canceled.
		context.AfterFunc(delayCtx, func() {
			delayTriggered = true
		})
		// Cancel the delay context to trigger the AfterFunc.
		delayCancel()
		// Wait for all registered AfterFunc callbacks to complete.
		synctest.Wait()
		require.True(t, delayTriggered, "Delay AfterFunc did not trigger")

		// Wait for all concurrent requests to complete.
		wg.Wait()
		close(candidateIds)

		// Collect all candidate IDs and verify they are unique.
		ids := make(map[string]bool)
		for id := range candidateIds {
			idStr := string(id)
			require.False(t, ids[idStr], fmt.Sprintf("Duplicate mining candidate ID found: %s", idStr))
			ids[idStr] = true
		}

		// Verify that we got the expected number of unique IDs.
		require.Equal(t, numRequests, len(ids),
			fmt.Sprintf("Expected %d unique mining candidate IDs, got %d", numRequests, len(ids)))
	})
}
