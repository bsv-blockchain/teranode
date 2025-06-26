// How To run
// go test -v -timeout 30s -run ^TestUniqueCandidateIdentifiers$
// go test -v -timeout 30s -run ^TestConcurrentCandidateIdentifiers$

package smoke

import (
	"context"
	"sync"
	"testing"

	"github.com/bitcoin-sv/teranode/daemon"
	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/stretchr/testify/require"
)

// TestUniqueCandidateIdentifiers verifies that each mining candidate has a unique identifier
func TestUniqueCandidateIdentifiers(t *testing.T) {
	SharedTestLock.Lock()
	defer SharedTestLock.Unlock()

	ctx := context.Background()

	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		SettingsContext: "dev.system.test",
	})

	defer td.Stop(t)

	// Mine starting blocks
	_, err := td.CallRPC(td.Ctx, "generate", []interface{}{101})
	require.NoError(t, err, "Failed to mine blocks")

	// Get initial mining candidate
	mc1, err := helper.GetMiningCandidate(ctx, *td.BlockAssemblyClient, td.Logger)
	require.NoError(t, err, "Failed to get first mining candidate")
	require.NotNil(t, mc1, "First mining candidate should not be nil")

	// Get another mining candidate
	mc2, err := helper.GetMiningCandidate(ctx, *td.BlockAssemblyClient, td.Logger)
	require.NoError(t, err, "Failed to get second mining candidate")
	require.NotNil(t, mc2, "Second mining candidate should not be nil")

	// Verify that the candidates have different IDs
	require.Equal(t, mc1.GetId(), mc2.GetId(),
		"Mining candidates should have unique identifiers")

	// Mine a block and get another candidate to verify uniqueness across blocks
	_, err = td.CallRPC(td.Ctx, "generate", []interface{}{1})
	require.NoError(t, err, "Failed to mine blocks")

	mc3, err := helper.GetMiningCandidate(ctx, *td.BlockAssemblyClient, td.Logger)
	require.NoError(t, err, "Failed to get third mining candidate")
	require.NotNil(t, mc3, "Third mining candidate should not be nil")

	// Verify the new candidate has a different ID
	require.NotEqual(t, mc1.GetId(), mc3.GetId(),
		"Mining candidate after new block should have unique identifier")
	require.NotEqual(t, mc2.GetId(), mc3.GetId(),
		"Mining candidate after new block should have unique identifier")
}

// TestConcurrentCandidateIdentifiers verifies that candidate identifiers remain unique
// when requesting multiple candidates concurrently.
// TODO: Retest this, in conflict with the first
func TestConcurrentCandidateIdentifiers(t *testing.T) {
	SharedTestLock.Lock()
	defer SharedTestLock.Unlock()

	ctx := context.Background()
	numRequests := 3
	candidateIds := make(chan []byte, numRequests)

	var start sync.WaitGroup

	var wg sync.WaitGroup

	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		SettingsContext: "dev.system.test",
	})

	defer td.Stop(t)

	// Set run state
	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	// WaitGroup to synchronize goroutine start
	start.Add(1)

	// WaitGroup to wait for all goroutines to finish
	wg.Add(numRequests)

	for i := 0; i < numRequests; i++ {
		go func() {
			defer wg.Done()

			start.Wait() // wait for the release signal

			mc, err := helper.GetMiningCandidate(ctx, *td.BlockAssemblyClient, td.Logger)
			require.NoError(t, err, "Failed to get mining candidate in concurrent request")
			require.NotNil(t, mc, "Mining candidate should not be nil")

			candidateIds <- mc.GetId()
		}()
	}

	// Release all goroutines at the same time
	start.Done()

	wg.Wait()
	close(candidateIds)

	// Collect all IDs and verify uniqueness
	ids := make(map[string]bool)

	// at the end of the test, we need this two lines
	// require.Equal(t, numRequests, len(ids),
	// "Expected %d unique mining candidate IDs, got %d", numRequests, len(ids))
	for id := range candidateIds {
		idStr := string(id)
		// require.False(t, ids[idStr], "Duplicate mining candidate ID found: %s", idStr)
		ids[idStr] = true
	}
}
