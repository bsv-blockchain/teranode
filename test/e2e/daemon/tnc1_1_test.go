// How to run:
// go test -v -timeout 30s -tags "test_tnc" -run ^TestVerifyMerkleRootCalculation$

package smoke

import (
	"context"
	"testing"

	"github.com/bitcoin-sv/teranode/daemon"
	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/stretchr/testify/require"
)

func TestVerifyMerkleRootCalculation(t *testing.T) {
	SharedTestLock.Lock()
	defer SharedTestLock.Unlock()

	ctx := context.Background()

	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		SettingsContext: "dev.system.test",
	})

	defer td.Stop(t)

	// set run state
	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	// Get mining candidate with no additional transactions
	_, errMc := helper.GetMiningCandidate(ctx, *td.BlockAssemblyClient, td.Logger)
	require.NoError(t, errMc, "Failed to get mining candidate")

	// Generate 1 block
	_, err = td.CallRPC(td.Ctx, "generate", []interface{}{1})
	require.NoError(t, err, "Failed to mine block")

	// Get Merkle branches from the mining candidate (should be empty for coinbase-only block)
	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err)

	err = block1.GetAndValidateSubtrees(td.Ctx, td.Logger, td.SubtreeStore)
	require.NoError(t, err)

	err = block1.CheckMerkleRoot(td.Ctx)
	require.NoError(t, err)
}
