//go:build 1.24 || test_tnb

// How to run tests:
//
// Run all tests in the suite:
// go test -v -tags test_tnb ./test/tnb
//
// Run a specific test:
// go test -v -tags test_tnb -run "^TestTNB1TestSuite$/TestReceiveExtendedFormatTx$"
// go test -v -tags test_tnb -run "^TestTNB1TestSuite$/TestNoReformattingRequired$"
//
// Run with test coverage:
// go test -v -tags test_tnb -coverprofile=coverage.out ./test/tnb
// go tool cover -html=coverage.out

//Settings:
// Uses validator_sendBatchSize.docker.teranode.test.tnb1Test=10

//Steps:
// 1. Create and send transactions concurrently
// 2. Get the block height
// 3. Get mining candidate
// 4. Subscribe to blockchain service and get the subtree hash
// 5. Check if all the transaction hashes are included in the subtree
// TODO: Send the same transactions through TxDistributor and check if they are included in each nodes' subtree

// How to run manually:
// cd test/tnb
// go test -v -run "^TestTNB1TestSuite$/TestSendTxsInBatch$" -tags test_tnb

package tnb

import (
	"context"
	"testing"
	"testing/synctest"

	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/bitcoin-sv/teranode/test/utils/tconfig"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type TNB1SyncTestSuite struct {
	helper.TeranodeTestSuite
}

func TestTNB1SyncTestSuite(t *testing.T) {
	suite.Run(t, &TNB1SyncTestSuite{
		TeranodeTestSuite: helper.TeranodeTestSuite{
			TConfig: tconfig.LoadTConfig(
				map[string]any{
					tconfig.KeyTeranodeContexts: []string{
						"docker.teranode1.test.tnb1Test",
						"docker.teranode2.test.tnb1Test",
						"docker.teranode3.test.tnb1Test",
					},
				},
			),
		},
	},
	)
}

func (suite *TNB1SyncTestSuite) TestNoReformattingRequired() {
	synctest.Run(func() {
		// Set up the test context and variables.
		testEnv := suite.TeranodeTestEnv
		ctx, cancel := context.WithCancel(testEnv.Context)
		defer cancel()
		t := suite.T()
		node := testEnv.Nodes[0]
		logger := testEnv.Logger

		// Create multiple transactions.
		txHashes := make([]chainhash.Hash, 0)
		for i := 0; i < 5; i++ {
			txHash, err := helper.CreateAndSendTx(ctx, node)
			require.NoError(t, err)
			txHashes = append(txHashes, txHash)
		}

		// Generate a block.
		_, err := helper.GenerateBlocks(ctx, node, 1, logger)
		require.NoError(t, err)

		// Instead of waiting for 5 seconds with time.Sleep, simulate a delay using AfterFunc.
		delayCtx, delayCancel := context.WithCancel(ctx)
		var delayTriggered bool
		// Register an AfterFunc that sets delayTriggered to true when delayCtx is canceled.
		context.AfterFunc(delayCtx, func() {
			delayTriggered = true
		})
		// Cancel the delay context to trigger the AfterFunc.
		delayCancel()
		// Wait for all registered AfterFunc callbacks to complete.
		synctest.Wait()
		require.True(t, delayTriggered, "Delay AfterFunc did not trigger")

		// Get the best block and verify transactions.
		block, err := helper.GetBestBlockV2(ctx, node)
		require.NoError(t, err)
		require.NotNil(t, block)

		// Get and validate subtrees.
		err = block.GetAndValidateSubtrees(ctx, logger, node.ClientSubtreestore, nil)
		require.NoError(t, err)

		// Create a map to track which transactions have been found.
		foundTxs := make(map[string]bool)
		for _, txHash := range txHashes {
			foundTxs[txHash.String()] = false
		}

		// Check each subtree for transactions.
		for _, subtree := range block.SubtreeSlices {
			for _, n := range subtree.Nodes {
				if _, exists := foundTxs[n.Hash.String()]; exists {
					foundTxs[n.Hash.String()] = true
				}
			}
		}

		// Verify all transactions were found.
		missingTxs := make([]string, 0)
		for txHash, found := range foundTxs {
			if !found {
				missingTxs = append(missingTxs, txHash)
			}
		}
		require.Empty(t, missingTxs, "Transactions not found in block's subtrees: %v", missingTxs)
	})
}
