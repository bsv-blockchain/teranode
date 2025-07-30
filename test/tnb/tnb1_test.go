// How to run tests:
//
// Run all tests in the suite:
// go test -v  ./test/tnb
//
// Run a specific test:
// go test -v -run "^TestTNB1TestSuite$/TestSendTxsInBatch$" ./test/tnb/tnb1_test.go

// Run with test coverage:
// go test -v -coverprofile=coverage.out ./test/tnb
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

package tnb

import (
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/model"
	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/bitcoin-sv/teranode/test/utils/tconfig"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type TNB1TestSuite struct {
	helper.TeranodeTestSuite
}

func TestTNB1TestSuite(t *testing.T) {
	suite.Run(t, &TNB1TestSuite{
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

func (suite *TNB1TestSuite) TestSendTxsInBatch() {
	testEnv := suite.TeranodeTestEnv
	ctx := testEnv.Context
	t := suite.T()
	node1 := testEnv.Nodes[0]
	blockchainNode0 := node1.BlockchainClient
	logger := testEnv.Logger

	require.Eventually(t, func() bool {
		_, _, err := blockchainNode0.GetBestBlockHeader(ctx)
		if err != nil {
			logger.Infof("Waiting for blockchain service: %v", err)
		}
		return err == nil
	}, 15*time.Second, 500*time.Millisecond, "Blockchain service not ready in time")

	blockchainSubscription, err := blockchainNode0.Subscribe(ctx, "test-tnb1")
	require.NoError(t, err, "error subscribing to blockchain service")

	subtreeReady := make(chan []chainhash.Hash, 1)

	txHashesFromSubtree := make([]chainhash.Hash, 0)

	url := "http://" + testEnv.Nodes[0].AssetURL

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case notification := <-blockchainSubscription:
				if notification.Type == model.NotificationType_Subtree {
					subtreeHash, err := chainhash.NewHash(notification.Hash)
					testEnv.Logger.Infof("subtreeHash: %v", subtreeHash)
					require.NoError(t, err)

					txHashesFromSubtree, err := helper.GetSubtreeTxHashes(ctx, logger, subtreeHash, url, node1.Settings) //nolint:ineffassign // requires testing the error

					subtreeReady <- txHashesFromSubtree

					testEnv.Logger.Infof("txHashes from subtree: %v", txHashesFromSubtree)
				}
			}
		}
	}()

	for i := 0; i < 1; i++ {
		block1, err := node1.BlockchainClient.GetBlockByHeight(ctx, 1)
		require.NoError(t, err)
		parenTx := block1.CoinbaseTx
		_, txHashesSent, err := node1.CreateAndSendTxsConcurrently(t, ctx, parenTx, 10)
		if err != nil {
			t.Errorf("Failed to create and send raw txs: %v", err)
		}

		baClient := node1.BlockassemblyClient
		_, err = helper.GetMiningCandidate(ctx, baClient, logger)

		if err != nil {
			t.Errorf("Failed to mine block: %v", err)
		}

		select {
		case txHashesFromSubtree = <-subtreeReady:
			// ok, ricevuto
		case <-time.After(60 * time.Second):
			t.Fatal("Timeout waiting for subtree notification")
		}

		testEnv.Logger.Infof("txHashesSent sent: %v", txHashesSent)

		// Verify that all transactions in txHashesSent are included in txHashesFromSubtree
		for _, txHash := range txHashesSent {
			found := false

			for _, txHashFromSubtree := range txHashesFromSubtree {
				if *txHash == txHashFromSubtree {
					found = true
					break
				}
			}

			require.True(t, found, "txHash not found in txHashesFromSubtree")
		}
	}
}
