//go:build 1.24 || test_tnd || test_functional

// How to run this test:
// $ cd test/tnd/
// $ GOEXPERIMENT=synctest go test -v -run "^TestTND1_1TestSuite$/TestBlockPropagationWithNotifications$" -tags=test_tnd

package tnd

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/bitcoin-sv/teranode/model"
	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type TND1_1SyncTestSuite struct {
	helper.TeranodeTestSuite
}

func TestTND1_1SyncTestSuite(t *testing.T) {
	suite.Run(t, &TND1_1SyncTestSuite{})
}

// TestBlockPropagationWithNotifications verifies block propagation between nodes
// by subscribing to blockchain notifications on receiving nodes
func (suite *TND1_1SyncTestSuite) TestBlockPropagationWithNotifications() {
	synctest.Run(func() {
		// Set up the test context and variables
		t := suite.T()
		testEnv := suite.TeranodeTestEnv
		ctx, cancel := context.WithCancel(testEnv.Context)
		defer cancel()
		logger := testEnv.Logger
		url := "http://" + testEnv.Nodes[0].AssetURL

		// Set up notification channels for receiving nodes
		var wg sync.WaitGroup
		blockNotifications := make(map[int]chan *model.Notification)
		receivedBlocks := make(map[int][]byte)
		var mu sync.Mutex

		// Subscribe to blockchain notifications on all nodes except Node0
		for i := 1; i < len(testEnv.Nodes); i++ {
			node := testEnv.Nodes[i]
			notifChan := make(chan *model.Notification, 10)
			blockNotifications[i] = notifChan

			blockchainSubscription, err := node.BlockchainClient.Subscribe(ctx, "test-block-propagation")
			require.NoError(t, err, "Failed to subscribe to blockchain notifications on Node%d", i)

			wg.Add(1)
			go func(nodeIndex int) {
				defer wg.Done()
				for {
					select {
					case <-ctx.Done():
						return
					case notification, ok := <-blockchainSubscription:
						if !ok {
							return
						}
						if notification.Type == model.NotificationType_Block {
							mu.Lock()
							receivedBlocks[nodeIndex] = notification.Hash
							mu.Unlock()
							t.Logf("Node%d received block notification: %x", nodeIndex, notification.Hash)
							// Register an AfterFunc to signal that a notification has been processed.
							context.AfterFunc(ctx, func() {})
						}
					}
				}
			}(i)
		}

		// Get the initial block height from Node0
		initialHeight, err := helper.GetBlockHeight(url)
		require.NoError(t, err)
		t.Logf("Initial block height: %d", initialHeight)

		// Mine multiple blocks on Node0
		numBlocks := uint32(5)
		targetHeight := initialHeight + numBlocks
		minedBlockHashes := make([][]byte, 0, numBlocks)
		for i := uint32(0); i < numBlocks; i++ {
			minedBlockBytes, err := helper.MineBlock(ctx, testEnv.Nodes[0].Settings, testEnv.Nodes[0].BlockassemblyClient, logger)
			require.NoError(t, err, "Failed to mine block %d", i+1)
			minedBlockHashes = append(minedBlockHashes, minedBlockBytes)
			t.Logf("Mined block %d: %x", i+1, minedBlockBytes)
		}

		// Wait for blocks to propagate to all nodes using the helper function.
		t.Log("Waiting for blocks to propagate to all nodes...")
		for _, nodeURL := range []string{
			"http://" + testEnv.Nodes[0].AssetURL,
			"http://" + testEnv.Nodes[1].AssetURL,
			"http://" + testEnv.Nodes[2].AssetURL,
		} {
			err := helper.WaitForBlockHeight(nodeURL, targetHeight, 60)
			require.NoError(t, err)
			currentHeight, err := helper.GetBlockHeight(nodeURL)
			require.NoError(t, err)
			t.Logf("Node at %s reached height %d", nodeURL, currentHeight)
		}

		// Instead of a fixed sleep, simulate a delay using AfterFunc.
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

		// Cancel the context to end the subscription goroutines and wait for them to finish.
		cancel()
		wg.Wait()

		// Verify that all receiving nodes got block notifications
		mu.Lock()
		for nodeIndex, blockNotified := range receivedBlocks {
			require.NotNil(t, blockNotified, "Node%d did not receive block notification", nodeIndex)

			// Get block header from the node's database
			node := testEnv.Nodes[nodeIndex]
			blockNotifiedHash, err := chainhash.NewHash(blockNotified)
			require.NoError(t, err, "Failed to create block hash from bytes on Node%d", nodeIndex)

			block, _, err := node.BlockChainDB.GetBlockHeaders(ctx, blockNotifiedHash, 1)
			require.NoError(t, err, "Failed to get block from database on Node%d", nodeIndex)
			require.NotNil(t, block, "Block not found in database on Node%d", nodeIndex)

			// Get all block headers from initial height
			headers, meta, err := node.BlockchainClient.GetBlockHeadersFromHeight(ctx, initialHeight+1, numBlocks)
			require.NoError(t, err, "Failed to get block headers from Node%d", nodeIndex)
			require.Equal(t, int(numBlocks), len(headers), "Unexpected number of headers from Node%d", nodeIndex)

			// Verify block heights and chain continuity
			for i, header := range headers {
				require.Equal(t, initialHeight+uint32(i)+1, meta[i].Height,
					"Unexpected block height for block %d on Node%d", i+1, nodeIndex)
				if i > 0 {
					require.Equal(t, headers[i-1].Hash().String(), header.HashPrevBlock.String(),
						"Block %d does not reference previous block on Node%d", i+1, nodeIndex)
				}
			}
		}
		mu.Unlock()

		// Verify that all nodes have the same best block header
		header0, meta0, err := testEnv.Nodes[0].BlockchainClient.GetBestBlockHeader(ctx)
		require.NoError(t, err)
		t.Logf("Node 0 best block header: %s at height %d", header0.Hash(), meta0.Height)

		for i := 1; i < len(testEnv.Nodes); i++ {
			headerN, metaN, err := testEnv.Nodes[i].BlockchainClient.GetBestBlockHeader(ctx)
			require.NoError(t, err)
			t.Logf("Node %d best block header: %s at height %d", i, headerN.Hash(), metaN.Height)
			require.Equal(t, header0.Hash(), headerN.Hash(),
				"Best block header mismatch between Node0 and Node%d", i)
			require.Equal(t, meta0.Height, metaN.Height,
				"Best block height mismatch between Node0 and Node%d", i)
		}
	})
}
