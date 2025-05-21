// How to run this test manually:
// $ go test -v -run "^TestTNA4TestSuite$/TestBlockBroadcast$" -tags test_tna ./test/tna/tna4_test.go
//
// To run all TNA tests:
// $ go test -v -tags test_tna ./...
//
// Prerequisites:
// 1. Docker must be running
// 2. Docker compose must be installed
// 3. The following ports must be available:
//    - 16090-16092: Node API ports
//
// Test Description:
// This test verifies TNA-4 requirement: Teranode must broadcast the block to all nodes
// when it finds a proof-of-work.
//
// The test uses three Teranode instances in a Docker environment to verify that:
// 1. When Node0 mines a block, it broadcasts it to the network
// 2. Other nodes receive block notifications through the blockchain subscription
// 3. The broadcast block contains all expected transactions and valid proof-of-work

package tna

import (
	"sync"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/model"
	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/bitcoin-sv/teranode/test/utils/tconfig"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// TNA4TestSuite tests that Teranode broadcasts blocks to all nodes when it
// finds a proof-of-work, ensuring network-wide block propagation.
type TNA4TestSuite struct {
	helper.TeranodeTestSuite
}

// TestTNA4TestSuite runs the TNA4TestSuite test suite.
func TestTNA4TestSuite(t *testing.T) {
	suite.Run(t, &TNA4TestSuite{
		TeranodeTestSuite: helper.TeranodeTestSuite{
			TConfig: tconfig.LoadTConfig(
				map[string]any{
					tconfig.KeyTeranodeContexts: []string{
						"docker.teranode1.test.tna1Test",
						"docker.teranode2.test.tna1Test",
						"docker.teranode3.test.tna1Test",
					},
				},
			),
		},
	},
	)
}

// TestBlockBroadcast verifies that Teranode broadcasts blocks to all nodes after finding
// a proof-of-work by monitoring block notifications on receiving nodes.
func (suite *TNA4TestSuite) TestBlockBroadcast() {
	testEnv := suite.TeranodeTestEnv
	ctx := testEnv.Context
	t := suite.T()
	logger := testEnv.Logger

	// Set up notification channels for receiving nodes (Node1 and Node2)
	var wg sync.WaitGroup

	blockNotifications := make(map[int]chan *model.Notification)

	receivedBlocks := make(map[int][]byte)
	var mu sync.Mutex

	// Subscribe to blockchain notifications on receiving nodes
	for i := 1; i < len(testEnv.Nodes); i++ {
		node := testEnv.Nodes[i]
		notifChan := make(chan *model.Notification, 10)
		blockNotifications[i] = notifChan

		blockchainSubscription, err := node.BlockchainClient.Subscribe(ctx, "test-block-broadcast")
		require.NoError(t, err, "Failed to subscribe to blockchain notifications on Node%d", i)

		wg.Add(1)

		go func(nodeIndex int, notification chan *model.Notification) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case notification := <-blockchainSubscription:
					if notification.Type == model.NotificationType_Block {
						mu.Lock()
						receivedBlocks[nodeIndex] = notification.Hash
						mu.Unlock()
						t.Logf("Node%d received block notification: %x", nodeIndex, notification.Hash)
					}
				}
			}
		}(i, notifChan)
	}

	node0 := testEnv.Nodes[0]
	block1, err := node0.BlockchainClient.GetBlockByHeight(ctx, 1)
	require.NoError(t, err)
	parenTx := block1.CoinbaseTx

	// Create and send transactions to be included in the block
	hashes, _, err := node0.CreateAndSendTxs(t, ctx, parenTx, 10)
	require.NoError(t, err, "Failed to create and send transactions")
	t.Logf("Created transactions with hashes: %v", hashes)

	// Mine a block containing the transactions on Node0
	var minedBlockBytes []byte
	minedBlockBytes, err = helper.MineBlock(ctx, node0.Settings, node0.BlockassemblyClient, logger)
	require.NoError(t, err, "Failed to mine block")

	// Wait for all nodes to receive the block notification with timeout
	timeout := time.After(10 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	expectedNodes := len(testEnv.Nodes) - 1 // excluding node0 (mining node)
waitLoop:
	for {
		select {
		case <-timeout:
			t.Fatalf("Timeout waiting for block notifications. Expected %d nodes, got notifications from %d nodes",
				expectedNodes, len(receivedBlocks))
		case <-ticker.C:
			mu.Lock()
			if len(receivedBlocks) == expectedNodes {
				mu.Unlock()
				break waitLoop
			}
			mu.Unlock()
		}
	}

	// Verify that all receiving nodes got the block notification
	mu.Lock()
	for nodeIndex, blockNotified := range receivedBlocks {
		require.NotNil(t, blockNotified, "Node%d did not receive block notification", nodeIndex)

		// Verify block contents on each receiving node
		node := testEnv.Nodes[nodeIndex]

		require.Equal(t, blockNotified, minedBlockBytes, "Node%d received incorrect block", nodeIndex)

		blockNotifiedHash, err := chainhash.NewHash(blockNotified)
		require.NoError(t, err, "Failed to create block model from bytes on Node%d", nodeIndex)

		block, _, err := node.BlockChainDB.GetBlockHeaders(ctx, blockNotifiedHash, 1)
		require.NoError(t, err, "Failed to get block from database on Node%d", nodeIndex)
		require.NotNil(t, block, "Block not found in database on Node%d", nodeIndex)

	}
	mu.Unlock()
}

// TODO: Also add the helper function to see that all the transactions are in the block
