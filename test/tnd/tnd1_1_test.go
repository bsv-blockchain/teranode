//go:build test_all || test_tnd || test_functional

// How to run this test:
// $ cd test/tnd/
// $ go test -v -run "^TestTND1_1TestSuite$/TestBlockPropagation$" -tags test_functional
// $ go test -v -run "^TestTND1_1TestSuite$/TestBlockPropagationWithNotifications$" -tags test_functional

package tnd

import (
	"sync"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/model"
	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type TND1_1TestSuite struct {
	helper.TeranodeTestSuite
}

func (suite *TND1_1TestSuite) TearDownTest() {
}

// TestBlockPropagation verifies that blocks are correctly propagated between nodes
// in the network after being mined. This test:
// 1. Gets initial block height
// 2. Mines multiple blocks on node0
// 3. Verifies blocks propagate to all nodes
// 4. Checks block headers match across nodes
func (suite *TND1_1TestSuite) TestBlockPropagation() {
	t := suite.T()
	testEnv := suite.TeranodeTestEnv
	ctx := testEnv.Context
	logger := testEnv.Logger
	url := "http://localhost:10090"
	node0 := testEnv.Nodes[0]
	node1 := testEnv.Nodes[1]
	node2 := testEnv.Nodes[2]

	// Get initial block height
	initialHeight, err := helper.GetBlockHeight(url)
	require.NoError(t, err)
	t.Logf("Initial block height: %d", initialHeight)

	// Mine multiple blocks on node0
	numBlocks := uint32(5)
	targetHeight := initialHeight + numBlocks

	for i := uint32(0); i < numBlocks; i++ {
		_, err = helper.MineBlockWithRPC(ctx, node0, logger)
		require.NoError(t, err)
		t.Logf("Mined block %d of %d", i+1, numBlocks)
	}

	// Wait for blocks to propagate to all nodes
	t.Log("Waiting for blocks to propagate to all nodes...")

	for _, nodeURL := range []string{
		"http://localhost:10090", // node0
		"http://localhost:12090", // node1
		"http://localhost:14090", // node2
	} {
		err := helper.WaitForBlockHeight(nodeURL, targetHeight, 60)
		require.NoError(t, err)
		currentHeight, err := helper.GetBlockHeight(nodeURL)
		require.NoError(t, err)
		t.Logf("Node at %s reached height %d", nodeURL, currentHeight)
	}

	// Get block headers from each node
	header0, meta0, err := node0.BlockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	t.Logf("Node 0 best block header: %s at height %d", header0.Hash(), meta0.Height)

	header1, meta1, err := node1.BlockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	t.Logf("Node 1 best block header: %s at height %d", header1.Hash(), meta1.Height)

	header2, meta2, err := node2.BlockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	t.Logf("Node 2 best block header: %s at height %d", header2.Hash(), meta2.Height)

	// Verify all nodes have same best block header
	assert.Equal(t, header0.Hash(), header1.Hash(), "Node 0 and Node 1 headers should match")
	assert.Equal(t, header0.Hash(), header2.Hash(), "Node 0 and Node 2 headers should match")

	// Verify all nodes have same height
	assert.Equal(t, meta0.Height, meta1.Height, "Node 0 and Node 1 heights should match")
	assert.Equal(t, meta0.Height, meta2.Height, "Node 0 and Node 2 heights should match")
	assert.Equal(t, targetHeight, meta0.Height, "Height should match target")

	// Get block headers for all mined blocks from each node
	headers0, meta0s, err := node0.BlockchainClient.GetBlockHeadersFromHeight(ctx, initialHeight+1, numBlocks)
	require.NoError(t, err)

	headers1, meta1s, err := node1.BlockchainClient.GetBlockHeadersFromHeight(ctx, initialHeight+1, numBlocks)
	require.NoError(t, err)

	headers2, meta2s, err := node2.BlockchainClient.GetBlockHeadersFromHeight(ctx, initialHeight+1, numBlocks)
	require.NoError(t, err)

	// Verify all blocks in the sequence match across nodes
	t.Log("Verifying all blocks in sequence match across nodes...")

	for i := 0; i < int(numBlocks); i++ {
		t.Logf("Verifying block at height %d", meta0s[i].Height)
		assert.Equal(t, headers0[i].Hash(), headers1[i].Hash(),
			"Block headers should match between node0 and node1 at height %d", meta0s[i].Height)
		assert.Equal(t, headers0[i].Hash(), headers2[i].Hash(),
			"Block headers should match between node0 and node2 at height %d", meta0s[i].Height)
		assert.Equal(t, meta0s[i].Height, meta1s[i].Height,
			"Block heights should match between node0 and node1 at height %d", meta0s[i].Height)
		assert.Equal(t, meta0s[i].Height, meta2s[i].Height,
			"Block heights should match between node0 and node2 at height %d", meta0s[i].Height)
	}
}

// TestBlockPropagationWithNotifications verifies block propagation between nodes
// by subscribing to blockchain notifications on receiving nodes
func (suite *TND1_1TestSuite) TestBlockPropagationWithNotifications() {
	t := suite.T()
	testEnv := suite.TeranodeTestEnv
	ctx := testEnv.Context
	logger := testEnv.Logger

	// Set up notification channels for receiving nodes
	var wg sync.WaitGroup

	blockNotifications := make(map[int]chan *model.Notification)
	receivedBlocks := make(map[int][]byte)

	var mu sync.Mutex

	// Subscribe to blockchain notifications on all nodes except node0
	for i := 1; i < len(testEnv.Nodes); i++ {
		node := testEnv.Nodes[i]
		notifChan := make(chan *model.Notification, 10)
		blockNotifications[i] = notifChan

		blockchainSubscription, err := node.BlockchainClient.Subscribe(ctx, "test-block-propagation")
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

	// Get initial block height
	initialHeight, err := helper.GetBlockHeight("http://localhost:10090")
	require.NoError(t, err)
	t.Logf("Initial block height: %d", initialHeight)

	// Mine multiple blocks on node0
	numBlocks := uint32(5)
	targetHeight := initialHeight + numBlocks

	// Store block hashes for verification
	minedBlockHashes := make([][]byte, 0, numBlocks)

	for i := uint32(0); i < numBlocks; i++ {
		minedBlockBytes, err := helper.MineBlock(ctx, testEnv.Nodes[0].BlockassemblyClient, logger)
		require.NoError(t, err, "Failed to mine block %d", i+1)

		minedBlockHashes = append(minedBlockHashes, minedBlockBytes)
		t.Logf("Mined block %d: %x", i+1, minedBlockBytes)
	}

	// Wait for blocks to propagate to all nodes
	t.Log("Waiting for blocks to propagate to all nodes...")

	for _, node := range []string{
		"http://localhost:10090", // node0
		"http://localhost:12090", // node1
		"http://localhost:14090", // node2
	} {
		err := helper.WaitForBlockHeight(node, targetHeight, 60)
		require.NoError(t, err)
		currentHeight, err := helper.GetBlockHeight(node)
		require.NoError(t, err)
		t.Logf("Node at %s reached height %d", node, currentHeight)
	}

	// Allow time for all block notifications to be processed
	time.Sleep(5 * time.Second)

	// Verify that all receiving nodes got block notifications
	mu.Lock()
	for nodeIndex, blockNotified := range receivedBlocks {
		require.NotNil(t, blockNotified, "Node%d did not receive block notification", nodeIndex)

		// Get block headers from each node
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

		// Verify block heights and chain
		// nolint: gosec
		for i, header := range headers {
			require.Equal(t, initialHeight+uint32(i)+1, meta[i].Height,
				"Unexpected block height for block %d on Node%d", i+1, nodeIndex)

			if i > 0 {
				require.Equal(t, headers[i-1].Hash().CloneBytes(), header.HashPrevBlock,
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
}

func TestTND1_1TestSuite(t *testing.T) {
	suite.Run(t, new(TND1_1TestSuite))
}
