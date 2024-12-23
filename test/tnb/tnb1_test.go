//go:build test_all || test_tnb

// How to run tests:
//
// Run all tests in the suite:
// go test -v -tags test_tnb ./test/tnb
//
// Run a specific test:
// go test -v -tags test_tnb -run "^TestTNB1TestSuite$/TestReceiveExtendedFormatTx$" ./test/tnb
// go test -v -tags test_tnb -run "^TestTNB1TestSuite$/TestNoReformattingRequired$" ./test/tnb
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
	"io"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type TNB1TestSuite struct {
	helper.TeranodeTestSuite
}

func (suite *TNB1TestSuite) InitSuite() {
	suite.SettingsMap = map[string]string{
		"SETTINGS_CONTEXT_1": "docker.teranode1.test.tnb1Test",
		"SETTINGS_CONTEXT_2": "docker.teranode2.test.tnb1Test",
		"SETTINGS_CONTEXT_3": "docker.teranode3.test.tnb1Test",
	}
}

func (suite *TNB1TestSuite) SetupTest() {
	suite.InitSuite()
	suite.SetupTestEnv(suite.SettingsMap, suite.DefaultComposeFiles(), false)
}

func (suite *TNB1TestSuite) TearDownTest() {
}

func (suite *TNB1TestSuite) TestSendTxsInBatch() {
	testEnv := suite.TeranodeTestEnv
	ctx := testEnv.Context
	t := suite.T()
	blockchainNode0 := testEnv.Nodes[0].BlockchainClient
	logger := testEnv.Logger

	blockchainSubscription, err := blockchainNode0.Subscribe(ctx, "test-tnb1")

	if err != nil {
		t.Errorf("error subscribing to blockchain service: %v", err)
		return
	}

	var subtreeReader io.ReadCloser

	txHashesFromSubtree := make([]chainhash.Hash, 0)

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

					subtreeReader, err = testEnv.Nodes[0].SubtreeStore.GetIoReader(ctx, subtreeHash.CloneBytes(), options.WithFileExtension("subtree"))
					require.NoError(t, err)

					defer func() {
						_ = subtreeReader.Close()
					}()

					// wait for the subtree to be written to disk
					time.Sleep(10 * time.Second)

					subtree := util.Subtree{}

					err = subtree.DeserializeFromReader(subtreeReader)
					if err != nil {
						t.Errorf("error deserializing subtree: %v", err)
					}

					testEnv.Logger.Infof("subtree: %v", subtree)

					testEnv.Logger.Infof("subtree length: %v", len(subtree.Nodes))

					for i := 0; i < len(subtree.Nodes); i++ {
						txHashesFromSubtree = append(txHashesFromSubtree, subtree.Nodes[i].Hash)
					}

					testEnv.Logger.Infof("txHashes from subtree: %v", txHashesFromSubtree)
				}
			}
		}
	}()

	for i := 0; i < 1; i++ {
		txHashesSent, err := helper.CreateAndSendTxsConcurrently(ctx, testEnv.Nodes[0], 10)
		if err != nil {
			t.Errorf("Failed to create and send raw txs: %v", err)
		}

		baClient := testEnv.Nodes[0].BlockassemblyClient
		_, err = helper.GetMiningCandidate(ctx, baClient, logger)

		if err != nil {
			t.Errorf("Failed to mine block: %v", err)
		}

		time.Sleep(120 * time.Second)

		testEnv.Logger.Infof("txHashesSent sent: %v", txHashesSent)

		// Verify that all transactions in txHashesSent are included in txHashesFromSubtree
		for _, txHash := range txHashesSent {
			found := false

			for _, txHashFromSubtree := range txHashesFromSubtree {
				if txHash == txHashFromSubtree {
					found = true
					break
				}
			}

			require.True(t, found, "txHash not found in txHashesFromSubtree")
		}
	}
}

func (suite *TNB1TestSuite) TestReceiveExtendedFormatTx() {
	testEnv := suite.TeranodeTestEnv
	ctx := testEnv.Context
	t := suite.T()
	node := testEnv.Nodes[0]
	logger := testEnv.Logger

	// Create a transaction with Extended Format
	tx, err := helper.CreateAndSendTx(ctx, node)
	require.NoError(t, err)
	require.NotNil(t, tx)

	_, err = helper.GenerateBlocks(ctx, node, 1, logger)
	require.NoError(t, err)

	// Wait for block processing
	time.Sleep(5 * time.Second)

	// Verify transaction exists in block
	block, err := helper.GetBestBlockV2(ctx, node)
	require.NoError(t, err)
	require.NotNil(t, block)

	// Get and validate subtrees
	err = block.GetAndValidateSubtrees(ctx, logger, node.SubtreeStore, nil)
	require.NoError(t, err)

	// Check if transaction exists in the block's subtrees
	found := false

	for _, subtree := range block.SubtreeSlices {
		for _, txHash := range subtree.Nodes {
			if txHash.Hash.String() == tx.String() {
				found = true
				break
			}
		}

		if found {
			break
		}
	}

	require.True(t, found, "Transaction not found in block's subtrees")
}

func (suite *TNB1TestSuite) TestNoReformattingRequired() {
	testEnv := suite.TeranodeTestEnv
	ctx := testEnv.Context
	t := suite.T()
	node := testEnv.Nodes[0]
	logger := testEnv.Logger

	// Create multiple transactions with different characteristics
	txHashes := make([]chainhash.Hash, 0)
	for i := 0; i < 5; i++ {
		txHash, err := helper.CreateAndSendTx(ctx, node)
		require.NoError(t, err)
		txHashes = append(txHashes, txHash)
	}

	// Generate a block
	_, err := helper.GenerateBlocks(ctx, node, 1, logger)
	require.NoError(t, err)

	// Wait for block processing
	time.Sleep(5 * time.Second)

	// Get the block and verify transactions
	block, err := helper.GetBestBlockV2(ctx, node)
	require.NoError(t, err)
	require.NotNil(t, block)

	// Get and validate subtrees
	err = block.GetAndValidateSubtrees(ctx, logger, node.SubtreeStore, nil)
	require.NoError(t, err)

	// Create a map to track which transactions we've found
	foundTxs := make(map[string]bool)
	for _, txHash := range txHashes {
		foundTxs[txHash.String()] = false
	}

	// Check each subtree for transactions
	for _, subtree := range block.SubtreeSlices {
		for _, node := range subtree.Nodes {
			if _, exists := foundTxs[node.Hash.String()]; exists {
				foundTxs[node.Hash.String()] = true
			}
		}
	}

	// Verify all transactions were found
	missingTxs := make([]string, 0)
	for txHash, found := range foundTxs {
		if !found {
			missingTxs = append(missingTxs, txHash)
		}
	}
	require.Empty(t, missingTxs, "Transactions not found in block's subtrees: %v", missingTxs)
}

func TestTNB1TestSuite(t *testing.T) {
	suite.Run(t, new(TNB1TestSuite))
}

func allTransactionsIncluded(pendingTxs map[chainhash.Hash]bool) bool {
	for _, included := range pendingTxs {
		if !included {
			return false
		}
	}

	return true
}
