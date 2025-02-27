//go:build test_all || test_tna

// How to run this test manually:
// $ cd test/tna
// $ go test -v -run "^TestTNA1TestSuite$/TestBroadcastNewTxAllNodes$" -tags test_tna

package tna

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/model"
	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/bitcoin-sv/teranode/test/utils/tconfig"
	"github.com/bitcoin-sv/teranode/test/utils/tstore"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type TNA1TestSuite struct {
	helper.TeranodeTestSuite
}

func TestTNA1TestSuite(t *testing.T) {
	suite.Run(t, &TNA1TestSuite{
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

func (suite *TNA1TestSuite) TestBroadcastNewTxAllNodes() {
	// Test setup
	testEnv := suite.TeranodeTestEnv
	ctx := testEnv.Context
	t := suite.T()

	blockchainClientNode0 := testEnv.Nodes[0].BlockchainClient

	block1, err := testEnv.Nodes[0].BlockchainClient.GetBlockByHeight(ctx, 1)
	require.NoError(t, err)

	var hashes []*chainhash.Hash

	var found int

	blockchainSubscription, err := blockchainClientNode0.Subscribe(ctx, "test-broadcast-pow")
	if err != nil {
		t.Errorf("error subscribing to blockchain service: %v", err)
		return
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case notification := <-blockchainSubscription:
				t.Logf("Received notification: %v", notification)

				if notification.Type == model.NotificationType_Subtree {
					hash, err := chainhash.NewHash(notification.Hash)
					require.NoError(t, err)

					hashes = append(hashes, hash)

					t.Logf("Length of hashes: %d", len(hashes))
				} else {
					t.Logf("other notifications than subtrees")
					t.Logf("notification type: %v", notification.Type)
				}
			}
		}
	}()

	time.Sleep(10 * time.Second)

	parenTx := block1.CoinbaseTx
	_, hashesTx, err := testEnv.Nodes[0].CreateAndSendTxs(ctx, parenTx, 30)

	if err != nil {
		t.Errorf("Failed to create and send raw txs: %v", err)
	}

	t.Logf("Hashes in created block: %v", hashesTx)

	if len(hashes) > 0 {
		t.Logf("First element of hashes: %v", hashes[0])
	} else {
		t.Log("hashes is empty!")
	}

	t.Logf("num of subtrees: %d", len(hashes))

	// Keep track of which transactions we've found
	foundTxs := make(map[string]bool) // Using string representation of hash as key
	remainingTxs := len(hashesTx)

	// Search inside teranode1, teranode2 and teranode3 subfolders
	for _, subtreeHash := range hashes {
		t.Logf("Subtree hash: %v,   subtreeHash string %v", subtreeHash, subtreeHash.String())

		for i := 1; i <= 3; i++ {
			subDir := fmt.Sprintf("teranode%d/subtreestore", i)
			subSubDir := subtreeHash.String()[:2]

			t.Logf("Checking directory: %s", subDir)
			t.Logf("Subdirectory: %s", subSubDir)

			filePath := filepath.Join(testEnv.TConfig.LocalSystem.DataDir, subDir, subSubDir, subtreeHash.String())
			t.Logf("Full path: %s", filePath)

			if resp, err := suite.TeranodeTestEnv.ComposeSharedStorage.Glob(ctx, &tstore.GlobRequest{RootPath: filePath}); err == nil {
				if len(resp.Paths) > 0 {
					t.Logf("Subtree %s exists.", filePath)
					found += 1

					subtreeStore := testEnv.Nodes[0].ClientSubtreestore

					// Check only transactions that haven't been found yet
					for _, txHash := range hashesTx {
						if foundTxs[txHash.String()] {
							continue // Skip if we already found this tx
						}

						exists, err := helper.TestTxInSubtree(ctx, testEnv.Logger, subtreeStore, subtreeHash.CloneBytes(), *txHash)
						require.NoError(t, err)

						if exists {
							t.Logf("Tx %s found in subtree %s", txHash.String(), subtreeHash.String())
							foundTxs[txHash.String()] = true
							remainingTxs--
						}
					}
				}
			} else if os.IsNotExist(err) {
				t.Logf("Subtree %s doesn't exists %s, filePath %v", subtreeHash.String(), subDir, filePath)
			} else {
				t.Logf("Error checking the file %s in %s filePath %v : %v ", subtreeHash.String(), subDir, filePath, err)
			}
		}
	}

	if found == 0 {
		t.Errorf("Test failed, no subtree found")
	}

	// Verify that all transactions were found
	require.Equal(t, 0, remainingTxs, "Not all transactions were found in the subtrees")
}
