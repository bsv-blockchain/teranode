//go:build 1.24 || test_tna_sync

package tna

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"testing/synctest"

	"github.com/bitcoin-sv/teranode/model"
	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/bitcoin-sv/teranode/test/utils/tconfig"
	"github.com/bitcoin-sv/teranode/test/utils/tstore"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type TNA1SyncTestSuite struct {
	helper.TeranodeTestSuite
}

func TestTNA1SyncTestSuite(t *testing.T) {
	suite.Run(t, &TNA1SyncTestSuite{
		TeranodeTestSuite: helper.TeranodeTestSuite{
			TConfig: tconfig.LoadTConfig(map[string]any{
				tconfig.KeyTeranodeContexts: []string{
					"docker.teranode1.test.tna1Test",
					"docker.teranode2.test.tna1Test",
					"docker.teranode3.test.tna1Test",
				},
			}),
		},
	})
}

func (suite *TNA1SyncTestSuite) TestBroadcastNewTxAllNodes() {

	synctest.Run(func() {

		testEnv := suite.TeranodeTestEnv
		ctx, cancel := context.WithCancel(testEnv.Context)
		defer cancel()
		t := suite.T()

		blockchainClientNode0 := testEnv.Nodes[0].BlockchainClient

		block1, err := blockchainClientNode0.GetBlockByHeight(ctx, 1)
		require.NoError(t, err)

		var hashes []*chainhash.Hash
		var found int
		notificationReceived := false

		blockchainSubscription, err := blockchainClientNode0.Subscribe(ctx, "test-broadcast-pow")
		require.NoError(t, err, "error subscribing to blockchain service")

		// Goroutine to handle notifications
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case notification, ok := <-blockchainSubscription:
					if !ok {
						return
					}
					t.Logf("Received notification: %v", notification)
					if notification.Type == model.NotificationType_Subtree {
						hash, err := chainhash.NewHash(notification.Hash)
						require.NoError(t, err)
						hashes = append(hashes, hash)
						// When notification arrives, use context.AfterFunc to track it
						context.AfterFunc(ctx, func() {
							notificationReceived = true
						})
					} else {
						t.Logf("Other notification of type: %v", notification.Type)
					}
				}
			}
		}()

		// Instead of time.Sleep, wait that AfterFunc being executed
		synctest.Wait()

		// Verify notification received
		require.True(t, notificationReceived, "Expected at least one subtree notification")

		// Continues to send txs
		parenTx := block1.CoinbaseTx
		_, hashesTx, err := testEnv.Nodes[0].CreateAndSendTxs(ctx, parenTx, 30)
		require.NoError(t, err, "Failed to create and send raw txs")

		t.Logf("Hashes in created block: %v", hashesTx)
		t.Logf("Number of subtree notifications received: %d", len(hashes))

		// Verify the presence of subtrees inside the Txs
		foundTxs := make(map[string]bool)
		remainingTxs := len(hashesTx)

		for _, subtreeHash := range hashes {
			t.Logf("Subtree hash: %v", subtreeHash.String())
			for i := 1; i <= 3; i++ {
				subDir := fmt.Sprintf("teranode%d/subtreestore", i)
				subSubDir := subtreeHash.String()[:2]
				filePath := filepath.Join(testEnv.TConfig.LocalSystem.DataDir, subDir, subSubDir, subtreeHash.String())
				t.Logf("Checking path: %s", filePath)
				if resp, err := testEnv.ComposeSharedStorage.Glob(ctx, &tstore.GlobRequest{RootPath: filePath}); err == nil {
					if len(resp.Paths) > 0 {
						t.Logf("Subtree %s exists.", filePath)
						found++
						subtreeStore := testEnv.Nodes[0].ClientSubtreestore
						for _, txHash := range hashesTx {
							if foundTxs[txHash.String()] {
								continue
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
					t.Logf("Subtree %s doesn't exist in %s, path: %v", subtreeHash.String(), subDir, filePath)
				} else {
					t.Logf("Error checking file %s in %s: %v", subtreeHash.String(), subDir, err)
				}
			}
		}

		require.NotEqual(t, 0, found, "Test failed, no subtree found")
		require.Equal(t, 0, remainingTxs, "Not all transactions were found in the subtrees")
	})
}
