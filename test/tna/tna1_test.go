//go:build test_all || test_tna

// How to run this test manually:
// $ go test -v -run "^TestTNA1TestSuite$/TestBroadcastNewTxAllNodes$" -tags test_tna ./test/tna/tna1_test.go

package tna

import (
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/model"
	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/bitcoin-sv/teranode/test/utils/tconfig"
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
	logger := testEnv.Logger
	url := "http://" + testEnv.Nodes[0].AssetURL
	found := 0

	blockchainClientNode0 := testEnv.Nodes[0].BlockchainClient

	block1, err := testEnv.Nodes[0].BlockchainClient.GetBlockByHeight(ctx, 1)
	require.NoError(t, err)

	var hashes []*chainhash.Hash

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
	_, hashesTx, err := testEnv.Nodes[0].CreateAndSendTxs(ctx, parenTx, 35)

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

	//check if at least 1 tx it's included inside the subtree receveid by notification
	txHashes, err := helper.GetSubtreeTxHashes(ctx, logger, hashes[0], url, testEnv.Nodes[0].Settings)
	require.NoError(t, err, "Failed to get subtree tx hashes: %v", err)
	t.Logf("Subtree tx hashes: %v", txHashes)

	hashSet := make(map[string]bool)

	for _, hash := range hashesTx {
		hashSet[hash.String()] = true
	}

	for _, txSubtrees := range txHashes {
		if hashSet[txSubtrees.String()] {
			found += 1
		}
	}

	if found > 0 {
		t.Logf("%d txs in common", found)
	} else {
		t.Fatalf("Test failed, no txs in common")
	}
}
