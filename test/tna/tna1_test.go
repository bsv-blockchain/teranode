//go:build test_tna || debug

// How to run this test manually:
// $ go test -v -run "^TestTNA1TestSuite$/TestBroadcastNewTxAllNodes$" -tags test_tna ./test/tna/tna1_test.go

package tna

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

	node1 := testEnv.Nodes[0]
	blockchainClientNode1 := node1.BlockchainClient

	var receivedSubtreeHashes []*chainhash.Hash

	blockchainSubscription, err := blockchainClientNode1.Subscribe(ctx, "test-broadcast-pow")
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

					receivedSubtreeHashes = append(receivedSubtreeHashes, hash)

					t.Logf("Length of hashes: %d", len(receivedSubtreeHashes))
				} else {
					t.Logf("other notifications than subtrees")
					t.Logf("notification type: %v", notification.Type)
				}
			}
		}
	}()

	block1, err := node1.BlockchainClient.GetBlockByHeight(ctx, 1)
	require.NoError(t, err)
	parenTx := block1.CoinbaseTx
	_, sentTxHashes, err := node1.CreateAndSendTxs(t, ctx, parenTx, 35)

	if err != nil {
		t.Errorf("Failed to create and send raw txs: %v", err)
	}

	t.Logf("Hashes in created block: %v", sentTxHashes)

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	timeout := time.After(10 * time.Second)

	success := false
	for !success {
		select {
		case <-timeout:
			t.Fatalf("Timeout waiting for transaction to appear in block assembly")
		case <-ticker.C:
			if len(receivedSubtreeHashes) > 0 {
				t.Logf("First element of hashes: %v", receivedSubtreeHashes[0])
				success = true
			} else {
				t.Log("hashes is empty!")
			}
		}
	}

	t.Logf("num of subtrees: %d", len(receivedSubtreeHashes))

	//check if at least 1 tx it's included inside the subtree received by notification
	txHashesInFirstSubtree, err := helper.GetSubtreeTxHashes(ctx, logger, receivedSubtreeHashes[0], url, node1.Settings)
	require.NoError(t, err, "Failed to get subtree tx hashes: %v", err)
	t.Logf("Subtree tx hashes: %v", txHashesInFirstSubtree)

	hashSet := make(map[string]bool)

	for _, hash := range sentTxHashes {
		hashSet[hash.String()] = true
	}

	for _, eachTxInTheSubtree := range txHashesInFirstSubtree {
		if hashSet[eachTxInTheSubtree.String()] {
			found += 1
		}
	}

	if found > 0 {
		t.Logf("%d txs in common", found)
	} else {
		t.Fatalf("Test failed, no txs in common")
	}

	//TODO: We are not checking if all 35 TXs are included inside the subtrees received, so we should have a check for that
}
