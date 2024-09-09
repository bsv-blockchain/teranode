//go:build tnbtests

//Settings:
// Uses validator_sendBatchSize.docker.ci.tnb1Test=10

//Steps:
// 1. Create and send transactions concurrently
// 2. Get the block height
// 3. Mine a block
// 4. Get the block height
// 5. Check if all the transaction hashes are included in the block
// 6. If not, mine another block
// 7. Repeat steps 4-6 until all transactions are included in the block
// 8. Get the best block headers from both nodes
// 9. Assert that the best block headers are equal (failing at the moment)

//How to run manually:
// cd test/tnb
// SETTINGS_CONTEXT=docker.ci.tc1.run go test -v -run "^TestTNB1TestSuite$/TestSendTxsInBatch$" -tags tnbtests

package tnb

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/test/setup"
	helper "github.com/bitcoin-sv/ubsv/test/utils"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type TNB1TestSuite struct {
	setup.BitcoinTestSuite
}

func (suite *TNB1TestSuite) InitSuite() {
	suite.SettingsMap = map[string]string{
		"SETTINGS_CONTEXT_1": "docker.ci.ubsv1.tnb1Test",
		"SETTINGS_CONTEXT_2": "docker.ci.ubsv2.tnb1Test",
		"SETTINGS_CONTEXT_3": "docker.ci.ubsv3.tnb1Test",
	}
}

func (suite *TNB1TestSuite) SetupTest() {
	suite.InitSuite()
	suite.BitcoinTestSuite.SetupTestWithCustomSettings(suite.SettingsMap)
}

func (suite *TNB1TestSuite) TearDownTest() {
}

func (suite *TNB1TestSuite) TestSendTxsInBatch() {

	ctx := context.Background()
	t := suite.T()
	framework := suite.Framework
	blockchainNode0 := framework.Nodes[0].BlockchainClient
	logger := framework.Logger

	blockchainSubscription, err := blockchainNode0.Subscribe(ctx, "test-tnb1")
	
	if err != nil {
		t.Errorf("error subscribing to blockchain service: %v", err)
		return
	}



	var subtreeReader io.ReadCloser
	txHashesFromSubtree := make([]chainhash.Hash,0)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case notification := <-blockchainSubscription:
				if notification.Type == model.NotificationType_Subtree {
					subtreeHash, err := chainhash.NewHash(notification.Hash)
					framework.Logger.Infof("subtreeHash: %v", subtreeHash)
					require.NoError(t, err)
					subtreeReader, err = framework.Nodes[0].SubtreeStore.GetIoReader(ctx, subtreeHash.CloneBytes(), options.WithFileExtension("subtree"))
					require.NoError(t, err)
					
					defer func() {
						_ = subtreeReader.Close()
					}()

					subtree := util.Subtree{}
								
					err = subtree.DeserializeFromReader(subtreeReader)
					if err != nil {
						t.Errorf("error deserializing subtree: %v", err)
					}
					
					framework.Logger.Infof("subtree: %v", subtree)
					
					framework.Logger.Infof("subtree length: %v", len(subtree.Nodes))
					
					for i := 0; i < len(subtree.Nodes); i++ {
						txHashesFromSubtree = append(txHashesFromSubtree, subtree.Nodes[i].Hash)
					}

					framework.Logger.Infof("txHashes from subtree: %v", txHashesFromSubtree)

				}
			}
		}
	}()

	for i := 0; i < 1; i++ {
		txHashesSent, err := helper.CreateAndSendRawTxsConcurrently(ctx, framework.Nodes[0], 10)
		if err != nil {
			t.Errorf("Failed to create and send raw txs: %v", err)
		}

		// height, _ := helper.GetBlockHeight(url)
		baClient := framework.Nodes[0].BlockassemblyClient
		_, err = helper.GetMiningCandidate(ctx, baClient, logger)
		// _, err = helper.MineBlock(ctx, baClient, logger)

		if err != nil {
			t.Errorf("Failed to mine block: %v", err)
		}

		time.Sleep(120 * time.Second)

		var o []options.Options
		o = append(o, options.WithFileExtension("block"))

		framework.Logger.Infof("txHashesSent sent: %v", txHashesSent)

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
