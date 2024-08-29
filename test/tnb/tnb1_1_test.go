//go:build tnbtests

package tnb

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/test/setup"
	helper "github.com/bitcoin-sv/ubsv/test/utils"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/suite"
	"gotest.tools/assert"
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

	url := "http://localhost:18090"
	ctx := context.Background()
	t := suite.T()
	framework := suite.Framework
	blockchainNode0 := framework.Nodes[0].BlockchainClient
	blockchainNode1 := framework.Nodes[1].BlockchainClient
	logger := framework.Logger

	for i := 0; i < 1; i++ {
		hashes, err := helper.CreateAndSendRawTxs(ctx, framework.Nodes[0], 30)
		if err != nil {
			t.Errorf("Failed to create and send raw txs: %v", err)
		}

		height, _ := helper.GetBlockHeight(url)
		baClient := framework.Nodes[0].BlockassemblyClient
		_, err = helper.MineBlock(ctx, baClient, logger)

		if err != nil {
			t.Errorf("Failed to mine block: %v", err)
		}

		time.Sleep(120 * time.Second)

		var o []options.Options
		o = append(o, options.WithFileExtension("block"))
		blockStore := framework.Nodes[0].Blockstore

		var newHeight int

		pendingTxs := make(map[chainhash.Hash]bool)
		for _, txHash := range hashes {
			pendingTxs[txHash] = false
		}

		for i := 0; i < 5; i++ {
			newHeight, _ = helper.GetBlockHeight(url)
			if newHeight > height {
				height = newHeight
				fmt.Println("Testing at height: ", height)
				mBlock, _ := blockchainNode0.GetBlockByHeight(framework.Context, uint32(newHeight))
				r, err := blockStore.GetIoReader(framework.Context, mBlock.Hash()[:], o...)

				if err != nil {
					t.Errorf("error getting block reader: %v", err)
				}

				if err == nil {
					for txHash, included := range pendingTxs {
						if included {
							continue
						}

						bl, err := helper.ReadFile(framework.Context, "block", framework.Logger, r, txHash, framework.Nodes[0].BlockstoreUrl)
						if err != nil {
							t.Errorf("error reading block for transaction hash %v: %v", txHash, err)
						} else if bl {
							pendingTxs[txHash] = true

							logger.Infof("Transaction hash %v included in block at height %d\n", txHash, newHeight)
						}
					}

					if allTransactionsIncluded(pendingTxs) {
						fmt.Println("All transactions included in blocks")
						return
					}
				}
			}
		}
	}

	// assert.True(t, bl, "All transactions not included in the block")
	headerNode1, _, _ := blockchainNode1.GetBestBlockHeader(ctx)
	headerNode0, _, _ := blockchainNode0.GetBestBlockHeader(ctx)
	assert.Equal(t, headerNode0.Hash(), headerNode1.Hash(), "Best block headers are not equal")
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
