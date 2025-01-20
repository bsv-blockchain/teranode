//go:build test_all || test_tnf

// How to run this test:
// $ cd test/tnf/
// $ SETTINGS_CONTEXT=docker.ci.tc1.run go test -v -run "^TestTNFTestSuite$/TestInvalidateBlock$" --tags tnftests

package tnf

import (
	"math/big"
	"testing"
	"time"

	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/bitcoin-sv/teranode/test/utils/tconfig"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type TNFTestSuite struct {
	helper.TeranodeTestSuite
}

func TestTNFTestSuite(t *testing.T) {
	suite.Run(t, &TNFTestSuite{
		TeranodeTestSuite: helper.TeranodeTestSuite{
			TConfig: tconfig.LoadTConfig(
				map[string]any{
					tconfig.KeyTeranodeContexts: []string{
						"docker.teranode1.test.tnf6",
						"docker.teranode2.test.tnf6.stage1",
						"docker.teranode3.test.tnf6",
					},
				},
			),
		},
	},
	)
}

const (
	NodeURL1 = "http://localhost:10090"
	NodeURL2 = "http://localhost:12090"
	NodeURL3 = "http://localhost:14090"
)

const (
	miner1 = "/m1-eu/"
	miner2 = "/m2-us/"
	miner3 = "/m3-asia/"
)

func (suite *TNFTestSuite) TestInvalidateBlock() {
	cluster := suite.TeranodeTestEnv
	ctx := cluster.Context
	t := suite.T()
	logger := cluster.Logger
	settingsMap := suite.TConfig.Teranode.SettingsMap()

	defer func() {
		if r := recover(); r != nil {
			t.Errorf("Recovered from panic: %v", r)

			_ = cluster.Compose.Down(cluster.Context)
		}
	}()

	blockchainNode1 := cluster.Nodes[0].BlockchainClient
	header1, meta1, _ := blockchainNode1.GetBestBlockHeader(ctx)
	logger.Infof("Best block header on Node 1: %s", header1.Hash().String())

	chainWork1 := new(big.Int).SetBytes(meta1.ChainWork)
	logger.Infof("Chainwork bytes on Node 1: %v", meta1.ChainWork)
	logger.Infof("Chainwork on Node 1: %v", chainWork1)

	blockchainNode2 := cluster.Nodes[1].BlockchainClient
	headerInvalidate, meta2, _ := blockchainNode2.GetBestBlockHeader(ctx)

	logger.Infof("Best block header on Miner 2: %s", headerInvalidate.Hash().String())

	chainWork2 := new(big.Int).SetBytes(meta2.ChainWork)
	logger.Infof("Chainwork on Node 2: %v", chainWork2)

	blockchainNode3 := cluster.Nodes[2].BlockchainClient

	header3, meta3, _ := blockchainNode3.GetBestBlockHeader(ctx)
	logger.Infof("Best block header on Node 3: %s", header3.Hash().String())

	chainWork3 := new(big.Int).SetBytes(meta3.ChainWork)
	logger.Infof("Chainwork on Node 3: %v", chainWork3)

	time.Sleep(60 * time.Second)

	// Stage 2
	settingsMap["SETTINGS_CONTEXT_1"] = "docker.ci.teranode2.tnf6.stage2"
	if err := cluster.RestartDockerNodes(settingsMap); err != nil {
		t.Errorf("Failed to restart nodes: %v", err)
	}

	time.Sleep(2 * time.Second)

	blockchainNode1 = cluster.Nodes[0].BlockchainClient
	header1, meta1, _ = blockchainNode1.GetBestBlockHeader(ctx)
	miner := meta1.Miner

	logger.Infof("Best block header on node1: %s", header1.Hash().String())
	logger.Infof("Best block Miner on node 1: %v", miner)

	if miner == miner2 {
		logger.Infof("Invalidating block on Miner 1")

		err := blockchainNode1.InvalidateBlock(ctx, header1.Hash())
		if err != nil {
			t.Errorf("Failed to invalidate block: %v", err)
		}
	}

	blockchainNode2 = cluster.Nodes[1].BlockchainClient
	header2, meta2, _ := blockchainNode2.GetBestBlockHeader(ctx)
	miner = meta2.Miner

	logger.Infof("Best block header on node 2: %s", header2.Hash().String())
	logger.Infof("Best block Miner on node 2: %v", miner)

	if miner == miner2 {
		logger.Infof("Invalidating block on Miner 2")

		err := blockchainNode2.InvalidateBlock(ctx, header2.Hash())
		if err != nil {
			t.Errorf("Failed to invalidate block: %v", err)
		}
	}

	blockchainNode3 = cluster.Nodes[2].BlockchainClient
	header3, meta3, _ = blockchainNode3.GetBestBlockHeader(ctx)
	miner = meta3.Miner

	logger.Infof("Best block header on node 3: %s", header3.Hash().String())
	logger.Infof("Best block Miner node 3: %v", miner)

	if miner == miner2 {
		logger.Infof("Invalidating block on Miner 3")

		err := blockchainNode3.InvalidateBlock(ctx, header3.Hash())
		if err != nil {
			t.Errorf("Failed to invalidate block: %v", err)
		}
	}

	header1, meta1, _ = blockchainNode1.GetBestBlockHeader(ctx)
	logger.Infof("meta1: %v", meta1)
	assert.NotEqual(t, meta1.Miner, miner2, "Should not be Miner 2")

	header3, _, _ = blockchainNode3.GetBestBlockHeader(ctx)

	assert.Equal(t, header1.Hash(), header3.Hash(), "Blocks should be equal")
}
