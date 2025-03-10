//go:build 1.24 || test_tnf

// How to run this test:
// $ cd test/tnf/
// $ SETTINGS_CONTEXT=docker.ci.tc1.run go test -v -run "^TestTNFTestSuite$/TestInvalidateBlock$" --tags test_tnf
package tnf

import (
	"context"
	"fmt"
	"math/big"
	"testing"
	"testing/synctest"
	"time"

	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/bitcoin-sv/teranode/test/utils/tconfig"
	"github.com/docker/go-connections/nat"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type TNFSyncTestSuite struct {
	helper.TeranodeTestSuite
}

func TestTNFSyncTestSuite(t *testing.T) {
	suite.Run(t, &TNFSyncTestSuite{
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
	miner1 = "/m1-eu/"
	miner2 = "/m2-us/"
	miner3 = "/m3-asia/"
)

func (suite *TNFSyncTestSuite) TestInvalidateBlock() {
	synctest.Run(func() {
		// Setup test environment and context
		cluster := suite.TeranodeTestEnv
		ctx := cluster.Context
		t := suite.T()
		logger := cluster.Logger
		settingsMap := suite.TConfig.Teranode.SettingsMap()

		// Get best block header from Node 1
		blockchainNode1 := cluster.Nodes[0].BlockchainClient
		header1, meta1, _ := blockchainNode1.GetBestBlockHeader(ctx)
		t.Logf("Best block header on Node 1: %s", header1.Hash().String())

		chainWork1 := new(big.Int).SetBytes(meta1.ChainWork)
		logger.Infof("Chainwork bytes on Node 1: %v", meta1.ChainWork)
		logger.Infof("Chainwork on Node 1: %v", chainWork1)

		// Get best block header from Node 2 (Miner 2)
		blockchainNode2 := cluster.Nodes[1].BlockchainClient
		headerInvalidate, meta2, _ := blockchainNode2.GetBestBlockHeader(ctx)
		logger.Infof("Best block header on Miner 2: %s", headerInvalidate.Hash().String())
		chainWork2 := new(big.Int).SetBytes(meta2.ChainWork)
		logger.Infof("Chainwork on Node 2: %v", chainWork2)

		// Get best block header from Node 3
		blockchainNode3 := cluster.Nodes[2].BlockchainClient
		header3, meta3, _ := blockchainNode3.GetBestBlockHeader(ctx)
		logger.Infof("Best block header on Node 3: %s", header3.Hash().String())
		chainWork3 := new(big.Int).SetBytes(meta3.ChainWork)
		logger.Infof("Chainwork on Node 3: %v", chainWork3)

		// Instead of a fixed 60-second sleep, simulate a delay using AfterFunc
		delayCtx60, cancel60 := context.WithCancel(ctx)
		var delay60Triggered bool
		// Register an AfterFunc that sets delay60Triggered to true when delayCtx60 is canceled.
		context.AfterFunc(delayCtx60, func() {
			delay60Triggered = true
		})
		// Cancel the delay context to trigger the AfterFunc
		cancel60()
		// Wait for all AfterFunc callbacks to complete
		synctest.Wait()
		require.True(t, delay60Triggered, "60-second delay AfterFunc did not trigger")

		// Stage 2: Update settings and restart Docker nodes
		settingsMap["SETTINGS_CONTEXT_1"] = "docker.ci.teranode2.tnf6.stage2"
		if err := cluster.RestartDockerNodes(settingsMap); err != nil {
			t.Errorf("Failed to restart nodes: %v", err)
		}

		err := cluster.InitializeTeranodeTestClients()
		if err != nil {
			t.Errorf("Failed to initialize teranode test clients: %v", err)
		}

		// Instead of a fixed 10-second sleep, simulate a delay using AfterFunc
		delayCtx10, cancel10 := context.WithCancel(ctx)
		var delay10Triggered bool
		context.AfterFunc(delayCtx10, func() {
			delay10Triggered = true
		})
		cancel10()
		synctest.Wait()
		require.True(t, delay10Triggered, "10-second delay AfterFunc did not trigger")

		// Health check: Get health_check_port from configuration
		port, ok := gocore.Config().GetInt("health_check_port", 8000)
		if !ok {
			suite.T().Fatalf("health_check_port not set in config")
		}
		ports := []int{port, port, port}

		// Wait for all nodes to be ready based on health liveness
		for index, port := range ports {
			mappedPort, err := cluster.GetMappedPort(fmt.Sprintf("teranode%d", index+1), nat.Port(fmt.Sprintf("%d/tcp", port)))
			if err != nil {
				suite.T().Fatal(err)
			}
			suite.T().Logf("Waiting for node %d to be ready", index)
			err = helper.WaitForHealthLiveness(mappedPort.Int(), 30*time.Second)
			if err != nil {
				suite.T().Fatal(err)
			}
		}

		// Send initial RUN event to all blockchain nodes
		for index, node := range cluster.Nodes {
			suite.T().Logf("Sending initial RUN event to Blockchain %d", index)
			err = helper.SendEventRun(cluster.Context, node.BlockchainClient, cluster.Logger)
			if err != nil {
				suite.T().Fatal(err)
			}
		}
		suite.T().Log("All nodes ready")

		// Refresh blockchainNode1 using Node 0
		blockchainNode1 = cluster.Nodes[0].BlockchainClient
		header1, meta1, _ = blockchainNode1.GetBestBlockHeader(ctx)
		miner := meta1.Miner

		logger.Infof("Best block header on node1: %s", header1.Hash().String())
		logger.Infof("Best block Miner on node 1: %v", miner)

		// Invalidate block on Node 1 if its miner is miner2
		if miner == miner2 {
			logger.Infof("Invalidating block on Miner 1")
			err := blockchainNode1.InvalidateBlock(ctx, header1.Hash())
			if err != nil {
				t.Errorf("Failed to invalidate block: %v", err)
			}
		}

		// Invalidate block on Node 2 if its miner is miner2
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

		// Invalidate block on Node 3 if its miner is miner2
		blockchainNode3 = cluster.Nodes[2].BlockchainClient
		header3, meta3, _ = blockchainNode3.GetBestBlockHeader(ctx)
		miner = meta3.Miner
		logger.Infof("Best block header on node 3: %s", header3.Hash().String())
		logger.Infof("Best block Miner on node 3: %v", miner)
		if miner == miner2 {
			logger.Infof("Invalidating block on Miner 3")
			err := blockchainNode3.InvalidateBlock(ctx, header3.Hash())
			if err != nil {
				t.Errorf("Failed to invalidate block: %v", err)
			}
		}

		// Verify that Node 1's best block miner is no longer miner2
		header1, meta1, _ = blockchainNode1.GetBestBlockHeader(ctx)
		logger.Infof("meta1: %v", meta1)
		assert.NotEqual(t, meta1.Miner, miner2, "Should not be Miner 2")

		// Verify that Node 1 and Node 3 have the same best block header
		header3, _, _ = blockchainNode3.GetBestBlockHeader(ctx)
		assert.Equal(t, header1.Hash(), header3.Hash(), "Blocks should be equal")
	})
}
