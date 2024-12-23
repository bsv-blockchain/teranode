//go:build test_all || test_tne || test_functional

package tne

// How to run this test:
// $ cd test/tne/
// $ go test -v -run "^TestTNE1_1TestSuite$/TestNode_DoNotVerifyTransactionsIfAlreadyVerified$" -tags test_functional
import (
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/services/blockchain/blockchain_api"
	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type TNE1_1TestSuite struct {
	helper.TeranodeTestSuite
}

func (suite *TNE1_1TestSuite) InitSuite() {
	suite.SettingsMap = map[string]string{
		"SETTINGS_CONTEXT_1": "docker.teranode1.test",
		"SETTINGS_CONTEXT_2": "docker.teranode2.test",
		"SETTINGS_CONTEXT_3": "docker.teranode3.test",
	}
}

func (suite *TNE1_1TestSuite) SetupTest() {
	suite.InitSuite()
	suite.SetupTestEnv(suite.SettingsMap, suite.DefaultComposeFiles(), false)
}

func (suite *TNE1_1TestSuite) TearDownTest() {
}

func (suite *TNE1_1TestSuite) TestNode_DoNotVerifyTransactionsIfAlreadyVerified() {
	t := suite.T()
	framework := suite.TeranodeTestEnv
	settingsMap := suite.SettingsMap
	logger := framework.Logger
	blockchainNode0 := framework.Nodes[0].BlockchainClient
	blockchainNode1 := framework.Nodes[1].BlockchainClient
	ctx := framework.Context

	settingsMap["SETTINGS_CONTEXT_1"] = "docker.teranode1.test.stopP2P"
	settingsMap["SETTINGS_CONTEXT_2"] = "docker.teranode2.test.stopP2P"
	settingsMap["SETTINGS_CONTEXT_3"] = "docker.teranode3.test.stopP2P"

	if err := framework.RestartDockerNodes(settingsMap); err != nil {
		t.Errorf("Failed to restart nodes: %v", err)
	}

	var err error
	// wait for all blockchain nodes to be ready
	for index, node := range suite.TeranodeTestEnv.Nodes {
		suite.T().Logf("Sending initial RUN event to Blockchain %d", index)

		err = helper.SendEventRun(suite.TeranodeTestEnv.Context, node.BlockchainClient, suite.TeranodeTestEnv.Logger)
		if err != nil {
			suite.T().Fatal(err)
		}
	}

	ports := []int{10000, 12000, 14000} // ports are defined in docker-compose.e2etest.yml
	for index, port := range ports {
		suite.T().Logf("Waiting for node %d to be ready", index)

		err = helper.WaitForHealthLiveness(port, 30*time.Second)
		if err != nil {
			suite.T().Fatal(err)
		}
	}

	suite.T().Log("All nodes ready")

	for i := 0; i < 5; i++ {
		for node := 0; node < 3; node++ {
			hashes, err := helper.CreateAndSendTxs(ctx, framework.Nodes[node], 1)
			if err != nil {
				t.Errorf("Failed to create and send raw txs: %v", err)
			}

			logger.Infof("Hashes: %v", hashes)

			_, err = helper.MineBlockWithRPC(ctx, framework.Nodes[node], logger)
			require.NoError(t, err)

			if err != nil {
				t.Errorf("Failed to mine block: %v", err)
			}
		}
	}

	settingsMap["SETTINGS_CONTEXT_1"] = "docker.teranode1.test"
	settingsMap["SETTINGS_CONTEXT_2"] = "docker.teranode2.test"
	settingsMap["SETTINGS_CONTEXT_3"] = "docker.teranode3.test"

	if err := framework.RestartDockerNodes(settingsMap); err != nil {
		t.Errorf("Failed to restart nodes: %v", err)
	}

	// Wait for nodes to be healthy
	for index, port := range ports {
		suite.T().Logf("Waiting for node %d to be ready", index)

		err = helper.WaitForHealthLiveness(port, 30*time.Second)
		if err != nil {
			suite.T().Fatal(err)
		}
	}

	// Wait for all nodes to reach RUNNING state
	timeout := time.After(30 * time.Second)
	ticker := time.NewTicker(500 * time.Millisecond)

	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			t.Fatal("Timeout waiting for nodes to reach RUNNING state")
		case <-ticker.C:
			allRunning := true

			for i, node := range framework.Nodes {
				state := node.BlockchainClient.GetFSMCurrentStateForE2ETestMode()
				logger.Infof("Node %d state: %s", i, state)

				if state != blockchain_api.FSMStateType_RUNNING {
					allRunning = false
					break
				}
			}

			if allRunning {
				logger.Infof("All nodes are in RUNNING state")
				goto mine
			}
		}
	}

mine:
	_, err = helper.MineBlockWithRPC(ctx, framework.Nodes[0], logger)
	require.NoError(t, err)

	for {
		select {
		case <-timeout:
			t.Fatal("Timeout waiting for nodes to reach RUNNING state")
		case <-ticker.C:
			allRunning := true

			for i, node := range framework.Nodes {
				state := node.BlockchainClient.GetFSMCurrentStateForE2ETestMode()
				logger.Infof("Node %d state: %s", i, state)

				if state != blockchain_api.FSMStateType_RUNNING {
					allRunning = false
					break
				}
			}

			if allRunning {
				logger.Infof("All nodes are in RUNNING state")
				goto checkHeaders
			}
		}
	}

checkHeaders:
	headerNode1, _, _ := blockchainNode1.GetBestBlockHeader(ctx)

	headerNode0, _, _ := blockchainNode0.GetBestBlockHeader(ctx)

	assert.Equal(t, headerNode0.Hash(), headerNode1.Hash(), "Best block headers are not equal")
}

func TestTNE1_1TestSuite(t *testing.T) {
	suite.Run(t, new(TNE1_1TestSuite))
}
