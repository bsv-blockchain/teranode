//go:build test_all || test_smoke || test_functional

// How to run each test:
// Clean up docker containers before running the test manually
// $ cd test/smoke/
// $ go test -v -run "^TestSanitywithLegacyTestSuite$/TestShouldSyncWithLegacyWhenInitialBlocksAreGeneratedOnLegacy$" -tags test_functional
// $ go test -v -run "^TestSanitywithLegacyTestSuite$/TestShouldSyncWithLegacy$" -tags test_functional
// $ go test -v -run "^TestSanitywithLegacyTestSuite$/TestShouldBanLegacyNodes$" -tags test_functional

package smoke

import (
	"fmt"
	"testing"
	"time"

	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/bitcoin-sv/teranode/test/utils/tconfig"
	"github.com/docker/go-connections/nat"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

const testHeight = uint32(0)
const legacySyncURL = "http://localhost:18332"

type SanitywithLegacyTestSuite struct {
	helper.TeranodeTestSuite
}

func TestSanitywithLegacyTestSuite(t *testing.T) {
	suite.Run(t, &SanitywithLegacyTestSuite{
		TeranodeTestSuite: helper.TeranodeTestSuite{
			TConfig: tconfig.LoadTConfig(
				map[string]any{
					tconfig.KeyInitBlockHeight: testHeight,
					tconfig.KeyLocalSystemComposes: []string{
						"../../docker-compose.e2etest.legacy.yml",
					},
					tconfig.KeyTeranodeContexts: []string{
						"docker.teranode1.test.legacy",
						"docker.teranode2.test.legacy",
						"docker.teranode3.test.legacy",
					},
				},
			),
		},
	},
	)
}

func (suite *SanitywithLegacyTestSuite) TestShouldSyncWithLegacyWhenInitialBlocksAreGeneratedOnLegacy() {
	t := suite.T()
	url := "http://" + suite.TeranodeTestEnv.Nodes[0].AssetURL

	height, _ := helper.GetBlockHeight(url)
	t.Logf("Block height before mining: %d\n", height)

	assert.Equal(t, testHeight, height, "Height should match with initial generated blocks")

	_, err := helper.CallRPC(legacySyncURL, "generate", []interface{}{101})
	assert.NoError(t, err, "Failed to generate blocks")

	err = helper.WaitForBlockHeight(url, testHeight+101, 60*time.Second)
	if err != nil {
		suite.T().Fatal(err)
	}

	height, _ = helper.GetBlockHeight(url)
	t.Logf("Block height after mining: %d\n", height)
	assert.Equal(t, testHeight+101, height, "Height should match with initial generated blocks")
}

func (suite *SanitywithLegacyTestSuite) TestShouldSyncWithLegacy() {
	t := suite.T()
	url := "http://" + suite.TeranodeTestEnv.Nodes[0].AssetURL

	height, _ := helper.GetBlockHeight(url)
	t.Logf("Block height before mining: %d\n", height)

	assert.Equal(t, testHeight, height, "Height should match with initial generated blocks")

	cluster := suite.TeranodeTestEnv
	if err := cluster.StopNode("teranode1"); err != nil {
		t.Errorf("Failed to restart nodes: %v", err)
	}

	_, err := helper.CallRPC(legacySyncURL, "generate", []interface{}{101})
	assert.NoError(t, err, "Failed to generate blocks")

	time.Sleep(20 * time.Second)

	if err := cluster.StartNode("teranode1"); err != nil {
		t.Errorf("Failed to restart nodes: %v", err)
	}

	ports := []int{8000}

	for index, port := range ports {
		mappedPort, err := suite.TeranodeTestEnv.GetMappedPort(fmt.Sprintf("teranode%d", index+1), nat.Port(fmt.Sprintf("%d/tcp", port)))
		if err != nil {
			suite.T().Fatal(err)
		}

		suite.T().Logf("Waiting for node %d to be ready", index)

		err = helper.WaitForHealthLiveness(mappedPort.Int(), 60*time.Second)
		if err != nil {
			suite.T().Fatal(err)
		}
	}

	err = helper.WaitForBlockHeight(url, testHeight+101, 60*time.Second)
	if err != nil {
		suite.T().Fatal(err)
	}

	height, _ = helper.GetBlockHeight(url)
	t.Logf("Block height after mining: %d\n", height)
	assert.Equal(t, testHeight+101, height, "Height should match with initial generated blocks")
}

func (suite *SanitywithLegacyTestSuite) TestShouldAllowBanLegacyAndTeranodePeers() {
	t := suite.T()
	url := "http://" + suite.TeranodeTestEnv.Nodes[0].AssetURL

	height, _ := helper.GetBlockHeight(url)
	t.Logf("Block height before mining: %d\n", height)

	assert.Equal(t, testHeight, height, "Height should match with initial generated blocks")

	cluster := suite.TeranodeTestEnv
	if err := cluster.StopNode("teranode1"); err != nil {
		t.Errorf("Failed to restart nodes: %v", err)
	}

	_, err := helper.CallRPC(legacySyncURL, "generate", []interface{}{101})
	assert.NoError(t, err, "Failed to generate blocks")

	time.Sleep(20 * time.Second)

	if err := cluster.StartNode("teranode1"); err != nil {
		t.Errorf("Failed to restart nodes: %v", err)
	}

	ports := []int{8000}

	for index, port := range ports {
		mappedPort, err := suite.TeranodeTestEnv.GetMappedPort(fmt.Sprintf("teranode%d", index+1), nat.Port(fmt.Sprintf("%d/tcp", port)))
		if err != nil {
			suite.T().Fatal(err)
		}

		suite.T().Logf("Waiting for node %d to be ready", index)

		err = helper.WaitForHealthLiveness(mappedPort.Int(), 60*time.Second)
		if err != nil {
			suite.T().Fatal(err)
		}
	}

	err = helper.WaitForBlockHeight(url, testHeight+101, 60*time.Second)
	if err != nil {
		suite.T().Fatal(err)
	}

	height, _ = helper.GetBlockHeight(url)
	t.Logf("Block height after mining: %d\n", height)
	assert.Equal(t, testHeight+101, height, "Height should match with initial generated blocks")

	// Define legacy nodes
	// svnode1 := suite.TeranodeTestEnv.LegacyNodes[0]
	svnode2 := suite.TeranodeTestEnv.LegacyNodes[1]
	// svnode3 := suite.TeranodeTestEnv.LegacyNodes[2]

	// Get IP addresses of legacy nodes
	// svnode1Address := suite.TeranodeTestEnv.GetLegacyContainerIPAddress(&svnode1)
	svnode2Address := suite.TeranodeTestEnv.GetLegacyContainerIPAddress(&svnode2)
	// svnode3Address := suite.TeranodeTestEnv.GetLegacyContainerIPAddress(&svnode3)

	node1 := suite.TeranodeTestEnv.Nodes[0]
	node2 := suite.TeranodeTestEnv.Nodes[1]
	node3 := suite.TeranodeTestEnv.Nodes[2]

	// All nodes should ban legacy node
	_, err = helper.CallRPC(node1.RPCURL, "setban", []interface{}{svnode2Address, "add", 180, false})
	require.NoError(t, err)

	_, err = helper.CallRPC(node2.RPCURL, "setban", []interface{}{svnode2Address, "add", 180, false})
	require.NoError(t, err)

	_, err = helper.CallRPC(node3.RPCURL, "setban", []interface{}{svnode2Address, "add", 180, false})
	require.NoError(t, err)
}
