//go:build test_all || test_tec

package tec

import (
	"testing"

	"github.com/bitcoin-sv/teranode/services/blockassembly/blockassembly_api"
	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/stretchr/testify/suite"
)

type TECTestSuite struct {
	helper.TeranodeTestSuite
}

func TestTECTestSuite(t *testing.T) {
	suite.Run(t, &TECTestSuite{})
}

func (suite *TECTestSuite) TestShutDownPropagationService() {
	t := suite.T()
	testenv := suite.TeranodeTestEnv
	settingsMap := suite.TConfig.Teranode.SettingsMap()
	ctx := testenv.Context
	emptyMessage := &blockassembly_api.EmptyMessage{}

	err := testenv.StartNode("teranode2")
	if err != nil {
		t.Errorf("Failed to start node: %v", err)
	}

	settingsMap["SETTINGS_CONTEXT_2"] = "docker.ci.teranode2.test.resilience.tc1"
	if err := testenv.RestartDockerNodes(settingsMap); err != nil {
		t.Errorf("Failed to restart nodes: %v", err)
	}

	blockchainHealth, _, err := testenv.Nodes[1].BlockchainClient.Health(ctx, true)
	if err != nil {
		t.Errorf("Failed to start blockchain: %v", err)
	}

	blockassemblyHealth, err := testenv.Nodes[1].BlockassemblyClient.BlockAssemblyAPIClient().HealthGRPC(ctx, emptyMessage)
	if err != nil {
		t.Errorf("Failure of blockassembly: %v", err)
	}

	coinbaseHealth, _, err := testenv.Nodes[1].CoinbaseClient.Health(ctx, true)
	if err != nil {
		t.Errorf("Failure of coinbase assembly: %v", err)
	}

	if blockchainHealth != 200 {
		t.Errorf("Expected blockchainHealth to be true, but got false")
	}
	if !blockassemblyHealth.Ok {
		t.Errorf("Expected blockassemblyHealth to be true, but got false")
	}
	if coinbaseHealth != 200 {
		t.Errorf("Expected coinbaseHealth to be true, but got false")
	}
}

func (suite *TECTestSuite) TestShutDownBlockAssembly() {

	t := suite.T()
	testenv := suite.TeranodeTestEnv
	settingsMap := suite.TConfig.Teranode.SettingsMap()
	ctx := testenv.Context

	settingsMap["SETTINGS_CONTEXT_2"] = "docker.ci.teranode2.test.resilience.tc2"
	if err := testenv.RestartDockerNodes(settingsMap); err != nil {
		t.Fatalf("Failed to restart nodes: %v", err)
	}

	blockchainHealth, _, err := testenv.Nodes[1].BlockchainClient.Health(ctx, true)
	if err != nil {
		t.Fatalf("Failed to start blockchain: %v", err)
	}

	coinbaseHealth, _, err := testenv.Nodes[1].CoinbaseClient.Health(ctx, true)
	if err != nil {
		t.Fatalf("Failure of coinbase assembly: %v", err)
	}

	if blockchainHealth != 200 {
		t.Errorf("Expected blockchainHealth to be true, but got false")
	}
	if coinbaseHealth != 200 {
		t.Errorf("Expected coinbaseHealth to be true, but got false")
	}
}

func (suite *TECTestSuite) TestShutDownBlockValidation() {
	t := suite.T()
	testenv := suite.TeranodeTestEnv
	settingsMap := suite.TConfig.Teranode.SettingsMap()
	ctx := testenv.Context
	emptyMessage := &blockassembly_api.EmptyMessage{}

	settingsMap["SETTINGS_CONTEXT_2"] = "docker.ci.teranode2.test.resilience.tc3"
	if err := testenv.RestartDockerNodes(settingsMap); err != nil {
		t.Fatalf("Failed to restart nodes: %v", err)
	}

	blockchainHealth, _, err := testenv.Nodes[1].BlockchainClient.Health(ctx, true)
	if err != nil {
		t.Fatalf("Failed to start blockchain: %v", err)
	}

	blockassemblyHealth, err := testenv.Nodes[1].BlockassemblyClient.BlockAssemblyAPIClient().HealthGRPC(ctx, emptyMessage)
	if err != nil {
		t.Fatalf("Failure of blockassembly: %v", err)
	}

	coinbaseHealth, _, err := testenv.Nodes[1].CoinbaseClient.Health(ctx, true)
	if err != nil {
		t.Fatalf("Failure of coinbase assembly: %v", err)
	}

	if blockchainHealth != 200 {
		t.Errorf("Expected blockchainHealth to be 200, but got %d", blockchainHealth)
	}
	if !blockassemblyHealth.Ok {
		t.Errorf("Expected blockassemblyHealth to be true, but got false")
	}
	if coinbaseHealth != 200 {
		t.Errorf("Expected coinbaseHealth to be 200, but got %d", coinbaseHealth)
	}
}

func (suite *TECTestSuite) TestShutDownBlockchain() {
	t := suite.T()
	testenv := suite.TeranodeTestEnv
	settingsMap := suite.TConfig.Teranode.SettingsMap()
	ctx := testenv.Context
	emptyMessage := &blockassembly_api.EmptyMessage{}

	defer func() {
		if r := recover(); r != nil {
			t.Errorf("Recovered from panic: %v", r)
			_ = testenv.Compose.Down(ctx)
		}
	}()

	settingsMap["SETTINGS_CONTEXT_2"] = "docker.ci.teranode2.test.resilience.tc4"
	if err := testenv.RestartDockerNodes(settingsMap); err != nil {
		t.Fatalf("Failed to restart nodes: %v", err)
	}

	blockassemblyHealth, err := testenv.Nodes[1].BlockassemblyClient.BlockAssemblyAPIClient().HealthGRPC(ctx, emptyMessage)
	if err != nil {
		t.Fatalf("Failure of blockassembly: %v", err)
	}

	coinbaseHealth, _, err := testenv.Nodes[1].CoinbaseClient.Health(ctx, true)
	if err != nil {
		t.Fatalf("Failure of coinbase assembly: %v", err)
	}

	if !blockassemblyHealth.Ok {
		t.Errorf("Expected blockassemblyHealth to be true, but got false")
	}
	if coinbaseHealth != 200 {
		t.Errorf("Expected coinbaseHealth to be 200, but got %d", coinbaseHealth)
	}
}

func (suite *TECTestSuite) TestShutDownP2P() {
	t := suite.T()
	testenv := suite.TeranodeTestEnv
	settingsMap := suite.TConfig.Teranode.SettingsMap()
	ctx := testenv.Context
	emptyMessage := &blockassembly_api.EmptyMessage{}

	settingsMap["SETTINGS_CONTEXT_2"] = "docker.ci.teranode2.test.resilience.tc5"
	if err := testenv.RestartDockerNodes(settingsMap); err != nil {
		t.Fatalf("Failed to restart nodes: %v", err)
	}

	blockchainHealth, _, err := testenv.Nodes[1].BlockchainClient.Health(ctx, true)
	if err != nil {
		t.Fatalf("Failed to start blockchain: %v", err)
	}

	blockassemblyHealth, err := testenv.Nodes[1].BlockassemblyClient.BlockAssemblyAPIClient().HealthGRPC(ctx, emptyMessage)
	if err != nil {
		t.Fatalf("Failure of blockassembly: %v", err)
	}

	coinbaseHealth, _, err := testenv.Nodes[1].CoinbaseClient.Health(ctx, true)
	if err != nil {
		t.Fatalf("Failure of coinbase assembly: %v", err)
	}

	if blockchainHealth != 200 {
		t.Errorf("Expected blockchainHealth to be 200, but got %d", blockchainHealth)
	}
	if !blockassemblyHealth.Ok {
		t.Errorf("Expected blockassemblyHealth to be true, but got false")
	}
	if coinbaseHealth != 200 {
		t.Errorf("Expected coinbaseHealth to be 200, but got %d", coinbaseHealth)
	}
}

func (suite *TECTestSuite) TestShutDownAsset() {
	t := suite.T()
	testenv := suite.TeranodeTestEnv
	settingsMap := suite.TConfig.Teranode.SettingsMap()
	ctx := testenv.Context
	emptyMessage := &blockassembly_api.EmptyMessage{}

	settingsMap["SETTINGS_CONTEXT_2"] = "docker.ci.teranode2.test.resilience.tc6"
	if err := testenv.RestartDockerNodes(settingsMap); err != nil {
		t.Fatalf("Failed to restart nodes: %v", err)
	}

	blockchainHealth, _, err := testenv.Nodes[1].BlockchainClient.Health(ctx, true)
	if err != nil {
		t.Fatalf("Failed to start blockchain: %v", err)
	}

	blockassemblyHealth, err := testenv.Nodes[1].BlockassemblyClient.BlockAssemblyAPIClient().HealthGRPC(ctx, emptyMessage)
	if err != nil {
		t.Fatalf("Failure of blockassembly: %v", err)
	}

	coinbaseHealth, _, err := testenv.Nodes[1].CoinbaseClient.Health(ctx, true)
	if err != nil {
		t.Fatalf("Failure of coinbase assembly: %v", err)
	}

	if blockchainHealth != 200 {
		t.Errorf("Expected blockchainHealth to be 200, but got %d", blockchainHealth)
	}
	if !blockassemblyHealth.Ok {
		t.Errorf("Expected blockassemblyHealth to be true, but got false")
	}
	if coinbaseHealth != 200 {
		t.Errorf("Expected coinbaseHealth to be 200, but got %d", coinbaseHealth)
	}
}
