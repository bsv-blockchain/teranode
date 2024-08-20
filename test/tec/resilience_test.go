//go:build tectests

package test

import (
	"testing"

	"github.com/bitcoin-sv/ubsv/services/blockassembly/blockassembly_api"
	"github.com/bitcoin-sv/ubsv/test/setup"
	"github.com/stretchr/testify/suite"
)

type TECTestSuite struct {
	setup.BitcoinTestSuite
}

func (suite *TECTestSuite) TestShutDownPropagationService() {

	t := suite.T()
	framework := suite.Framework
	settingsMap := suite.SettingsMap
	emptyMessage := &blockassembly_api.EmptyMessage{}

	err := framework.StartNode("ubsv-2")
	if err != nil {
		t.Errorf("Failed to start node: %v", err)
	}

	settingsMap["SETTINGS_CONTEXT_2"] = "docker.ci.ubsv2.test.resilience.tc1"
	if err := framework.RestartNodes(settingsMap); err != nil {
		t.Errorf("Failed to restart nodes: %v", err)
	}

	blockchainHealth, err := framework.Nodes[1].BlockchainClient.Health(framework.Context)
	if err != nil {
		t.Errorf("Failed to start blockchain: %v", err)
	}

	blockassemblyHealth, err := framework.Nodes[1].BlockassemblyClient.BlockAssemblyAPIClient().HealthGRPC(framework.Context, emptyMessage)
	if err != nil {
		t.Errorf("Failure of blockassembly: %v", err)
	}

	coinbaseHealth, err := framework.Nodes[1].CoinbaseClient.Health(framework.Context)
	if err != nil {
		t.Errorf("Failure of coinbase assembly: %v", err)
	}

	if !blockchainHealth.Ok {
		t.Errorf("Expected blockchainHealth to be true, but got false")
	}
	if !blockassemblyHealth.Ok {
		t.Errorf("Expected blockassemblyHealth to be true, but got false")
	}
	if !coinbaseHealth.Ok {
		t.Errorf("Expected coinbaseHealth to be true, but got false")
	}
}

func (suite *TECTestSuite) TestShutDownBlockAssembly() {

	t := suite.T()
	framework := suite.Framework
	settingsMap := suite.SettingsMap

	settingsMap["SETTINGS_CONTEXT_2"] = "docker.ci.ubsv2.test.resilience.tc2"
	if err := framework.RestartNodes(settingsMap); err != nil {
		t.Fatalf("Failed to restart nodes: %v", err)
	}

	blockchainHealth, err := framework.Nodes[1].BlockchainClient.Health(framework.Context)
	if err != nil {
		t.Fatalf("Failed to start blockchain: %v", err)
	}

	coinbaseHealth, err := framework.Nodes[1].CoinbaseClient.Health(framework.Context)
	if err != nil {
		t.Fatalf("Failure of coinbase assembly: %v", err)
	}

	if !blockchainHealth.Ok {
		t.Errorf("Expected blockchainHealth to be true, but got false")
	}
	if !coinbaseHealth.Ok {
		t.Errorf("Expected coinbaseHealth to be true, but got false")
	}
}

func (suite *TECTestSuite) TestShutDownBlockValidation() {
	t := suite.T()
	framework := suite.Framework
	settingsMap := suite.SettingsMap
	emptyMessage := &blockassembly_api.EmptyMessage{}

	settingsMap["SETTINGS_CONTEXT_2"] = "docker.ci.ubsv2.test.resilience.tc3"
	if err := framework.RestartNodes(settingsMap); err != nil {
		t.Errorf("Failed to restart nodes: %v", err)
	}

	blockchainHealth, err := framework.Nodes[1].BlockchainClient.Health(framework.Context)
	if err != nil {
		t.Errorf("Failed to start blockchain: %v", err)
	}

	blockassemblyHealth, err := framework.Nodes[1].BlockassemblyClient.BlockAssemblyAPIClient().HealthGRPC(framework.Context, emptyMessage)
	if err != nil {
		t.Errorf("Failure of blockassembly: %v", err)
	}

	coinbaseHealth, err := framework.Nodes[1].CoinbaseClient.Health(framework.Context)
	if err != nil {
		t.Errorf("Failure of coinbase assembly: %v", err)
	}

	if !blockchainHealth.Ok {
		t.Errorf("Expected blockchainHealth to be false, but got true")
	}
	if !blockassemblyHealth.Ok {
		t.Errorf("Expected blockassemblyHealth to be true, but got false")
	}
	if !coinbaseHealth.Ok {
		t.Errorf("Expected coinbaseHealth to be true, but got false")
	}
}

func (suite *TECTestSuite) TestShutDownBlockchain() {
	t := suite.T()
	framework := suite.Framework
	settingsMap := suite.SettingsMap
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("Recovered from panic: %v", r)
			_ = framework.Compose.Down(framework.Context)
		}
	}()
	emptyMessage := &blockassembly_api.EmptyMessage{}

	settingsMap["SETTINGS_CONTEXT_2"] = "docker.ci.ubsv2.test.resilience.tc4"
	if err := framework.RestartNodes(settingsMap); err != nil {
		t.Errorf("Failed to restart nodes: %v", err)
	}

	blockassemblyHealth, err := framework.Nodes[1].BlockassemblyClient.BlockAssemblyAPIClient().HealthGRPC(framework.Context, emptyMessage)
	if err != nil {
		t.Fatalf("Failure of blockassembly: %v", err)
	}

	coinbaseHealth, err := framework.Nodes[1].CoinbaseClient.Health(framework.Context)
	if err != nil {
		t.Fatalf("Failure of coinbase assembly: %v", err)
	}

	if !blockassemblyHealth.Ok {
		t.Errorf("Expected blockassemblyHealth to be true, but got false")
	}
	if !coinbaseHealth.Ok {
		t.Errorf("Expected coinbaseHealth to be true, but got false")
	}
}

func (suite *TECTestSuite) TestShutDownP2P() {
	t := suite.T()
	framework := suite.Framework
	settingsMap := suite.SettingsMap
	emptyMessage := &blockassembly_api.EmptyMessage{}

	settingsMap["SETTINGS_CONTEXT_2"] = "docker.ci.ubsv2.test.resilience.tc5"
	if err := framework.RestartNodes(settingsMap); err != nil {
		t.Errorf("Failed to restart nodes: %v", err)
	}

	blockchainHealth, err := framework.Nodes[1].BlockchainClient.Health(framework.Context)
	if err != nil {
		t.Errorf("Failed to start blockchain: %v", err)
	}

	blockassemblyHealth, err := framework.Nodes[1].BlockassemblyClient.BlockAssemblyAPIClient().HealthGRPC(framework.Context, emptyMessage)
	if err != nil {
		t.Errorf("Failure of blockassembly: %v", err)
	}

	coinbaseHealth, err := framework.Nodes[1].CoinbaseClient.Health(framework.Context)
	if err != nil {
		t.Errorf("Failure of coinbase assembly: %v", err)
	}

	if !blockchainHealth.Ok {
		t.Errorf("Expected blockchainHealth to be false, but got true")
	}
	if !blockassemblyHealth.Ok {
		t.Errorf("Expected blockassemblyHealth to be true, but got false")
	}
	if !coinbaseHealth.Ok {
		t.Errorf("Expected coinbaseHealth to be true, but got false")
	}
}

func (suite *TECTestSuite) TestShutDownAsset() {
	t := suite.T()
	framework := suite.Framework
	settingsMap := suite.SettingsMap
	emptyMessage := &blockassembly_api.EmptyMessage{}

	settingsMap["SETTINGS_CONTEXT_2"] = "docker.ci.ubsv2.test.resilience.tc6"
	if err := framework.RestartNodes(settingsMap); err != nil {
		t.Errorf("Failed to restart nodes: %v", err)
	}

	blockchainHealth, err := framework.Nodes[1].BlockchainClient.Health(framework.Context)
	if err != nil {
		t.Errorf("Failed to start blockchain: %v", err)
	}

	blockassemblyHealth, err := framework.Nodes[1].BlockassemblyClient.BlockAssemblyAPIClient().HealthGRPC(framework.Context, emptyMessage)
	if err != nil {
		t.Errorf("Failure of blockassembly: %v", err)
	}

	coinbaseHealth, err := framework.Nodes[1].CoinbaseClient.Health(framework.Context)
	if err != nil {
		t.Errorf("Failure of coinbase assembly: %v", err)
	}

	if !blockchainHealth.Ok {
		t.Errorf("Expected blockchainHealth to be false, but got true")
	}
	if !blockassemblyHealth.Ok {
		t.Errorf("Expected blockassemblyHealth to be true, but got false")
	}
	if !coinbaseHealth.Ok {
		t.Errorf("Expected coinbaseHealth to be true, but got false")
	}
}

func TestTECTestSuite(t *testing.T) {
	suite.Run(t, new(TECTestSuite))
}
