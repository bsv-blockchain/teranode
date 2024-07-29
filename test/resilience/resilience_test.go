package test

import (
	"fmt"
	"os"
	"testing"

	"github.com/bitcoin-sv/ubsv/services/blockassembly/blockassembly_api"
	tf "github.com/bitcoin-sv/ubsv/test/test_framework"
)

var (
	framework   *tf.BitcoinTestFramework
	settingsMap map[string]string
)

func TestMain(m *testing.M) {
	setupBitcoinTestFramework()
	defer tearDownBitcoinTestFramework()

	m.Run()

	// os.Exit(0)
}

func setupBitcoinTestFramework() {
	framework = tf.NewBitcoinTestFramework([]string{"../../docker-compose.yml", "../../docker-compose.aerospike.override.yml", "../../docker-compose.e2etest.override.yml"})
	settingsMap = map[string]string{
		"SETTINGS_CONTEXT_1": "docker.ci.ubsv1.tc1",
		"SETTINGS_CONTEXT_2": "docker.ci.ubsv2.tc1",
		"SETTINGS_CONTEXT_3": "docker.ci.ubsv3.tc1",
	}
	if err := framework.SetupNodes(settingsMap); err != nil {
		fmt.Printf("Error setting up nodes: %v\n", err)
		os.Exit(1)
	}
}

func tearDownBitcoinTestFramework() {
	if err := framework.StopNodesWithRmVolume(); err != nil {
		fmt.Printf("Error stopping nodes: %v\n", err)
	}
	err := os.RemoveAll("../../data")
	if err != nil {
		fmt.Printf("Error removing data directory: %v\n", err)
	}
}

func TestShutDownPropagationService(t *testing.T) {

	emptyMessage := &blockassembly_api.EmptyMessage{}

	err := framework.StartNode("ubsv-2")
	if err != nil {
		t.Errorf("Failed to start node: %v", err)
	}

	settingsMap["SETTINGS_CONTEXT_2"] = "docker.ci.ubsv2.test.resilience.tc1"
	if err := framework.RestartNodes(settingsMap); err != nil {
		t.Errorf("Failed to restart nodes: %v", err)
	}

	//ctx := context.Background()
	//var logLevelStr, _ = gocore.Config().Get("logLevel", "INFO")
	//logger := ulogger.New("testRun", ulogger.WithLevel(logLevelStr))

	blockchainHealth, err := framework.Nodes[1].BlockchainClient.Health(framework.Context)
	if err != nil {
		t.Errorf("Failed to start blockchain: %v", err)
	}

	blockchainAssemblyHealth, err := framework.Nodes[1].BlockassemblyClient.BlockAssemblyAPIClient().HealthGRPC(framework.Context, emptyMessage)
	if err != nil {
		t.Errorf("Failure of blockchain assembly: %v", err)
	}

	coinbaseHealth, err := framework.Nodes[1].CoinbaseClient.Health(framework.Context)
	if err != nil {
		t.Errorf("Failure of coinbase assembly: %v", err)
	}

	if !blockchainHealth.Ok {
		t.Errorf("Expected blockchainHealth to be true, but got false")
	}
	if !blockchainAssemblyHealth.Ok {
		t.Errorf("Expected blockchainAssemblyHealth to be true, but got false")
	}
	if !coinbaseHealth.Ok {
		t.Errorf("Expected coinbaseHealth to be true, but got false")
	}
}

func TestShutDownBlockAssembly(t *testing.T) {

	// emptyMessage := &blockassembly_api.EmptyMessage{}

	settingsMap["SETTINGS_CONTEXT_2"] = "docker.ci.ubsv2.test.resilience.tc2"
	if err := framework.RestartNodes(settingsMap); err != nil {
		t.Errorf("Failed to restart nodes: %v", err)
	}

	blockchainHealth, err := framework.Nodes[1].BlockchainClient.Health(framework.Context)
	if err != nil {
		t.Errorf("Failed to start blockchain: %v", err)
	}

	// blockchainAssemblyHealth, err := framework.Nodes[1].BlockassemblyClient.BlockAssemblyAPIClient().HealthGRPC(framework.Context, emptyMessage)
	// if err != nil {
	// 	t.Errorf("Failure of blockchain assembly: %v", err)
	// }

	coinbaseHealth, err := framework.Nodes[1].CoinbaseClient.Health(framework.Context)
	if err != nil {
		t.Errorf("Failure of coinbase assembly: %v", err)
	}

	if !blockchainHealth.Ok {
		t.Errorf("Expected blockchainHealth to be true, but got false")
	}
	// if blockchainAssemblyHealth.Ok {
	// 	t.Errorf("Expected blockchainAssemblyHealth to be false, but got true")
	// }
	if !coinbaseHealth.Ok {
		t.Errorf("Expected coinbaseHealth to be true, but got false")
	}
}

func TestShutDownBlockValidation(t *testing.T) {
	emptyMessage := &blockassembly_api.EmptyMessage{}

	settingsMap["SETTINGS_CONTEXT_2"] = "docker.ci.ubsv2.test.resilience.tc3"
	if err := framework.RestartNodes(settingsMap); err != nil {
		t.Errorf("Failed to restart nodes: %v", err)
	}

	blockchainHealth, err := framework.Nodes[1].BlockchainClient.Health(framework.Context)
	if err != nil {
		t.Errorf("Failed to start blockchain: %v", err)
	}

	blockchainAssemblyHealth, err := framework.Nodes[1].BlockassemblyClient.BlockAssemblyAPIClient().HealthGRPC(framework.Context, emptyMessage)
	if err != nil {
		t.Errorf("Failure of blockchain assembly: %v", err)
	}

	coinbaseHealth, err := framework.Nodes[1].CoinbaseClient.Health(framework.Context)
	if err != nil {
		t.Errorf("Failure of coinbase assembly: %v", err)
	}

	if blockchainHealth.Ok {
		t.Errorf("Expected blockchainHealth to be false, but got true")
	}
	if !blockchainAssemblyHealth.Ok {
		t.Errorf("Expected blockchainAssemblyHealth to be true, but got false")
	}
	if !coinbaseHealth.Ok {
		t.Errorf("Expected coinbaseHealth to be true, but got false")
	}
}

func TestShutDownBlockchain(t *testing.T) {
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

	// blockchainHealth, err := framework.Nodes[1].BlockchainClient.Health(framework.Context)
	// if err != nil {
	// 	t.Errorf("Failed to start blockchain: %v", err)
	// }

	blockchainAssemblyHealth, err := framework.Nodes[1].BlockassemblyClient.BlockAssemblyAPIClient().HealthGRPC(framework.Context, emptyMessage)
	if err != nil {
		t.Errorf("Failure of blockchain assembly: %v", err)
	}

	coinbaseHealth, err := framework.Nodes[1].CoinbaseClient.Health(framework.Context)
	if err != nil {
		t.Errorf("Failure of coinbase assembly: %v", err)
	}

	// if blockchainHealth.Ok {
	// 	t.Errorf("Expected blockchainHealth to be false, but got true")
	// }
	if !blockchainAssemblyHealth.Ok {
		t.Errorf("Expected blockchainAssemblyHealth to be true, but got false")
	}
	if !coinbaseHealth.Ok {
		t.Errorf("Expected coinbaseHealth to be true, but got false")
	}
}

func TestShutDownP2P(t *testing.T) {
	emptyMessage := &blockassembly_api.EmptyMessage{}

	settingsMap["SETTINGS_CONTEXT_2"] = "docker.ci.ubsv2.test.resilience.tc5"
	if err := framework.RestartNodes(settingsMap); err != nil {
		t.Errorf("Failed to restart nodes: %v", err)
	}

	blockchainHealth, err := framework.Nodes[1].BlockchainClient.Health(framework.Context)
	if err != nil {
		t.Errorf("Failed to start blockchain: %v", err)
	}

	blockchainAssemblyHealth, err := framework.Nodes[1].BlockassemblyClient.BlockAssemblyAPIClient().HealthGRPC(framework.Context, emptyMessage)
	if err != nil {
		t.Errorf("Failure of blockchain assembly: %v", err)
	}

	coinbaseHealth, err := framework.Nodes[1].CoinbaseClient.Health(framework.Context)
	if err != nil {
		t.Errorf("Failure of coinbase assembly: %v", err)
	}

	if !blockchainHealth.Ok {
		t.Errorf("Expected blockchainHealth to be false, but got true")
	}
	if !blockchainAssemblyHealth.Ok {
		t.Errorf("Expected blockchainAssemblyHealth to be true, but got false")
	}
	if !coinbaseHealth.Ok {
		t.Errorf("Expected coinbaseHealth to be true, but got false")
	}
}

func TestShutDownAsset(t *testing.T) {
	emptyMessage := &blockassembly_api.EmptyMessage{}

	settingsMap["SETTINGS_CONTEXT_2"] = "docker.ci.ubsv2.test.resilience.tc6"
	if err := framework.RestartNodes(settingsMap); err != nil {
		t.Errorf("Failed to restart nodes: %v", err)
	}

	blockchainHealth, err := framework.Nodes[1].BlockchainClient.Health(framework.Context)
	if err != nil {
		t.Errorf("Failed to start blockchain: %v", err)
	}

	blockchainAssemblyHealth, err := framework.Nodes[1].BlockassemblyClient.BlockAssemblyAPIClient().HealthGRPC(framework.Context, emptyMessage)
	if err != nil {
		t.Errorf("Failure of blockchain assembly: %v", err)
	}

	coinbaseHealth, err := framework.Nodes[1].CoinbaseClient.Health(framework.Context)
	if err != nil {
		t.Errorf("Failure of coinbase assembly: %v", err)
	}

	if !blockchainHealth.Ok {
		t.Errorf("Expected blockchainHealth to be false, but got true")
	}
	if !blockchainAssemblyHealth.Ok {
		t.Errorf("Expected blockchainAssemblyHealth to be true, but got false")
	}
	if !coinbaseHealth.Ok {
		t.Errorf("Expected coinbaseHealth to be true, but got false")
	}
}
