package setup

import (
	"os"
	"os/exec"
	"testing"
	"time"

	tf "github.com/bitcoin-sv/ubsv/test/test_framework"
	helper "github.com/bitcoin-sv/ubsv/test/utils"
	"github.com/stretchr/testify/suite"
)

const stringTrue = "true"

type BitcoinTestSuite struct {
	suite.Suite
	Framework    *tf.BitcoinTestFramework
	ComposeFiles []string
	SettingsMap  map[string]string
}

func (suite *BitcoinTestSuite) DefaultComposeFiles() []string {
	return []string{"../../docker-compose.e2etest.yml"}
}

func (suite *BitcoinTestSuite) DefaultSettingsMap() map[string]string {
	return map[string]string{
		"SETTINGS_CONTEXT_1": "docker.ci.ubsv1.tc1",
		"SETTINGS_CONTEXT_2": "docker.ci.ubsv2.tc1",
		"SETTINGS_CONTEXT_3": "docker.ci.ubsv3.tc1",
	}
}

const (
	NodeURL1 = "http://localhost:10090"
	NodeURL2 = "http://localhost:12090"
	NodeURL3 = "http://localhost:14090"
)

func (suite *BitcoinTestSuite) SetupTestWithCustomComposeAndSettings(settingsMap map[string]string, composeFiles []string) {
	var err error

	suite.ComposeFiles = composeFiles
	suite.SettingsMap = settingsMap

	isGitHubActions := os.Getenv("GITHUB_ACTIONS") == stringTrue

	err = removeDataDirectory("../../data/test", isGitHubActions)
	if err != nil {
		suite.T().Fatal(err)
	}

	err = cleanUpE2EContainers(isGitHubActions)
	if err != nil {
		suite.T().Fatal(err)
	}

	suite.T().Log("Initializing BitcoinTestFramework")

	suite.Framework, err = helper.SetupBitcoinTestFramework(suite.ComposeFiles, suite.SettingsMap)
	if err != nil {
		suite.T().Cleanup(suite.Framework.Cancel)
		suite.T().Fatalf("Failed to set up BitcoinTestFramework: %v", err)
	}

	time.Sleep(2 * time.Second)

	err = suite.Framework.GetClientHandles()
	if err != nil {
		suite.T().Fatal(err)
	}

	suite.T().Logf("Sending initial RUN event to Node1")

	err = suite.Framework.Nodes[0].BlockchainClient.Run(suite.Framework.Context)
	if err != nil {
		suite.T().Fatal(err)
	}

	suite.T().Logf("Sending initial RUN event to Node2")

	err = suite.Framework.Nodes[1].BlockchainClient.Run(suite.Framework.Context)
	if err != nil {
		suite.T().Fatal(err)
	}

	suite.T().Logf("Sending initial RUN event to Node3")

	err = suite.Framework.Nodes[2].BlockchainClient.Run(suite.Framework.Context)
	if err != nil {
		suite.T().Fatal(err)
	}

	err = helper.WaitForBlockHeight(NodeURL1, 200, 120)
	if err != nil {
		suite.T().Fatal(err)
	}

	err = helper.WaitForBlockHeight(NodeURL2, 200, 120)
	if err != nil {
		suite.T().Fatal(err)
	}

	err = helper.WaitForBlockHeight(NodeURL3, 200, 120)

	if err != nil {
		suite.T().Fatal(err)
	}

	suite.T().Log("BitcoinTestFramework setup completed")
}

func (suite *BitcoinTestSuite) SetupTestWithCustomComposeAndSettingsSkipChecks(settingsMap map[string]string, composeFiles []string, skipClientHandles bool) {
	var err error

	suite.ComposeFiles = composeFiles
	suite.SettingsMap = settingsMap

	isGitHubActions := os.Getenv("GITHUB_ACTIONS") == stringTrue
	err = removeDataDirectory("../../data/test", isGitHubActions)

	if err != nil {
		suite.T().Fatal(err)
	}

	suite.T().Log("Initializing BitcoinTestFramework")

	suite.Framework, err = helper.SetupBitcoinTestFramework(suite.ComposeFiles, suite.SettingsMap)
	if err != nil {
		suite.T().Fatalf("Failed to set up BitcoinTestFramework: %v", err)
	}

	if !skipClientHandles {
		err = suite.Framework.GetClientHandles()
		if err != nil {
			suite.T().Fatal(err)
		}
	}

	suite.T().Log("BitcoinTestFramework setup completed")
}

func (suite *BitcoinTestSuite) SetupTestWithCustomComposeAndSettingsDoNotReset(settingsMap map[string]string, composeFiles []string) {
	var err error

	suite.ComposeFiles = composeFiles
	suite.SettingsMap = settingsMap

	isGitHubActions := os.Getenv("GITHUB_ACTIONS") == stringTrue
	err = removeDataDirectory("../../data", isGitHubActions)

	if err != nil {
		suite.T().Fatal(err)
	}

	suite.T().Log("Initializing BitcoinTestFramework")
	suite.Framework, err = helper.SetupBitcoinTestFramework(suite.ComposeFiles, suite.SettingsMap)

	if err != nil {
		suite.T().Fatal(err)
	}

	err = helper.WaitForBlockHeight(NodeURL1, 300, 180)
	if err != nil {
		suite.T().Fatalf("Failed to set up BitcoinTestFramework: %v", err)
	}

	suite.T().Log("BitcoinTestFramework setup completed")
}

func (suite *BitcoinTestSuite) SetupTestWithCustomSettings(settingsMap map[string]string) {
	suite.SetupTestWithCustomComposeAndSettings(settingsMap, suite.DefaultComposeFiles())
}

func (suite *BitcoinTestSuite) SetupTest() {
	suite.SetupTestWithCustomComposeAndSettings(suite.DefaultSettingsMap(), suite.DefaultComposeFiles())
}

func (suite *BitcoinTestSuite) TearDownTest() {
	if err := helper.TearDownBitcoinTestFramework(suite.Framework); err != nil {
		suite.T().Cleanup(suite.Framework.Cancel)
	}

	isGitHubActions := os.Getenv("GITHUB_ACTIONS") == stringTrue
	err := removeDataDirectory("../../data", isGitHubActions)

	if err != nil {
		suite.T().Fatal(err)
	}

	suite.T().Cleanup(suite.Framework.Cancel)
}

func TestBitcoinTestSuite(t *testing.T) {
	suite.Run(t, new(BitcoinTestSuite))
}

func removeDataDirectory(dir string, useSudo bool) error {
	var cmd *exec.Cmd

	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return nil
	}

	if !useSudo {
		cmd = exec.Command("rm", "-rf", dir)
	} else {
		cmd = exec.Command("sudo", "rm", "-rf", dir)
	}

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err := cmd.Run()
	if err != nil {
		return err
	}

	return nil
}

func cleanUpE2EContainers(isGitHubActions bool) (err error) {
	if isGitHubActions {
		return nil
	}

	defer func() {
		if r := recover(); r != nil {
			err = nil
		}
	}()

	cmd := exec.Command("bash", "-c", "docker ps -a -q --filter label=com.docker.compose.project=e2e | xargs docker rm -f")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err = cmd.Run()
	if err != nil {
		return err
	}

	return nil
}
