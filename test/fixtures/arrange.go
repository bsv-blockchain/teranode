package arrange

import (
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	tenv "github.com/bitcoin-sv/ubsv/test/testenv"
	helper "github.com/bitcoin-sv/ubsv/test/utils"
	"github.com/stretchr/testify/suite"
)

const stringTrue = "true"

type TeranodeTestSuite struct {
	suite.Suite
	TeranodeTestEnv *tenv.TeranodeTestEnv
	ComposeFiles    []string
	SettingsMap     map[string]string
}

func (suite *TeranodeTestSuite) DefaultComposeFiles() []string {
	return []string{"../../docker-compose.e2etest.yml"}
}

func (suite *TeranodeTestSuite) DefaultSettingsMap() map[string]string {
	return map[string]string{
		"SETTINGS_CONTEXT_1": "docker.ubsv1.test",
		"SETTINGS_CONTEXT_2": "docker.ubsv2.test",
		"SETTINGS_CONTEXT_3": "docker.ubsv3.test",
	}
}

const (
	NodeURL1 = "http://localhost:10090"
	NodeURL2 = "http://localhost:12090"
	NodeURL3 = "http://localhost:14090"
)

func (suite *TeranodeTestSuite) SetupTest() {
	suite.SetupTestEnv(suite.DefaultSettingsMap(), suite.DefaultComposeFiles(), false)
}

func (suite *TeranodeTestSuite) TearDownTest() {
	if err := TearDownTeranodeTestEnv(suite.TeranodeTestEnv); err != nil {
		if suite.T() != nil && suite.TeranodeTestEnv != nil {
			suite.T().Cleanup(suite.TeranodeTestEnv.Cancel)
		}
	}

	isGitHubActions := os.Getenv("GITHUB_ACTIONS") == stringTrue
	err := removeDataDirectory("../../data", isGitHubActions)

	if err != nil {
		suite.T().Fatal(err)
	}

	if suite.T() != nil && suite.TeranodeTestEnv != nil {
		suite.T().Cleanup(suite.TeranodeTestEnv.Cancel)
	}
}

func (suite *TeranodeTestSuite) SetupTestEnv(settingsMap map[string]string, composeFiles []string, skipSetUpTestClient bool) {
	var err error

	const ubsv1RPCEndpoint = "http://localhost:11292"

	suite.ComposeFiles = composeFiles
	suite.SettingsMap = settingsMap

	isGitHubActions := os.Getenv("GITHUB_ACTIONS") == stringTrue

	suite.T().Log("Removing data directory")

	err = removeDataDirectory("../../data/test", isGitHubActions)
	if err != nil {
		suite.T().Fatal(err)
	}

	suite.T().Log("Cleaning up test containers")

	err = cleanUpE2EContainers(isGitHubActions)
	if err != nil {
		suite.T().Fatal(err)
	}

	suite.T().Log("Setting up TeranodeTestEnv")

	suite.TeranodeTestEnv, err = SetupTeranodeTestEnv(suite.ComposeFiles, suite.SettingsMap)
	if err != nil {
		if suite.T() != nil && suite.TeranodeTestEnv != nil {
			suite.T().Cleanup(suite.TeranodeTestEnv.Cancel)
		}
		suite.T().Fatalf("Failed to set up TeranodeTestEnv: %v", err)
	}

	time.Sleep(30 * time.Second)

	if !skipSetUpTestClient {
		err = suite.TeranodeTestEnv.InitializeTeranodeTestClients()
		if err != nil {
			suite.T().Fatal(err)
		}

		suite.T().Logf("Sending initial RUN event to Node1")

		err = suite.TeranodeTestEnv.Nodes[0].BlockchainClient.Run(suite.TeranodeTestEnv.Context, "test")
		if err != nil {
			suite.T().Fatal(err)
		}

		suite.T().Logf("Sending initial RUN event to Node2")

		err = suite.TeranodeTestEnv.Nodes[1].BlockchainClient.Run(suite.TeranodeTestEnv.Context, "test")
		if err != nil {
			suite.T().Fatal(err)
		}

		suite.T().Logf("Sending initial RUN event to Node3")

		err = suite.TeranodeTestEnv.Nodes[2].BlockchainClient.Run(suite.TeranodeTestEnv.Context, "test")
		if err != nil {
			suite.T().Fatal(err)
		}

		time.Sleep(10 * time.Second)

		// Min height possible is 101
		// whatever height you specify, make sure :
		// mine_initial_blocks_count.docker = [height]
		// blockvalidation_maxPreviousBlockHeadersToCheck = [height - 1]
		height := uint32(101)

		// Generate blocks
		_, err = helper.CallRPC(ubsv1RPCEndpoint, "generate", []interface{}{101})
		if err != nil {
			suite.T().Fatal(err)
		}

		err = helper.WaitForBlockHeight(NodeURL1, height, 30)
		if err != nil {
			suite.T().Fatal(err)
		}

		err = helper.WaitForBlockHeight(NodeURL2, height, 30)
		if err != nil {
			suite.T().Fatal(err)
		}

		err = helper.WaitForBlockHeight(NodeURL3, height, 30)
		if err != nil {
			suite.T().Fatal(err)
		}
	}

	suite.T().Log("TeranodeTestEnv setup completed")
}

func TestTeranodeTestSuite(t *testing.T) {
	suite.Run(t, new(TeranodeTestSuite))
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

func SetupTeranodeTestEnv(composeFiles []string, settingsMap map[string]string) (*tenv.TeranodeTestEnv, error) {
	testEnv := tenv.NewTeraNodeTestEnv(composeFiles)
	if err := testEnv.SetupDockerNodes(settingsMap); err != nil {
		return nil, errors.NewConfigurationError("Error setting up nodes: %v\n", err)
	}

	return testEnv, nil
}

func TearDownTeranodeTestEnv(testEnv *tenv.TeranodeTestEnv) error {
	if err := testEnv.StopDockerNodes(); err != nil {
		return errors.NewConfigurationError("Error stopping nodes: %v\n", err)
	}

	return nil
}
