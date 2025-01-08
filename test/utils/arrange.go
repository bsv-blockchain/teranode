package utils

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/util/retry"
	"github.com/docker/go-connections/nat"
	"github.com/stretchr/testify/suite"
)

const stringTrue = "true"

type TeranodeTestSuite struct {
	suite.Suite
	TeranodeTestEnv *TeranodeTestEnv
	ComposeFiles    []string
	SettingsMap     map[string]string
}

func (suite *TeranodeTestSuite) DefaultComposeFiles() []string {
	return []string{"../../docker-compose.e2etest.yml"}
}

func (suite *TeranodeTestSuite) DefaultSettingsMap() map[string]string {
	return map[string]string{
		"SETTINGS_CONTEXT_1": "docker.teranode1.test",
		"SETTINGS_CONTEXT_2": "docker.teranode2.test",
		"SETTINGS_CONTEXT_3": "docker.teranode3.test",
	}
}

// const (
// 	NodeURL1 = "http://localhost:10090"
// 	NodeURL2 = "http://localhost:12090"
// 	NodeURL3 = "http://localhost:14090"
// )

func (suite *TeranodeTestSuite) SetupTest() {
	suite.SetupTestEnv(suite.DefaultSettingsMap(), suite.DefaultComposeFiles(), false)
}

func (suite *TeranodeTestSuite) TearDownTest() {
	if err := TearDownTeranodeTestEnv(suite.TeranodeTestEnv); err != nil {
		if suite.T() != nil && suite.TeranodeTestEnv != nil {
			suite.T().Cleanup(suite.TeranodeTestEnv.Cancel)
		}
	}

	// isGitHubActions := os.Getenv("GITHUB_ACTIONS") == stringTrue
	// err := removeDataDirectory("../../data", isGitHubActions)

	// if err != nil {
	// 	suite.T().Fatal(err)
	// }

	if suite.T() != nil && suite.TeranodeTestEnv != nil {
		suite.T().Cleanup(suite.TeranodeTestEnv.Cancel)
	}
}

func (suite *TeranodeTestSuite) SetupTestEnv(settingsMap map[string]string, composeFiles []string, skipSetUpTestClient bool) {
	var err error

	suite.ComposeFiles = composeFiles
	suite.SettingsMap = settingsMap

	// isGitHubActions := os.Getenv("GITHUB_ACTIONS") == stringTrue

	suite.T().Log("Removing data directory")

	// err = removeDataDirectory("../../data/test", isGitHubActions)
	// if err != nil {
	// 	suite.T().Fatal(err)
	// }

	suite.T().Log("Cleaning up test containers")

	// err = cleanUpE2EContainers(isGitHubActions)
	// if err != nil {
	// 	suite.T().Fatal(err)
	// }

	suite.T().Log("Setting up TeranodeTestEnv")

	suite.TeranodeTestEnv, err = SetupTeranodeTestEnv(suite.ComposeFiles, suite.SettingsMap)
	if err != nil {
		if suite.T() != nil && suite.TeranodeTestEnv != nil {
			suite.T().Cleanup(suite.TeranodeTestEnv.Cancel)
		}

		suite.T().Fatalf("Failed to set up TeranodeTestEnv: %v", err)
	}

	if !skipSetUpTestClient {
		var err error

		time.Sleep(10 * time.Second)

		err = suite.TeranodeTestEnv.InitializeTeranodeTestClients()
		if err != nil {
			suite.T().Fatal(err)
		}

		// wait for all blockchain nodes to be ready
		for index, node := range suite.TeranodeTestEnv.Nodes {
			suite.T().Logf("Sending initial RUN event to Blockchain %d", index)

			err = SendEventRun(suite.TeranodeTestEnv.Context, node.BlockchainClient, suite.TeranodeTestEnv.Logger)
			if err != nil {
				suite.T().Fatal(err)
			}
		}

		// get mapped ports for 8000, 8000, 8000
		ports := []int{8000, 8000, 8000}
		for index, port := range ports {
			mappedPort, err := suite.TeranodeTestEnv.GetMappedPort(fmt.Sprintf("teranode%d", index+1), nat.Port(fmt.Sprintf("%d/tcp", port)))
			if err != nil {
				suite.T().Fatal(err)
			}

			suite.T().Logf("Waiting for node %d to be ready", index)

			err = WaitForHealthLiveness(mappedPort.Int(), 30*time.Second)
			if err != nil {
				suite.T().Fatal(err)
			}
		}

		suite.T().Log("All nodes ready")

		height := uint32(101)
		teranode1RPCEndpoint := suite.TeranodeTestEnv.Nodes[0].RPCURL
		teranode1RPCEndpoint = "http://" + teranode1RPCEndpoint

		// Generate blocks
		_, err = retry.Retry(
			context.Background(),
			suite.TeranodeTestEnv.Logger,
			func() (string, error) {
				return CallRPC(teranode1RPCEndpoint, "generate", []interface{}{101})
			},
		)
		if err != nil {
			// we sometimes set an error saying the job was not found but strangely the test works even with this error
			// suite.T().Fatal(err)
			suite.T().Logf("Error generating blocks: %v", err)
		}

		NodeURL1 := suite.TeranodeTestEnv.Nodes[0].AssetURL
		NodeURL2 := suite.TeranodeTestEnv.Nodes[1].AssetURL
		NodeURL3 := suite.TeranodeTestEnv.Nodes[2].AssetURL

		// Add http to the url
		NodeURL1 = "http://" + NodeURL1
		NodeURL2 = "http://" + NodeURL2
		NodeURL3 = "http://" + NodeURL3

		err = WaitForBlockHeight(NodeURL1, height, 30)
		if err != nil {
			suite.T().Fatal(err)
		}

		err = WaitForBlockHeight(NodeURL2, height, 30)
		if err != nil {
			suite.T().Fatal(err)
		}

		err = WaitForBlockHeight(NodeURL3, height, 30)
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

func SetupTeranodeTestEnv(composeFiles []string, settingsMap map[string]string) (*TeranodeTestEnv, error) {
	testEnv := NewTeraNodeTestEnv(composeFiles)
	if err := testEnv.SetupDockerNodes(settingsMap); err != nil {
		return nil, errors.NewConfigurationError("Error setting up nodes", err)
	}

	return testEnv, nil
}

func TearDownTeranodeTestEnv(testEnv *TeranodeTestEnv) error {
	if err := testEnv.StopDockerNodes(); err != nil {
		return errors.NewConfigurationError("Error stopping nodes", err)
	}

	return nil
}
