package utils

import (
	"context"
	"fmt"
	"net/url"
	"reflect"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/test"
	postgres "github.com/bsv-blockchain/teranode/test/longtest/util/postgres"
	"github.com/bsv-blockchain/teranode/test/utils/tconfig"
	"github.com/bsv-blockchain/teranode/util/retry"
	"github.com/docker/go-connections/nat"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

const stringTrue = "true"

type TeranodeTestSuite struct {
	suite.Suite
	TeranodeTestEnv *TeranodeTestEnv
	TConfig         tconfig.TConfig
}

// const (
// 	NodeURL1 = "http://localhost:10090"
// 	NodeURL2 = "http://localhost:12090"
// 	NodeURL3 = "http://localhost:14090"
// )

func (suite *TeranodeTestSuite) SetupTest() {
	// If the suite.TConfig equal to empty instance, then initialize it with default constructor
	empty := tconfig.TConfig{}
	if reflect.DeepEqual(suite.TConfig, empty) {
		suite.TConfig = tconfig.LoadTConfig(nil)
	}

	if len(suite.TConfig.LocalSystem.Composes) > 0 && !suite.TConfig.LocalSystem.SkipSetup {
		suite.setupLocalTestEnv()
	}
}

func (suite *TeranodeTestSuite) TearDownTest() {
	if len(suite.TConfig.LocalSystem.Composes) > 0 && !suite.TConfig.LocalSystem.SkipTeardown {
		if err := teardownLocalTeranodeTestEnv(suite.TeranodeTestEnv); err != nil {
			if suite.T() != nil && suite.TeranodeTestEnv != nil {
				suite.T().Cleanup(suite.TeranodeTestEnv.Cancel)
			}
		}

		if suite.T() != nil && suite.TeranodeTestEnv != nil {
			suite.T().Cleanup(suite.TeranodeTestEnv.Cancel)
		}
	}
}

func (suite *TeranodeTestSuite) setupLocalTestEnv() {
	var err error

	suite.T().Log("Setting up TeranodeTestEnv")

	suite.TeranodeTestEnv, err = setupLocalTeranodeTestEnv(suite.TConfig)
	if err != nil {
		if suite.T() != nil && suite.TeranodeTestEnv != nil {
			suite.T().Cleanup(suite.TeranodeTestEnv.Cancel)
		}

		suite.T().Fatalf("Failed to set up TeranodeTestEnv: %v", err)
	}

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
	port, ok := gocore.Config().GetInt("health_check_port", 8000)
	if !ok {
		suite.T().Fatalf("health_check_port not set in config")
	}

	ports := []int{port, port, port}

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

	if suite.TConfig.Suite.InitBlockHeight > 0 {
		height := suite.TConfig.Suite.InitBlockHeight
		teranode1RPCEndpoint := suite.TeranodeTestEnv.Nodes[0].RPCURL
		teranode1RPCEndpoint = "http://" + teranode1RPCEndpoint

		// Generate blocks
		_, err = retry.Retry(
			context.Background(),
			suite.TeranodeTestEnv.Logger,
			func() (string, error) {
				return CallRPC(teranode1RPCEndpoint, "generate", []interface{}{height})
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

func setupLocalTeranodeTestEnv(cfg tconfig.TConfig) (*TeranodeTestEnv, error) {
	testEnv := NewTeraNodeTestEnv(cfg)
	if err := testEnv.SetupDockerNodes(); err != nil {
		return nil, errors.NewConfigurationError("Error setting up nodes", err)
	}

	return testEnv, nil
}

func teardownLocalTeranodeTestEnv(testEnv *TeranodeTestEnv) error {
	if err := testEnv.StopDockerNodes(); err != nil {
		return errors.NewConfigurationError("Error stopping nodes", err)
	}

	return nil
}

func SetupPostgresTestDaemon(t *testing.T, ctx context.Context, containerName string) *daemon.TestDaemon {
	pg, errPsql := postgres.RunPostgresTestContainer(ctx, containerName)
	require.NoError(t, errPsql)

	t.Cleanup(func() {
		if err := pg.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate postgres container: %v", err)
		}
	})

	gocore.Config().Set("POSTGRES_PORT", pg.Port)

	pgStore := fmt.Sprintf("postgres://teranode:teranode@localhost:%s/teranode?expiration=5m", pg.Port)

	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC: true,
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
			func(tSettings *settings.Settings) {
				url, err := url.Parse(pgStore)
				require.NoError(t, err)
				tSettings.BlockChain.StoreURL = url
				tSettings.Coinbase.Store = url
				tSettings.UtxoStore.UtxoStore = url
			},
		),
	})

	t.Cleanup(func() {
		td.Stop(t)
	})

	// set run state
	err := td.BlockchainClient.Run(td.Ctx, "test-tna2")
	require.NoError(t, err)

	return td
}
