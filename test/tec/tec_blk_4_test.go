//go:build test_all || test_tec || test_tec_blk_4

// How to run this test manually:
// $ cd test/tec
// $ go test -v -run "^TestTECBlk4TestSuite$/TestBlockValidationRecoverability$" -tags test_tec_blk_4

package tec

import (
	"fmt"
	"testing"

	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/bitcoin-sv/teranode/test/utils/tconfig"
	"github.com/stretchr/testify/suite"
)

type TECBlk4TestSuite struct {
	helper.TeranodeTestSuite
}

func (suite *TECBlk4TestSuite) InitSuite() {
	suite.TConfig = tconfig.LoadTConfig(
		map[string]any{
			tconfig.KeySuiteComposes: []string{
				"../../docker-compose.yml",
				"../../docker-compose.aerospike.override.yml",
				"../../docker-compose.e2etest.yml",
				"../docker-compose.utxo.override.yml",
			},
			tconfig.KeyTeranodeContexts: []string{
				"docker.teranode1.test.tec4",
				"docker.teranode1.test.tec4",
				"docker.teranode1.test.tec4",
			},
		},
	)
}

func (suite *TECBlk4TestSuite) SetupTest() {
	suite.InitSuite()
	suite.SetupTestEnv(suite.TConfig.Teranode.SettingsMap(), suite.TConfig.Suite.Composes, false)
}

func (suite *TECBlk4TestSuite) TestBlockValidationRecoverability() {
	t := suite.T()
	testenv := suite.TeranodeTestEnv
	ctx := testenv.Context

	fmt.Println("Setting up Teranode without Block Validation...")

	blockchainHealth, _, err := testenv.Nodes[1].BlockchainClient.Health(ctx, true)
	if err != nil {
		t.Fatalf("Failed to start blockchain: %v", err)
	}

	if blockchainHealth != 200 {
		t.Errorf("Expected blockchainHealth to be 200, but got %d", blockchainHealth)
	}

	fmt.Println("Block Validation Recoverability Test passed successfully...")
}

func TestTECBlk4TestSuite(t *testing.T) {
	suite.Run(t, new(TECBlk4TestSuite))
}
