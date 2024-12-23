//go:build test_all || test_tec || test_tec_blk_6

// How to run this test manually:
// $ cd test/tec
// $ go test -v -run "^TestTECBlk6TestSuite$/TestAssetServerRecoverability$" -tags test_tec_blk_6

package tec

import (
	"fmt"
	"testing"

	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/stretchr/testify/suite"
)

type TECBlk6TestSuite struct {
	helper.TeranodeTestSuite
}

func (suite *TECBlk6TestSuite) InitSuite() {
	suite.SettingsMap = map[string]string{
		"SETTINGS_CONTEXT_1": "docker.teranode1.test.tec6",
		"SETTINGS_CONTEXT_2": "docker.teranode1.test.tec6",
		"SETTINGS_CONTEXT_3": "docker.teranode1.test.tec6",
	}
}

func (suite *TECBlk6TestSuite) SetupTest() {
	suite.InitSuite()
	suite.SetupTestEnv(
		suite.SettingsMap,
		[]string{"../../docker-compose.yml", "../../docker-compose.aerospike.override.yml", "../../docker-compose.e2etest.yml", "../docker-compose.utxo.override.yml"},
		false,
	)
}

func (suite *TECBlk6TestSuite) TestAssetServerRecoverability() {
	t := suite.T()
	testenv := suite.TeranodeTestEnv
	ctx := testenv.Context

	fmt.Println("Setting up Teranode - Testing Asset Server with asset_httpAddress...")

	blockchainHealth, _, err := testenv.Nodes[1].BlockchainClient.Health(ctx, true)
	if err != nil {
		t.Fatalf("Failed to start blockchain: %v", err)
	}

	if blockchainHealth != 200 {
		t.Errorf("Expected blockchainHealth to be 200, but got %d", blockchainHealth)
	}

	fmt.Println("Asset Server Recoverability Test passed successfully...")
}

func (suite *TECBlk6TestSuite) TestAssetServerRecoverabilityStartup() {
	t := suite.T()
	testenv := suite.TeranodeTestEnv
	ctx := testenv.Context

	fmt.Println("Setting up Teranode - Testing Asset Server without asset_httpAddress...")

	suite.SetupTestEnv(
		suite.SettingsMap,
		[]string{"../../docker-compose.yml", "../../docker-compose.aerospike.override.yml", "../../docker-compose.e2etest.yml", "../docker-compose.asset.override.yml"},
		false,
	)

	blockchainHealth, _, err := testenv.Nodes[1].BlockchainClient.Health(ctx, true)
	if err != nil {
		t.Fatalf("Failed to start blockchain: %v", err)
	}

	if blockchainHealth != 200 {
		t.Errorf("Expected blockchainHealth to be 200, but got %d", blockchainHealth)
	}

	fmt.Println("Asset Server Startup Recoverability Test passed successfully...")
}

func TestTECBlk6TestSuite(t *testing.T) {
	suite.Run(t, new(TECBlk6TestSuite))
}
