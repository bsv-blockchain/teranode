//go:build test_all || test_tec || test_tec_blk_4

package tec

import (
	"fmt"
	"testing"

	helper "github.com/bitcoin-sv/ubsv/test/utils"
	"github.com/stretchr/testify/suite"
)

type TECBlk4TestSuite struct {
	helper.TeranodeTestSuite
}

func (suite *TECBlk4TestSuite) InitSuite() {
	suite.SettingsMap = map[string]string{
		"SETTINGS_CONTEXT_1": "docker.ci.ubsv1.tec4",
		"SETTINGS_CONTEXT_2": "docker.ci.ubsv2.tec4",
		"SETTINGS_CONTEXT_3": "docker.ci.ubsv3.tec4",
	}
}

func (suite *TECBlk4TestSuite) SetupTest() {
	suite.InitSuite()
	suite.SetupTestEnv(
		suite.SettingsMap,
		[]string{"../../docker-compose.yml", "../../docker-compose.aerospike.override.yml", "../../docker-compose.e2etest.yml", "../docker-compose.utxo.override.yml"},
		false,
	)
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
