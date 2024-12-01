//go:build tecblk3test

package resilience

import (
	"fmt"
	"testing"

	arrange "github.com/bitcoin-sv/ubsv/test/fixtures"
	"github.com/stretchr/testify/suite"
)

type TECBlk3TestSuite struct {
	arrange.TeranodeTestSuite
}

func (suite *TECBlk3TestSuite) InitSuite() {
	suite.SettingsMap = map[string]string{
		"SETTINGS_CONTEXT_1": "docker.ci.ubsv1.tec3",
		"SETTINGS_CONTEXT_2": "docker.ci.ubsv2.tec3",
		"SETTINGS_CONTEXT_3": "docker.ci.ubsv3.tec3",
	}
}

func (suite *TECBlk3TestSuite) SetupTest() {
	suite.InitSuite()
	suite.SetupTestEnv(
		suite.SettingsMap,
		[]string{"../../docker-compose.yml", "../../docker-compose.aerospike.override.yml", "../../docker-compose.e2etest.yml", "../docker-compose.utxo.override.yml"},
		false,
	)
}

func (suite *TECBlk3TestSuite) TestBlockAssemblyRecoverability() {
	t := suite.T()
	testenv := suite.TeranodeTestEnv
	ctx := testenv.Context

	fmt.Println("Setting up Teranode with invalid blockassembly_grpcAddress...")

	blockchainHealth, _, err := testenv.Nodes[1].BlockchainClient.Health(ctx, true)
	if err != nil {
		t.Fatalf("Failed to start blockchain: %v", err)
	}

	if blockchainHealth != 200 {
		t.Errorf("Expected blockchainHealth to be 200, but got %d", blockchainHealth)
	}

	fmt.Println("Block Assembly Recoverability Test passed successfully...")
}

func TestTECBlk3TestSuite(t *testing.T) {
	suite.Run(t, new(TECBlk3TestSuite))
}
