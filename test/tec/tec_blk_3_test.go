//go:build tecblk3test

package test

import (
	"testing"

	"github.com/bitcoin-sv/ubsv/test/setup"
	"github.com/stretchr/testify/suite"
)

type TECBlk3TestSuite struct {
	setup.BitcoinTestSuite
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
	suite.SetupTestWithCustomComposeAndSettingsSkipChecks(
		suite.SettingsMap,
		[]string{"../../docker-compose.yml", "../../docker-compose.aerospike.override.yml", "../../docker-compose.e2etest.yml"}, false,
	)
}

func (suite *TECBlk3TestSuite) TearDownTest() {

}

func (suite *TECBlk3TestSuite) TestBlockAssemblyRecoverability() {
	t := suite.T()
	blockchainHealth, err := suite.Framework.Nodes[1].BlockchainClient.Health(suite.Framework.Context)

	t.Log("Setting up Teranode with invalid blockassembly_grpcAddress...")

	if err != nil {
		t.Errorf("Failed to start blockchain: %v", err)
		return
	}

	if !blockchainHealth.Ok {
		t.Errorf("Expected blockchainHealth to be false, but got true")
		return
	}

	t.Log("Block Assembly Recoverability Test passed successfully...")
}

func TestTECBlk3TestSuite(t *testing.T) {

	suite.Run(t, new(TECBlk3TestSuite))
}
