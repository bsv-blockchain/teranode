//go:build tecblk3test

package test

import (
	"fmt"
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
		"SETTINGS_CONTEXT_2": "docker.ci.ubsv1.tec3",
		"SETTINGS_CONTEXT_3": "docker.ci.ubsv1.tec3",
	}
}

func (suite *TECBlk3TestSuite) SetupTest() {
	suite.SetupTestWithCustomComposeAndSettingsSkipChecks(
		suite.SettingsMap,
		[]string{"../../docker-compose.yml", "../../docker-compose.aerospike.override.yml", "../../docker-compose.e2etest.override.yml"},
	)
}

func (suite *TECBlk3TestSuite) TearDownTest() {

}

func (suite *TECBlk3TestSuite) TestBlockAssemblyRecoverability() {
	fmt.Println("Setting up Teranode with invalid blockassembly_grpcAddress...")
}

func TestTECBlk3TestSuite(t *testing.T) {

	suite.Run(t, new(TECBlk3TestSuite))
}
