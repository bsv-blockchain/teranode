//go:build tecblk5test

package test

import (
	"fmt"
	"testing"

	"github.com/bitcoin-sv/ubsv/test/setup"
	"github.com/stretchr/testify/suite"
)

type TECBlk5TestSuite struct {
	setup.BitcoinTestSuite
}

func (suite *TECBlk5TestSuite) InitSuite() {
	suite.SettingsMap = map[string]string{
		"SETTINGS_CONTEXT_1": "docker.ci.ubsv2.tec5",
		"SETTINGS_CONTEXT_2": "docker.ci.ubsv3.tec5",
		"SETTINGS_CONTEXT_3": "docker.ci.ubsv4.tec5",
	}
}

func (suite *TECBlk5TestSuite) SetupTest() {
	suite.InitSuite()
	suite.SetupTestWithCustomComposeAndSettingsSkipChecks(
		suite.SettingsMap,
		[]string{"../../docker-compose.yml", "../../docker-compose.aerospike.override.yml", "../../docker-compose.e2etest.yml"}, true,
	)
}

func (suite *TECBlk5TestSuite) TearDownTest() {

}

func (suite *TECBlk5TestSuite) TestP2PRecoverability() {
	fmt.Println("Setting up Teranode without P2P httpListenAddress...")
}

func TestTECBlk5TestSuite(t *testing.T) {
	suite.Run(t, new(TECBlk5TestSuite))
}
