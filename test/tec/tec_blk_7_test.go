//go:build tecblk7test

package test

import (
	"testing"

	"github.com/bitcoin-sv/ubsv/test/setup"
	"github.com/stretchr/testify/suite"
)

type TECBlk7TestSuite struct {
	setup.BitcoinTestSuite
}

func (suite *TECBlk7TestSuite) InitSuite() {
	suite.SettingsMap = map[string]string{
		"SETTINGS_CONTEXT_1": "docker.ci.ubsv1.tec7",
		"SETTINGS_CONTEXT_2": "docker.ci.ubsv2.tec7",
		"SETTINGS_CONTEXT_3": "docker.ci.ubsv3.tec7",
	}
}

func (suite *TECBlk7TestSuite) SetupTest() {
	suite.InitSuite()
	suite.SetupTestWithCustomComposeAndSettingsSkipChecks(
		suite.SettingsMap,
		[]string{"../../docker-compose.yml", "../../docker-compose.aerospike.override.yml", "../../docker-compose.e2etest.yml"}, false,
	)
}

func (suite *TECBlk7TestSuite) TearDownTest() {

}

func (suite *TECBlk7TestSuite) TestKafkaServiceRecoverability() {
	t := suite.T()
	t.Log("Setting up Teranode - Test Kafka Service Wrong Port 9999-...")
}

func TestTECBlk7TestSuite(t *testing.T) {
	suite.Run(t, new(TECBlk7TestSuite))
}
