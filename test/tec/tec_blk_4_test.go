//go:build tecblk4test

package test

import (
	"fmt"
	"testing"

	"github.com/bitcoin-sv/ubsv/test/setup"
	"github.com/stretchr/testify/suite"
)

type TECBlk4TestSuite struct {
	setup.BitcoinTestSuite
}

func (suite *TECBlk4TestSuite) InitSuite() {
	suite.SettingsMap = map[string]string{
		"SETTINGS_CONTEXT_1": "docker.ci.ubsv1.tec4",
		"SETTINGS_CONTEXT_2": "docker.ci.ubsv1.tec4",
		"SETTINGS_CONTEXT_3": "docker.ci.ubsv1.tec4",
	}
}

func (suite *TECBlk4TestSuite) SetupTest() {
	suite.SetupTestWithCustomComposeAndSettings(
		suite.SettingsMap,
		[]string{"../../docker-compose.yml", "../../docker-compose.aerospike.override.yml", "../../docker-compose.e2etest.yml"},
	)
}

func (suite *TECBlk4TestSuite) TearDownTest() {

}

func (suite *TECBlk4TestSuite) TestBlockValidationRecoverability() {
	fmt.Println("Setting up Teranode without Block Validation...")
}

func TestTECBlk4TestSuite(t *testing.T) {
	suite.Run(t, new(TECBlk4TestSuite))
}
