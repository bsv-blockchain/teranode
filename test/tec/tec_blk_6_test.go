//go:build tecblk6test

package test

import (
	"testing"

	"github.com/bitcoin-sv/ubsv/test/setup"
	"github.com/stretchr/testify/suite"
)

type TECBlk6TestSuite struct {
	setup.BitcoinTestSuite
}

func (suite *TECBlk6TestSuite) InitSuite() {
	suite.SettingsMap = map[string]string{
		"SETTINGS_CONTEXT_1": "docker.ci.ubsv1.tec6",
		"SETTINGS_CONTEXT_2": "docker.ci.ubsv1.tec6",
		"SETTINGS_CONTEXT_3": "docker.ci.ubsv1.tec6",
	}
}

const (
	NodeURL1 = "http://localhost:18090"
	NodeURL2 = "http://localhost:28090"
	NodeURL3 = "http://localhost:38090"
)

func (suite *TECBlk6TestSuite) SetupTest() {
	suite.InitSuite()
	suite.SetupTestWithCustomComposeAndSettings(
		suite.SettingsMap,
		[]string{"../../docker-compose.yml", "../../docker-compose.aerospike.override.yml", "../../docker-compose.e2etest.yml"},
	)
}

func (suite *TECBlk6TestSuite) TestAssetServerRecoverability() {
	t := suite.T()
	t.Log("Setting up Teranode - Testing Asset Server with asset_httpAddress...")
}

func (suite *TECBlk6TestSuite) TestAssetServerRecoverabilityStartup() {
	t := suite.T()
	t.Log("Setting up Teranode - Testing Asset Server without asset_httpAddress...")
	suite.SetupTestWithCustomComposeAndSettings(
		suite.SettingsMap,
		[]string{"../../docker-compose.yml", "../../docker-compose.aerospike.override.yml", "../../docker-compose.e2etest.override.yml", "../docker-compose.asset.override.yml"},
	)
}

func TestTECBlk6TestSuite(t *testing.T) {
	suite.Run(t, new(TECBlk6TestSuite))
}
