//go:build test_all || test_tec || test_tec_blk_2

package tec

import (
	"fmt"
	"testing"

	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/stretchr/testify/suite"
)

type TECBlk2TestSuite struct {
	helper.TeranodeTestSuite
}

func (suite *TECBlk2TestSuite) InitSuite() {
	suite.SettingsMap = map[string]string{
		"SETTINGS_CONTEXT_1": "docker.ci.teranode1.tec2",
		"SETTINGS_CONTEXT_2": "docker.ci.teranode2.tec2",
		"SETTINGS_CONTEXT_3": "docker.ci.teranode3.tec2",
	}
}

func (suite *TECBlk2TestSuite) SetupTest() {
	suite.InitSuite()
	suite.SetupTestEnv(
		suite.SettingsMap,
		[]string{"../../docker-compose.yml", "../../docker-compose.aerospike.override.yml", "../../docker-compose.e2etest.yml", "../docker-compose.utxo.override.yml"},
		false,
	)
}

func (suite *TECBlk2TestSuite) TestUtxoStoreRecoverability() {
	fmt.Println("Setting up Teranode without utxostore...")
}

func TestTECBlk2TestSuite(t *testing.T) {
	suite.Run(t, new(TECBlk2TestSuite))
}
