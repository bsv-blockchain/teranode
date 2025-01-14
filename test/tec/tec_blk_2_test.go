//go:build test_all || test_tec || test_tec_blk_2

package tec

import (
	"fmt"
	"testing"

	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/bitcoin-sv/teranode/test/utils/tconfig"
	"github.com/stretchr/testify/suite"
)

type TECBlk2TestSuite struct {
	helper.TeranodeTestSuite
}

func (suite *TECBlk2TestSuite) InitSuite() {
	suite.TConfig = tconfig.LoadTConfig(
		map[string]any{
			tconfig.KeyLocalSystemComposes: []string{
				"../../docker-compose.yml",
				"../../docker-compose.aerospike.override.yml",
				"../../docker-compose.e2etest.yml",
				"../docker-compose.utxo.override.yml",
			},
			tconfig.KeyTeranodeContexts: []string{
				"docker.ci.teranode1.tec2",
				"docker.ci.teranode2.tec2",
				"docker.ci.teranode3.tec2",
			},
		},
	)
}

func (suite *TECBlk2TestSuite) SetupTest() {
	suite.InitSuite()
	suite.SetupTestEnv(false)
}

func (suite *TECBlk2TestSuite) TestUtxoStoreRecoverability() {
	fmt.Println("Setting up Teranode without utxostore...")
}

func TestTECBlk2TestSuite(t *testing.T) {
	suite.Run(t, new(TECBlk2TestSuite))
}
