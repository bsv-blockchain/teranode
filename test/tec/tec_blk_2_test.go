//go:build tecblk2test

package test

import (
	"fmt"
	"testing"

	"github.com/bitcoin-sv/ubsv/test/setup"
	"github.com/stretchr/testify/suite"
)

type TECBlk2TestSuite struct {
	setup.BitcoinTestSuite
}

func (suite *TECBlk2TestSuite) SetupTest() {
	suite.SetupTestWithCustomComposeAndSettings(
		suite.SettingsMap,
		[]string{"../../docker-compose.yml", "../../docker-compose.aerospike.override.yml", "../../docker-compose.e2etest.override.yml", "../../docker-compose.utxo.override.yml"},
	)
}

func (suite *TECBlk2TestSuite) TestUtxoStoreRecoverability() {
	fmt.Println("Setting up Teranode without utxostore...")
}

func TestTECBlk2TestSuite(t *testing.T) {
	suite.Run(t, new(TECBlk2TestSuite))
}
