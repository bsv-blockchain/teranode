//go:build test_all || test_tec || test_tec_blk_7

// How to run this test manually:
// $ cd test/tec
// $ go test -v -run "^TestTECBlk7TestSuite$/TestKafkaServiceRecoverability$" -tags test_tec_blk_7

package tec

import (
	"fmt"
	"testing"

	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/stretchr/testify/suite"
)

type TECBlk7TestSuite struct {
	helper.TeranodeTestSuite
}

func (suite *TECBlk7TestSuite) InitSuite() {
	suite.SettingsMap = map[string]string{
		"SETTINGS_CONTEXT_1": "docker.teranode1.test.tec7",
		"SETTINGS_CONTEXT_2": "docker.teranode1.test.tec7",
		"SETTINGS_CONTEXT_3": "docker.teranode1.test.tec7",
	}
}

func (suite *TECBlk7TestSuite) SetupTest() {
	suite.InitSuite()
	suite.SetupTestEnv(
		suite.SettingsMap,
		[]string{"../../docker-compose.yml", "../../docker-compose.aerospike.override.yml", "../../docker-compose.e2etest.yml", "../docker-compose.utxo.override.yml"},
		false,
	)
}

func (suite *TECBlk7TestSuite) TestKafkaServiceRecoverability() {
	t := suite.T()
	testenv := suite.TeranodeTestEnv
	ctx := testenv.Context

	fmt.Println("Setting up Teranode - Test Kafka Service Wrong Port 9999...")

	blockchainHealth, _, err := testenv.Nodes[1].BlockchainClient.Health(ctx, true)
	if err != nil {
		t.Fatalf("Failed to start blockchain: %v", err)
	}

	if blockchainHealth != 200 {
		t.Errorf("Expected blockchainHealth to be 200, but got %d", blockchainHealth)
	}

	fmt.Println("Kafka Service Recoverability Test passed successfully...")
}

func TestTECBlk7TestSuite(t *testing.T) {
	suite.Run(t, new(TECBlk7TestSuite))
}
