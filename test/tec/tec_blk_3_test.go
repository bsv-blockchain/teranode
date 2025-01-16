//go:build test_all || test_tec || test_tec_blk_3

// How to run this test manually:
// $ cd test/tec
// $ go test -v -run "^TestTECBlk3TestSuite$/TestBlockAssemblyRecoverability$" -tags test_tec_blk_3

package tec

import (
	"fmt"
	"testing"

	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/bitcoin-sv/teranode/test/utils/tconfig"
	"github.com/stretchr/testify/suite"
)

type TECBlk3TestSuite struct {
	helper.TeranodeTestSuite
}

func TestTECBlk3TestSuite(t *testing.T) {
	suite.Run(t, &TECBlk3TestSuite{
		TeranodeTestSuite: helper.TeranodeTestSuite{
			TConfig: tconfig.LoadTConfig(
				map[string]any{
					tconfig.KeyLocalSystemComposes: []string{
						"../../docker-compose.yml",
						"../../docker-compose.aerospike.override.yml",
						"../../docker-compose.e2etest.yml",
						"../docker-compose.utxo.override.yml",
					},
					tconfig.KeyTeranodeContexts: []string{
						"docker.teranode1.test.tec3",
						"docker.teranode1.test.tec3",
						"docker.teranode1.test.tec3",
					},
				},
			),
		},
	},
	)
}

func (suite *TECBlk3TestSuite) TestBlockAssemblyRecoverability() {
	t := suite.T()
	testenv := suite.TeranodeTestEnv
	ctx := testenv.Context

	fmt.Println("Setting up Teranode with invalid blockassembly_grpcAddress...")

	blockchainHealth, _, err := testenv.Nodes[1].BlockchainClient.Health(ctx, true)
	if err != nil {
		t.Fatalf("Failed to start blockchain: %v", err)
	}

	if blockchainHealth != 200 {
		t.Errorf("Expected blockchainHealth to be 200, but got %d", blockchainHealth)
	}

	fmt.Println("Block Assembly Recoverability Test passed successfully...")
}
