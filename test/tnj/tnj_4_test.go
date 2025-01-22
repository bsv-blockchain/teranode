//go:build test_all || test_tnj

package tnj

// How to run these tests:
// 1. Run all TNJ tests:
//    go test -tags=test_tnj ./test/tnj/... -v
//
// 2. Run this specific test:
//    go test -tags=test_tnj ./test/tnj/tnj_4_test.go -v
//
// 3. Run a specific test function:
//    go test -tags=test_tnj ./test/tnj/tnj_4_test.go -v -run TestTNJ4TestSuite/TestBlockSubsidy
//
// 4. If you're inside the test/tnj directory:
//    go test -tags=test_tnj -v                           # Run all tests in current directory
//    go test -tags=test_tnj -v tnj_4_test.go            # Run this specific test
//    go test -tags=test_tnj -v -run TestTNJ4TestSuite/TestBlockSubsidy tnj_4_test.go  # Run specific test function
//
// Note: These tests require Docker to be running as they use containerized Teranode instances

import (
	"fmt"
	"testing"
	"time"

	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/bitcoin-sv/teranode/test/utils/tconfig"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type TNJ4TestSuite struct {
	helper.TeranodeTestSuite
}

func TestTNJ4TestSuite(t *testing.T) {
	suite.Run(t, &TNJ4TestSuite{
		TeranodeTestSuite: helper.TeranodeTestSuite{
			TConfig: tconfig.LoadTConfig(
				map[string]any{
					tconfig.KeyTeranodeContexts: []string{
						"docker.teranode1.test.tnj4Test",
						"docker.teranode2.test.tnj4Test",
						"docker.teranode3.test.tnj4Test",
					},
				},
			),
		},
	},
	)
}

func (suite *TNJ4TestSuite) TestBlockSubsidy() {
	t := suite.T()
	testEnv := suite.TeranodeTestEnv
	ctx := testEnv.Context

	ba := testEnv.Nodes[0].BlockassemblyClient

	logger := testEnv.Logger

	for i := 0; i < 32; i++ {
		_, errTXs := helper.SendTXsWithDistributorV2(ctx, testEnv.Nodes[0], logger, testEnv.Nodes[0].Settings, 0)
		if errTXs != nil {
			t.Errorf("Failed to send txs with distributor: %v", errTXs)
		}
	}

	mc0, errmc0 := ba.GetMiningCandidate(ctx)
	if errmc0 != nil {
		t.Errorf("Error getting mining candidate on node 0")
	}

	coinbaseValueBlock := mc0.CoinbaseValue
	logger.Infof("Coinbase value mining candidate 0: %d", coinbaseValueBlock)

	_, errMineBlock := helper.MineBlockWithCandidate(ctx, testEnv.Nodes[0].Settings, testEnv.Nodes[0].BlockassemblyClient, mc0, logger)
	if errMineBlock != nil {
		t.Errorf("Failed to mine block: %v", errMineBlock)
	}

	block, errblock := helper.GetBestBlockV2(ctx, testEnv.Nodes[0])
	if errblock != nil {
		t.Errorf("Error getting best block")
	}

	coinbaseTX := block.CoinbaseTx
	amount := coinbaseTX.TotalOutputSatoshis()
	assert.Greater(t, amount, uint64(5000000000))

	newTx, errCBTx := helper.UseCoinbaseUtxoV2(ctx, testEnv.Nodes[0], coinbaseTX)
	if errCBTx != nil {
		t.Errorf("Failed to use coinbase utxo: %v", errCBTx)
	}

	fmt.Printf("Transaction sent: %s \n", newTx)
	time.Sleep(10 * time.Second)

	_, err := helper.GenerateBlocks(ctx, testEnv.Nodes[0], 100, logger)
	if err != nil {
		t.Errorf("Failed to generate blocks: %v", err)
	}

	blockStore := testEnv.Nodes[0].Blockstore

	blockchainClient := testEnv.Nodes[0].BlockchainClient
	header, meta, _ := blockchainClient.GetBestBlockHeader(ctx)
	testEnv.Logger.Infof("Best block header: %v", header.String())

	bl, err := helper.CheckIfTxExistsInBlock(ctx, blockStore, testEnv.Nodes[0].BlockstoreURL, header.Hash()[:], meta.Height, newTx, logger)
	if err != nil {
		t.Errorf("error checking if tx exists in block: %v", err)
	}

	assert.Equal(t, false, bl, "Test Tx was not expected in the block")
}
