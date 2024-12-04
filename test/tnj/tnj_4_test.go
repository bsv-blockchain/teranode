//go:build test_all || test_tnj

package tnj

import (
	"fmt"
	"testing"
	"time"

	helper "github.com/bitcoin-sv/ubsv/test/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type TNJ4TestSuite struct {
	helper.TeranodeTestSuite
}

func (suite *TNJ4TestSuite) InitSuite() {
	suite.SettingsMap = map[string]string{
		"SETTINGS_CONTEXT_1": "docker.ci.ubsv1.tnj4Test",
		"SETTINGS_CONTEXT_2": "docker.ci.ubsv2.tnj4Test",
		"SETTINGS_CONTEXT_3": "docker.ci.ubsv3.tnj4Test",
	}
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

	_, errMineBlock := helper.MineBlockWithCandidate(ctx, testEnv.Nodes[0].BlockassemblyClient, mc0, logger)
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

	_, err := helper.MineBlockWithRPC(testEnv.Context, testEnv.Nodes[0], logger)

	if err != nil {
		t.Errorf("Failed to mine block: %v", err)
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

func TestTNJ4TestSuite(t *testing.T) {
	suite.Run(t, new(TNJ4TestSuite))
}
