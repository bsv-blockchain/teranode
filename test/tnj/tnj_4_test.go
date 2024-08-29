//go:build tnjtests

package tnj

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/test/setup"
	helper "github.com/bitcoin-sv/ubsv/test/utils"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type TNJ4TestSuite struct {
	setup.BitcoinTestSuite
}

func (suite *TNJ4TestSuite) InitSuite() {
	suite.SettingsMap = map[string]string{
		"SETTINGS_CONTEXT_1": "docker.ci.ubsv1.tnj4Test",
		"SETTINGS_CONTEXT_2": "docker.ci.ubsv2.tnj4Test",
		"SETTINGS_CONTEXT_3": "docker.ci.ubsv3.tnj4Test",
	}
}

func (suite *TNJ4TestSuite) SetupTest() {
	suite.InitSuite()
	suite.BitcoinTestSuite.SetupTestWithCustomSettings(suite.SettingsMap)
}

func (suite *TNJ4TestSuite) TestBlockSubsidy() {
	ctx := context.Background()
	t := suite.T()
	framework := suite.Framework

	ba := framework.Nodes[0].BlockassemblyClient

	var logLevelStr, _ = gocore.Config().Get("logLevel", "INFO")
	logger := ulogger.New("test", ulogger.WithLevel(logLevelStr))

	for i := 0; i < 32; i++ {
		_, errTXs := helper.SendTXsWithDistributor(ctx, framework.Nodes[0], logger, 0)
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

	_, errMineBlock := helper.MineBlockWithCandidate(ctx, framework.Nodes[0].BlockassemblyClient, mc0, logger)
	if errMineBlock != nil {
		t.Errorf("Failed to mine block: %v", errMineBlock)
	}

	block, errblock := helper.GetBestBlock(ctx, framework.Nodes[0])
	if errblock != nil {
		t.Errorf("Error getting best block")
	}

	coinbaseTX := block.CoinbaseTx
	amount := coinbaseTX.TotalOutputSatoshis()
	assert.Greater(t, amount, uint64(5000000000))

	newTx, errCBTx := helper.UseCoinbaseUtxo(ctx, framework.Nodes[0], coinbaseTX)
	if errCBTx != nil {
		t.Errorf("Failed to use coinbase utxo: %v", errCBTx)
	}

	fmt.Printf("Transaction sent: %s \n", newTx)
	time.Sleep(10 * time.Second)

	_, err := helper.MineBlock(framework.Context, framework.Nodes[0].BlockassemblyClient, framework.Logger)

	if err != nil {
		t.Errorf("Failed to mine block: %v", err)
	}

	blockStore := framework.Nodes[0].Blockstore

	blockchainClient := framework.Nodes[0].BlockchainClient
	header, meta, _ := blockchainClient.GetBestBlockHeader(ctx)
	framework.Logger.Infof("Best block header: %v", header.String())

	bl, err := helper.CheckIfTxExistsInBlock(ctx, blockStore, framework.Nodes[0].BlockstoreUrl, header.Hash()[:], meta.Height, newTx, framework.Logger)
	if err != nil {
		t.Errorf("error checking if tx exists in block: %v", err)
	}

	assert.Equal(t, false, bl, "Test Tx was not expected in the block")
}

func TestTNJ4TestSuite(t *testing.T) {
	suite.Run(t, new(TNJ4TestSuite))
}
