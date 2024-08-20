//go:build tnctests

package tnc

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/test/setup"
	helper "github.com/bitcoin-sv/ubsv/test/utils"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
)

type TNC1TestSuite struct {
	setup.BitcoinTestSuite
}

func (suite *TNC1TestSuite) InitSuite() {
	suite.SettingsMap = map[string]string{
		"SETTINGS_CONTEXT_1": "docker.ci.ubsv1.tnc1Test",
		"SETTINGS_CONTEXT_2": "docker.ci.ubsv2.tnc1Test",
		"SETTINGS_CONTEXT_3": "docker.ci.ubsv3.tnc1Test",
	}
}

func (suite *TNC1TestSuite) SetupTest() {
	suite.InitSuite()
	suite.BitcoinTestSuite.SetupTestWithCustomSettings(suite.SettingsMap)
}

func (suite *TNC1TestSuite) TestCandidateContainsAllTxs() {
	ctx := context.Background()
	t := suite.T()
	framework := suite.Framework
	node0 := framework.Nodes[0]
	blockchainClientNode0 := node0.BlockchainClient
	var hashes []*chainhash.Hash

	var logLevelStr, _ = gocore.Config().Get("logLevel", "INFO")
	logger := ulogger.New("test", ulogger.WithLevel(logLevelStr))

	blockchainSubscription, err := blockchainClientNode0.Subscribe(ctx, "test-tnc1")
	if err != nil {
		t.Errorf("error subscribing to blockchain service: %v", err)
		return
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case notification := <-blockchainSubscription:
				if notification.Type == model.NotificationType_Subtree {
					hash, err := chainhash.NewHash(notification.Hash)
					require.NoError(t, err)
					hashes = append(hashes, hash)
					fmt.Println("Length of hashes:", len(hashes))
				} else {
					fmt.Println("other notifications than subtrees")
					fmt.Println(notification.Type)
				}
			}
		}
	}()

	_, errTXs := helper.SendTXsWithDistributor(ctx, framework.Nodes[0], logger, 10000)
	if errTXs != nil {
		t.Errorf("Failed to send txs with distributor: %v", errTXs)
	}

	if len(hashes) > 0 {
		fmt.Println("First element of hashes:", hashes[0])
	} else {
		t.Errorf("No subtrees detected: cannot calculate Merkleproofs")
	}

	fmt.Println("num of subtrees:", len(hashes))

	mc0, err0 := helper.GetMiningCandidate(ctx, framework.Nodes[0].BlockassemblyClient, logger)
	mc1, err1 := helper.GetMiningCandidate(ctx, framework.Nodes[1].BlockassemblyClient, logger)
	mc2, err2 := helper.GetMiningCandidate(ctx, framework.Nodes[2].BlockassemblyClient, logger)
	mp0 := utils.ReverseAndHexEncodeSlice(mc0.GetMerkleProof()[0])
	mp1 := utils.ReverseAndHexEncodeSlice(mc1.GetMerkleProof()[0])
	mp2 := utils.ReverseAndHexEncodeSlice(mc2.GetMerkleProof()[0])

	fmt.Println("Merkleproofs:")
	fmt.Println(mp0)
	fmt.Println(mp1)
	fmt.Println(mp2)

	if mp0 != mp1 || mp1 != mp2 {
		t.Errorf("Merkle proofs are different")
	}

	// Calculate MerkleProof for other TXs sent
	if err0 != nil {
		t.Errorf("Failed to get mining candidate 0: %v", err0)
	}
	if err1 != nil {
		t.Errorf("Failed to get mining candidate 1: %v", err1)
	}
	if err2 != nil {
		t.Errorf("Failed to get mining candidate 2: %v", err2)
	}

}

func (suite *TNC1TestSuite) TestCheckHashPrevBlockCandidate() {
	ctx := context.Background()
	var logLevelStr, _ = gocore.Config().Get("logLevel", "INFO")
	t := suite.T()
	framework := suite.Framework
	logger := ulogger.New("test", ulogger.WithLevel(logLevelStr))
	ba := framework.Nodes[0].BlockassemblyClient
	bc := framework.Nodes[0].BlockchainClient

	_, errTXs := helper.SendTXsWithDistributor(ctx, framework.Nodes[0], logger, 10000)
	if errTXs != nil {
		t.Errorf("Failed to send txs with distributor: %v", errTXs)
	}

	block0bytes, errMine0 := helper.MineBlock(ctx, framework.Nodes[0].BlockassemblyClient, logger)
	if errMine0 != nil {
		t.Errorf("Failed to mine block: %v", errMine0)
	}

	mc0, errmc0 := ba.GetMiningCandidate(ctx)
	if errmc0 != nil {
		t.Errorf("error getting mining candidate: %v", errmc0)
	}

	bestBlockheader, _, errBbh := bc.GetBestBlockHeader(ctx)
	if errBbh != nil {
		t.Errorf("error getting best block header: %v", errBbh)
	}

	block0, err0 := framework.Nodes[0].BlockchainClient.GetBlock(ctx, (*chainhash.Hash)(block0bytes))
	if err0 != nil {
		t.Errorf("Failed to get block: %v", err0)
	}

	prevHash, errHash := chainhash.NewHash(mc0.PreviousHash)

	if errHash != nil {
		t.Errorf("error getting previous hash: %v", errHash)
	}

	if bestBlockheader.String() != block0.Header.String() || bestBlockheader.String() != prevHash.String() {
		t.Errorf("Teranode working on incorrect prevHash")
	}
}

func (suite *TNC1TestSuite) TestCoinbaseTXAmount() {
	ctx := context.Background()
	t := suite.T()
	framework := suite.Framework

	var logLevelStr, _ = gocore.Config().Get("logLevel", "INFO")
	logger := ulogger.New("test", ulogger.WithLevel(logLevelStr))

	ba := framework.Nodes[0].BlockassemblyClient
	bc := framework.Nodes[0].BlockchainClient

	mc0, errmc0 := ba.GetMiningCandidate(ctx)
	if errmc0 != nil {
		t.Errorf("Error getting mining candidate on node 0")
	}

	coinbaseValueBlock := mc0.CoinbaseValue
	logger.Infof("Coinbase value mining candidate 0: %d", coinbaseValueBlock)

	_, bbhmeta, errbb := bc.GetBestBlockHeader(ctx)
	if errbb != nil {
		t.Errorf("Error getting best block")
	}
	block, errblock := bc.GetBlockByHeight(ctx, bbhmeta.Height)
	if errblock != nil {
		t.Errorf("Error getting block by height")
	}
	coinbaseTX := block.CoinbaseTx
	amount := coinbaseTX.TotalOutputSatoshis()
	logger.Infof("Amount inside block coinbase tx: %d", amount)

	if amount != coinbaseValueBlock {
		t.Errorf("Error calculating Coinbase Tx amount")
	}
}

func (suite *TNC1TestSuite) TestCoinbaseTXAmount2() {
	ctx := context.Background()
	t := suite.T()
	framework := suite.Framework

	ba := framework.Nodes[0].BlockassemblyClient

	var logLevelStr, _ = gocore.Config().Get("logLevel", "INFO")
	logger := ulogger.New("test", ulogger.WithLevel(logLevelStr))

	_, errTXs := helper.SendTXsWithDistributor(ctx, framework.Nodes[0], logger, 9000)
	if errTXs != nil {
		t.Errorf("Failed to send txs with distributor: %v", errTXs)
	}

	mc0, errmc0 := ba.GetMiningCandidate(ctx)
	if errmc0 != nil {
		t.Errorf("Error getting mining candidate on node 0")
	}

	coinbaseValueBlock := mc0.CoinbaseValue
	logger.Infof("Coinbase value mining candidate 0: %d", coinbaseValueBlock)

	block, errblock := helper.GetBestBlock(ctx, framework.Nodes[0])
	if errblock != nil {
		t.Errorf("Error getting best block")
	}

	logger.Infof("Header of the best block %v", block.Header.String())
	logger.Infof("Length of subtree slices %d", len(block.SubtreeSlices))

	coinbaseTX := block.CoinbaseTx
	amount := coinbaseTX.TotalOutputSatoshis()
	logger.Infof("Amount inside block coinbase tx: %d\n", amount)
	logger.Infof("Fees: %d", coinbaseValueBlock-amount)

	if coinbaseValueBlock < amount {
		t.Errorf("Error calculating fees")
	}
}

func TestTNC1TestSuite(t *testing.T) {
	suite.Run(t, new(TNC1TestSuite))
}
