//go:build test_all || test_tnc

// How to run this test manually:
// $ cd test/tnc
// $ go test -v -run "^TestTNC1TestSuite$/TestCandidateContainsAllTxs$" -tags test_tnc
// $ go test -v -run "^TestTNC1TestSuite$/TestCheckHashPrevBlockCandidate$" -tags test_tnc
// $ go test -v -run "^TestTNC1TestSuite$/TestCoinbaseTXAmount$" -tags test_tnc
// $ go test -v -run "^TestTNC1TestSuite$/TestCoinbaseTXAmount2$" -tags test_tnc

package tnc

import (
	"fmt"
	"testing"

	"github.com/bitcoin-sv/teranode/model"
	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/bitcoin-sv/teranode/test/utils/tconfig"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type TNC1TestSuite struct {
	helper.TeranodeTestSuite
}

func (suite *TNC1TestSuite) InitSuite() {
	suite.TConfig = tconfig.LoadTConfig(
		map[string]any{
			tconfig.KeyTeranodeContexts: []string{
				"docker.teranode1.test.tnc1Test",
				"docker.teranode2.test.tnc1Test",
				"docker.teranode3.test.tnc1Test",
			},
		},
	)
}

func (suite *TNC1TestSuite) SetupTest() {
	suite.InitSuite()
	suite.SetupTestEnv(suite.TConfig.Teranode.SettingsMap(), suite.TConfig.Suite.Composes, false)
}

// func (suite *TNC1TestSuite) TearDownTest() {
// }

// TNC-1.1
func (suite *TNC1TestSuite) TestCandidateContainsAllTxs() {
	testEnv := suite.TeranodeTestEnv
	ctx := testEnv.Context
	t := suite.T()
	node0 := testEnv.Nodes[0]
	blockchainClientNode0 := node0.BlockchainClient

	var hashes []*chainhash.Hash

	logger := testEnv.Logger

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

	_, errTXs := helper.SendTXsWithDistributorV2(ctx, testEnv.Nodes[0], logger, testEnv.Nodes[0].Settings, 10000)
	if errTXs != nil {
		t.Errorf("Failed to send txs with distributor: %v", errTXs)
	}

	mc0, err0 := helper.GetMiningCandidate(ctx, testEnv.Nodes[0].BlockassemblyClient, logger)
	mc1, err1 := helper.GetMiningCandidate(ctx, testEnv.Nodes[1].BlockassemblyClient, logger)
	mc2, err2 := helper.GetMiningCandidate(ctx, testEnv.Nodes[2].BlockassemblyClient, logger)
	mp0 := utils.ReverseAndHexEncodeSlice(mc0.GetMerkleProof()[0])
	mp1 := utils.ReverseAndHexEncodeSlice(mc1.GetMerkleProof()[0])
	mp2 := utils.ReverseAndHexEncodeSlice(mc2.GetMerkleProof()[0])

	fmt.Println("Merkleproofs:")
	fmt.Println(mp0)
	fmt.Println(mp1)
	fmt.Println(mp2)

	if len(hashes) > 0 {
		fmt.Println("First element of hashes:", hashes[0])
	} else {
		t.Errorf("No subtrees detected: cannot calculate Merkleproofs")
	}

	fmt.Println("num of subtrees:", len(hashes))

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

// TNC-1.2
func (suite *TNC1TestSuite) TestCheckHashPrevBlockCandidate() {
	testEnv := suite.TeranodeTestEnv
	ctx := testEnv.Context
	t := suite.T()
	logger := testEnv.Logger
	ba := testEnv.Nodes[0].BlockassemblyClient
	bc := testEnv.Nodes[0].BlockchainClient

	_, errTXs := helper.SendTXsWithDistributorV2(ctx, testEnv.Nodes[0], logger, testEnv.Nodes[0].Settings, 10000)
	if errTXs != nil {
		t.Errorf("Failed to send txs with distributor: %v", errTXs)
	}

	_, errMine0 := helper.MineBlockWithRPC(ctx, testEnv.Nodes[0], logger)
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

	prevHash, errHash := chainhash.NewHash(mc0.PreviousHash)

	if errHash != nil {
		t.Errorf("error getting previous hash: %v", errHash)
	}

	if bestBlockheader.String() != prevHash.String() {
		t.Errorf("Teranode working on incorrect prevHash")
	}
}

// TNC-1.3-TC-01
func (suite *TNC1TestSuite) TestCoinbaseTXAmount() {
	testEnv := suite.TeranodeTestEnv
	ctx := testEnv.Context
	t := suite.T()

	logger := testEnv.Logger

	ba := testEnv.Nodes[0].BlockassemblyClient
	bc := testEnv.Nodes[0].BlockchainClient

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

// TNC-1.3-TC-02
func (suite *TNC1TestSuite) TestCoinbaseTXAmount2() {
	testEnv := suite.TeranodeTestEnv
	ctx := testEnv.Context
	t := suite.T()

	ba := testEnv.Nodes[0].BlockassemblyClient

	logger := testEnv.Logger

	_, errTXs := helper.SendTXsWithDistributorV2(ctx, testEnv.Nodes[0], logger, testEnv.Nodes[0].Settings, 9000)
	if errTXs != nil {
		t.Errorf("Failed to send txs with distributor: %v", errTXs)
	}

	mc0, errmc0 := ba.GetMiningCandidate(ctx)
	if errmc0 != nil {
		t.Errorf("Error getting mining candidate on node 0")
	}

	coinbaseValueBlock := mc0.CoinbaseValue
	logger.Infof("Coinbase value mining candidate 0: %d", coinbaseValueBlock)

	block, errblock := helper.GetBestBlockV2(ctx, testEnv.Nodes[0])
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
